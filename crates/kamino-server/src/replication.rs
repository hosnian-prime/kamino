//! Phase 5 replication primitives surfaced to the dispatcher.
//!
//! The dispatcher pre-assigns an LWW timestamp on the primary, commits the
//! write locally, then fans the same `(key, value, ts)` out to every live
//! backup via the [`RoutingProvider`] forwarder. This module owns:
//!
//! - [`TimestampSource`]: node-wide simplified HLC (`max(prev + 1, wall)`).
//! - [`replicate_put`] / [`replicate_delete`]: per-op fan-out helpers that
//!   count successful acks and report whether `write_quorum` was met.
//!
//! The dispatcher itself does the local commit, the quorum decision, and
//! the `-QUORUM` reply — this module is plumbing only.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::SystemTime;

use bytes::Bytes;
use kamino_cluster::RoutingProvider;
use kamino_protocol::{Command, Frame, PutCommandOptions};
use tracing::warn;

/// Simplified Hybrid Logical Clock per `docs/04-replication.md`.
///
/// Each call to [`Self::next`] returns `max(prev + 1, wall_time_nanos)` and
/// stores it back atomically. The clock never decreases; observing an
/// external timestamp (e.g. from a client `TS` override or a peer's write
/// arriving via the forwarder) advances the floor via [`Self::observe`].
#[derive(Debug)]
pub(crate) struct TimestampSource {
    last: AtomicI64,
}

impl TimestampSource {
    pub(crate) const fn new() -> Self {
        Self {
            last: AtomicI64::new(0),
        }
    }

    /// Claim the next monotonic timestamp.
    pub(crate) fn next(&self) -> i64 {
        let wall = current_wall_nanos();
        loop {
            let prev = self.last.load(Ordering::Acquire);
            let candidate = prev.saturating_add(1).max(wall);
            if self
                .last
                .compare_exchange_weak(prev, candidate, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return candidate;
            }
        }
    }

    /// Advance the floor so subsequent [`Self::next`] calls return values
    /// strictly greater than `observed`.
    pub(crate) fn observe(&self, observed: i64) {
        self.last.fetch_max(observed, Ordering::AcqRel);
    }
}

fn current_wall_nanos() -> i64 {
    // `UNIX_EPOCH` predates every realistic deployment; treat a backward
    // clock as "no wall info" by clamping to zero. The monotonic
    // `prev + 1` branch still guarantees strict-ordering.
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0_i64, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// Outcome of a single replication fan-out (PUT or DEL).
#[derive(Debug, Default)]
pub(crate) struct ReplicationOutcome {
    /// Number of backups that acknowledged the write.
    pub(crate) acks: u32,
    /// Number of backups that failed (timeout, `ServerGone`, error reply).
    pub(crate) failures: u32,
    /// First error encountered, kept for diagnostics in the `-QUORUM`
    /// reply.
    pub(crate) first_error: Option<String>,
}

/// Fan a primary-stamped `DM.PUT` out to `backups`. Each peer receives the
/// command verbatim with `options.timestamp = Some(ts)` so its dispatcher
/// can route the request through `DMap::put_lww`. Returns an aggregate
/// outcome the caller can compare against `write_quorum`.
///
/// `dmap`, `key`, `value` are cloned by the forwarder when serialising;
/// callers pass owned `Bytes` to avoid reallocation.
pub(crate) async fn replicate_put(
    routing: &Arc<dyn RoutingProvider>,
    backups: &[SocketAddr],
    dmap: Bytes,
    key: Bytes,
    value: Bytes,
    options: PutCommandOptions,
) -> ReplicationOutcome {
    if backups.is_empty() {
        return ReplicationOutcome::default();
    }
    let mut futures = Vec::with_capacity(backups.len());
    for addr in backups {
        let cmd = Command::DmPut {
            dmap: dmap.clone(),
            key: key.clone(),
            value: value.clone(),
            options: options.clone(),
        };
        futures.push(routing.forward_command(*addr, cmd));
    }
    let replies = futures::future::join_all(futures).await;
    aggregate(replies, "DM.PUT")
}

/// Fan an `INTERNAL.NODE.GETWITHTS` out to every backup in parallel.
/// Each reply is decoded into either `Some((value, ts))` or `None`
/// (missing key, transport failure, or unexpected reply shape). The
/// returned `Vec` preserves the order of `backups` so callers can pair
/// it back up with the original addresses for read-repair.
pub(crate) async fn read_from_backups(
    routing: &Arc<dyn RoutingProvider>,
    backups: &[SocketAddr],
    dmap: Bytes,
    key: Bytes,
) -> Vec<Option<(Vec<u8>, i64)>> {
    if backups.is_empty() {
        return Vec::new();
    }
    let mut futures = Vec::with_capacity(backups.len());
    for addr in backups {
        let cmd = Command::InternalNodeGetWithTs {
            dmap: dmap.clone(),
            key: key.clone(),
        };
        futures.push(routing.forward_command(*addr, cmd));
    }
    let replies = futures::future::join_all(futures).await;
    replies.into_iter().map(decode_get_with_ts).collect()
}

fn decode_get_with_ts(
    reply: Result<Frame, kamino_cluster::ClusterError>,
) -> Option<(Vec<u8>, i64)> {
    match reply {
        Ok(Frame::Array(Some(items))) if items.len() == 2 => {
            let value = match &items[0] {
                Frame::Bulk(kamino_protocol::BulkString(Some(v))) => v.to_vec(),
                _ => return None,
            };
            let ts = match &items[1] {
                Frame::Integer(ts) => *ts,
                _ => return None,
            };
            Some((value, ts))
        }
        Ok(Frame::Bulk(kamino_protocol::BulkString(None))) => None,
        Ok(other) => {
            warn!(reply = ?other, "unexpected INTERNAL.NODE.GETWITHTS reply");
            None
        }
        Err(e) => {
            warn!(error = %e, "INTERNAL.NODE.GETWITHTS transport error");
            None
        }
    }
}

/// Fan a primary-side `DM.DEL <dmap> <key>` out to `backups`. Unlike
/// `replicate_put` this is single-key — multi-key cross-partition DEL is
/// handled separately by the existing fan-out in `handlers::dm_del`.
pub(crate) async fn replicate_delete(
    routing: &Arc<dyn RoutingProvider>,
    backups: &[SocketAddr],
    dmap: Bytes,
    key: Bytes,
) -> ReplicationOutcome {
    if backups.is_empty() {
        return ReplicationOutcome::default();
    }
    let mut futures = Vec::with_capacity(backups.len());
    for addr in backups {
        let cmd = Command::DmDel {
            dmap: dmap.clone(),
            keys: vec![key.clone()],
        };
        futures.push(routing.forward_command(*addr, cmd));
    }
    let replies = futures::future::join_all(futures).await;
    aggregate(replies, "DM.DEL")
}

fn aggregate(
    replies: Vec<Result<Frame, kamino_cluster::ClusterError>>,
    op: &str,
) -> ReplicationOutcome {
    let mut out = ReplicationOutcome::default();
    for r in replies {
        match r {
            Ok(Frame::SimpleString(s)) if s == "OK" => out.acks += 1,
            // DM.DEL replies with an integer count; treat any non-error as an ack.
            Ok(Frame::Integer(_)) => out.acks += 1,
            Ok(Frame::Bulk(_)) => {
                // NX/XX path returns null bulk on rejection — that's a
                // *successful* replication for our purposes (the primary's
                // local commit decided the outcome already; the backup
                // reaching the same conclusion is an ack).
                out.acks += 1;
            }
            Ok(Frame::Error(e)) => {
                out.failures += 1;
                if out.first_error.is_none() {
                    out.first_error = Some(format!("{op} backup error: {e}"));
                }
                warn!(reply = %e, "{op} backup replied with error frame");
            }
            Ok(other) => {
                out.failures += 1;
                if out.first_error.is_none() {
                    out.first_error = Some(format!("{op} unexpected reply: {other:?}"));
                }
            }
            Err(e) => {
                out.failures += 1;
                if out.first_error.is_none() {
                    out.first_error = Some(format!("{op} transport error: {e}"));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_source_strictly_monotonic() {
        let s = TimestampSource::new();
        let t1 = s.next();
        let t2 = s.next();
        let t3 = s.next();
        assert!(
            t1 < t2 && t2 < t3,
            "ts must strictly increase: {t1} {t2} {t3}"
        );
    }

    #[test]
    fn timestamp_source_observes_external() {
        let s = TimestampSource::new();
        let far = i64::MAX / 2;
        s.observe(far);
        let next = s.next();
        assert!(
            next > far,
            "next must exceed observed floor {far}, got {next}"
        );
    }
}
