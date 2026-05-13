//! Command handlers. Each function takes the parsed command arguments and
//! returns the [`Frame`] to send back. Side effects (`HELLO` flipping the
//! codec, `QUIT` closing the connection) are signalled via [`HandlerOutcome`].

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kamino_client::{Client, DMap, DMapOptions, Error as ClientError, PutOptions, ScanOptions};
use kamino_cluster::{PubSubProvider, RoutingProvider, SubAck};
use kamino_core::ReplicationMode;
use kamino_protocol::{BulkString, Frame, HelloArgs};

use crate::replication::{self, TimestampSource};
use crate::state::{AuthState, ConnState};

/// Side effects a handler may request from the connection loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerOutcome {
    Continue,
    UpgradeToResp3,
    Close,
}

#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) frame: Frame,
    pub(crate) outcome: HandlerOutcome,
}

impl Response {
    pub(crate) const fn ok(frame: Frame) -> Self {
        Self {
            frame,
            outcome: HandlerOutcome::Continue,
        }
    }
}

const NOAUTH: &str = "NOAUTH Authentication required";
const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled";
const NO_PASSWORD_SET: &str = "ERR Client sent AUTH, but no password is set";

pub(crate) fn ping(message: Option<&Bytes>) -> Response {
    let frame = message.map_or_else(
        || Frame::SimpleString("PONG".into()),
        |m| Frame::Bulk(BulkString::from_bytes(m.clone())),
    );
    Response::ok(frame)
}

pub(crate) fn quit() -> Response {
    Response {
        frame: Frame::ok(),
        outcome: HandlerOutcome::Close,
    }
}

pub(crate) fn auth(
    state: &mut ConnState,
    server_password: &str,
    cluster_secret: &str,
    username: Option<&Bytes>,
    password: &Bytes,
) -> Response {
    // Match the inter-node `cluster_secret` first: an empty secret means
    // "this deployment does not run inter-node auth", in which case we
    // ignore that branch and fall through to client-password matching.
    if !cluster_secret.is_empty() && password.as_ref() == cluster_secret.as_bytes() {
        state.auth = AuthState::Authenticated;
        state.internode = true;
        return Response::ok(Frame::ok());
    }
    if server_password.is_empty() {
        return Response::ok(Frame::Error(NO_PASSWORD_SET.into()));
    }
    if let Some(u) = username {
        if u.as_ref() != b"default" {
            return Response::ok(Frame::Error(WRONGPASS.into()));
        }
    }
    if password.as_ref() == server_password.as_bytes() {
        state.auth = AuthState::Authenticated;
        state.internode = false;
        Response::ok(Frame::ok())
    } else {
        Response::ok(Frame::Error(WRONGPASS.into()))
    }
}

pub(crate) fn hello(
    state: &mut ConnState,
    server_password: &str,
    cluster_secret: &str,
    args: &HelloArgs,
    server_version: &str,
    server_id: u64,
) -> Response {
    if let Some((username, password)) = &args.auth {
        let resp = auth(
            state,
            server_password,
            cluster_secret,
            username.as_ref(),
            password,
        );
        if matches!(resp.frame, Frame::Error(_)) {
            return resp;
        }
    } else if matches!(state.auth, AuthState::Unauthenticated) {
        return Response::ok(Frame::Error(NOAUTH.into()));
    }

    let upgrade = match args.protocol_version {
        None | Some(2) => false,
        Some(3) => true,
        Some(other) => {
            return Response::ok(Frame::Error(format!(
                "NOPROTO unsupported protocol version {other}"
            )));
        }
    };

    if let Some(name) = &args.client_name {
        state.client_name = Some(String::from_utf8_lossy(name).into_owned());
    }

    let frame = build_hello_response(server_version, upgrade, server_id);
    Response {
        frame,
        outcome: if upgrade {
            HandlerOutcome::UpgradeToResp3
        } else {
            HandlerOutcome::Continue
        },
    }
}

fn build_hello_response(version: &str, resp3: bool, server_id: u64) -> Frame {
    let proto = if resp3 { 3_i64 } else { 2_i64 };
    let pairs: Vec<(Frame, Frame)> = vec![
        (
            Frame::Bulk(BulkString::from("server")),
            Frame::Bulk(BulkString::from("kamino")),
        ),
        (
            Frame::Bulk(BulkString::from("version")),
            Frame::Bulk(BulkString::from(version)),
        ),
        (
            Frame::Bulk(BulkString::from("proto")),
            Frame::Integer(proto),
        ),
        (
            Frame::Bulk(BulkString::from("id")),
            #[allow(clippy::cast_possible_wrap)]
            Frame::Integer(server_id as i64),
        ),
        (
            Frame::Bulk(BulkString::from("mode")),
            Frame::Bulk(BulkString::from("standalone")),
        ),
        (
            Frame::Bulk(BulkString::from("role")),
            Frame::Bulk(BulkString::from("master")),
        ),
        (
            Frame::Bulk(BulkString::from("modules")),
            Frame::Array(Some(Vec::new())),
        ),
    ];
    if resp3 {
        Frame::Map(pairs)
    } else {
        let mut flat = Vec::with_capacity(pairs.len() * 2);
        for (k, v) in pairs {
            flat.push(k);
            flat.push(v);
        }
        Frame::Array(Some(flat))
    }
}

pub(crate) fn cluster_routing_table(
    provider: Option<&Arc<dyn kamino_cluster::RoutingProvider>>,
) -> Response {
    // Wire shape: single bulk string carrying the MessagePack-encoded
    // routing table, or `+NORT` (no routing table) if none has been built
    // yet. Clients deserialise with the same `rmp-serde` named-map decoder
    // used internally — forward-compatibility is the routing-table's
    // contract, not the RESP framing's.
    provider.and_then(|p| p.routing_table_bytes()).map_or_else(
        || Response::ok(Frame::SimpleString("NORT".into())),
        |bytes| Response::ok(Frame::Bulk(BulkString::from_bytes(Bytes::from(bytes)))),
    )
}

pub(crate) fn cluster_ready(
    provider: Option<&Arc<dyn kamino_cluster::RoutingProvider>>,
) -> Response {
    let ready = provider.is_some_and(|p| p.is_ready());
    if ready {
        Response::ok(Frame::ok())
    } else {
        Response::ok(Frame::Error("NOTREADY cluster not yet ready".into()))
    }
}

pub(crate) fn internal_node_update_routing(
    provider: Option<&Arc<dyn kamino_cluster::RoutingProvider>>,
    table: &Bytes,
) -> Response {
    let Some(p) = provider else {
        return Response::ok(Frame::Error(
            "ERR cluster runtime not active on this node".into(),
        ));
    };
    let status = match p.apply_routing_update(table) {
        Ok(kamino_cluster::ApplyRoutingOutcome::Accepted) => "OK",
        Ok(kamino_cluster::ApplyRoutingOutcome::Stale) => "STALE",
        Ok(kamino_cluster::ApplyRoutingOutcome::UnsupportedSchema) => "SCHEMA",
        Err(e) => return Response::ok(Frame::Error(format!("ERR routing decode: {e}"))),
    };
    // Phase 6 — `docs/12-failure-handling.md` "Left-Over Data Reports":
    // the receiver piggybacks its current orphan list onto the routing-
    // table-push reply. The coordinator (or any caller) parses the
    // second array element; legacy callers that only inspect the first
    // element keep working because RESP arrays are unambiguous on the
    // wire.
    let orphans = p.local_orphans();
    let mut orphan_frames: Vec<Frame> = Vec::with_capacity(orphans.len());
    for (part, dmap) in orphans {
        orphan_frames.push(Frame::Array(Some(vec![
            Frame::Integer(i64::from(part)),
            Frame::Bulk(BulkString::from(dmap.as_bytes())),
        ])));
    }
    Response::ok(Frame::Array(Some(vec![
        Frame::SimpleString(status.into()),
        Frame::Array(Some(orphan_frames)),
    ])))
}

pub(crate) const fn internal_node_length_of_part(_partition_id: u32) -> Response {
    // Phase 4A: the storage engine isn't partition-aware yet, so the
    // count is always 0. The wire shape is locked in now so peers using
    // `INTERNAL.NODE.LENGTHOFPART` for balancer planning (Phase 6) need
    // no parser changes.
    Response::ok(Frame::Integer(0))
}

pub(crate) fn cluster_members(
    provider: Option<&Arc<dyn kamino_cluster::MemberProvider>>,
) -> Response {
    // Encoding (one inner array per member):
    //   [id_hex, name, addr, discovery_addr, birthdate_ns_str, is_coordinator]
    //
    // RESP2 lacks a boolean type; `is_coordinator` is encoded as the string
    // "1" or "0" so RESP2 clients can parse it without HELLO 3.
    let members = provider.map(|p| p.members()).unwrap_or_default();
    let mut outer = Vec::with_capacity(members.len());
    for m in members {
        let row = vec![
            Frame::Bulk(BulkString::from(m.id.to_string().as_str())),
            Frame::Bulk(BulkString::from(m.name.as_str())),
            Frame::Bulk(BulkString::from(m.addr.to_string().as_str())),
            Frame::Bulk(BulkString::from(m.discovery_addr.to_string().as_str())),
            Frame::Bulk(BulkString::from(m.birthdate.to_string().as_str())),
            Frame::Bulk(BulkString::from(if m.is_coordinator { "1" } else { "0" })),
        ];
        outer.push(Frame::Array(Some(row)));
    }
    Response::ok(Frame::Array(Some(outer)))
}

pub(crate) fn stats(snap: crate::metrics::MetricsSnapshot, server_version: &str) -> Response {
    let pairs: [(&str, String); 5] = [
        ("uptime_secs", snap.uptime_secs.to_string()),
        ("connections", snap.current_connections.to_string()),
        ("connections_total", snap.total_connections.to_string()),
        ("commands_total", snap.commands_total.to_string()),
        ("version", server_version.to_string()),
    ];
    let mut frames = Vec::with_capacity(pairs.len() * 2 + 2);
    for (k, v) in &pairs {
        frames.push(Frame::Bulk(BulkString::from(*k)));
        frames.push(Frame::Bulk(BulkString::from(v.as_str())));
    }
    frames.push(Frame::Bulk(BulkString::from("mode")));
    frames.push(Frame::Bulk(BulkString::from("standalone")));
    Response::ok(Frame::Array(Some(frames)))
}

fn map_client_error(err: ClientError, _command: &str) -> Frame {
    match err {
        ClientError::KeyNotFound => Frame::Bulk(BulkString::null()),
        ClientError::KeyAlreadyExists | ClientError::KeyNotExists => {
            Frame::Bulk(BulkString::null())
        }
        ClientError::LockNotAcquired => Frame::Error("ERR lock not acquired".into()),
        ClientError::NoSuchLock => Frame::Error("ERR no such lock".into()),
        ClientError::InvalidCursor => {
            Frame::Error("CURSOR partition migrated; restart scan".into())
        }
        ClientError::Timeout => Frame::Error("ERR operation timed out".into()),
        ClientError::Storage(s) => match s.as_ref() {
            kamino_storage::Error::KeyTooLarge { .. } => Frame::Error("ERR key too large".into()),
            kamino_storage::Error::ValueTooLarge { .. } => {
                Frame::Error("ERR value too large".into())
            }
            kamino_storage::Error::EngineFull => Frame::Error("ERR engine is full".into()),
            other => Frame::Error(format!("ERR storage error: {other}")),
        },
        ClientError::NotANumber { expected, got } => {
            Frame::Error(format!("ERR value is not an {expected}: {got}"))
        }
        ClientError::InvalidArgument(msg) => Frame::Error(format!("ERR {msg}")),
        ClientError::Serialization(msg) => Frame::Error(format!("ERR serialization: {msg}")),
        ClientError::Unsupported(op) => Frame::Error(format!("ERR {op} not supported")),
        ClientError::DMapNotFound(name) => Frame::Error(format!("ERR dmap not found: {name}")),
        ClientError::Config(c) => Frame::Error(format!("ERR config: {c}")),
        // `kamino_client::Error` is `#[non_exhaustive]`; new variants surface
        // through `Display` so the server never silently drops a remote-side
        // error. The embedded client (the only data path in Phase 2) only
        // emits the variants matched above.
        other => Frame::Error(format!("ERR {other}")),
    }
}

async fn dmap_handle(client: &Arc<dyn Client>, name: &Bytes) -> Result<Arc<dyn DMap>, Frame> {
    let name_str = match std::str::from_utf8(name) {
        Ok(s) => s,
        Err(e) => return Err(Frame::Error(format!("ERR invalid dmap name: {e}"))),
    };
    client
        .new_dmap(name_str, DMapOptions::default())
        .await
        .map_err(|e| map_client_error(e, "DM.*"))
}

fn key_str(key: &Bytes) -> Result<&str, Frame> {
    std::str::from_utf8(key).map_err(|e| Frame::Error(format!("ERR invalid key utf-8: {e}")))
}

#[allow(clippy::too_many_arguments)] // each argument is independent context
pub(crate) async fn dm_put(
    client: &Arc<dyn Client>,
    routing: Option<&Arc<dyn RoutingProvider>>,
    ts_source: &TimestampSource,
    from_peer: bool,
    dmap: &Bytes,
    key: &Bytes,
    value: &Bytes,
    options: kamino_protocol::PutCommandOptions,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };

    // Backup-side arrival (cluster_secret-authenticated peer): the primary
    // owns the LWW stamp, we just merge.
    if from_peer {
        let put_opts = put_options_from_command(&options);
        return match d.put_lww(k, value, put_opts).await {
            // `applied = true` is the steady-state path; `false` means an
            // out-of-order replication arrived after a newer write and was
            // discarded by LWW. Both outcomes are "the cluster's invariant
            // is preserved" so we report success to the primary so it can
            // count the ack toward `write_quorum`.
            Ok(_) => Response::ok(Frame::ok()),
            Err(ClientError::KeyAlreadyExists | ClientError::KeyNotExists) => {
                Response::ok(Frame::Bulk(BulkString::null()))
            }
            Err(e) => Response::ok(map_client_error(e, "DM.PUT")),
        };
    }

    // Primary path: assign a canonical timestamp, commit locally, then fan
    // out to live backups. If `options.timestamp` is set (client TS
    // override) we honor it and advance our floor so future stamps stay
    // monotonic.
    let stamp = options.timestamp.map_or_else(
        || ts_source.next(),
        |ts| {
            ts_source.observe(ts);
            ts
        },
    );
    let stamped_options = kamino_protocol::PutCommandOptions {
        timestamp: Some(stamp),
        ..options.clone()
    };
    let put_opts = put_options_from_command(&stamped_options);

    match d.put(k, value, put_opts).await {
        Ok(()) => {}
        Err(ClientError::KeyAlreadyExists | ClientError::KeyNotExists) => {
            // NX/XX rejected: do *not* replicate — the cluster state is
            // unchanged and backups must not see a phantom write.
            return Response::ok(Frame::Bulk(BulkString::null()));
        }
        Err(e) => return Response::ok(map_client_error(e, "DM.PUT")),
    }

    let Some(routing) = routing else {
        // Standalone (no cluster runtime): replication is a no-op.
        return Response::ok(Frame::ok());
    };
    let backups = routing.backup_addrs_for_key(dmap, key);
    if backups.is_empty() {
        return Response::ok(Frame::ok());
    }
    let settings = routing.replication_settings();
    let write_quorum = settings.write_quorum.max(1);

    match settings.mode {
        ReplicationMode::Sync => {
            let outcome = replication::replicate_put(
                routing,
                &backups,
                dmap.clone(),
                key.clone(),
                value.clone(),
                stamped_options,
            )
            .await;
            // Primary's own commit counts as one ack toward write_quorum.
            let total = 1_u32 + outcome.acks;
            if total < write_quorum {
                let err = outcome
                    .first_error
                    .unwrap_or_else(|| "no backup acked in time".into());
                return Response::ok(Frame::Error(format!(
                    "QUORUM write_quorum={write_quorum} not met (acks={total}): {err}",
                )));
            }
            Response::ok(Frame::ok())
        }
        ReplicationMode::Async => {
            // Fire-and-forget; primary returns success immediately. Data
            // loss on primary crash before propagation is the documented
            // trade-off (`docs/04-replication.md` "Asynchronous").
            let routing = Arc::clone(routing);
            let dmap = dmap.clone();
            let key = key.clone();
            let value = value.clone();
            tokio::spawn(async move {
                let _ = replication::replicate_put(
                    &routing,
                    &backups,
                    dmap,
                    key,
                    value,
                    stamped_options,
                )
                .await;
            });
            Response::ok(Frame::ok())
        }
    }
}

const fn put_options_from_command(options: &kamino_protocol::PutCommandOptions) -> PutOptions {
    PutOptions {
        ex: options.ex,
        px: options.px,
        exat: options.exat,
        pxat: options.pxat,
        nx: options.nx,
        xx: options.xx,
        timestamp: options.timestamp,
    }
}

pub(crate) async fn dm_get(
    client: &Arc<dyn Client>,
    routing: Option<&Arc<dyn RoutingProvider>>,
    ts_source: &TimestampSource,
    from_peer: bool,
    dmap: &Bytes,
    key: &Bytes,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };

    // Internode arrivals always read locally — the primary that fanned the
    // request out doesn't want a recursive quorum read.
    if from_peer {
        return match d.get(k).await {
            Ok(resp) => Response::ok(Frame::Bulk(BulkString::from(resp.value))),
            Err(ClientError::KeyNotFound) => Response::ok(Frame::Bulk(BulkString::null())),
            Err(e) => Response::ok(map_client_error(e, "DM.GET")),
        };
    }

    let local = match d.get(k).await {
        Ok(resp) => Some(resp),
        Err(ClientError::KeyNotFound) => None,
        Err(e) => return Response::ok(map_client_error(e, "DM.GET")),
    };

    // Determine whether quorum / repair fan-out is needed.
    let Some(routing) = routing else {
        return local.map_or_else(
            || Response::ok(Frame::Bulk(BulkString::null())),
            |r| Response::ok(Frame::Bulk(BulkString::from(r.value))),
        );
    };

    // Phase 6 fragmented-partition fallback: if local missed AND the
    // routing table still lists previous owners for this partition (we
    // were just promoted and the balancer hasn't migrated yet), query
    // those owners sequentially. The first hit wins; ties are resolved by
    // highest timestamp on the way back. This precedes the read_quorum
    // fan-out because a fragmented partition by definition can't satisfy
    // the steady-state replica set yet.
    if local.is_none() {
        let prior = routing.previous_owners_for_key(dmap, key);
        if !prior.is_empty() {
            let recovered =
                read_from_prior_owners(routing, &prior, dmap.clone(), key.clone()).await;
            if let Some((value, _ts)) = recovered {
                return Response::ok(Frame::Bulk(BulkString::from(value)));
            }
        }
    }
    let settings = routing.replication_settings();
    let needs_fanout = settings.read_quorum > 1 || settings.read_repair;
    if !needs_fanout {
        return local.map_or_else(
            || Response::ok(Frame::Bulk(BulkString::null())),
            |r| Response::ok(Frame::Bulk(BulkString::from(r.value))),
        );
    }

    let backups = routing.backup_addrs_for_key(dmap, key);
    // Phase 6: when read_repair is on, the read fan-out also covers any
    // previous owners listed in the fragmented-partition window. This
    // matches docs/12-failure-handling.md "Read Repair":
    //   "Every GET reads from primary + backups + previous owners".
    let prior_for_repair: Vec<std::net::SocketAddr> = if settings.read_repair {
        routing.previous_owners_for_key(dmap, key)
    } else {
        Vec::new()
    };
    if backups.is_empty() && prior_for_repair.is_empty() {
        return local.map_or_else(
            || Response::ok(Frame::Bulk(BulkString::null())),
            |r| Response::ok(Frame::Bulk(BulkString::from(r.value))),
        );
    }

    // Pull (value, ts) from every live backup. Each `(Option<value>, ts)`
    // pair (or None on transport failure) feeds the LWW reducer below.
    let mut replies =
        replication::read_from_backups(routing, &backups, dmap.clone(), key.clone()).await;
    // Append (value, ts) from each previous owner, when read_repair is on,
    // so the LWW reducer also picks up a stranded-on-old-primary write.
    if !prior_for_repair.is_empty() {
        let prior_replies =
            replication::read_from_backups(routing, &prior_for_repair, dmap.clone(), key.clone())
                .await;
        replies.extend(prior_replies);
    }
    // Push the primary's local read into the same shape.
    replies.push(local.map(|r| (r.value, r.timestamp)));

    let winner = pick_highest_ts(&replies);
    let Some((winner_value, winner_ts)) = winner else {
        return Response::ok(Frame::Bulk(BulkString::null()));
    };

    if settings.read_repair {
        // Propagate the winner to every replica whose stamp is strictly
        // lower (or whose copy is missing entirely). The repair targets
        // include both the current backups and any surviving previous
        // owners — the latter so a stale fragmented-partition copy is
        // brought up to date even before the balancer migrates it.
        let backup_replies = &replies[..backups.len()];
        let prior_replies = if prior_for_repair.is_empty() {
            &[][..]
        } else {
            &replies[backups.len()..backups.len() + prior_for_repair.len()]
        };
        let mut stale_peers = stale_replicas(&backups, backup_replies, winner_ts);
        stale_peers.extend(stale_replicas(&prior_for_repair, prior_replies, winner_ts));
        if !stale_peers.is_empty() {
            ts_source.observe(winner_ts);
            let opts = kamino_protocol::PutCommandOptions {
                timestamp: Some(winner_ts),
                ..Default::default()
            };
            // Fire-and-forget repair — surfacing repair failures to the
            // client would re-introduce the unavailability we replicate
            // *against*. Errors are logged inside the replication helper.
            let routing = Arc::clone(routing);
            let dmap = dmap.clone();
            let key = key.clone();
            let value = Bytes::copy_from_slice(&winner_value);
            tokio::spawn(async move {
                let _ = replication::replicate_put(&routing, &stale_peers, dmap, key, value, opts)
                    .await;
            });
        }
    }
    Response::ok(Frame::Bulk(BulkString::from(winner_value)))
}

/// Pick the `(value, ts)` with the highest timestamp from the fan-out
/// replies. `None` entries (missing key / transport failure) are dropped.
fn pick_highest_ts(replies: &[Option<(Vec<u8>, i64)>]) -> Option<(Vec<u8>, i64)> {
    replies
        .iter()
        .filter_map(|r| r.as_ref())
        .max_by_key(|(_, ts)| *ts)
        .map(|(v, ts)| (v.clone(), *ts))
}

/// Query every previous owner in parallel via `INTERNAL.NODE.GETWITHTS`
/// and return the highest-timestamp hit, if any. Used by Phase 6's
/// fragmented-partition read fallback in [`dm_get`].
async fn read_from_prior_owners(
    routing: &Arc<dyn RoutingProvider>,
    prior: &[std::net::SocketAddr],
    dmap: Bytes,
    key: Bytes,
) -> Option<(Vec<u8>, i64)> {
    let replies = replication::read_from_backups(routing, prior, dmap, key).await;
    pick_highest_ts(&replies)
}

/// Identify backups whose stored timestamp is strictly below the winner
/// (or who returned no value at all). The primary's local copy is the
/// last entry in `replies`; backups occupy the first `backups.len()`
/// slots in the same order.
fn stale_replicas(
    backups: &[std::net::SocketAddr],
    replies: &[Option<(Vec<u8>, i64)>],
    winner_ts: i64,
) -> Vec<std::net::SocketAddr> {
    backups
        .iter()
        .zip(replies.iter())
        .filter_map(|(addr, reply)| {
            let needs_repair = reply.as_ref().is_none_or(|(_, ts)| *ts < winner_ts);
            needs_repair.then_some(*addr)
        })
        .collect()
}

/// Handler for `INTERNAL.NODE.GETWITHTS`. Peers authenticated with
/// `cluster_secret` use it to satisfy the primary's read-quorum / read-
/// repair fan-out. Returns either a 2-element array `[value, ts]` or a
/// null bulk if the key is missing.
pub(crate) async fn internal_node_get_with_ts(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.get(k).await {
        Ok(resp) => Response::ok(Frame::Array(Some(vec![
            Frame::Bulk(BulkString::from(resp.value)),
            Frame::Integer(resp.timestamp),
        ]))),
        Err(ClientError::KeyNotFound) => Response::ok(Frame::Bulk(BulkString::null())),
        Err(e) => Response::ok(map_client_error(e, "INTERNAL.NODE.GETWITHTS")),
    }
}

/// Handler for `INTERNAL.NODE.MOVEFRAGMENT`. The balancer on the previous
/// owner exports a `(dmap, partition_id)` shard and ships it here; we
/// LWW-merge into local storage and reply `+OK` (or `-ERR <reason>` on
/// decode / merge / ownership failure). Phase 6 —
/// `docs/12-failure-handling.md` "Ownership Transfer Protocol" steps 4–7.
pub(crate) async fn internal_node_move_fragment(
    client: &Arc<dyn Client>,
    routing: Option<&Arc<dyn RoutingProvider>>,
    partition_id: u32,
    partition_type: kamino_protocol::PartitionType,
    dmap: &Bytes,
    payload: &[u8],
) -> Response {
    // Phase 6 only ships primary migrations; backup migrations are reserved
    // (see protocol command doc). Reject the off-spec type explicitly so a
    // future-version sender gets a clear error rather than a silent merge
    // under the wrong ownership semantics.
    if partition_type != kamino_protocol::PartitionType::Primary {
        return Response::ok(Frame::Error(
            "ERR MOVEFRAGMENT backup-type migrations are not supported in this version".into(),
        ));
    }
    // Step 4 of the Ownership Transfer Protocol: receiver verifies that
    // its current routing table actually maps this partition to itself.
    // Without this check a stale sender could push data onto a peer that
    // no longer owns the partition — leaving "twice-orphaned" data that
    // the future balancer would have to migrate again. The check is
    // best-effort (no provider → accept; single-node deployments have no
    // routing table to consult).
    if let Some(routing) = routing {
        if !routing.owns_partition(partition_id) {
            return Response::ok(Frame::Error(format!(
                "MIGRATION not_owner partition {partition_id}",
            )));
        }
    }
    let dmap_name = match key_str(dmap) {
        Ok(s) => s.to_string(),
        Err(f) => return Response::ok(f),
    };
    match client
        .import_partition(&dmap_name, partition_id, payload)
        .await
    {
        Ok(applied) => {
            // Step 7: publish `FragmentReceivedEvent`. Phase 6 routes
            // events through `tracing` for now; Phase 7 swaps in the
            // real `cluster.events` pub/sub channel once it lands.
            tracing::info!(
                dmap = %dmap_name,
                partition_id,
                applied,
                event = "fragment-received",
                "INTERNAL.NODE.MOVEFRAGMENT accepted",
            );
            Response::ok(Frame::ok())
        }
        Err(e) => {
            tracing::warn!(
                dmap = %dmap_name,
                partition_id,
                error = %e,
                "INTERNAL.NODE.MOVEFRAGMENT rejected",
            );
            Response::ok(map_client_error(e, "INTERNAL.NODE.MOVEFRAGMENT"))
        }
    }
}

pub(crate) async fn dm_del(
    client: &Arc<dyn Client>,
    routing: Option<&Arc<dyn kamino_cluster::RoutingProvider>>,
    from_peer: bool,
    dmap: &Bytes,
    keys: &[Bytes],
) -> Response {
    if keys.is_empty() {
        return Response::ok(Frame::Integer(0));
    }

    // Standalone (no routing) OR a backup receiving a replication DEL:
    // delete locally and reply with the count, no further fan-out.
    if routing.is_none() || from_peer {
        return dm_del_local(client, dmap, keys).await;
    }
    let routing = routing.expect("checked above");

    // Bucket keys by primary owner. `None` = local; `Some(addr)` = remote.
    let mut buckets: std::collections::HashMap<Option<std::net::SocketAddr>, Vec<Bytes>> =
        std::collections::HashMap::new();
    for key in keys {
        let owner = routing.route_key(dmap, key);
        buckets.entry(owner).or_default().push(key.clone());
    }

    let crossed_partitions = buckets.len() > 1
        || (buckets.len() == 1 && buckets.keys().next().is_some_and(Option::is_some));
    if crossed_partitions && routing.multi_key_strict() {
        return Response::ok(Frame::Error(
            "CROSSPARTITION keys span multiple partitions; multi_key_strict is enabled".into(),
        ));
    }

    let mut deleted = 0_i64;
    let mut first_error: Option<String> = None;
    if let Some(local_keys) = buckets.remove(&None) {
        match dm_del_local_count(client, dmap, &local_keys).await {
            Ok(n) => deleted += n,
            Err(frame) => return Response::ok(frame),
        }
        // Phase 5: replicate every successful local delete to the partition's
        // live backups. Single-key fan-out — multi-key DEL crossing
        // partitions still uses `forward_dm_del` per-peer above.
        if let Err(err) = replicate_local_deletes(routing, dmap, &local_keys).await {
            // Replication failure surfaces as PARTIAL so callers see that
            // the local delete happened but backups may diverge.
            return Response::ok(Frame::SimpleString(format!("PARTIAL {deleted} {err}")));
        }
    }
    // Fan out remote buckets in parallel — bounded by the per-peer
    // forwarder inflight semaphore inside RoutingProvider::forward_dm_del.
    let remote: Vec<(std::net::SocketAddr, Vec<Bytes>)> = buckets
        .into_iter()
        .filter_map(|(addr, keys)| addr.map(|a| (a, keys)))
        .collect();
    if !remote.is_empty() {
        let mut futures = Vec::with_capacity(remote.len());
        for (addr, keys) in remote {
            futures.push(routing.forward_dm_del(addr, dmap.clone(), keys));
        }
        let results = futures::future::join_all(futures).await;
        for r in results {
            match r {
                Ok(n) => deleted += n,
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(format!("{e}"));
                    }
                }
            }
        }
    }

    if let Some(err) = first_error {
        return Response::ok(Frame::SimpleString(format!("PARTIAL {deleted} {err}")));
    }
    Response::ok(Frame::Integer(deleted))
}

/// Replicate every key the primary just deleted locally to the partition's
/// live backups. Errors are surfaced verbatim so the caller can decide
/// between `+OK` and `+PARTIAL`.
async fn replicate_local_deletes(
    routing: &Arc<dyn kamino_cluster::RoutingProvider>,
    dmap: &Bytes,
    keys: &[Bytes],
) -> Result<(), String> {
    let settings = routing.replication_settings();
    if settings.replica_count <= 1 {
        return Ok(());
    }
    for key in keys {
        let backups = routing.backup_addrs_for_key(dmap, key);
        if backups.is_empty() {
            continue;
        }
        let outcome =
            replication::replicate_delete(routing, &backups, dmap.clone(), key.clone()).await;
        let total = 1_u32 + outcome.acks;
        if total < settings.write_quorum.max(1) {
            return Err(outcome
                .first_error
                .unwrap_or_else(|| "DEL replication below quorum".into()));
        }
    }
    Ok(())
}

async fn dm_del_local(client: &Arc<dyn Client>, dmap: &Bytes, keys: &[Bytes]) -> Response {
    match dm_del_local_count(client, dmap, keys).await {
        Ok(n) => Response::ok(Frame::Integer(n)),
        Err(frame) => Response::ok(frame),
    }
}

async fn dm_del_local_count(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    keys: &[Bytes],
) -> Result<i64, Frame> {
    let d = dmap_handle(client, dmap).await?;
    let mut deleted = 0_i64;
    for key in keys {
        let k = key_str(key)?;
        match d.delete(k).await {
            Ok(true) => deleted += 1,
            Ok(false) => {}
            Err(e) => return Err(map_client_error(e, "DM.DEL")),
        }
    }
    Ok(deleted)
}

pub(crate) async fn dm_expire(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    duration: Duration,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.expire(k, duration).await {
        Ok(()) => Response::ok(Frame::Integer(1)),
        Err(ClientError::KeyNotFound) => Response::ok(Frame::Integer(0)),
        Err(e) => Response::ok(map_client_error(e, "DM.EXPIRE")),
    }
}

pub(crate) async fn dm_incr(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    delta: i64,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.incr(k, delta).await {
        Ok(v) => Response::ok(Frame::Integer(v)),
        Err(e) => Response::ok(map_client_error(e, "DM.INCR")),
    }
}

pub(crate) async fn dm_decr(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    delta: i64,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.decr(k, delta).await {
        Ok(v) => Response::ok(Frame::Integer(v)),
        Err(e) => Response::ok(map_client_error(e, "DM.DECR")),
    }
}

pub(crate) async fn dm_get_put(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    value: &Bytes,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.get_put(k, value).await {
        Ok(Some(prev)) => Response::ok(Frame::Bulk(BulkString::from(prev.value))),
        Ok(None) => Response::ok(Frame::Bulk(BulkString::null())),
        Err(e) => Response::ok(map_client_error(e, "DM.GETPUT")),
    }
}

pub(crate) async fn dm_incr_by_float(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    delta: f64,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.incr_by_float(k, delta).await {
        Ok(v) => Response::ok(Frame::Bulk(BulkString::from(format!("{v}")))),
        Err(e) => Response::ok(map_client_error(e, "DM.INCRBYFLOAT")),
    }
}

pub(crate) async fn dm_destroy(client: &Arc<dyn Client>, dmap: &Bytes) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    match d.destroy().await {
        Ok(()) => Response::ok(Frame::ok()),
        Err(e) => Response::ok(map_client_error(e, "DM.DESTROY")),
    }
}

pub(crate) async fn dm_scan(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    partition_id: u32,
    _cursor: u64,
    options: kamino_protocol::ScanCommandOptions,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let match_pattern = match options.match_pattern {
        Some(p) => match String::from_utf8(p.to_vec()) {
            Ok(s) => Some(s),
            Err(e) => {
                return Response::ok(Frame::Error(format!("ERR invalid MATCH pattern: {e}")));
            }
        },
        None => None,
    };
    let scan_opts = ScanOptions {
        count: options.count.map(|c| c as usize),
        match_pattern,
    };
    let mut cursor = match d.scan(partition_id, scan_opts).await {
        Ok(c) => c,
        Err(e) => return Response::ok(map_client_error(e, "DM.SCAN")),
    };
    let mut items: Vec<Frame> = Vec::new();
    loop {
        match cursor.next().await {
            Ok(Some((k, v))) => {
                items.push(Frame::Bulk(BulkString::from(k)));
                items.push(Frame::Bulk(BulkString::from(v)));
            }
            Ok(None) => break,
            Err(e) => return Response::ok(map_client_error(e, "DM.SCAN")),
        }
    }
    let _ = cursor.close().await;
    // Phase 2: embedded cursor is materialised eagerly — return cursor "0"
    // (Redis convention for "iteration complete") plus the array.
    let pair = vec![
        Frame::Bulk(BulkString::from("0")),
        Frame::Array(Some(items)),
    ];
    Response::ok(Frame::Array(Some(pair)))
}

// --- Phase 7 pub/sub handlers -----------------------------------------------

/// `RESP3`-aware single-message-or-array reply. SUBSCRIBE/PSUBSCRIBE/etc
/// always reply with multiple acks (one per channel/pattern) so the
/// connection loop sends every entry as a separate frame.
#[derive(Debug)]
pub(crate) struct MultiResponse {
    pub(crate) frames: Vec<Frame>,
    pub(crate) outcome: HandlerOutcome,
}

impl MultiResponse {
    pub(crate) const fn ok(frames: Vec<Frame>) -> Self {
        Self {
            frames,
            outcome: HandlerOutcome::Continue,
        }
    }
}

const NO_PUBSUB_PROVIDER: &str = "ERR pub/sub not enabled on this server";

fn require_pubsub(
    provider: Option<&Arc<dyn PubSubProvider>>,
) -> Result<&Arc<dyn PubSubProvider>, Frame> {
    provider.ok_or_else(|| Frame::Error(NO_PUBSUB_PROVIDER.into()))
}

fn ensure_conn_registered(
    provider: &Arc<dyn PubSubProvider>,
    state: &mut ConnState,
    pubsub_sender: &tokio::sync::mpsc::Sender<kamino_cluster::DeliveredMessage>,
) -> u64 {
    if let Some(id) = state.pub_sub_id {
        return id;
    }
    let id = provider.allocate_conn_id();
    provider.register_conn(id, pubsub_sender.clone());
    state.pub_sub_id = Some(id);
    id
}

fn build_sub_ack(kind: &str, ack: &SubAck) -> Frame {
    let channel = if ack.channel.is_empty() {
        Frame::Bulk(BulkString::null())
    } else {
        Frame::Bulk(BulkString::from_bytes(ack.channel.clone()))
    };
    Frame::Array(Some(vec![
        Frame::Bulk(BulkString::from(kind)),
        channel,
        Frame::Integer(i64::try_from(ack.total_subscriptions).unwrap_or(i64::MAX)),
    ]))
}

pub(crate) fn subscribe(
    provider: Option<&Arc<dyn PubSubProvider>>,
    state: &mut ConnState,
    sender: &tokio::sync::mpsc::Sender<kamino_cluster::DeliveredMessage>,
    channels: &[Bytes],
) -> MultiResponse {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => {
            return MultiResponse::ok(vec![f]);
        }
    };
    let id = ensure_conn_registered(provider, state, sender);
    let acks = provider.subscribe(id, channels);
    state.pub_sub_count = acks
        .last()
        .map_or(state.pub_sub_count, |a| a.total_subscriptions);
    let frames = acks.iter().map(|a| build_sub_ack("subscribe", a)).collect();
    MultiResponse::ok(frames)
}

pub(crate) fn psubscribe(
    provider: Option<&Arc<dyn PubSubProvider>>,
    state: &mut ConnState,
    sender: &tokio::sync::mpsc::Sender<kamino_cluster::DeliveredMessage>,
    patterns: &[Bytes],
) -> MultiResponse {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => {
            return MultiResponse::ok(vec![f]);
        }
    };
    let id = ensure_conn_registered(provider, state, sender);
    let acks = provider.psubscribe(id, patterns);
    state.pub_sub_count = acks
        .last()
        .map_or(state.pub_sub_count, |a| a.total_subscriptions);
    let frames = acks
        .iter()
        .map(|a| build_sub_ack("psubscribe", a))
        .collect();
    MultiResponse::ok(frames)
}

pub(crate) fn unsubscribe(
    provider: Option<&Arc<dyn PubSubProvider>>,
    state: &mut ConnState,
    channels: &[Bytes],
) -> MultiResponse {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => return MultiResponse::ok(vec![f]),
    };
    let Some(id) = state.pub_sub_id else {
        // Not subscribed to anything — emit a single null-channel ack with
        // total=0 per Redis convention.
        let ack = SubAck {
            channel: Bytes::new(),
            is_pattern: false,
            total_subscriptions: 0,
        };
        return MultiResponse::ok(vec![build_sub_ack("unsubscribe", &ack)]);
    };
    let filter = if channels.is_empty() {
        None
    } else {
        Some(channels)
    };
    let acks = provider.unsubscribe(id, filter);
    state.pub_sub_count = acks
        .last()
        .map_or(state.pub_sub_count, |a| a.total_subscriptions);
    let frames = acks
        .iter()
        .map(|a| build_sub_ack("unsubscribe", a))
        .collect();
    MultiResponse::ok(frames)
}

pub(crate) fn punsubscribe(
    provider: Option<&Arc<dyn PubSubProvider>>,
    state: &mut ConnState,
    patterns: &[Bytes],
) -> MultiResponse {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => return MultiResponse::ok(vec![f]),
    };
    let Some(id) = state.pub_sub_id else {
        let ack = SubAck {
            channel: Bytes::new(),
            is_pattern: true,
            total_subscriptions: 0,
        };
        return MultiResponse::ok(vec![build_sub_ack("punsubscribe", &ack)]);
    };
    let filter = if patterns.is_empty() {
        None
    } else {
        Some(patterns)
    };
    let acks = provider.punsubscribe(id, filter);
    state.pub_sub_count = acks
        .last()
        .map_or(state.pub_sub_count, |a| a.total_subscriptions);
    let frames = acks
        .iter()
        .map(|a| build_sub_ack("punsubscribe", a))
        .collect();
    MultiResponse::ok(frames)
}

pub(crate) async fn publish(
    provider: Option<&Arc<dyn PubSubProvider>>,
    channel: Bytes,
    message: Bytes,
) -> Response {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => return Response::ok(f),
    };
    let count = provider.publish(channel, message).await;
    Response::ok(Frame::Integer(i64::try_from(count).unwrap_or(i64::MAX)))
}

pub(crate) fn internal_node_publish(
    provider: Option<&Arc<dyn PubSubProvider>>,
    channel: &Bytes,
    message: &Bytes,
) -> Response {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => return Response::ok(f),
    };
    let name = String::from_utf8_lossy(channel).into_owned();
    let count = provider.publish_local(&name, message);
    Response::ok(Frame::Integer(i64::try_from(count).unwrap_or(i64::MAX)))
}

pub(crate) fn pubsub_channels(
    provider: Option<&Arc<dyn PubSubProvider>>,
    pattern: Option<&Bytes>,
) -> Response {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => return Response::ok(f),
    };
    let pat = pattern.map(|p| String::from_utf8_lossy(p).into_owned());
    let chans = provider.pubsub_channels(pat.as_deref());
    let items = chans
        .into_iter()
        .map(|c| Frame::Bulk(BulkString::from(c)))
        .collect();
    Response::ok(Frame::Array(Some(items)))
}

pub(crate) fn pubsub_numsub(
    provider: Option<&Arc<dyn PubSubProvider>>,
    channels: &[Bytes],
) -> Response {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => return Response::ok(f),
    };
    let pairs = provider.pubsub_numsub(channels);
    let mut items = Vec::with_capacity(pairs.len() * 2);
    for (name, count) in pairs {
        items.push(Frame::Bulk(BulkString::from_bytes(name)));
        items.push(Frame::Integer(i64::try_from(count).unwrap_or(i64::MAX)));
    }
    Response::ok(Frame::Array(Some(items)))
}

pub(crate) fn pubsub_numpat(provider: Option<&Arc<dyn PubSubProvider>>) -> Response {
    let provider = match require_pubsub(provider) {
        Ok(p) => p,
        Err(f) => return Response::ok(f),
    };
    Response::ok(Frame::Integer(
        i64::try_from(provider.pubsub_numpat()).unwrap_or(i64::MAX),
    ))
}

/// Build the wire frame for a delivered pub/sub message. Uses RESP3 push
/// frames when the connection negotiated RESP3, falling back to a
/// plain RESP array on RESP2 (the Redis legacy shape).
pub(crate) fn deliver_message_frame(
    msg: &kamino_cluster::DeliveredMessage,
    version: kamino_protocol::ProtocolVersion,
) -> Frame {
    use kamino_protocol::ProtocolVersion;
    let items = msg.pattern.as_ref().map_or_else(
        || {
            vec![
                Frame::Bulk(BulkString::from("message")),
                Frame::Bulk(BulkString::from(msg.channel.as_str())),
                Frame::Bulk(BulkString::from_bytes(msg.payload.clone())),
            ]
        },
        |p| {
            vec![
                Frame::Bulk(BulkString::from("pmessage")),
                Frame::Bulk(BulkString::from(p.as_str())),
                Frame::Bulk(BulkString::from(msg.channel.as_str())),
                Frame::Bulk(BulkString::from_bytes(msg.payload.clone())),
            ]
        },
    );
    match version {
        ProtocolVersion::Resp3 => Frame::Push(items),
        ProtocolVersion::Resp2 => Frame::Array(Some(items)),
    }
}
