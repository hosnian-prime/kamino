//! Coordinator periodic build-and-push loop.
//!
//! Per `docs/03-cluster-management.md` ("Coordinator Responsibilities") only
//! the coordinator builds and pushes the routing table. Every node still runs
//! this loop — non-coordinators short-circuit each tick. That keeps the
//! coordinator failover instant: the second-oldest member starts pushing the
//! moment SWIM marks the old coordinator dead, without any election protocol.
//!
//! Signature semantics (`docs/02-consistent-hashing.md`): bumped on every
//! detected topology change. We track the last-built topology as a hash over
//! the sorted member-id list; ties under simultaneous-coordinator scenarios
//! resolve via signature comparison on the receiver (see
//! [`crate::routing::store::RoutingTableStore`]).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher as _};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kamino_core::hasher::Hasher;
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::error::ClusterResult;
use crate::membership::MembershipView;
use crate::routing::store::{ApplyRoutingOutcome, RoutingTableStore};
use crate::routing::table::RoutingTable;

/// Inter-node routing-table push. Phase 4A ships a no-op
/// [`LocalOnlyPusher`]; Phase 4B will provide the real RESP-based
/// implementation. The trait lives here so the coordinator loop stays
/// transport-agnostic.
#[async_trait]
pub trait RoutingPusher: Send + Sync {
    /// Send the encoded routing table to `target`. Implementations should
    /// honour `internode_request_timeout` themselves.
    async fn push(&self, target: SocketAddr, table_bytes: &[u8]) -> ClusterResult<()>;
}

/// Default no-op pusher used until the Forwarder lands in Phase 4B. The
/// coordinator still applies the built table to its own
/// [`RoutingTableStore`]; remote members get nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct LocalOnlyPusher;

#[async_trait]
impl RoutingPusher for LocalOnlyPusher {
    async fn push(&self, _target: SocketAddr, _table_bytes: &[u8]) -> ClusterResult<()> {
        Ok(())
    }
}

/// Static parameters for [`run_coordinator_loop`].
#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct CoordinatorParams {
    /// Local member id — used to decide whether *this* node is the
    /// coordinator at each tick.
    pub local_id: MemberId,
    /// Membership source.
    pub view: MembershipView,
    /// Hasher for the consistent-hash ring.
    pub hasher: Arc<dyn Hasher>,
    /// Local routing-table store; the coordinator applies its own builds here.
    pub store: Arc<RoutingTableStore>,
    /// Inter-node pusher.
    pub pusher: Arc<dyn RoutingPusher>,
    /// Wakeup cadence; should equal `routing.push_interval`.
    pub push_interval: Duration,
    /// Partitions per cluster (immutable post-bootstrap).
    pub partition_count: u32,
    /// Vnodes per member on the consistent-hash ring.
    pub virtual_nodes_per_member: u32,
    /// Bounded-load factor (`>= 1.0`).
    pub load_factor: f64,
    /// Replicas per partition (`>= 1`).
    pub replica_count: u32,
    /// Cooperative cancellation.
    pub cancel: CancellationToken,
}

/// Shared monotonic signature counter. Exposed so the [`Cluster`] runtime can
/// surface it for `CLUSTER.READY` / diagnostics.
#[derive(Debug, Default)]
pub struct SignatureClock(AtomicU64);

impl SignatureClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(0)))
    }
    pub fn current(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::AcqRel) + 1
    }
}

/// Run until the cancellation token fires.
///
/// On every tick (`routing.push_interval`):
/// 1. If this node is not the coordinator → noop.
/// 2. If the topology has changed since the last build → bump signature,
///    rebuild the table, apply locally.
/// 3. Push the current table to every peer regardless of topology change.
///
/// Pushing every tick means a peer that briefly missed a previous push
/// (network blip, late registration, transient process pressure) still
/// converges. Receivers reject by signature, so repeating an unchanged
/// table is cheap on the recipient side.
pub async fn run_coordinator_loop(params: CoordinatorParams, signature: Arc<SignatureClock>) {
    let mut ticker = interval(params.push_interval);
    let mut last_topology_hash: Option<u64> = None;
    let mut current_table: Option<RoutingTable> = None;
    debug!(
        push_interval = ?params.push_interval,
        "routing coordinator loop started",
    );

    loop {
        tokio::select! {
            biased;
            () = params.cancel.cancelled() => {
                debug!("routing coordinator loop cancelled");
                return;
            }
            _ = ticker.tick() => {}
        }

        let snapshot = params.view.snapshot();
        if snapshot.is_empty() {
            trace!("no live members; skipping routing build");
            continue;
        }
        // Index 0 is the coordinator (snapshot is sorted by (birthdate, id)).
        let is_coord = snapshot[0].id == params.local_id;
        if !is_coord {
            trace!("not coordinator; skipping routing build");
            continue;
        }

        let topo = topology_hash(&snapshot);
        let topology_changed = last_topology_hash != Some(topo);
        if topology_changed {
            let next_sig = signature.next();
            let Some(table) = RoutingTable::build(
                snapshot.clone(),
                params.hasher.as_ref(),
                params.partition_count,
                params.virtual_nodes_per_member,
                params.load_factor,
                params.replica_count,
                next_sig,
            ) else {
                warn!("routing-table build returned None despite non-empty members");
                continue;
            };

            let local = params.store.apply(table.clone());
            match local {
                ApplyRoutingOutcome::Accepted => {
                    info!(
                        signature = next_sig,
                        members = snapshot.len(),
                        "routing table built and applied locally",
                    );
                }
                ApplyRoutingOutcome::Stale => {
                    warn!(
                        signature = next_sig,
                        "local store rejected our own build — another coordinator is active",
                    );
                }
                ApplyRoutingOutcome::UnsupportedSchema => {
                    warn!("local store rejected our own build (schema)");
                }
            }

            last_topology_hash = Some(topo);
            current_table = Some(table);
        }

        let Some(table) = current_table.as_ref() else {
            continue;
        };
        let bytes = match table.to_msgpack() {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "failed to encode routing table");
                continue;
            }
        };
        push_to_peers(&params, &snapshot, &bytes).await;
    }
}

async fn push_to_peers(params: &CoordinatorParams, members: &[Member], bytes: &[u8]) {
    // CPU-bounded parallelism per `docs/02-consistent-hashing.md` (the
    // doc-recommended cap; bounded concurrent send is fully exercised by
    // the Forwarder in Phase 4B).
    let parallel = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4)
        .max(1);
    let mut iter = members.iter().filter(|m| m.id != params.local_id);
    loop {
        let batch: Vec<&Member> = iter.by_ref().take(parallel).collect();
        if batch.is_empty() {
            break;
        }
        let mut handles = Vec::with_capacity(batch.len());
        for m in batch {
            let pusher = Arc::clone(&params.pusher);
            let target = m.addr;
            let bytes = bytes.to_vec();
            handles.push(tokio::spawn(async move {
                if let Err(e) = pusher.push(target, &bytes).await {
                    warn!(target = %target, error = %e, "routing push failed");
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }
}

fn topology_hash(members: &[Member]) -> u64 {
    // Order-independent hash so member-list shuffles that don't actually
    // change membership don't trip a push. `(id, birthdate)` pairs are
    // hashed individually and XORed — XOR is associative + commutative so
    // permutation-invariant.
    members.iter().fold(0_u64, |acc, m| {
        let mut h = DefaultHasher::new();
        m.id.as_u64().hash(&mut h);
        m.birthdate.hash(&mut h);
        acc ^ h.finish()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kamino_core::hasher::XxHasher;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Mutex;

    fn mk(id: u64, birthdate: u64, port: u16) -> Member {
        Member::new(
            MemberId::from_raw(id),
            format!("n{id}"),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2),
            birthdate,
        )
    }

    #[derive(Debug, Default)]
    struct RecordingPusher {
        sent: Mutex<Vec<(SocketAddr, Vec<u8>)>>,
    }

    #[async_trait]
    impl RoutingPusher for RecordingPusher {
        async fn push(&self, target: SocketAddr, bytes: &[u8]) -> ClusterResult<()> {
            self.sent.lock().unwrap().push((target, bytes.to_vec()));
            Ok(())
        }
    }

    #[test]
    fn topology_hash_order_independent() {
        let a = [mk(1, 100, 3320), mk(2, 200, 3322), mk(3, 300, 3324)];
        let mut b = a.clone();
        b.reverse();
        assert_eq!(topology_hash(&a), topology_hash(&b));
    }

    #[test]
    fn topology_hash_changes_on_join() {
        let a = [mk(1, 100, 3320), mk(2, 200, 3322)];
        let b = [mk(1, 100, 3320), mk(2, 200, 3322), mk(3, 300, 3324)];
        assert_ne!(topology_hash(&a), topology_hash(&b));
    }

    #[tokio::test(start_paused = true)]
    async fn coordinator_pushes_on_first_tick_and_skips_when_unchanged() {
        let local = mk(1, 100, 3320);
        let view = MembershipView::bootstrap(local.clone());
        let pusher = Arc::new(RecordingPusher::default());
        let store = Arc::new(RoutingTableStore::new());
        let sig = SignatureClock::new();
        let cancel = CancellationToken::new();
        let params = CoordinatorParams {
            local_id: local.id,
            view,
            hasher: Arc::new(XxHasher),
            store: Arc::clone(&store),
            pusher: Arc::clone(&pusher) as Arc<dyn RoutingPusher>,
            push_interval: Duration::from_millis(50),
            partition_count: 271,
            virtual_nodes_per_member: 20,
            load_factor: 1.25,
            replica_count: 1,
            cancel: cancel.clone(),
        };
        let handle = tokio::spawn(run_coordinator_loop(params, Arc::clone(&sig)));
        // Advance enough to trigger several ticks.
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();
        let _ = handle.await;

        // First tick built and applied locally; signature should be 1.
        assert_eq!(sig.current(), 1);
        assert_eq!(store.signature(), 1);
        // No remote peers — nothing pushed.
        assert!(pusher.sent.lock().unwrap().is_empty());
    }
}
