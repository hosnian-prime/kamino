//! Phase 4 routing-table acceptance tests.
//!
//! Three scenarios per `ROADMAP.md` §6 Phase 4 acceptance:
//!
//! 1. **Distribution**: 3-node cluster → keys distribute roughly evenly
//!    within the bounded-load ceiling.
//! 2. **Convergence**: the coordinator's routing table arrives on every
//!    other node via the simulated `INTERNAL.NODE.UPDATEROUTING` push, and
//!    each receiver's `RoutingTableStore` accepts it.
//! 3. **Coordinator failover signature monotonicity**: after the original
//!    coordinator leaves, the second-oldest's first push has a strictly
//!    higher signature, so old broadcasts cannot resurrect a stale table.

#![allow(clippy::similar_names)] // `pushed` (assertion result) vs `pusher` (test fixture)

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kamino_cluster::routing::coordinator::RoutingPusher;
use kamino_cluster::transport::MockHub;
use kamino_cluster::{
    Cluster, ClusterDeps, ClusterResult, RoutingProvider, RoutingTable, StaticDiscovery,
};
use kamino_core::clock::SystemClock;
use kamino_core::config::Config;
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use parking_lot::Mutex;
use tokio::time::{Instant, sleep};

fn next_mock_port() -> u16 {
    static SEQ: AtomicU16 = AtomicU16::new(45_000);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Routes table pushes back into the recipient clusters' own
/// `apply_routing_update` methods. Models Phase 4B's Forwarder for testing
/// purposes — the real implementation will go over RESP.
#[derive(Default)]
struct DirectPusher {
    targets: Mutex<HashMap<SocketAddr, Arc<Cluster>>>,
}

impl DirectPusher {
    fn register(&self, addr: SocketAddr, cluster: Arc<Cluster>) {
        self.targets.lock().insert(addr, cluster);
    }
}

#[async_trait]
impl RoutingPusher for DirectPusher {
    async fn push(&self, target: SocketAddr, bytes: &[u8]) -> ClusterResult<()> {
        let cluster_opt = self.targets.lock().get(&target).cloned();
        if let Some(cluster) = cluster_opt {
            let _ = cluster.apply_routing_update(bytes)?;
        }
        // Unknown target — silently drop, modelling "peer not yet known".
        // The continuous-push behaviour of the coordinator loop covers the
        // late-registration case on the next tick.
        Ok(())
    }
}

fn deps_with_pusher(
    transport: Arc<dyn kamino_cluster::Transport>,
    discovery: Arc<StaticDiscovery>,
    local: Member,
    pusher: Arc<dyn RoutingPusher>,
) -> ClusterDeps {
    let mut config = Config::default();
    config.discovery.peers = Vec::new();
    config.discovery.max_join_attempts = 4;
    config.discovery.join_retry_interval = Duration::from_millis(20);
    config.discovery.bootstrap_timeout = Duration::from_millis(500);
    config.discovery.leave_timeout = Duration::from_millis(100);
    config.swim.probe_interval = Duration::from_millis(80);
    config.swim.probe_timeout = Duration::from_millis(40);
    config.swim.suspicion_multiplier = 3;
    config.routing.push_interval = Duration::from_millis(100);
    ClusterDeps {
        config,
        transport,
        discovery,
        clock: Arc::new(SystemClock),
        local,
        hasher: None,
        routing_pusher: Some(pusher),
    }
}

async fn bootstrap_routing_node(
    hub: &MockHub,
    pusher: Arc<DirectPusher>,
    name: &str,
    birthdate: u64,
    peers: Vec<SocketAddr>,
) -> (Arc<Cluster>, SocketAddr, SocketAddr) {
    let discovery_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), next_mock_port());
    // Use a distinct RESP "addr" per node so the pusher can route by it.
    let resp_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), next_mock_port());
    let transport = Arc::new(hub.endpoint(discovery_addr));
    let discovery = Arc::new(StaticDiscovery::from_addrs(peers));
    let local = Member::new(
        MemberId::new_random(),
        name,
        resp_addr,
        discovery_addr,
        birthdate,
    );
    let cluster = Cluster::bootstrap(deps_with_pusher(
        transport,
        discovery,
        local,
        pusher.clone() as Arc<dyn RoutingPusher>,
    ))
    .await
    .expect("bootstrap");
    pusher.register(resp_addr, Arc::clone(&cluster));
    (cluster, resp_addr, discovery_addr)
}

async fn wait_until<F: FnMut() -> bool>(deadline: Duration, mut cond: F) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if cond() {
            return true;
        }
        sleep(Duration::from_millis(25)).await;
    }
    false
}

#[tokio::test]
async fn three_node_cluster_distributes_partitions_evenly() {
    let hub = MockHub::new();
    let pusher = Arc::new(DirectPusher::default());

    let (a, _ra, da) = bootstrap_routing_node(&hub, pusher.clone(), "A", 100, vec![]).await;
    let (b, _rb, _db) = bootstrap_routing_node(&hub, pusher.clone(), "B", 200, vec![da]).await;
    let (c, _rc, _dc) = bootstrap_routing_node(&hub, pusher.clone(), "C", 300, vec![da]).await;

    let converged = wait_until(Duration::from_secs(3), || {
        a.snapshot_members().len() == 3
            && b.snapshot_members().len() == 3
            && c.snapshot_members().len() == 3
    })
    .await;
    assert!(
        converged,
        "expected 3-member SWIM convergence; A={}, B={}, C={}",
        a.snapshot_members().len(),
        b.snapshot_members().len(),
        c.snapshot_members().len(),
    );

    // Wait for the coordinator to push a routing table that every node
    // accepts.
    let pushed = wait_until(Duration::from_secs(3), || {
        a.routing_signature() > 0 && b.routing_signature() > 0 && c.routing_signature() > 0
    })
    .await;
    assert!(
        pushed,
        "every node must receive a routing-table push: A={} B={} C={}",
        a.routing_signature(),
        b.routing_signature(),
        c.routing_signature(),
    );

    // Sanity: same signature across the cluster — only one coordinator
    // built a table in steady state.
    assert_eq!(a.routing_signature(), b.routing_signature());
    assert_eq!(b.routing_signature(), c.routing_signature());

    // Pull the table off A (the oldest = coordinator) and verify the
    // bounded-load invariant.
    let rt_bytes = a.routing_table_bytes().expect("coordinator has table");
    let rt = RoutingTable::from_msgpack(&rt_bytes).unwrap();
    let mut load: HashMap<MemberId, u32> = HashMap::new();
    for p in 0..rt.partition_count() {
        let prim = rt.primary_for(p).unwrap();
        *load.entry(prim.id).or_default() += 1;
    }
    // 271 partitions / 3 members ≈ 90; load_factor=1.25 → cap ≈ 113.
    for (id, count) in load {
        assert!(
            count <= 114,
            "member {id} owns {count} primaries — exceeds bounded-load ceiling",
        );
    }

    // Shut everything down so the test cleans up promptly.
    let _ = a.shutdown().await;
    let _ = b.shutdown().await;
    let _ = c.shutdown().await;
}

#[tokio::test]
async fn signature_strictly_increases_after_coordinator_leave() {
    let hub = MockHub::new();
    let pusher = Arc::new(DirectPusher::default());
    let (a, _ra, da) = bootstrap_routing_node(&hub, pusher.clone(), "A", 100, vec![]).await;
    let (b, _rb, _db) = bootstrap_routing_node(&hub, pusher.clone(), "B", 200, vec![da]).await;
    let (c, _rc, _dc) = bootstrap_routing_node(&hub, pusher.clone(), "C", 300, vec![da]).await;

    // Wait for full SWIM + routing convergence.
    let ready = wait_until(Duration::from_secs(3), || {
        a.snapshot_members().len() == 3
            && b.snapshot_members().len() == 3
            && c.snapshot_members().len() == 3
            && a.routing_signature() > 0
            && b.routing_signature() > 0
            && c.routing_signature() > 0
    })
    .await;
    assert!(ready, "initial convergence failed");
    let sig_before = a.routing_signature();
    assert!(sig_before >= 1);

    // A is the coordinator (oldest birthdate). Take it down.
    let _ = a.shutdown().await;

    // B is now the coordinator. Its next push must carry a signature
    // strictly greater than the one A produced.
    let bumped = wait_until(Duration::from_secs(3), || {
        b.routing_signature() > sig_before && c.routing_signature() > sig_before
    })
    .await;
    assert!(
        bumped,
        "post-failover coordinator must produce a higher signature: B={} C={} (before={})",
        b.routing_signature(),
        c.routing_signature(),
        sig_before,
    );

    let _ = b.shutdown().await;
    let _ = c.shutdown().await;
}
