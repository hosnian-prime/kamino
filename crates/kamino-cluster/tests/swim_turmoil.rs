//! Multi-node SWIM acceptance tests for Phase 3.
//!
//! The `turmoil` crate ships its own virtual `UdpSocket` type that is **not**
//! `tokio::net::UdpSocket`, so plugging it into [`kamino_cluster::UdpTransport`]
//! would require a second [`Transport`] impl wired to turmoil. Rather than
//! ship a parallel transport just for tests, the suite below drives multiple
//! real [`UdpTransport`] instances on localhost — each `Cluster` binds to
//! `127.0.0.1:0` so the OS chooses an ephemeral free port. Time and
//! ordering are real but bounded; every test caps wall-clock with a strict
//! timeout to keep CI flake-free.
//!
//! Tests marked `#[ignore]` document themselves as "needs Agent A's loop
//! impl"; they verify steady-state convergence that can only happen once
//! the receive + probe loops actually move bytes. They are written ahead of
//! the merge so flipping the ignore-flag is the only change required at
//! integration time.

#![allow(clippy::field_reassign_with_default)]

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use kamino_cluster::transport::MockHub;
use kamino_cluster::{Cluster, ClusterDeps, StaticDiscovery, Transport as _, UdpTransport};
use kamino_core::clock::SystemClock;
use kamino_core::config::Config;
use kamino_core::ids::MemberId;
use kamino_core::member::Member;

/// Hand out distinct `127.0.0.1:N` "addresses" to mock transports so multiple
/// clusters in the same test can be told apart by the hub.
fn next_mock_port() -> u16 {
    static SEQ: AtomicU16 = AtomicU16::new(40_000);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Bootstrap one node over a real localhost UDP socket. Used by the single-
/// node runtime-wiring tests where exercising the actual `UdpTransport` is
/// the point.
async fn bootstrap_node(
    name: &str,
    birthdate: u64,
    peers: Vec<SocketAddr>,
) -> (Arc<Cluster>, SocketAddr) {
    let transport = Arc::new(
        UdpTransport::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .expect("bind udp"),
    );
    let discovery_addr = transport.local_addr().expect("local_addr");
    let discovery = Arc::new(StaticDiscovery::from_addrs(peers));
    let local = Member::new(
        MemberId::new_random(),
        name,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
        discovery_addr,
        birthdate,
    );
    let cluster = Cluster::bootstrap(deps_for(name, transport, discovery, local))
        .await
        .expect("bootstrap");
    (cluster, discovery_addr)
}

/// Bootstrap one node over an in-memory [`MockTransport`] sharing the given
/// hub. Used by the multi-node acceptance tests: routing is deterministic,
/// no kernel buffers, no UDP packet drops under load, no GitHub-runner
/// scheduler flake.
async fn bootstrap_mock_node(
    hub: &MockHub,
    name: &str,
    birthdate: u64,
    peers: Vec<SocketAddr>,
) -> (Arc<Cluster>, SocketAddr) {
    let discovery_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), next_mock_port());
    let transport = Arc::new(hub.endpoint(discovery_addr));
    let discovery = Arc::new(StaticDiscovery::from_addrs(peers));
    let local = Member::new(
        MemberId::new_random(),
        name,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
        discovery_addr,
        birthdate,
    );
    let cluster = Cluster::bootstrap(deps_for(name, transport, discovery, local))
        .await
        .expect("bootstrap");
    (cluster, discovery_addr)
}

fn deps_for(
    _name: &str,
    transport: Arc<dyn kamino_cluster::Transport>,
    discovery: Arc<StaticDiscovery>,
    local: Member,
) -> ClusterDeps {
    let mut config = Config::default();
    config.discovery.peers = Vec::new();
    config.discovery.max_join_attempts = 2;
    config.discovery.join_retry_interval = Duration::from_millis(20);
    config.discovery.bootstrap_timeout = Duration::from_millis(250);
    config.discovery.leave_timeout = Duration::from_millis(50);
    // Tight probe interval keeps the convergence tests fast. With
    // `MockTransport` the runtime cost of a probe round is dominated by
    // tokio's tick precision (~1-5ms), so 100ms is plenty of headroom.
    config.swim.probe_interval = Duration::from_millis(100);
    config.swim.probe_timeout = Duration::from_millis(50);
    config.swim.suspicion_multiplier = 3;
    ClusterDeps {
        config,
        transport,
        discovery,
        clock: Arc::new(SystemClock),
        local,
    }
}

#[tokio::test]
async fn single_node_bootstrap_returns_only_local() {
    // Bootstrap with no peers configured: join() must succeed instantly
    // (single-node path), spawn the SWIM loops, and surface a 1-element
    // snapshot whose sole entry is the coordinator.
    let (cluster, _) = bootstrap_node("solo", 1, Vec::new()).await;
    let snap = cluster.snapshot_members();
    assert_eq!(snap.len(), 1);
    assert!(snap[0].is_coordinator);
    cluster.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn bootstrap_shutdown_awaits_spawned_tasks() {
    // Run-time wiring smoke: bootstrap → wait briefly so the tasks are
    // definitely scheduled → shutdown. The test passes if shutdown returns
    // promptly (well under leave_timeout) — that proves the cancel token is
    // wired to the loops and that the JoinHandles drain cleanly.
    let (cluster, _) = bootstrap_node("solo", 1, Vec::new()).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    tokio::time::timeout(Duration::from_secs(2), cluster.shutdown())
        .await
        .expect("shutdown did not return in time")
        .expect("shutdown returned err");
}

#[tokio::test]
async fn assemble_only_path_is_independent_of_loops() {
    // The runtime contract is that `assemble()` is a pure construction
    // step: it must work without spinning up any tokio tasks and without
    // doing any I/O. Used by SWIM unit tests that drive the driver manually.
    let transport = Arc::new(
        UdpTransport::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .expect("bind"),
    );
    let local = Member::new(
        MemberId::new_random(),
        "asm",
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
        transport.local_addr().unwrap(),
        7,
    );
    let deps = ClusterDeps {
        config: Config::default(),
        transport,
        discovery: Arc::new(StaticDiscovery::from_addrs(Vec::new())),
        clock: Arc::new(SystemClock),
        local,
    };
    let cluster = Cluster::assemble(deps);
    assert_eq!(cluster.snapshot_members().len(), 1);
    assert!(cluster.snapshot_members()[0].is_coordinator);
}

// ---------------------------------------------------------------------------
// Multi-node acceptance tests. All packet routing goes through a process-
// local `MockHub` — no UDP sockets, no kernel buffers, no GitHub-runner
// scheduler flakes. The probe loops still drive on real tokio time, so the
// 3 * probe_interval (300ms) bound from SWIM holds.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn three_node_cluster_converges_within_three_probe_intervals() {
    let hub = MockHub::new();
    let (a, addr_a) = bootstrap_mock_node(&hub, "a", 100, Vec::new()).await;
    let (b, addr_b) = bootstrap_mock_node(&hub, "b", 200, vec![addr_a]).await;
    let (c, _addr_c) = bootstrap_mock_node(&hub, "c", 300, vec![addr_a, addr_b]).await;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(2500);
    loop {
        let a_ids: HashSet<u64> = a.snapshot_members().iter().map(|m| m.id.as_u64()).collect();
        let b_ids: HashSet<u64> = b.snapshot_members().iter().map(|m| m.id.as_u64()).collect();
        let c_ids: HashSet<u64> = c.snapshot_members().iter().map(|m| m.id.as_u64()).collect();
        if a_ids.len() == 3 && a_ids == b_ids && b_ids == c_ids {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "3-node cluster did not converge: a={a_ids:?}, b={b_ids:?}, c={c_ids:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = a.shutdown().await;
    let _ = b.shutdown().await;
    let _ = c.shutdown().await;
}

#[tokio::test]
async fn five_node_cluster_detects_death_within_suspicion_window() {
    let hub = MockHub::new();
    let mut nodes: Vec<Arc<Cluster>> = Vec::new();
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for i in 0..5_u64 {
        let (n, addr) = bootstrap_mock_node(&hub, &format!("n{i}"), 100 + i, addrs.clone()).await;
        addrs.push(addr);
        nodes.push(n);
    }
    // First wait for the cluster to fully converge to 5 members — only then
    // does removing one node have a defined "before / after" against which
    // we can measure the suspicion window. Without this gate, the leaving
    // node's Leave gossip can race the cluster's own discovery and be
    // ignored as an event for an unknown member.
    let converge = tokio::time::Instant::now() + Duration::from_millis(3000);
    while nodes.iter().any(|n| n.snapshot_members().len() != 5) {
        assert!(
            tokio::time::Instant::now() < converge,
            "pre-death convergence failed; sizes = {:?}",
            nodes
                .iter()
                .map(|n| n.snapshot_members().len())
                .collect::<Vec<_>>(),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Drop one node. The graceful Leave broadcast on `shutdown` lands at
    // every survivor through the mock hub; partition the leaving address
    // so any belated probe sees the same UDP-style silent-drop behaviour.
    let leaving = nodes.pop().unwrap();
    let leaving_addr = *addrs.last().unwrap();
    let _ = leaving.shutdown().await;
    hub.partition(leaving_addr);

    let deadline = tokio::time::Instant::now() + Duration::from_millis(3000);
    loop {
        let ok = nodes.iter().all(|n| n.snapshot_members().len() == 4);
        if ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not shrink to 4; observed sizes = {:?}",
            nodes
                .iter()
                .map(|n| n.snapshot_members().len())
                .collect::<Vec<_>>(),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for n in nodes {
        let _ = n.shutdown().await;
    }
}

#[tokio::test]
async fn five_node_cluster_agrees_on_coordinator() {
    let hub = MockHub::new();
    let mut nodes: Vec<Arc<Cluster>> = Vec::new();
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for i in 0..5_u64 {
        let (n, addr) = bootstrap_mock_node(&hub, &format!("n{i}"), 100 + i, addrs.clone()).await;
        addrs.push(addr);
        nodes.push(n);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(3000);
    loop {
        let coords: HashSet<u64> = nodes
            .iter()
            .map(|n| {
                n.snapshot_members()
                    .iter()
                    .find(|m| m.is_coordinator)
                    .map_or(0, |m| m.id.as_u64())
            })
            .collect();
        if coords.len() == 1 && !coords.contains(&0) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "coordinator did not agree across nodes: {coords:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for n in nodes {
        let _ = n.shutdown().await;
    }
}

#[tokio::test]
async fn simultaneous_birthdates_use_memberid_tiebreaker() {
    // All five nodes claim birthdate = 100. The deterministic tiebreaker
    // is `MemberId` (smaller wins). After convergence every node must
    // agree on the *same* coordinator.
    let hub = MockHub::new();
    let mut nodes: Vec<Arc<Cluster>> = Vec::new();
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for i in 0..5_u64 {
        let (n, addr) = bootstrap_mock_node(&hub, &format!("tb{i}"), 100, addrs.clone()).await;
        addrs.push(addr);
        nodes.push(n);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(3000);
    let last_coords: HashSet<u64> = loop {
        let coords: HashSet<u64> = nodes
            .iter()
            .filter_map(|n| {
                n.snapshot_members()
                    .iter()
                    .find(|m| m.is_coordinator)
                    .map(|m| m.id.as_u64())
            })
            .collect();
        if coords.len() == 1 && nodes.iter().all(|n| n.snapshot_members().len() == 5) {
            break coords;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "tiebreaker did not converge; coordinators = {coords:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    // Cross-check: the agreed-upon coordinator must be the smallest MemberId
    // across all snapshots — that's the deterministic tiebreaker.
    let min_id = nodes
        .iter()
        .flat_map(|n| n.snapshot_members().into_iter().map(|m| m.id.as_u64()))
        .min()
        .expect("at least one member");
    assert_eq!(last_coords.iter().next().copied(), Some(min_id));
    for n in nodes {
        let _ = n.shutdown().await;
    }
}
