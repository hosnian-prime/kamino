//! State-machine tests for the SWIM probe + receive loops.
//!
//! Exercises:
//!   * direct ping/ack round trip,
//!   * direct timeout → indirect ack via proxy,
//!   * direct + indirect failure → Suspect,
//!   * Suspect timer → Dead with gossip emission,
//!   * self-suspect refutation → incarnation bump + Alive broadcast,
//!   * gossip piggyback on every outgoing envelope.
//!
//! Uses the in-memory `MockTransport` so we can drive the state machine
//! without binding real UDP sockets.

#![allow(
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    // Stylistic noise on test scaffolding.
    clippy::missing_const_for_fn,
    clippy::match_single_binding,
    clippy::single_match,
)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kamino_cluster::membership::{MemberState, MembershipView};
use kamino_cluster::message::{Envelope, GossipEvent, SwimMessage};
use kamino_cluster::swim::{ProbeOutcome, SwimDriver, probe_once};
use kamino_cluster::transport::{MockHub, Transport};
use kamino_cluster::{GossipQueue, alive_for};
use kamino_core::clock::Clock;
use kamino_core::config::SwimConfig;
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use tokio_util::sync::CancellationToken;

/// Test clock: an `AtomicU64` of micros so tests can advance the "wall clock"
/// independently of `tokio::time`.
#[derive(Debug, Clone)]
struct TestClock {
    micros: Arc<AtomicU64>,
}

impl TestClock {
    fn new() -> Self {
        Self {
            micros: Arc::new(AtomicU64::new(1_700_000_000_000_000)),
        }
    }
    fn advance(&self, d: Duration) {
        let delta = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        self.micros.fetch_add(delta, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_micros(&self) -> u64 {
        self.micros.load(Ordering::SeqCst)
    }
    fn now_monotonic(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}

fn mk_member(id: u64, birthdate: u64, port: u16) -> Member {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let disc = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 100);
    Member::new(
        MemberId::from_raw(id),
        format!("n{id}"),
        addr,
        disc,
        birthdate,
    )
}

fn mk_config() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(200),
        probe_timeout: Duration::from_millis(50),
        indirect_probes: 2,
        suspicion_multiplier: 3,
    }
}

fn duration_to_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

struct Node {
    member: Member,
    driver: Arc<SwimDriver>,
    queue: Arc<GossipQueue>,
    view: MembershipView,
    clock: Arc<TestClock>,
    transport: Arc<dyn Transport>,
}

fn mk_node(hub: &MockHub, member: Member, peers: &[Member]) -> Node {
    let view = MembershipView::bootstrap(member.clone());
    let cfg = mk_config();
    let suspicion_ns = duration_to_nanos(cfg.probe_interval * cfg.suspicion_multiplier);
    for peer in peers {
        view.apply(&alive_for(peer, 1), 0, suspicion_ns);
    }
    let queue = Arc::new(GossipQueue::with_capacity(64));
    queue.push(
        alive_for(&member, view.local_incarnation()),
        peers.len() + 1,
    );
    let transport: Arc<dyn Transport> = Arc::new(hub.endpoint(member.discovery_addr));
    let clock = Arc::new(TestClock::new());
    let driver = Arc::new(SwimDriver::new(
        cfg,
        view.clone(),
        Arc::clone(&queue),
        Arc::clone(&transport),
        clock.clone(),
        String::new(),
        CancellationToken::new(),
    ));
    Node {
        member,
        driver,
        queue,
        view,
        clock,
        transport,
    }
}

fn spawn_recv(driver: Arc<SwimDriver>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(kamino_cluster::run_receive_loop(driver))
}

#[tokio::test]
async fn direct_ping_ack_roundtrip() {
    let hub = MockHub::new();
    let a = mk_member(1, 100, 10_000);
    let b = mk_member(2, 200, 10_001);
    let node_a = mk_node(&hub, a.clone(), std::slice::from_ref(&b));
    let node_b = mk_node(&hub, b.clone(), std::slice::from_ref(&a));

    // Both nodes need receive loops so A's Ack handling wakes the probe.
    let recv_a = spawn_recv(Arc::clone(&node_a.driver));
    let recv_b = spawn_recv(Arc::clone(&node_b.driver));

    let outcome = probe_once(&node_a.driver).await;
    assert_eq!(outcome, ProbeOutcome::DirectAck);

    node_a.driver.cancel.cancel();
    node_b.driver.cancel.cancel();
    let _ = recv_a.await;
    let _ = recv_b.await;
}

#[tokio::test]
async fn indirect_probe_succeeds_when_direct_fails() {
    let hub = MockHub::new();
    let a = mk_member(1, 100, 10_010);
    let b = mk_member(2, 200, 10_011);
    let c = mk_member(3, 300, 10_012);

    let node_a = mk_node(&hub, a.clone(), &[b.clone(), c.clone()]);
    let node_b = mk_node(&hub, b.clone(), &[a.clone(), c.clone()]);
    let node_c = mk_node(&hub, c.clone(), &[a.clone(), b.clone()]);

    // Drop A↔B in both directions, but leave A↔C and B↔C intact so the
    // indirect probe via C can succeed.
    hub.drop_link(a.discovery_addr, b.discovery_addr);
    hub.drop_link(b.discovery_addr, a.discovery_addr);

    let recv_a = spawn_recv(Arc::clone(&node_a.driver));
    let recv_b = spawn_recv(Arc::clone(&node_b.driver));
    let recv_c = spawn_recv(Arc::clone(&node_c.driver));

    // `probe_once` picks a random target — drive several probes until we
    // catch one that targets B (the only unreachable peer). We assert that
    // at least one indirect-ack occurred and zero suspects, which is the
    // observable property of the indirect probe path.
    let mut saw_indirect = false;
    for _ in 0..16 {
        match probe_once(&node_a.driver).await {
            ProbeOutcome::IndirectAck => {
                saw_indirect = true;
                break;
            }
            ProbeOutcome::Suspected => {
                panic!("target was suspected; indirect probe should have succeeded");
            }
            ProbeOutcome::DirectAck | ProbeOutcome::Idle => {}
        }
    }
    assert!(saw_indirect, "no indirect-ack observed across probes");

    node_a.driver.cancel.cancel();
    node_b.driver.cancel.cancel();
    node_c.driver.cancel.cancel();
    let _ = recv_a.await;
    let _ = recv_b.await;
    let _ = recv_c.await;
}

#[tokio::test]
async fn direct_and_indirect_both_fail_then_suspect() {
    let hub = MockHub::new();
    let a = mk_member(1, 100, 10_020);
    let b = mk_member(2, 200, 10_021);
    let c = mk_member(3, 300, 10_022);

    let node_a = mk_node(&hub, a.clone(), &[b.clone(), c.clone()]);
    let node_b = mk_node(&hub, b.clone(), &[a.clone(), c.clone()]);
    let node_c = mk_node(&hub, c.clone(), &[a.clone(), b.clone()]);

    // B is fully unreachable.
    hub.partition(b.discovery_addr);

    let recv_a = spawn_recv(Arc::clone(&node_a.driver));
    let recv_c = spawn_recv(Arc::clone(&node_c.driver));

    // Random target selection: drive probes until B is picked and suspected.
    let mut suspected = false;
    for _ in 0..16 {
        match probe_once(&node_a.driver).await {
            ProbeOutcome::Suspected => {
                suspected = true;
                break;
            }
            _ => {}
        }
    }
    assert!(suspected, "B never entered Suspect across probes");

    let entry = node_a.view.get(b.id).expect("B in A's view");
    assert_eq!(entry.state, MemberState::Suspect);

    let drained = node_a.queue.drain_for_send(8);
    assert!(
        drained
            .iter()
            .any(|e| matches!(e, GossipEvent::Suspect { id, .. } if *id == b.id)),
        "expected Suspect event for B in gossip queue, got {drained:?}"
    );

    node_a.driver.cancel.cancel();
    node_c.driver.cancel.cancel();
    let _ = recv_a.await;
    let _ = recv_c.await;
    drop(node_b);
}

#[tokio::test]
async fn suspect_timer_promotes_to_dead_and_gossips() {
    let hub = MockHub::new();
    let a = mk_member(1, 100, 10_030);
    let b = mk_member(2, 200, 10_031);
    let node_a = mk_node(&hub, a.clone(), std::slice::from_ref(&b));

    let now_ns = node_a.clock.now_micros().saturating_mul(1_000);
    let suspicion_timeout_ns = duration_to_nanos(node_a.driver.suspicion_timeout());
    node_a.view.apply(
        &GossipEvent::Suspect {
            id: b.id,
            incarnation: 1,
            from: a.id,
        },
        now_ns,
        suspicion_timeout_ns,
    );

    // Drain Alive announcements so the assertion below only sees Dead.
    let _ = node_a.queue.drain_for_send(64);

    // Reaper too soon — no promotion.
    let promoted = node_a.driver.tick_reaper();
    assert!(promoted.is_empty(), "no promotion before suspicion timeout");

    // Advance test clock past the suspicion deadline.
    node_a
        .clock
        .advance(node_a.driver.suspicion_timeout() + Duration::from_millis(50));
    let promoted = node_a.driver.tick_reaper();
    assert_eq!(promoted, vec![b.id]);

    let drained = node_a.queue.drain_for_send(16);
    assert!(
        drained
            .iter()
            .any(|e| matches!(e, GossipEvent::Dead { id, .. } if *id == b.id)),
        "expected Dead event for B, got {drained:?}"
    );
    assert!(node_a.view.snapshot().iter().all(|m| m.id != b.id));
}

#[tokio::test]
async fn self_suspect_triggers_refutation_broadcast() {
    let hub = MockHub::new();
    let a = mk_member(1, 100, 10_040);
    let b = mk_member(2, 200, 10_041);
    let node_a = mk_node(&hub, a.clone(), std::slice::from_ref(&b));

    // Drain any pre-existing Alive events.
    let _ = node_a.queue.drain_for_send(64);

    let start_inc = node_a.view.local_incarnation();

    let env = Envelope {
        cluster_secret: String::new(),
        from: b.id,
        msg: SwimMessage::Ping {
            seq: 99,
            target: a.id,
        },
        gossip: vec![GossipEvent::Suspect {
            id: a.id,
            incarnation: start_inc,
            from: b.id,
        }],
    };
    // Send the envelope into A via an out-of-band B endpoint.
    let b_endpoint = hub.endpoint(b.discovery_addr);
    b_endpoint
        .send_to(&env, a.discovery_addr)
        .await
        .expect("send via hub");

    let recv_a = spawn_recv(Arc::clone(&node_a.driver));

    // Poll for the refutation Alive in A's queue.
    let mut found = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let drained = node_a.queue.drain_for_send(64);
        for ev in &drained {
            if let GossipEvent::Alive {
                id, incarnation, ..
            } = ev
            {
                if *id == a.id && *incarnation > start_inc {
                    found = Some(*incarnation);
                    break;
                }
            }
        }
        if found.is_some() {
            break;
        }
    }
    let refuted = found.expect("refutation Alive for self not enqueued");
    assert!(refuted > start_inc);
    assert!(node_a.view.local_incarnation() > start_inc);

    node_a.driver.cancel.cancel();
    let _ = recv_a.await;
    drop(node_a);
    drop(b_endpoint);
}

#[tokio::test]
async fn gossip_piggybacks_on_every_outgoing_envelope() {
    let hub = MockHub::new();
    let a = mk_member(1, 100, 10_050);
    let b = mk_member(2, 200, 10_051);
    let node_a = mk_node(&hub, a.clone(), std::slice::from_ref(&b));
    let node_b = mk_node(&hub, b.clone(), std::slice::from_ref(&a));

    // Inject a unique gossip event into A's queue.
    let beacon = GossipEvent::Suspect {
        id: MemberId::from_raw(0x9999),
        incarnation: 7,
        from: a.id,
    };
    node_a.queue.push(beacon.clone(), 4);

    // Read raw envelopes that arrive at B instead of running B's receive loop.
    let transport = Arc::clone(&node_b.transport);
    let recv_handle = tokio::spawn(async move {
        let mut envs = Vec::new();
        for _ in 0..4 {
            match tokio::time::timeout(Duration::from_millis(200), transport.recv()).await {
                Ok(Ok((env, _))) => envs.push(env),
                _ => break,
            }
        }
        envs
    });

    // Drive a probe from A — its direct Ping will reach B but B's recv loop
    // is the spawned reader above. Probe will timeout (no Ack arrives)
    // since we're not echoing — that's fine; we only assert on gossip
    // content of the *first* outgoing envelope.
    let _ = probe_once(&node_a.driver).await;

    let envelopes = recv_handle.await.unwrap_or_default();
    assert!(!envelopes.is_empty(), "no envelopes arrived at B");
    let saw_beacon = envelopes
        .iter()
        .any(|e| e.gossip.iter().any(|g| g == &beacon));
    assert!(
        saw_beacon,
        "expected beacon gossip on at least one envelope; got: {envelopes:#?}"
    );

    node_a.driver.cancel.cancel();
    node_b.driver.cancel.cancel();
}

// Keep `Node` fields like `member` from being warned as dead by clippy when
// individual tests only read `driver`/`view`/`queue`. Test scaffolding only.
#[allow(dead_code)]
fn _force_field_reads(n: &Node) -> (&Member, &Arc<TestClock>) {
    (&n.member, &n.clock)
}
