//! ROADMAP §6 Phase 5 acceptance tests — kill-primary, LWW convergence,
//! quorum stress.
//!
//! The runtime under test is the same `kamino-server` accepted-loop used in
//! production, driven by a stub [`RoutingProvider`] that lets us script:
//!
//! - which addresses count as "backups" for a given key,
//! - whether `member_count_quorum` is currently satisfied,
//! - which RESP reply the forwarder returns for each peer.
//!
//! End-to-end clients hit the bound TCP address through
//! [`MultiNodeRemoteClient`]; the dispatch path is the real one, including
//! the Phase 5 fan-out + LWW merge layers.

#![allow(
    clippy::similar_names, // server_p / server_b mirror each other deliberately
    clippy::field_reassign_with_default, // tests build Config field-by-field for clarity
    clippy::doc_markdown, // doc references to literal config knobs read fine without backticks
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_client::{Client, DMapOptions, MultiNodeRemoteClient, PutOptions};
use kamino_cluster::{ApplyRoutingOutcome, ClusterError, ReplicationSettings, RoutingProvider};
use kamino_core::{Clock, Config, Hasher, Mode, ReplicationMode, SystemClock, XxHasher};
use kamino_protocol::{Command, Frame};
use kamino_server::Server;
use kamino_storage::{Locker, RamBlock, StorageEngine};

/// Bind a `kamino-server` on `127.0.0.1:0` with an embedded backend and the
/// supplied stub routing provider. Wraps the standard Phase 4 surface so
/// these tests focus on Phase 5 semantics.
async fn start_server(
    mut config: Config,
    routing: Option<Arc<dyn RoutingProvider>>,
) -> (Server, Arc<EmbeddedClient>) {
    config.mode = Mode::Standalone;
    config.network.bind_port = 0;
    config.network.bind_addr = std::net::IpAddr::from([127, 0, 0, 1]);

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
    let locker = Locker::new();
    let table_size = usize::try_from(config.storage.table_size.0).unwrap_or(usize::MAX);
    let max_garbage_ratio = config.storage.max_garbage_ratio;
    let factory: EngineFactory = Arc::new(move || -> Box<dyn StorageEngine> {
        Box::new(RamBlock::new(table_size, max_garbage_ratio))
    });
    let deps = EmbeddedDeps {
        clock,
        hasher,
        locker,
        engine_factory: factory,
        partition_count: config.core.partition_count,
    };
    let embedded = EmbeddedClient::new(deps);
    let erased: Arc<dyn Client> = Arc::clone(&embedded) as Arc<dyn Client>;
    let server = Server::bind_with_providers(&config, erased, None, routing)
        .await
        .expect("bind_with_providers");
    (server, embedded)
}

/// Routing provider with the full Phase 5 surface. Tests construct one and
/// hand it to *both* the primary (to fan out) and each backup (so the
/// `internode` flag flips through cluster_secret auth).
///
/// The forwarder reaches backups by establishing a real TCP connection
/// authenticated with `cluster_secret`, so the backup-side dispatcher
/// observes `state.internode = true` and runs the LWW path.
#[derive(Debug)]
struct ScriptedRouter {
    /// Backups for every key — uniform across the test.
    backups: Vec<SocketAddr>,
    /// Whether `member_quorum_satisfied()` returns true.
    quorum_ok: std::sync::atomic::AtomicBool,
    /// Effective replication settings — cloned on every call.
    settings: Mutex<ReplicationSettings>,
    /// Real forwarder pointed at backup servers. Phase 4 forwarder already
    /// authenticates via cluster_secret on the handshake.
    forwarder: kamino_cluster::Forwarder,
}

impl ScriptedRouter {
    fn new(backups: Vec<SocketAddr>, settings: ReplicationSettings, cluster_secret: &str) -> Self {
        let cfg = kamino_cluster::ForwarderConfig {
            cluster_secret: cluster_secret.into(),
            ..Default::default()
        };
        Self {
            backups,
            quorum_ok: std::sync::atomic::AtomicBool::new(true),
            settings: Mutex::new(settings),
            forwarder: kamino_cluster::Forwarder::new(cfg),
        }
    }
}

impl RoutingProvider for ScriptedRouter {
    fn routing_table_bytes(&self) -> Option<Vec<u8>> {
        None
    }
    fn apply_routing_update(&self, _bytes: &[u8]) -> Result<ApplyRoutingOutcome, ClusterError> {
        Ok(ApplyRoutingOutcome::Accepted)
    }
    fn is_ready(&self) -> bool {
        true
    }
    fn routing_signature(&self) -> u64 {
        1
    }
    fn route_key(&self, _dmap_name: &[u8], _key: &[u8]) -> Option<SocketAddr> {
        None // every key is locally owned (no MOVED)
    }
    fn partition_for_key(&self, _dmap_name: &[u8], _key: &[u8]) -> u32 {
        42
    }
    fn multi_key_strict(&self) -> bool {
        false
    }
    fn backup_addrs_for_key(&self, _dmap_name: &[u8], _key: &[u8]) -> Vec<SocketAddr> {
        self.backups.clone()
    }
    fn member_quorum_satisfied(&self) -> bool {
        self.quorum_ok.load(Ordering::Acquire)
    }
    fn replication_settings(&self) -> ReplicationSettings {
        *self.settings.lock().unwrap()
    }
    fn forward_command<'a>(
        &'a self,
        peer: SocketAddr,
        cmd: Command,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Frame, ClusterError>> + Send + 'a>>
    {
        Box::pin(async move { self.forwarder.send(peer, cmd).await })
    }
    fn forward_dm_del<'a>(
        &'a self,
        _peer: SocketAddr,
        _dmap: Bytes,
        _keys: Vec<Bytes>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<i64, ClusterError>> + Send + 'a>>
    {
        Box::pin(async { Ok(0) })
    }
}

/// Stub router for backup servers — exposes the cluster_secret-aware
/// dispatcher path without trying to fan out further.
#[derive(Debug)]
struct LeafRouter;

impl RoutingProvider for LeafRouter {
    fn routing_table_bytes(&self) -> Option<Vec<u8>> {
        None
    }
    fn apply_routing_update(&self, _bytes: &[u8]) -> Result<ApplyRoutingOutcome, ClusterError> {
        Ok(ApplyRoutingOutcome::Accepted)
    }
    fn is_ready(&self) -> bool {
        true
    }
    fn routing_signature(&self) -> u64 {
        1
    }
    fn route_key(&self, _dmap_name: &[u8], _key: &[u8]) -> Option<SocketAddr> {
        None
    }
    fn partition_for_key(&self, _dmap_name: &[u8], _key: &[u8]) -> u32 {
        42
    }
    fn multi_key_strict(&self) -> bool {
        false
    }
    fn forward_dm_del<'a>(
        &'a self,
        _peer: SocketAddr,
        _dmap: Bytes,
        _keys: Vec<Bytes>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<i64, ClusterError>> + Send + 'a>>
    {
        Box::pin(async { Ok(0) })
    }
}

/// Increment a monotonic port counter so every test allocates fresh
/// addresses — relevant for SocketAddr equality assertions.
static SEED: AtomicU64 = AtomicU64::new(0);
fn unique_seed() -> u64 {
    SEED.fetch_add(1, Ordering::Relaxed)
}

fn cluster_config(secret: &str) -> Config {
    let mut cfg = Config::default();
    cfg.mode = Mode::Standalone;
    cfg.auth.cluster_secret = secret.into();
    // Make sure replication runs on real Phase 5 settings.
    cfg.core.replica_count = 2;
    cfg.core.write_quorum = 2;
    cfg
}

#[tokio::test]
async fn write_replicates_to_backup_and_survives_primary_loss() {
    // ROADMAP §6 Phase 5 acceptance #1: "kill a primary, reads served from
    // backup, no data loss". We bring up two servers (P + B), wire P to
    // fan out to B via the real forwarder, write through P, then shut P
    // down and assert B still serves the value.
    let _ = unique_seed();
    let secret = "phase5-secret-1";

    // Backup server first so we have its address ready for the router.
    let cfg_b = cluster_config(secret);
    let leaf: Arc<dyn RoutingProvider> = Arc::new(LeafRouter);
    let (server_b, embedded_b) = start_server(cfg_b, Some(leaf)).await;
    let addr_b = server_b.local_addr();
    let shutdown_b = server_b.shutdown_handle();
    let server_b_task = tokio::spawn(server_b.run());

    // Pre-create the dmap on B so the LWW path can land an entry — the
    // embedded client lazily allocates on first put either way; we just
    // give the test a hook.
    let _ = embedded_b
        .new_dmap("repl", DMapOptions::default())
        .await
        .unwrap();

    // Primary server: replicate every key to B.
    let cfg_p = cluster_config(secret);
    let settings = ReplicationSettings {
        replica_count: 2,
        write_quorum: 2,
        read_quorum: 1,
        read_repair: false,
        mode: ReplicationMode::Sync,
    };
    let router: Arc<dyn RoutingProvider> =
        Arc::new(ScriptedRouter::new(vec![addr_b], settings, secret));
    let (server_p, _embedded_p) = start_server(cfg_p, Some(router)).await;
    let addr_p = server_p.local_addr();
    let shutdown_p = server_p.shutdown_handle();
    let server_p_task = tokio::spawn(server_p.run());

    // Client writes through P. With write_quorum=2 we need B's ack so the
    // write proves replication is real, not just local.
    let client = MultiNodeRemoteClient::connect(vec![format!("{addr_p}")], None)
        .await
        .expect("client connect to P");
    let dmap = client
        .new_dmap("repl", DMapOptions::default())
        .await
        .expect("new_dmap");
    dmap.put("k", b"v-replicated", PutOptions::default())
        .await
        .expect("put through primary must reach quorum");

    drop(dmap);
    let _ = client.close().await;

    // Kill P. B should still hold the value (replicated via cluster_secret-
    // authed forwarder, LWW-merged into B's embedded storage).
    shutdown_p.trigger();
    let _ = server_p_task.await;

    // Verify B has the entry by talking to it directly through a fresh
    // remote client.
    let client_b = MultiNodeRemoteClient::connect(vec![format!("{addr_b}")], None)
        .await
        .expect("client connect to B");
    let dmap_b = client_b
        .new_dmap("repl", DMapOptions::default())
        .await
        .expect("new_dmap on B");
    let got = dmap_b
        .get("k")
        .await
        .expect("backup must serve the replicated value");
    assert_eq!(got.value, b"v-replicated");

    drop(dmap_b);
    let _ = client_b.close().await;
    shutdown_b.trigger();
    let _ = server_b_task.await;
}

#[tokio::test]
async fn write_quorum_unmet_returns_quorum_error() {
    // ROADMAP §6 Phase 5 acceptance #3 (variant): if the backup is
    // unreachable, write_quorum=2 cannot be met and the primary surfaces
    // -QUORUM instead of silently succeeding.
    let _ = unique_seed();
    let secret = "phase5-secret-2";

    // Configure the primary to fan out to an address that nothing is
    // listening on. The forwarder will fail to connect → no ack →
    // write_quorum unmet.
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let settings = ReplicationSettings {
        replica_count: 2,
        write_quorum: 2,
        read_quorum: 1,
        read_repair: false,
        mode: ReplicationMode::Sync,
    };
    let router: Arc<dyn RoutingProvider> =
        Arc::new(ScriptedRouter::new(vec![dead], settings, secret));

    let cfg_p = cluster_config(secret);
    let (server_p, _embedded_p) = start_server(cfg_p, Some(router)).await;
    let addr_p = server_p.local_addr();
    let shutdown_p = server_p.shutdown_handle();
    let server_p_task = tokio::spawn(server_p.run());

    let client = MultiNodeRemoteClient::connect(vec![format!("{addr_p}")], None)
        .await
        .expect("client connect");
    let dmap = client
        .new_dmap("repl", DMapOptions::default())
        .await
        .unwrap();
    let err = dmap
        .put("k", b"v", PutOptions::default())
        .await
        .expect_err("put must fail when quorum cannot be met");
    let msg = format!("{err}");
    assert!(
        msg.contains("QUORUM") || msg.contains("quorum"),
        "expected quorum-related error, got {msg:?}",
    );

    drop(dmap);
    let _ = client.close().await;
    shutdown_p.trigger();
    let _ = server_p_task.await;
}

#[tokio::test]
async fn member_count_quorum_rejects_writes_when_unsatisfied() {
    // ROADMAP §6 Phase 5 acceptance #3: `member_count_quorum = 2` cluster
    // of 3 nodes correctly rejects writes when 2 nodes are unreachable.
    // We simulate the surviving node's view: `member_quorum_satisfied`
    // returns false because too few live members remain.
    let _ = unique_seed();
    let secret = "phase5-secret-3";

    let settings = ReplicationSettings {
        replica_count: 2,
        write_quorum: 1,
        read_quorum: 1,
        read_repair: false,
        mode: ReplicationMode::Sync,
    };
    let router_inner = ScriptedRouter::new(vec![], settings, secret);
    router_inner.quorum_ok.store(false, Ordering::Release);
    let router: Arc<dyn RoutingProvider> = Arc::new(router_inner);

    let cfg = cluster_config(secret);
    let (server, _embedded) = start_server(cfg, Some(router)).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let task = tokio::spawn(server.run());

    let client = MultiNodeRemoteClient::connect(vec![format!("{addr}")], None)
        .await
        .expect("connect");
    let dmap = client
        .new_dmap("repl", DMapOptions::default())
        .await
        .unwrap();
    let err = dmap
        .put("k", b"v", PutOptions::default())
        .await
        .expect_err("must reject when member quorum unsatisfied");
    assert!(
        format!("{err}").to_uppercase().contains("QUORUM"),
        "expected -QUORUM, got {err}",
    );

    drop(dmap);
    let _ = client.close().await;
    shutdown.trigger();
    let _ = task.await;
}

#[tokio::test]
async fn lww_concurrent_write_resolves_deterministically() {
    // ROADMAP §6 Phase 5 acceptance #2 (variant): with two concurrent
    // writes carrying explicit timestamps, the larger one wins on the
    // backup after replication and the loser is silently dropped — the
    // documented LWW semantics. The test asserts *which* write the backup
    // keeps (deterministic by TS) so the silent-loss path is observable.
    let _ = unique_seed();
    let secret = "phase5-secret-4";

    let cfg_b = cluster_config(secret);
    let leaf: Arc<dyn RoutingProvider> = Arc::new(LeafRouter);
    let (server_b, _embedded_b) = start_server(cfg_b, Some(leaf)).await;
    let addr_b = server_b.local_addr();
    let shutdown_b = server_b.shutdown_handle();
    let server_b_task = tokio::spawn(server_b.run());

    let settings = ReplicationSettings {
        replica_count: 2,
        write_quorum: 2,
        read_quorum: 1,
        read_repair: false,
        mode: ReplicationMode::Sync,
    };
    let router: Arc<dyn RoutingProvider> =
        Arc::new(ScriptedRouter::new(vec![addr_b], settings, secret));
    let cfg_p = cluster_config(secret);
    let (server_p, _embedded_p) = start_server(cfg_p, Some(router)).await;
    let addr_p = server_p.local_addr();
    let shutdown_p = server_p.shutdown_handle();
    let server_p_task = tokio::spawn(server_p.run());

    let client = MultiNodeRemoteClient::connect(vec![format!("{addr_p}")], None)
        .await
        .unwrap();
    let dmap = client
        .new_dmap("repl", DMapOptions::default())
        .await
        .unwrap();

    // Write the "loser" with the smaller TS first, then the "winner" with
    // the larger TS. Both go through the primary, both replicate to B.
    dmap.put(
        "k",
        b"loser",
        PutOptions {
            timestamp: Some(100),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    dmap.put(
        "k",
        b"winner",
        PutOptions {
            timestamp: Some(200),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Now write the loser AGAIN with TS=50 — older than what's already on
    // the backup. The backup's LWW path must drop it; B still serves
    // "winner". The acceptance bar from the docs is that the resolution
    // is deterministic, not that no write is lost.
    dmap.put(
        "k",
        b"stale",
        PutOptions {
            timestamp: Some(50),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    drop(dmap);
    let _ = client.close().await;
    shutdown_p.trigger();
    let _ = server_p_task.await;

    // Ask B directly. It must still hold "winner" — the highest TS.
    let client_b = MultiNodeRemoteClient::connect(vec![format!("{addr_b}")], None)
        .await
        .unwrap();
    let dmap_b = client_b
        .new_dmap("repl", DMapOptions::default())
        .await
        .unwrap();
    let got = dmap_b.get("k").await.unwrap();
    assert_eq!(
        got.value, b"winner",
        "LWW must resolve to the highest-TS write deterministically",
    );

    drop(dmap_b);
    let _ = client_b.close().await;
    shutdown_b.trigger();
    let _ = server_b_task.await;
}
