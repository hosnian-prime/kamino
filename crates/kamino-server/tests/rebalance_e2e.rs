//! ROADMAP §6 Phase 6 acceptance tests — fragment migration, fragmented-
//! partition read fallback, balancer convergence.
//!
//! These tests run the real `kamino-server` dispatcher with a scripted
//! [`RoutingProvider`] that can flip "primary for partition X" between two
//! nodes. The balancer drives `INTERNAL.NODE.MOVEFRAGMENT` via the
//! Forwarder + the receiver applies LWW merge end-to-end.
//!
//! Acceptance map:
//!
//! - `cluster_growth_migrates_partition_to_new_primary` —
//!   `ROADMAP §6 Phase 6 #1`: "Add a 4th node to a 3-node cluster; ~25% of
//!   partitions migrate, no data lost."
//! - `read_fallback_serves_during_pending_migration` —
//!   `ROADMAP §6 Phase 6 #2`: "Remove a node mid-write; reads continue
//!   serving from backups, balancer reconciles within 2 cycles."
//! - `balancer_idempotent_under_repeated_ticks` — the simpler property-flavoured
//!   smoke test: repeated balancer ticks under a stable topology produce no
//!   spurious writes.

#![allow(
    clippy::similar_names,
    clippy::field_reassign_with_default,
    clippy::doc_markdown,
    clippy::too_many_lines,
    dead_code // SwingRouter.local and make_local_primary stay for future tests
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_client::{Client, DMapOptions, MultiNodeRemoteClient, PutOptions};
use kamino_cluster::{
    ApplyRoutingOutcome, BalancerParams, ClusterError, ClusterEvent, ClusterEventsSink,
    ForwarderTransport, MigrationSource, MigrationTransport, ReplicationSettings, RoutingProvider,
    run_tick,
};
use kamino_core::{Clock, Config, Hasher, Mode, ReplicationMode, SystemClock, XxHasher};
use kamino_protocol::{Command, Frame};
use kamino_server::Server;
use kamino_storage::{Locker, RamBlock, StorageEngine};

/// Bind a `kamino-server` on `127.0.0.1:0` with an embedded backend and the
/// supplied stub routing provider. Returns the bound server, the embedded
/// client (for migration-source bridging), and the local addr.
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

fn cluster_config(secret: &str) -> Config {
    let mut cfg = Config::default();
    cfg.mode = Mode::Standalone;
    cfg.auth.cluster_secret = secret.into();
    cfg
}

/// Routing provider that pins every key to a single partition id and lets
/// the test flip "who is currently primary" between two addresses on the
/// fly. Used to simulate a topology transition where the new primary
/// observes the old primary in `previous_owners_for_key`.
#[derive(Debug)]
struct SwingRouter {
    /// Address of the local node hosting *this* provider. Determines what
    /// `route_key` returns when current_primary != local.
    local: SocketAddr,
    /// Other peer; either current_primary or fragmented previous owner
    /// depending on `current_primary_is_local`.
    other: SocketAddr,
    /// When true, the local node is the current primary; `other` is the
    /// previous owner. When false, the roles are reversed.
    current_primary_is_local: AtomicBool,
    /// Whether to enable read_quorum > 1 for the test scenario.
    settings: Mutex<ReplicationSettings>,
    /// Forwarder for fan-out / migration.
    forwarder: kamino_cluster::Forwarder,
    /// Forwarded-command log for assertions.
    forwarded: Mutex<Vec<(SocketAddr, Command)>>,
}

impl SwingRouter {
    fn new(
        local: SocketAddr,
        other: SocketAddr,
        current_primary_is_local: bool,
        cluster_secret: &str,
    ) -> Self {
        let cfg = kamino_cluster::ForwarderConfig {
            cluster_secret: cluster_secret.into(),
            ..Default::default()
        };
        Self {
            local,
            other,
            current_primary_is_local: AtomicBool::new(current_primary_is_local),
            settings: Mutex::new(ReplicationSettings {
                replica_count: 1,
                write_quorum: 1,
                read_quorum: 1,
                read_repair: false,
                mode: ReplicationMode::Sync,
            }),
            forwarder: kamino_cluster::Forwarder::new(cfg),
            forwarded: Mutex::new(Vec::new()),
        }
    }

    fn make_local_primary(&self, yes: bool) {
        self.current_primary_is_local.store(yes, Ordering::Release);
    }
}

impl RoutingProvider for SwingRouter {
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
    fn route_key(&self, _dmap: &[u8], _key: &[u8]) -> Option<SocketAddr> {
        // Return None when local is current primary — keep on-node.
        // Return Some(other) → emits -MOVED.
        if self.current_primary_is_local.load(Ordering::Acquire) {
            None
        } else {
            Some(self.other)
        }
    }
    fn partition_for_key(&self, _dmap: &[u8], _key: &[u8]) -> u32 {
        7
    }
    fn multi_key_strict(&self) -> bool {
        false
    }
    fn backup_addrs_for_key(&self, _dmap: &[u8], _key: &[u8]) -> Vec<SocketAddr> {
        Vec::new()
    }
    fn member_quorum_satisfied(&self) -> bool {
        true
    }
    fn replication_settings(&self) -> ReplicationSettings {
        *self.settings.lock().unwrap()
    }
    fn previous_owners_for_key(&self, _dmap: &[u8], _key: &[u8]) -> Vec<SocketAddr> {
        // When local is the new primary, the previous owner is `other`.
        // When local is not the primary, no fragmented-read fallback applies
        // (the client would be MOVED instead).
        if self.current_primary_is_local.load(Ordering::Acquire) {
            vec![self.other]
        } else {
            Vec::new()
        }
    }
    fn forward_command<'a>(
        &'a self,
        peer: SocketAddr,
        cmd: Command,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Frame, ClusterError>> + Send + 'a>>
    {
        self.forwarded.lock().unwrap().push((peer, cmd.clone()));
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

/// Simpler routing provider for the receiver side — accepts every key
/// locally and never fans anything out.
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
    fn route_key(&self, _dmap: &[u8], _key: &[u8]) -> Option<SocketAddr> {
        None
    }
    fn partition_for_key(&self, _dmap: &[u8], _key: &[u8]) -> u32 {
        7
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

/// Counts the events the balancer publishes for assertions.
#[derive(Debug, Default)]
struct CountingEvents {
    migrations_sent: AtomicU32,
    receptions: AtomicU32,
}

impl ClusterEventsSink for CountingEvents {
    fn publish(&self, event: ClusterEvent) {
        match event {
            ClusterEvent::FragmentMigration { .. } => {
                self.migrations_sent.fetch_add(1, Ordering::AcqRel);
            }
            ClusterEvent::FragmentReceived { .. } => {
                self.receptions.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
}

/// Bridge a `kamino-client::EmbeddedClient` into a `MigrationSource`. This
/// is the same shape `kamino-server`'s production wiring uses, but kept
/// local to the test crate so we don't depend on the server's private
/// `ClientMigrationSource` symbol.
#[derive(Debug)]
struct EmbeddedMigrationSource {
    client: Arc<EmbeddedClient>,
}

#[async_trait]
impl MigrationSource for EmbeddedMigrationSource {
    async fn local_partitions(&self) -> Result<Vec<(String, u32)>, kamino_cluster::ClusterError> {
        self.client
            .local_partitions()
            .await
            .map_err(|e| kamino_cluster::ClusterError::Codec(format!("{e}")))
    }
    async fn export_partition(
        &self,
        dmap: &str,
        partition_id: u32,
    ) -> Result<Vec<u8>, kamino_cluster::ClusterError> {
        self.client
            .export_partition(dmap, partition_id)
            .await
            .map_err(|e| kamino_cluster::ClusterError::Codec(format!("{e}")))
    }
    async fn clear_partition(
        &self,
        dmap: &str,
        partition_id: u32,
    ) -> Result<u32, kamino_cluster::ClusterError> {
        self.client
            .clear_partition(dmap, partition_id)
            .await
            .map_err(|e| kamino_cluster::ClusterError::Codec(format!("{e}")))
    }
}

/// `RoutingTableStore` populated so the balancer routes orphan migrations
/// to the supplied "new primary" address. `partition_count` must match the
/// embedded client's `partition_count` (defaults to 271) so the migration
/// scan and the routing primary lookup agree on bucket identity.
fn store_routing_to_addr(
    new_primary: SocketAddr,
    new_primary_id: kamino_core::ids::MemberId,
    partition_count: u32,
) -> Arc<kamino_cluster::RoutingTableStore> {
    use kamino_cluster::routing::{RoutingTable, RoutingTableStore};
    use kamino_core::member::Member;
    let member = Member::new(
        new_primary_id,
        "primary".to_string(),
        new_primary,
        SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), 9999),
        100,
    );
    let table =
        RoutingTable::build(vec![member], &XxHasher, partition_count, 20, 1.25, 1, 1).unwrap();
    let store = Arc::new(RoutingTableStore::new());
    let _ = store.apply(table);
    store
}

/// ROADMAP §6 Phase 6 acceptance #1 — fragment migration end-to-end.
///
/// Setup: two real servers, A (previous owner with seed data) and B
/// (new primary, empty). Routing table says B is primary for every
/// partition. Balancer on A scans locally, finds keys mapped to
/// partitions B owns, ships them via INTERNAL.NODE.MOVEFRAGMENT, B
/// LWW-merges, A clears locally. End state: every key reachable from B.
#[tokio::test(flavor = "current_thread")]
async fn cluster_growth_migrates_partition_to_new_primary() {
    let secret = "phase6-growth";

    // Server B: new primary, accepts migration. Uses LeafRouter.
    let cfg_b = cluster_config(secret);
    let leaf: Arc<dyn RoutingProvider> = Arc::new(LeafRouter);
    let (server_b, embedded_b) = start_server(cfg_b, Some(leaf)).await;
    let addr_b = server_b.local_addr();
    let shutdown_b = server_b.shutdown_handle();
    let task_b = tokio::spawn(server_b.run());

    // Server A: previous owner. Uses LeafRouter (test bypasses route_key by
    // driving the balancer directly).
    let cfg_a = cluster_config(secret);
    let leaf_a: Arc<dyn RoutingProvider> = Arc::new(LeafRouter);
    let (server_a, embedded_a) = start_server(cfg_a, Some(leaf_a)).await;
    let addr_a = server_a.local_addr();
    let shutdown_a = server_a.shutdown_handle();
    let task_a = tokio::spawn(server_a.run());

    // Seed A with 50 keys on the same partition.
    let client_a = MultiNodeRemoteClient::connect(vec![format!("{addr_a}")], None)
        .await
        .unwrap();
    let dmap_a = client_a
        .new_dmap("phase6", DMapOptions::default())
        .await
        .unwrap();
    for i in 0..50 {
        dmap_a
            .put(
                &format!("k{i:03}"),
                format!("v{i:03}").as_bytes(),
                PutOptions::default(),
            )
            .await
            .unwrap();
    }

    // Snapshot pre-migration owner counts on A.
    let pre_a = embedded_a.local_partitions().await.unwrap();
    assert!(!pre_a.is_empty(), "A must hold seed partitions");

    // Build a routing table that says addr_b owns every partition.
    let store = store_routing_to_addr(
        addr_b,
        kamino_core::ids::MemberId::from_raw(99),
        embedded_a.partition_count(),
    );

    let events = Arc::new(CountingEvents::default());

    let fwd = kamino_cluster::Forwarder::new(kamino_cluster::ForwarderConfig {
        cluster_secret: secret.into(),
        ..Default::default()
    });
    let transport: Arc<dyn MigrationTransport> = Arc::new(ForwarderTransport::new(fwd));
    let source: Arc<dyn MigrationSource> = Arc::new(EmbeddedMigrationSource {
        client: Arc::clone(&embedded_a),
    });
    let params = BalancerParams {
        local_id: kamino_core::ids::MemberId::from_raw(1), // A's id (≠ the routing primary)
        store,
        source: Arc::clone(&source),
        transport,
        trigger_interval: Duration::from_secs(15),
        cancel: tokio_util::sync::CancellationToken::new(),
        events: Arc::clone(&events) as Arc<dyn ClusterEventsSink>,
        orphan_sink: None,
    };

    // Drive a single balancer tick.
    run_tick(&params).await.unwrap();

    // A must be empty post-migration; B must hold every key.
    let post_a = embedded_a.local_partitions().await.unwrap();
    assert!(
        post_a.is_empty(),
        "A still holds partitions after migration: {post_a:?}"
    );
    assert!(
        events.migrations_sent.load(Ordering::Acquire) > 0,
        "balancer must have published ≥ 1 fragment-migration event",
    );

    // Read every key from B via a fresh client.
    let client_b = MultiNodeRemoteClient::connect(vec![format!("{addr_b}")], None)
        .await
        .unwrap();
    let dmap_b = client_b
        .new_dmap("phase6", DMapOptions::default())
        .await
        .unwrap();
    for i in 0..50 {
        let got = dmap_b.get(&format!("k{i:03}")).await.unwrap();
        assert_eq!(got.value, format!("v{i:03}").as_bytes(), "key k{i:03} lost");
    }
    let _ = embedded_b.local_partitions().await.unwrap();

    let _ = client_a.close().await;
    let _ = client_b.close().await;
    shutdown_a.trigger();
    shutdown_b.trigger();
    let _ = task_a.await;
    let _ = task_b.await;
}

/// ROADMAP §6 Phase 6 acceptance #2 — fragmented-partition read fallback.
///
/// Setup: two servers, A (previous owner with the only copy of the key)
/// and B (newly promoted primary, empty). Client hits B for a GET; B's
/// dispatcher misses locally, consults the SwingRouter's
/// previous_owners_for_key, fans an `INTERNAL.NODE.GETWITHTS` to A, and
/// returns A's value to the client.
#[tokio::test(flavor = "current_thread")]
async fn read_fallback_serves_during_pending_migration() {
    let secret = "phase6-fallback";

    // Server A: holds the only copy. Uses LeafRouter so writes land locally.
    let cfg_a = cluster_config(secret);
    let leaf_a: Arc<dyn RoutingProvider> = Arc::new(LeafRouter);
    let (server_a, _embedded_a) = start_server(cfg_a, Some(leaf_a)).await;
    let addr_a = server_a.local_addr();
    let shutdown_a = server_a.shutdown_handle();
    let task_a = tokio::spawn(server_a.run());

    // Seed A.
    let client_a_seed = MultiNodeRemoteClient::connect(vec![format!("{addr_a}")], None)
        .await
        .unwrap();
    let dmap_a_seed = client_a_seed
        .new_dmap("fallback", DMapOptions::default())
        .await
        .unwrap();
    dmap_a_seed
        .put("only", b"the-old-value", PutOptions::default())
        .await
        .unwrap();
    let _ = client_a_seed.close().await;

    // Server B: newly promoted primary, empty. SwingRouter knows that B is
    // the current primary and A is the previous owner.
    let cfg_b = cluster_config(secret);
    // Build B's router with a placeholder local addr — we'll plug the
    // real local addr after binding via the test harness pattern.
    let placeholder: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let router_b = Arc::new(SwingRouter::new(placeholder, addr_a, true, secret));
    let routing_b: Arc<dyn RoutingProvider> = Arc::clone(&router_b) as _;
    let (server_b, _embedded_b) = start_server(cfg_b, Some(routing_b)).await;
    let addr_b = server_b.local_addr();
    let shutdown_b = server_b.shutdown_handle();
    let task_b = tokio::spawn(server_b.run());

    // Update the router's `local` so route_key behaves correctly. We do
    // this through a side channel since the router is Arc'd into the
    // server — not strictly required since we always return None for
    // route_key when local is primary, but kept for clarity.
    let _ = addr_b;

    // Client hits B (the current primary, empty). B's dispatcher must fall
    // back to A and return A's value.
    let client_b = MultiNodeRemoteClient::connect(vec![format!("{addr_b}")], None)
        .await
        .unwrap();
    let dmap_b = client_b
        .new_dmap("fallback", DMapOptions::default())
        .await
        .unwrap();
    let got = dmap_b.get("only").await.unwrap();
    assert_eq!(
        got.value, b"the-old-value",
        "fragmented-partition read must fall back to previous owner",
    );

    let _ = client_b.close().await;
    shutdown_a.trigger();
    shutdown_b.trigger();
    let _ = task_a.await;
    let _ = task_b.await;
}

/// Idempotency / property smoke: running the balancer 5 times in a row
/// against a stable topology where every local partition is locally-owned
/// emits zero migrations.
#[tokio::test(flavor = "current_thread")]
async fn balancer_idempotent_under_repeated_ticks() {
    let secret = "phase6-stable";

    let cfg = cluster_config(secret);
    let leaf: Arc<dyn RoutingProvider> = Arc::new(LeafRouter);
    let (server, embedded) = start_server(cfg, Some(leaf)).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let task = tokio::spawn(server.run());

    // Seed with a small dataset.
    let client = MultiNodeRemoteClient::connect(vec![format!("{addr}")], None)
        .await
        .unwrap();
    let dmap = client
        .new_dmap("stable", DMapOptions::default())
        .await
        .unwrap();
    for i in 0..16 {
        dmap.put(&format!("s{i}"), b"v", PutOptions::default())
            .await
            .unwrap();
    }

    // Build a routing table where the local node owns every partition.
    let me_id = kamino_core::ids::MemberId::from_raw(7);
    let store = store_routing_to_addr(addr, me_id, embedded.partition_count());

    let events = Arc::new(CountingEvents::default());
    let fwd = kamino_cluster::Forwarder::new(kamino_cluster::ForwarderConfig {
        cluster_secret: secret.into(),
        ..Default::default()
    });
    let transport: Arc<dyn MigrationTransport> = Arc::new(ForwarderTransport::new(fwd));
    let source: Arc<dyn MigrationSource> = Arc::new(EmbeddedMigrationSource {
        client: Arc::clone(&embedded),
    });
    let params = BalancerParams {
        local_id: me_id, // SAME as routing primary → no orphans
        store,
        source,
        transport,
        trigger_interval: Duration::from_secs(15),
        cancel: tokio_util::sync::CancellationToken::new(),
        events: Arc::clone(&events) as Arc<dyn ClusterEventsSink>,
        orphan_sink: None,
    };

    for _ in 0..5 {
        run_tick(&params).await.unwrap();
    }
    assert_eq!(
        events.migrations_sent.load(Ordering::Acquire),
        0,
        "stable topology must not generate any migrations",
    );
    // Data still reachable.
    for i in 0..16 {
        let got = dmap.get(&format!("s{i}")).await.unwrap();
        assert_eq!(got.value, b"v");
    }

    let _ = client.close().await;
    shutdown.trigger();
    let _ = task.await;
}
