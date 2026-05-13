//! Cluster runtime: ties membership, SWIM, gossip, transport, and discovery
//! into a single owned object.
//!
//! Lifecycle:
//!
//! ```text
//! Cluster::bootstrap(config, deps)        // bind UDP, register self
//!   └─ join()                             // try peers until success or bootstrap_timeout
//!       └─ spawn run_receive_loop          // drains the UDP socket
//!       └─ spawn run_probe_loop            // sends pings at probe_interval
//!       └─ spawn reaper_loop               // promotes Suspect → Dead
//!
//! cluster.snapshot_members()              // for CLUSTER.MEMBERS
//! cluster.shutdown().await                // graceful leave + cancel tasks
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use kamino_core::clock::Clock;
use kamino_core::config::Config;
use kamino_core::hasher::{Hasher, XxHasher};
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::balancer::{
    BalancerParams, ClusterEvent, ClusterEventsSink, ForwarderTransport, MigrationSource,
    MigrationTransport, OrphanSink, TracingEventsSink, run_balancer_loop,
};
use crate::discovery::DiscoveryPlugin;
use crate::error::{ClusterError, ClusterResult};
use crate::forwarder::{Forwarder, ForwarderConfig};
use crate::gossip::GossipQueue;
use crate::join::{self, JoinParams};
use crate::membership::MembershipView;
use crate::pubsub::{DeliveredMessage, PubSubProvider, PubSubService, SubAck};
use crate::routing::coordinator::{
    CoordinatorParams, LocalOnlyPusher, RoutingPusher, SignatureClock, run_coordinator_loop,
};
use crate::routing::store::{ApplyRoutingOutcome, RoutingTableStore};
use crate::routing::table::RoutingTable;
use crate::swim::{SwimDriver, run_probe_loop, run_receive_loop};
use crate::transport::Transport;

/// External dependencies the cluster runtime needs.
#[allow(missing_debug_implementations)]
pub struct ClusterDeps {
    pub config: Config,
    pub transport: Arc<dyn Transport>,
    pub discovery: Arc<dyn DiscoveryPlugin>,
    pub clock: Arc<dyn Clock>,
    /// Local node identity. Caller is responsible for advertising the
    /// correct RESP `addr` (Phase 2 server) — the cluster runtime only
    /// owns the SWIM discovery socket.
    pub local: Member,
    /// Hash function for the consistent-hash ring. `None` selects the
    /// default [`XxHasher`].
    pub hasher: Option<Arc<dyn Hasher>>,
    /// Inter-node routing-table pusher. `None` defaults to a [`Forwarder`]
    /// configured from `config.network.internode_*`; tests pass a custom
    /// implementation (e.g. the in-process `DirectPusher`).
    pub routing_pusher: Option<Arc<dyn RoutingPusher>>,
}

/// Running cluster.
#[allow(missing_debug_implementations, clippy::struct_field_names)]
pub struct Cluster {
    view: MembershipView,
    queue: Arc<GossipQueue>,
    transport: Arc<dyn Transport>,
    discovery: Arc<dyn DiscoveryPlugin>,
    clock: Arc<dyn Clock>,
    cancel: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    local_id: MemberId,
    local_member: Member,
    cluster_secret: String,
    swim_config: kamino_core::config::SwimConfig,
    discovery_config: kamino_core::config::DiscoveryConfig,
    leave_timeout: std::time::Duration,
    routing_store: Arc<RoutingTableStore>,
    routing_signature: Arc<SignatureClock>,
    hasher: Arc<dyn Hasher>,
    routing_pusher: Arc<dyn RoutingPusher>,
    forwarder: Option<Forwarder>,
    core_config: kamino_core::config::CoreConfig,
    routing_config: kamino_core::config::RoutingConfig,
    network_config: kamino_core::config::NetworkConfig,
    member_count_quorum: u32,
    balancer_trigger_interval: std::time::Duration,
    /// Phase 6 `LeftOverDataReport` cache, refreshed by the balancer loop
    /// on every tick. `local_orphans()` returns a clone of this so the
    /// handler stays synchronous.
    orphan_cache: Mutex<Vec<(u32, String)>>,
    /// Phase 7 pub/sub registry. Wrapping the service in an `Arc` here
    /// lets `Cluster` implement `PubSubProvider` directly while the
    /// server-side connection handlers share the same `Arc` for direct
    /// access to `register_conn` / `cleanup_conn` outside the dispatcher.
    pubsub: Arc<PubSubService>,
    /// Whether `cluster.events` publication is enabled. Mirrors
    /// `[events] enable_cluster_events_channel` per
    /// `docs/09-configuration.md`.
    events_channel_enabled: bool,
}

impl Cluster {
    /// Build a cluster but do not start the background tasks. Useful for
    /// tests that drive the SWIM driver manually.
    pub fn assemble(deps: ClusterDeps) -> Self {
        let view = MembershipView::bootstrap(deps.local.clone());
        let queue = Arc::new(GossipQueue::with_capacity(256));
        // Seed the queue with our own Alive announcement so the first probe
        // gossips our presence.
        queue.push(
            crate::message::alive_for(&deps.local, view.local_incarnation()),
            1,
        );
        let hasher: Arc<dyn Hasher> = deps.hasher.unwrap_or_else(|| Arc::new(XxHasher));
        let (routing_pusher, forwarder) = if let Some(p) = deps.routing_pusher {
            (p, None)
        } else {
            let fwd = Forwarder::new(forwarder_config_from(&deps.config));
            let pusher: Arc<dyn RoutingPusher> = Arc::new(fwd.clone());
            (pusher, Some(fwd))
        };
        // `LocalOnlyPusher` stays available as the cluster-runtime-absent
        // fallback (e.g. unit tests that instantiate a `Cluster` without
        // any peer connectivity) — keep the import alive at the type level.
        let _ = LocalOnlyPusher;
        Self {
            view,
            queue,
            transport: deps.transport,
            discovery: deps.discovery,
            clock: deps.clock,
            cancel: CancellationToken::new(),
            tasks: Mutex::new(Vec::new()),
            local_id: deps.local.id,
            local_member: deps.local.clone(),
            cluster_secret: deps.config.auth.cluster_secret.clone(),
            swim_config: deps.config.swim.clone(),
            discovery_config: deps.config.discovery.clone(),
            leave_timeout: deps.config.discovery.leave_timeout,
            routing_store: Arc::new(RoutingTableStore::new()),
            routing_signature: SignatureClock::new(),
            hasher,
            routing_pusher,
            forwarder,
            core_config: deps.config.core.clone(),
            routing_config: deps.config.routing.clone(),
            network_config: deps.config.network.clone(),
            member_count_quorum: deps.config.core.member_count_quorum,
            balancer_trigger_interval: deps.config.balancer.trigger_interval,
            orphan_cache: Mutex::new(Vec::new()),
            pubsub: Arc::new(PubSubService::new()),
            events_channel_enabled: deps.config.events.enable_cluster_events_channel,
        }
    }

    /// Update the cached orphan list. Called by the balancer loop every
    /// tick — the handler reads from this cache to fill the
    /// `LeftOverDataReport` field of `INTERNAL.NODE.UPDATEROUTING` replies.
    pub fn record_orphans(&self, orphans: Vec<(u32, String)>) {
        *self.orphan_cache.lock() = orphans;
    }

    /// Underlying inter-node forwarder if one was created by
    /// `Cluster::assemble`. Returns `None` when the caller supplied a
    /// custom `RoutingPusher` (typical in tests).
    #[must_use]
    pub fn forwarder(&self) -> Option<Forwarder> {
        self.forwarder.clone()
    }

    /// Shared pub/sub registry. The server-side connection handlers use
    /// this directly to register their per-conn `mpsc::Sender` and
    /// clean up on disconnect.
    #[must_use]
    pub fn pubsub(&self) -> Arc<PubSubService> {
        Arc::clone(&self.pubsub)
    }

    /// Whether `cluster.events` publication is enabled.
    #[must_use]
    pub const fn events_channel_enabled(&self) -> bool {
        self.events_channel_enabled
    }

    /// Live peer addresses (excluding the local node) — used by the pub/sub
    /// cluster fan-out and the [`ClusterEventsSink`] adapter.
    fn live_peer_addrs(&self) -> Vec<SocketAddr> {
        self.view
            .snapshot()
            .into_iter()
            .filter(|m| m.id != self.local_id)
            .map(|m| m.addr)
            .collect()
    }

    /// `[network]` knobs for downstream wiring (e.g. server-side MOVED
    /// thresholds and multi-key strictness).
    #[must_use]
    pub const fn network_config(&self) -> &kamino_core::config::NetworkConfig {
        &self.network_config
    }

    /// Assemble the cluster, run `join()` (best-effort, capped by
    /// `bootstrap_timeout`), then spawn the SWIM receive and probe loops.
    ///
    /// Returned as an `Arc<Self>` so callers can register the same instance
    /// as a `MemberProvider` with the RESP server.
    ///
    /// `join` errors are downgraded to a `warn!` so that nodes brought up
    /// before their peers don't fail to start. Single-node deployments (no
    /// peers configured **and** discovery returns nothing) succeed silently.
    pub async fn bootstrap(deps: ClusterDeps) -> ClusterResult<Arc<Self>> {
        // Run init + register on the discovery plugin once (idempotent for
        // the static and DNS plugins; matters for K8s/Consul in later
        // phases). `register` advertises this node in the external
        // discovery registry; `shutdown` calls `deregister` on the way out.
        deps.discovery.init().await?;
        deps.discovery.register().await?;

        let swim_config = deps.config.swim.clone();
        let discovery_config = deps.config.discovery.clone();
        let bootstrap_timeout = discovery_config.bootstrap_timeout;
        let clock = Arc::clone(&deps.clock);

        let cluster = Arc::new(Self::assemble(deps));

        // Best-effort join. Respect bootstrap_timeout as a hard ceiling so a
        // misconfigured peer list cannot block start-up forever.
        let join_params = JoinParams {
            discovery: discovery_config,
            probe_timeout: swim_config.probe_timeout,
        };
        let join_future = join::join(&cluster, join_params);
        match tokio::time::timeout(bootstrap_timeout, join_future).await {
            Ok(Ok(n)) => {
                info!(peers = n, "cluster join handshake complete");
            }
            Ok(Err(e)) => {
                warn!(error = %e, "cluster join failed; continuing in single-node mode");
            }
            Err(_) => {
                warn!(
                    timeout = ?bootstrap_timeout,
                    "cluster join timed out; continuing in single-node mode",
                );
            }
        }

        // Spawn SWIM loops. Both loops co-operate via the shared cancel
        // token; shutdown() cancels and awaits them in order.
        let driver = Arc::new(cluster.driver(clock, swim_config));
        let recv_driver = Arc::clone(&driver);
        let probe_driver = Arc::clone(&driver);
        let recv_handle = tokio::spawn(async move {
            run_receive_loop(recv_driver).await;
        });
        let probe_handle = tokio::spawn(async move {
            run_probe_loop(probe_driver).await;
        });
        // Spawn the routing-table coordinator loop. Every node runs it; non-
        // coordinators short-circuit each tick.
        let coordinator_params = CoordinatorParams {
            local_id: cluster.local_id,
            view: cluster.view.clone(),
            hasher: Arc::clone(&cluster.hasher),
            store: Arc::clone(&cluster.routing_store),
            pusher: Arc::clone(&cluster.routing_pusher),
            push_interval: cluster.routing_config.push_interval,
            partition_count: cluster.core_config.partition_count,
            virtual_nodes_per_member: cluster.core_config.virtual_nodes_per_member,
            load_factor: cluster.core_config.load_factor,
            replica_count: cluster.core_config.replica_count,
            cancel: cluster.cancel.clone(),
        };
        let sig = Arc::clone(&cluster.routing_signature);
        let coord_handle = tokio::spawn(async move {
            run_coordinator_loop(coordinator_params, sig).await;
        });
        {
            let mut tasks = cluster.tasks.lock();
            tasks.push(recv_handle);
            tasks.push(probe_handle);
            tasks.push(coord_handle);
        }
        debug!("cluster SWIM + routing loops spawned");
        Ok(cluster)
    }

    /// Local membership view (cheap clone).
    #[must_use]
    pub fn view(&self) -> MembershipView {
        self.view.clone()
    }

    /// Build a [`SwimDriver`] for the assembled cluster. Caller decides
    /// whether to run the probe + receive loops or test them in isolation.
    pub fn driver(
        &self,
        clock: Arc<dyn Clock>,
        config: kamino_core::config::SwimConfig,
    ) -> SwimDriver {
        SwimDriver::new(
            config,
            self.view.clone(),
            Arc::clone(&self.queue),
            Arc::clone(&self.transport),
            clock,
            self.cluster_secret.clone(),
            self.cancel.clone(),
        )
    }

    /// `CLUSTER.MEMBERS` payload: snapshot of live members.
    #[must_use]
    pub fn snapshot_members(&self) -> Vec<Member> {
        self.view.snapshot()
    }

    /// Local node id.
    #[must_use]
    pub const fn local_id(&self) -> MemberId {
        self.local_id
    }

    /// Discovery plugin name (for logs).
    #[must_use]
    pub fn discovery_name(&self) -> &'static str {
        self.discovery.name()
    }

    /// Register a spawned background task so `shutdown` can await it.
    pub fn register_task(&self, task: JoinHandle<()>) {
        self.tasks.lock().push(task);
    }

    /// Cancellation token shared across all SWIM tasks.
    #[must_use]
    pub fn cancel(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Gossip queue (used by the join handshake).
    #[must_use]
    pub fn gossip_queue(&self) -> Arc<GossipQueue> {
        Arc::clone(&self.queue)
    }

    /// Underlying transport (used by the join handshake to send pings to
    /// bootstrap peers before any background loops exist).
    #[must_use]
    pub fn transport(&self) -> Arc<dyn Transport> {
        Arc::clone(&self.transport)
    }

    /// Spawn the Phase 6 balancer loop. The server runtime owns the
    /// `MigrationSource` (which knows how to scan local storage) and
    /// optionally a custom `events` sink — defaults to a tracing-only
    /// adapter.
    ///
    /// Returns silently if no forwarder is available (test deployments
    /// that wire a custom `RoutingPusher` and skip the real one): the
    /// balancer can't migrate without a transport. Tests that need to
    /// drive the balancer manually call [`crate::run_tick`] directly.
    pub fn spawn_balancer(
        self: &Arc<Self>,
        source: Arc<dyn MigrationSource>,
        events: Option<Arc<dyn ClusterEventsSink>>,
    ) {
        let Some(fwd) = self.forwarder.clone() else {
            debug!("no forwarder available; balancer loop will not run");
            return;
        };
        let transport: Arc<dyn MigrationTransport> = Arc::new(ForwarderTransport::new(fwd));
        let events: Arc<dyn ClusterEventsSink> =
            events.unwrap_or_else(|| Arc::new(TracingEventsSink));
        let orphan_sink: Arc<dyn OrphanSink> = Arc::clone(self) as Arc<dyn OrphanSink>;
        let params = BalancerParams {
            local_id: self.local_id,
            store: Arc::clone(&self.routing_store),
            source,
            transport,
            trigger_interval: self.balancer_trigger_interval,
            cancel: self.cancel.clone(),
            events,
            orphan_sink: Some(orphan_sink),
        };
        let handle = tokio::spawn(async move {
            run_balancer_loop(params).await;
        });
        self.tasks.lock().push(handle);
        debug!("balancer loop spawned");
    }

    /// Spawn the Phase 7 membership-event loop. Polls the local
    /// membership view at `interval` and publishes `node-join` /
    /// `node-left` events into `events` whenever a peer's `Alive` set
    /// changes. Delivery is best-effort (the same at-most-once contract
    /// the rest of pub/sub uses).
    ///
    /// Returns silently if the supplied sink wouldn't publish anything
    /// useful — e.g. caller passed [`TracingEventsSink`] without
    /// enabling `cluster.events`. Tests that need deterministic event
    /// observation construct the loop manually.
    pub fn spawn_membership_events(
        self: &Arc<Self>,
        events: Arc<dyn ClusterEventsSink>,
        interval: std::time::Duration,
    ) {
        type Snapshot =
            std::collections::BTreeMap<kamino_core::ids::MemberId, (String, std::net::SocketAddr)>;
        fn build_snapshot(view: &MembershipView, local_id: MemberId) -> Snapshot {
            view.snapshot()
                .into_iter()
                .filter(|m| m.id != local_id)
                .map(|m| (m.id, (m.name, m.addr)))
                .collect()
        }
        let view = self.view.clone();
        let local_id = self.local_id;
        let cancel = self.cancel.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip the immediate first tick
            let mut last: Snapshot = build_snapshot(&view, local_id);
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                let now: Snapshot = build_snapshot(&view, local_id);
                // Find new members (in `now` but not in `last`).
                for (id, (name, addr)) in &now {
                    if !last.contains_key(id) {
                        events.publish(ClusterEvent::NodeJoin {
                            member: name.clone(),
                            addr: *addr,
                        });
                    }
                }
                // Find departed members (in `last` but not in `now`).
                for (id, (name, addr)) in &last {
                    if !now.contains_key(id) {
                        events.publish(ClusterEvent::NodeLeft {
                            member: name.clone(),
                            addr: *addr,
                        });
                    }
                }
                last = now;
            }
        });
        self.tasks.lock().push(handle);
        debug!(?interval, "membership-event loop spawned");
    }

    /// Spawn the Phase 6 empty-fragment cleanup loop. Wakes up every
    /// `routing.check_empty_fragments_interval` and calls
    /// `Client::cleanup_empty_fragments` to reclaim bytes left behind by
    /// the balancer's `clear_partition` calls. Defaults to a no-op for
    /// engines without garbage (`RamBlock` reclaims tables whose
    /// `garbage_ratio` crosses the threshold).
    pub fn spawn_fragment_cleanup(self: &Arc<Self>, cleaner: Arc<dyn FragmentCleaner>) {
        let interval = self.routing_config.check_empty_fragments_interval;
        let cancel = self.cancel.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip the immediate first-tick; production deployments expect
            // a full interval before the first sweep.
            ticker.tick().await;
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                match cleaner.cleanup_empty_fragments().await {
                    Ok(reclaimed) if reclaimed > 0 => {
                        info!(reclaimed_bytes = reclaimed, "empty-fragment cleanup");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(error = %e, "empty-fragment cleanup failed; will retry");
                    }
                }
            }
        });
        self.tasks.lock().push(handle);
        debug!(?interval, "fragment cleanup loop spawned");
    }

    /// `[balancer]` trigger interval (for tests + diagnostics).
    #[must_use]
    pub const fn balancer_trigger_interval(&self) -> std::time::Duration {
        self.balancer_trigger_interval
    }

    /// Discovery plugin handle.
    #[must_use]
    pub fn discovery(&self) -> Arc<dyn DiscoveryPlugin> {
        Arc::clone(&self.discovery)
    }

    /// Local member descriptor (full `Member`, not just the id).
    #[must_use]
    pub fn local_member(&self) -> Member {
        self.local_member.clone()
    }

    /// Configured `cluster_secret` for outgoing envelopes.
    #[must_use]
    pub fn cluster_secret(&self) -> &str {
        &self.cluster_secret
    }

    /// SWIM tuning (for tests).
    #[must_use]
    pub const fn swim_config(&self) -> &kamino_core::config::SwimConfig {
        &self.swim_config
    }

    /// Discovery tuning (for tests).
    #[must_use]
    pub const fn discovery_config(&self) -> &kamino_core::config::DiscoveryConfig {
        &self.discovery_config
    }

    /// Clock shared with the SWIM driver.
    #[must_use]
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    /// Shared routing-table store. Cheap clone.
    #[must_use]
    pub fn routing_store(&self) -> Arc<RoutingTableStore> {
        Arc::clone(&self.routing_store)
    }

    /// Current routing signature (`0` if no table has been seen yet).
    #[must_use]
    pub fn routing_signature(&self) -> u64 {
        self.routing_store.signature()
    }

    /// `member_count_quorum` from `[core]`.
    #[must_use]
    pub const fn member_count_quorum(&self) -> u32 {
        self.member_count_quorum
    }

    /// Apply an `INTERNAL.NODE.UPDATEROUTING` payload. Returns the outcome
    /// (`Accepted` / `Stale` / `UnsupportedSchema`). The decoder is tolerant
    /// of unknown fields per `docs/15-compatibility.md`.
    pub fn apply_routing_update(&self, bytes: &[u8]) -> ClusterResult<ApplyRoutingOutcome> {
        let table = RoutingTable::from_msgpack(bytes)?;
        // If a peer's table has a higher signature than ours, advance the
        // local signature clock so any future build we do as coordinator
        // doesn't collide with it.
        let incoming_sig = table.signature;
        let outcome = self.routing_store.apply(table);
        if outcome == ApplyRoutingOutcome::Accepted {
            // Best-effort: bump the local clock so a freshly-promoted
            // coordinator continues monotonically.
            self.bump_signature_to_at_least(incoming_sig);
        }
        Ok(outcome)
    }

    fn bump_signature_to_at_least(&self, target: u64) {
        while self.routing_signature.current() < target {
            self.routing_signature.next();
        }
    }

    /// True iff every readiness gate (`docs/06-network-protocol.md`
    /// `CLUSTER.READY`) is satisfied:
    /// 1. SWIM membership is non-empty (we joined).
    /// 2. A routing table with `signature > 0` is stored.
    /// 3. Live member count satisfies `member_count_quorum`.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        if !self.routing_store.is_populated() {
            return false;
        }
        if self.routing_store.signature() == 0 {
            return false;
        }
        let live = self.view.live_count();
        let quorum = self.member_count_quorum as usize;
        live >= quorum
    }

    /// Gracefully leave the cluster: broadcast a Leave gossip event,
    /// wait `leave_timeout`, then cancel and join all tasks.
    pub async fn shutdown(self: Arc<Self>) -> ClusterResult<()> {
        let incarnation = self.view.local_incarnation();
        let leave_event = crate::message::GossipEvent::Leave {
            id: self.local_id,
            incarnation,
        };
        self.queue.push(leave_event.clone(), self.view.live_count());

        // Fan the Leave out to every known peer directly so it lands even if
        // the periodic probe doesn't fire before shutdown.
        let snap = self.view.snapshot();
        for peer in snap {
            if peer.id == self.local_id {
                continue;
            }
            let env = crate::message::Envelope {
                cluster_secret: self.cluster_secret.clone(),
                from: self.local_id,
                msg: crate::message::SwimMessage::Ping {
                    seq: 0,
                    target: peer.id,
                },
                gossip: vec![leave_event.clone()],
            };
            let _ = self.transport.send_to(&env, peer.discovery_addr).await;
        }

        // Give the gossip a brief window to propagate before we go silent.
        let _ = tokio::time::timeout(self.leave_timeout, async {
            tokio::time::sleep(self.leave_timeout / 2).await;
        })
        .await;

        self.cancel.cancel();
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *self.tasks.lock());
        for task in handles {
            let _ = task.await;
        }
        // Deregister from the external discovery system before shutting
        // down plugin-owned resources (Consul / K8s / cloud APIs).
        let _ = self.discovery.deregister().await;
        let _ = self.discovery.shutdown().await;
        Ok(())
    }
}

/// Trait the RESP server calls when handling `CLUSTER.MEMBERS`.
///
/// In Phase 3 the only implementor is [`Cluster`] itself; in standalone-only
/// deployments (no cluster runtime) the server falls back to a stub that
/// returns just the local node, so existing single-node tooling keeps
/// working.
pub trait MemberProvider: Send + Sync {
    fn members(&self) -> Vec<MemberSummary>;
}

impl MemberProvider for Cluster {
    fn members(&self) -> Vec<MemberSummary> {
        self.view
            .snapshot()
            .iter()
            .map(MemberSummary::from)
            .collect()
    }
}

impl OrphanSink for Cluster {
    fn record_orphans(&self, orphans: Vec<(u32, String)>) {
        Self::record_orphans(self, orphans);
    }
}

/// Storage seam for the Phase 6 empty-fragment cleanup loop. The server
/// runtime implements this with a thin `Arc<dyn Client>` wrapper so the
/// cluster crate stays decoupled from the client trait.
#[async_trait::async_trait]
pub trait FragmentCleaner: Send + Sync {
    /// Returns total bytes reclaimed across all local fragments.
    async fn cleanup_empty_fragments(&self) -> ClusterResult<usize>;
}

/// Replication tuning surfaced to the RESP server. Mirrors the relevant
/// `[core]` knobs from `kamino-core::config::CoreConfig` so the server
/// can enforce write/read quorum at the op boundary without re-reading
/// the full config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationSettings {
    pub replica_count: u32,
    pub write_quorum: u32,
    pub read_quorum: u32,
    pub read_repair: bool,
    pub mode: kamino_core::ReplicationMode,
}

/// Trait the RESP server calls for `CLUSTER.ROUTINGTABLE`, `CLUSTER.READY`,
/// and `INTERNAL.NODE.UPDATEROUTING`.
///
/// Standalone-only deployments (no cluster runtime) get a `None` provider on
/// the server and the handlers respond with sensible single-node defaults.
pub trait RoutingProvider: Send + Sync {
    /// Encoded routing table bytes, or `None` if no table has been built yet.
    fn routing_table_bytes(&self) -> Option<Vec<u8>>;

    /// Apply an `INTERNAL.NODE.UPDATEROUTING` payload.
    fn apply_routing_update(&self, bytes: &[u8]) -> Result<ApplyRoutingOutcome, ClusterError>;

    /// Readiness gate per `docs/06-network-protocol.md` `CLUSTER.READY`.
    fn is_ready(&self) -> bool;

    /// Current signature (0 if not populated).
    fn routing_signature(&self) -> u64;

    /// Decide where a `(dmap, key)` should be handled. `None` keeps it
    /// local. `Some(addr)` means "this server is not the primary; the
    /// client should hit `addr` instead" — surfaced as `-MOVED` per
    /// `docs/02-consistent-hashing.md`.
    fn route_key(&self, dmap_name: &[u8], key: &[u8]) -> Option<SocketAddr>;

    /// Partition id for `(dmap, key)` under the configured `partition_count`.
    fn partition_for_key(&self, dmap_name: &[u8], key: &[u8]) -> u32;

    /// Whether the server must reject multi-key requests that cross
    /// partitions (`network.multi_key_strict`).
    fn multi_key_strict(&self) -> bool;

    /// Live backup addresses for the partition owning `(dmap, key)`.
    /// Empty when this node is *not* the primary for that partition
    /// (replication fan-out only runs on the primary).
    ///
    /// Phase 5 — `docs/04-replication.md` "Backup Owner Selection".
    fn backup_addrs_for_key(&self, _dmap_name: &[u8], _key: &[u8]) -> Vec<SocketAddr> {
        Vec::new()
    }

    /// Previous owners for the partition holding `(dmap, key)` — every
    /// entry in `RoutingTable::primary[part]` after index 0. Used by the
    /// Phase 6 fragmented-partition read path: after a topology change,
    /// reads on the new primary fall back to previous owners until the
    /// balancer migrates the data. Returns an empty vec for non-fragmented
    /// partitions or when this node isn't on the primary side.
    ///
    /// Phase 6 — `docs/12-failure-handling.md` "Behavior During Fragmentation".
    fn previous_owners_for_key(&self, _dmap_name: &[u8], _key: &[u8]) -> Vec<SocketAddr> {
        Vec::new()
    }

    /// Whether the local node currently owns `partition_id` per the
    /// applied routing table (either as the current primary at
    /// `primary[part][0]` or as one of the prior owners during a
    /// fragmented transition). Used by the Phase 6 MOVEFRAGMENT receiver
    /// to refuse imports against partitions the table doesn't actually
    /// map to us — defends against stale-routing senders.
    ///
    /// Default `true` keeps Phase 4/5 tests that pass a minimal
    /// `RoutingProvider` working without touching every stub.
    fn owns_partition(&self, _partition_id: u32) -> bool {
        true
    }

    /// Local orphans — `(partition_id, dmap_name)` pairs that this node
    /// holds but the *current* routing table says it no longer owns. Used
    /// by the Phase 6 `LeftOverDataReport` piggyback on
    /// `INTERNAL.NODE.UPDATEROUTING` replies so the coordinator (or any
    /// caller) can see which migrations are pending.
    ///
    /// Default empty so non-cluster tests skip the surface entirely.
    fn local_orphans(&self) -> Vec<(u32, String)> {
        Vec::new()
    }

    /// Whether the live member count satisfies `member_count_quorum`.
    /// Returns `true` for standalone deployments (single-member quorum).
    ///
    /// Phase 5 — `docs/04-replication.md` "Member Count Quorum".
    fn member_quorum_satisfied(&self) -> bool {
        true
    }

    /// Current replication tuning. Default values match the
    /// shipped `Config::default()` (single-replica, sync, no read repair).
    fn replication_settings(&self) -> ReplicationSettings {
        ReplicationSettings {
            replica_count: 1,
            write_quorum: 1,
            read_quorum: 1,
            read_repair: false,
            mode: kamino_core::ReplicationMode::Sync,
        }
    }

    /// Forward an arbitrary RESP command to `peer` and return the raw reply
    /// frame. The caller interprets the frame shape — used by Phase 5
    /// replication fan-out so the server handler can apply per-command
    /// reply parsing without baking it into the trait.
    fn forward_command<'a>(
        &'a self,
        _peer: SocketAddr,
        _cmd: kamino_protocol::Command,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<kamino_protocol::Frame, ClusterError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(ClusterError::ServerGone(
                "forward_command unimplemented on this RoutingProvider".into(),
            ))
        })
    }

    /// Forward an already-grouped `DM.DEL <dmap> <keys...>` to `peer` and
    /// return the deleted-count.
    ///
    /// Returns `Err(ClusterError::*)` on transport failure; the caller
    /// translates that into the `+PARTIAL` reply per
    /// `docs/06-network-protocol.md` Multi-Key Operations.
    fn forward_dm_del<'a>(
        &'a self,
        peer: SocketAddr,
        dmap: bytes::Bytes,
        keys: Vec<bytes::Bytes>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<i64, ClusterError>> + Send + 'a>>;
}

impl RoutingProvider for Cluster {
    fn routing_table_bytes(&self) -> Option<Vec<u8>> {
        let snap = self.routing_store.snapshot()?;
        snap.to_msgpack().ok()
    }

    fn apply_routing_update(&self, bytes: &[u8]) -> Result<ApplyRoutingOutcome, ClusterError> {
        Self::apply_routing_update(self, bytes)
    }

    fn is_ready(&self) -> bool {
        Self::is_ready(self)
    }

    fn routing_signature(&self) -> u64 {
        Self::routing_signature(self)
    }

    fn route_key(&self, dmap_name: &[u8], key: &[u8]) -> Option<SocketAddr> {
        let snap = self.routing_store.snapshot()?;
        let part = crate::routing::partition_for(
            self.hasher.as_ref(),
            dmap_name,
            key,
            self.core_config.partition_count,
        );
        let primary = snap.primary_for(part)?;
        if primary.id == self.local_id {
            None
        } else {
            Some(primary.addr)
        }
    }

    fn partition_for_key(&self, dmap_name: &[u8], key: &[u8]) -> u32 {
        crate::routing::partition_for(
            self.hasher.as_ref(),
            dmap_name,
            key,
            self.core_config.partition_count,
        )
    }

    fn multi_key_strict(&self) -> bool {
        self.network_config.multi_key_strict
    }

    fn backup_addrs_for_key(&self, dmap_name: &[u8], key: &[u8]) -> Vec<SocketAddr> {
        // Routing fan-out only runs on the primary. If the local node isn't
        // primary for this partition, return empty — replication is the
        // primary's job per `docs/04-replication.md`.
        let Some(snap) = self.routing_store.snapshot() else {
            return Vec::new();
        };
        let part = crate::routing::partition_for(
            self.hasher.as_ref(),
            dmap_name,
            key,
            self.core_config.partition_count,
        );
        let Some(primary) = snap.primary_for(part) else {
            return Vec::new();
        };
        if primary.id != self.local_id {
            return Vec::new();
        }
        snap.backups_for(part).iter().map(|m| m.addr).collect()
    }

    fn previous_owners_for_key(&self, dmap_name: &[u8], key: &[u8]) -> Vec<SocketAddr> {
        let Some(snap) = self.routing_store.snapshot() else {
            return Vec::new();
        };
        let part = crate::routing::partition_for(
            self.hasher.as_ref(),
            dmap_name,
            key,
            self.core_config.partition_count,
        );
        let owners = snap.owners_for(part);
        // Index 0 is the current primary; the rest are prior owners from a
        // recent topology transition. Only emit the prior list when this
        // node is itself the current primary — non-primaries hit MOVED, not
        // a fragmented-read path.
        if owners.first().map(|m| m.id) != Some(self.local_id) {
            return Vec::new();
        }
        owners.iter().skip(1).map(|m| m.addr).collect()
    }

    fn owns_partition(&self, partition_id: u32) -> bool {
        let Some(snap) = self.routing_store.snapshot() else {
            // No routing table yet → accept (single-node bootstrap path).
            return true;
        };
        snap.owners_for(partition_id)
            .iter()
            .any(|m| m.id == self.local_id)
    }

    fn local_orphans(&self) -> Vec<(u32, String)> {
        self.orphan_cache.lock().clone()
    }

    fn member_quorum_satisfied(&self) -> bool {
        let live = self.view.live_count();
        let quorum = self.member_count_quorum as usize;
        live >= quorum
    }

    fn replication_settings(&self) -> ReplicationSettings {
        ReplicationSettings {
            replica_count: self.core_config.replica_count,
            write_quorum: self.core_config.write_quorum,
            read_quorum: self.core_config.read_quorum,
            read_repair: self.core_config.read_repair,
            mode: self.core_config.replication_mode,
        }
    }

    fn forward_command<'a>(
        &'a self,
        peer: SocketAddr,
        cmd: kamino_protocol::Command,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<kamino_protocol::Frame, ClusterError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(fwd) = self.forwarder.clone() else {
                return Err(ClusterError::ServerGone(format!(
                    "forwarder unavailable; cannot reach {peer}",
                )));
            };
            fwd.send(peer, cmd).await
        })
    }

    fn forward_dm_del<'a>(
        &'a self,
        peer: SocketAddr,
        dmap: bytes::Bytes,
        keys: Vec<bytes::Bytes>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<i64, ClusterError>> + Send + 'a>>
    {
        Box::pin(async move {
            let Some(fwd) = self.forwarder.clone() else {
                return Err(ClusterError::ServerGone(format!(
                    "forwarder unavailable; cannot reach {peer}",
                )));
            };
            let cmd = kamino_protocol::Command::DmDel { dmap, keys };
            match fwd.send(peer, cmd).await? {
                kamino_protocol::Frame::Integer(n) => Ok(n),
                kamino_protocol::Frame::SimpleString(s) if s.starts_with("PARTIAL ") => {
                    // Peer itself fanned out and returned partial — parse
                    // "PARTIAL <n> <err>" and surface the count. Detail in
                    // the error tail rides our own ServerGone for now.
                    let mut parts = s.splitn(3, ' ');
                    let _ = parts.next();
                    let count = parts
                        .next()
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    let rest = parts.next().unwrap_or("").to_string();
                    Err(ClusterError::ServerGone(format!(
                        "downstream partial: count={count}, err={rest}",
                    )))
                }
                kamino_protocol::Frame::Error(e) => Err(ClusterError::ServerGone(e)),
                other => Err(ClusterError::Codec(format!(
                    "unexpected DM.DEL reply: {other:?}",
                ))),
            }
        })
    }
}

impl PubSubProvider for Cluster {
    fn allocate_conn_id(&self) -> u64 {
        self.pubsub.next_conn_id()
    }
    fn register_conn(&self, conn_id: u64, sender: tokio::sync::mpsc::Sender<DeliveredMessage>) {
        self.pubsub.register_conn(conn_id, sender);
    }
    fn cleanup_conn(&self, conn_id: u64) {
        self.pubsub.cleanup_conn(conn_id);
    }
    fn subscribe(&self, conn_id: u64, channels: &[bytes::Bytes]) -> Vec<SubAck> {
        self.pubsub.subscribe(conn_id, channels)
    }
    fn psubscribe(&self, conn_id: u64, patterns: &[bytes::Bytes]) -> Vec<SubAck> {
        self.pubsub.psubscribe(conn_id, patterns)
    }
    fn unsubscribe(&self, conn_id: u64, channels: Option<&[bytes::Bytes]>) -> Vec<SubAck> {
        self.pubsub.unsubscribe(conn_id, channels)
    }
    fn punsubscribe(&self, conn_id: u64, patterns: Option<&[bytes::Bytes]>) -> Vec<SubAck> {
        self.pubsub.punsubscribe(conn_id, patterns)
    }
    fn publish<'a>(
        &'a self,
        channel: bytes::Bytes,
        message: bytes::Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = usize> + Send + 'a>> {
        Box::pin(async move {
            let channel_name = String::from_utf8_lossy(&channel).into_owned();
            let mut total = self.pubsub.publish_local(&channel_name, &message);
            let Some(fwd) = self.forwarder.clone() else {
                return total;
            };
            let peers = self.live_peer_addrs();
            if peers.is_empty() {
                return total;
            }
            let cmd = kamino_protocol::Command::InternalNodePublish {
                channel: channel.clone(),
                message: message.clone(),
            };
            let mut futures = Vec::with_capacity(peers.len());
            for peer in peers {
                let cmd_clone = cmd.clone();
                let fwd = fwd.clone();
                futures.push(async move { fwd.send(peer, cmd_clone).await });
            }
            let replies = futures::future::join_all(futures).await;
            for r in replies {
                match r {
                    Ok(kamino_protocol::Frame::Integer(n)) if n >= 0 => {
                        total = total.saturating_add(usize::try_from(n).unwrap_or(usize::MAX));
                    }
                    Ok(other) => {
                        tracing::warn!(reply = ?other, "INTERNAL.NODE.PUBLISH unexpected reply");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "INTERNAL.NODE.PUBLISH transport error");
                    }
                }
            }
            total
        })
    }
    fn publish_local(&self, channel: &str, message: &bytes::Bytes) -> usize {
        self.pubsub.publish_local(channel, message)
    }
    fn pubsub_channels(&self, pattern: Option<&str>) -> Vec<String> {
        self.pubsub.pubsub_channels(pattern)
    }
    fn pubsub_numsub(&self, channels: &[bytes::Bytes]) -> Vec<(bytes::Bytes, usize)> {
        self.pubsub.pubsub_numsub(channels)
    }
    fn pubsub_numpat(&self) -> usize {
        self.pubsub.pubsub_numpat()
    }
}

/// `ClusterEvent` sink that publishes JSON-encoded events into the
/// `cluster.events` pub/sub channel. The cluster runtime selects this
/// when `[events] enable_cluster_events_channel = true`; otherwise the
/// [`TracingEventsSink`] is used.
///
/// Delivery is best-effort: the channel is over pub/sub, which is
/// documented at-most-once. Consumers that need ground truth should
/// poll `CLUSTER.MEMBERS` / `CLUSTER.ROUTINGTABLE` periodically.
///
/// [`TracingEventsSink`]: crate::balancer::TracingEventsSink
#[derive(Debug)]
pub struct PubSubEventsSink {
    service: Arc<PubSubService>,
    /// Local node's `Member.name`. Used to fill the `"from"`/`"to"`
    /// fields in `cluster.events` JSON so the rendered shape matches
    /// `docs/11-pubsub.md` (`{"type":"fragment-migration","from":"node-1","to":"node-3"}`).
    local_name: String,
}

impl PubSubEventsSink {
    /// Build a sink that writes into the supplied [`PubSubService`]
    /// using `local_name` as the local-node label in rendered JSON.
    #[must_use]
    pub const fn new(service: Arc<PubSubService>, local_name: String) -> Self {
        Self {
            service,
            local_name,
        }
    }
}

const CLUSTER_EVENTS_CHANNEL: &str = "cluster.events";

impl ClusterEventsSink for PubSubEventsSink {
    fn publish(&self, event: ClusterEvent) {
        let payload = match &event {
            // FragmentMigration is published by the *sender*, so
            // `from = self`, `to = peer`.
            ClusterEvent::FragmentMigration {
                dmap,
                partition_id,
                peer,
                entries,
            } => format!(
                "{{\"type\":\"fragment-migration\",\"dmap\":\"{}\",\"partition\":{},\"from\":\"{}\",\"to\":\"{}\",\"entries\":{}}}",
                escape_json(dmap),
                partition_id,
                escape_json(&self.local_name),
                peer,
                entries,
            ),
            // FragmentReceived is published by the *receiver*, so
            // `from = peer`, `to = self`.
            ClusterEvent::FragmentReceived {
                dmap,
                partition_id,
                peer,
                applied,
            } => format!(
                "{{\"type\":\"fragment-received\",\"dmap\":\"{}\",\"partition\":{},\"from\":\"{}\",\"to\":\"{}\",\"applied\":{}}}",
                escape_json(dmap),
                partition_id,
                peer,
                escape_json(&self.local_name),
                applied,
            ),
            ClusterEvent::NodeJoin { member, addr } => format!(
                "{{\"type\":\"node-join\",\"member\":\"{}\",\"addr\":\"{}\"}}",
                escape_json(member),
                addr,
            ),
            ClusterEvent::NodeLeft { member, addr } => format!(
                "{{\"type\":\"node-left\",\"member\":\"{}\",\"addr\":\"{}\"}}",
                escape_json(member),
                addr,
            ),
        };
        let body = bytes::Bytes::from(payload);
        self.service.publish_local(CLUSTER_EVENTS_CHANNEL, &body);
    }
}

/// Minimal JSON string escaper: enough for member ids, dmap names, and
/// IPv4/IPv6 socket addresses (which never contain control chars).
fn escape_json(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    for c in input.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn forwarder_config_from(cfg: &kamino_core::config::Config) -> ForwarderConfig {
    ForwarderConfig {
        pool_size: cfg.network.internode_pool_size,
        inflight_per_conn: cfg.network.internode_inflight_per_conn,
        connect_timeout: cfg.network.internode_connect_timeout,
        request_timeout: cfg.network.internode_request_timeout,
        reconnect_backoff_min: cfg.network.internode_reconnect_backoff_min,
        reconnect_backoff_max: cfg.network.internode_reconnect_backoff_max,
        cluster_secret: cfg.auth.cluster_secret.clone(),
    }
}

/// Plain DTO for the `CLUSTER.MEMBERS` command response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberSummary {
    pub id: MemberId,
    pub name: String,
    pub addr: SocketAddr,
    pub discovery_addr: SocketAddr,
    pub birthdate: u64,
    pub is_coordinator: bool,
}

impl From<&Member> for MemberSummary {
    fn from(m: &Member) -> Self {
        Self {
            id: m.id,
            name: m.name.clone(),
            addr: m.addr,
            discovery_addr: m.discovery_addr,
            birthdate: m.birthdate,
            is_coordinator: m.is_coordinator,
        }
    }
}

#[cfg(test)]
mod tests {
    //! Phase 7 cross-check: `PubSubEventsSink` renders the doc-mandated
    //! JSON shape (`docs/11-pubsub.md` "Cluster Event Channel"):
    //!
    //! ```json
    //! { "type": "node-join",          "member": "node-3", "addr": "10.0.1.3:3320" }
    //! { "type": "node-left",          "member": "node-2", "addr": "10.0.1.2:3320" }
    //! { "type": "fragment-migration", "partition": 42, "from": "node-1", "to": "node-3" }
    //! ```
    use super::*;
    use bytes::Bytes;
    use tokio::sync::mpsc;

    fn collect_one(service: &Arc<PubSubService>) -> mpsc::Receiver<DeliveredMessage> {
        let id = service.next_conn_id();
        let (tx, rx) = mpsc::channel(8);
        service.register_conn(id, tx);
        service.subscribe(id, &[Bytes::from_static(b"cluster.events")]);
        rx
    }

    async fn drain_one(rx: &mut mpsc::Receiver<DeliveredMessage>) -> String {
        let m = rx.recv().await.expect("event delivered");
        String::from_utf8(m.payload.to_vec()).expect("utf8")
    }

    #[tokio::test]
    async fn fragment_migration_renders_from_to_per_doc() {
        let svc = Arc::new(PubSubService::new());
        let mut rx = collect_one(&svc);
        let sink = PubSubEventsSink::new(Arc::clone(&svc), "node-1".into());
        sink.publish(ClusterEvent::FragmentMigration {
            dmap: "sessions".into(),
            partition_id: 42,
            peer: "10.0.1.3:3320".parse().unwrap(),
            entries: 7,
        });
        let body = drain_one(&mut rx).await;
        assert!(body.contains("\"type\":\"fragment-migration\""));
        assert!(body.contains("\"partition\":42"));
        assert!(body.contains("\"from\":\"node-1\""));
        assert!(body.contains("\"to\":\"10.0.1.3:3320\""));
        assert!(body.contains("\"entries\":7"));
    }

    #[tokio::test]
    async fn fragment_received_renders_from_to_inverted() {
        let svc = Arc::new(PubSubService::new());
        let mut rx = collect_one(&svc);
        let sink = PubSubEventsSink::new(Arc::clone(&svc), "node-3".into());
        sink.publish(ClusterEvent::FragmentReceived {
            dmap: "sessions".into(),
            partition_id: 42,
            peer: "10.0.1.1:3320".parse().unwrap(),
            applied: 5,
        });
        let body = drain_one(&mut rx).await;
        assert!(body.contains("\"type\":\"fragment-received\""));
        // The receiver side renders `from = peer`, `to = self`.
        assert!(body.contains("\"from\":\"10.0.1.1:3320\""));
        assert!(body.contains("\"to\":\"node-3\""));
        assert!(body.contains("\"applied\":5"));
    }

    #[tokio::test]
    async fn node_join_uses_member_name() {
        let svc = Arc::new(PubSubService::new());
        let mut rx = collect_one(&svc);
        let sink = PubSubEventsSink::new(Arc::clone(&svc), "node-1".into());
        sink.publish(ClusterEvent::NodeJoin {
            member: "node-3".into(),
            addr: "10.0.1.3:3320".parse().unwrap(),
        });
        let body = drain_one(&mut rx).await;
        assert!(body.contains("\"type\":\"node-join\""));
        assert!(body.contains("\"member\":\"node-3\""));
        assert!(body.contains("\"addr\":\"10.0.1.3:3320\""));
    }

    #[tokio::test]
    async fn node_left_uses_member_name() {
        let svc = Arc::new(PubSubService::new());
        let mut rx = collect_one(&svc);
        let sink = PubSubEventsSink::new(Arc::clone(&svc), "node-1".into());
        sink.publish(ClusterEvent::NodeLeft {
            member: "node-2".into(),
            addr: "10.0.1.2:3320".parse().unwrap(),
        });
        let body = drain_one(&mut rx).await;
        assert!(body.contains("\"type\":\"node-left\""));
        assert!(body.contains("\"member\":\"node-2\""));
    }
}
