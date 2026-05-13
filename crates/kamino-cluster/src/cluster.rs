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

use crate::discovery::DiscoveryPlugin;
use crate::error::{ClusterError, ClusterResult};
use crate::gossip::GossipQueue;
use crate::join::{self, JoinParams};
use crate::membership::MembershipView;
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
    /// Inter-node routing-table pusher. `None` uses [`LocalOnlyPusher`]
    /// (Phase 4A default; the real Forwarder-based pusher lands in 4B).
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
    core_config: kamino_core::config::CoreConfig,
    routing_config: kamino_core::config::RoutingConfig,
    member_count_quorum: u32,
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
        let routing_pusher: Arc<dyn RoutingPusher> = deps
            .routing_pusher
            .unwrap_or_else(|| Arc::new(LocalOnlyPusher));
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
            core_config: deps.config.core.clone(),
            routing_config: deps.config.routing.clone(),
            member_count_quorum: deps.config.core.member_count_quorum,
        }
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
