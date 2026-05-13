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
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::discovery::DiscoveryPlugin;
use crate::error::ClusterResult;
use crate::gossip::GossipQueue;
use crate::membership::MembershipView;
use crate::swim::SwimDriver;
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
}

/// Running cluster.
#[allow(missing_debug_implementations)]
pub struct Cluster {
    view: MembershipView,
    queue: Arc<GossipQueue>,
    transport: Arc<dyn Transport>,
    discovery: Arc<dyn DiscoveryPlugin>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    local_id: MemberId,
    cluster_secret: String,
    leave_timeout: std::time::Duration,
}

impl Cluster {
    /// Build a cluster but do not start the background tasks. Useful for
    /// tests that drive the SWIM driver manually.
    pub fn assemble(deps: ClusterDeps) -> ClusterResult<Self> {
        let view = MembershipView::bootstrap(deps.local.clone());
        let queue = Arc::new(GossipQueue::with_capacity(256));
        // Seed the queue with our own Alive announcement so the first probe
        // gossips our presence.
        queue.push(
            crate::message::alive_for(&deps.local, view.local_incarnation()),
            1,
        );
        Ok(Self {
            view,
            queue,
            transport: deps.transport,
            discovery: deps.discovery,
            cancel: CancellationToken::new(),
            tasks: Vec::new(),
            local_id: deps.local.id,
            cluster_secret: deps.config.auth.cluster_secret.clone(),
            leave_timeout: deps.config.discovery.leave_timeout,
        })
    }

    /// Local membership view (cheap clone).
    #[must_use]
    pub fn view(&self) -> MembershipView {
        self.view.clone()
    }

    /// Build a SwimDriver for the assembled cluster. Caller decides whether
    /// to run the probe + receive loops or test them in isolation.
    pub fn driver(&self, clock: Arc<dyn Clock>, config: kamino_core::config::SwimConfig) -> SwimDriver {
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
    pub fn local_id(&self) -> MemberId {
        self.local_id
    }

    /// Discovery plugin name (for logs).
    #[must_use]
    pub fn discovery_name(&self) -> &'static str {
        self.discovery.name()
    }

    /// Register a spawned background task so `shutdown` can await it.
    pub fn register_task(&mut self, task: JoinHandle<()>) {
        self.tasks.push(task);
    }

    /// Cancellation token shared across all SWIM tasks.
    #[must_use]
    pub fn cancel(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Gracefully leave the cluster: broadcast a Leave gossip event,
    /// wait `leave_timeout`, then cancel and join all tasks.
    pub async fn shutdown(mut self) -> ClusterResult<()> {
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
        for task in std::mem::take(&mut self.tasks) {
            let _ = task.await;
        }
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
