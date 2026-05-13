//! SWIM failure-detector state machine.
//!
//! The pieces wired together here:
//!
//! * a [`crate::transport::Transport`] for sending and receiving envelopes,
//! * a shared [`crate::membership::MembershipView`] for state,
//! * a [`crate::gossip::GossipQueue`] for piggybacked dissemination, and
//! * a periodic *probe loop* that drives ping → ack / ping-req → indirect-ack
//!   per `docs/03-cluster-management.md` "Failure Detection".
//!
//! The implementation lives in submodules below so unit tests can target
//! each piece in isolation without spinning up real UDP sockets.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use kamino_core::clock::Clock;
use kamino_core::config::SwimConfig;
use kamino_core::ids::MemberId;
use parking_lot::Mutex as SyncMutex;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::gossip::GossipQueue;
use crate::membership::MembershipView;
use crate::transport::Transport;

/// Map of in-flight probe sequence numbers to a oneshot that the receive loop
/// fires when the matching `Ack` or `IndirectAck` arrives.
pub(crate) type InFlightMap = Arc<SyncMutex<HashMap<u64, oneshot::Sender<()>>>>;

impl std::fmt::Debug for SwimDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwimDriver")
            .field("config", &self.config)
            .field("cluster_secret_set", &!self.cluster_secret.is_empty())
            .finish_non_exhaustive()
    }
}

/// Inputs to the SWIM driver. Cheap to construct; the driver does not take
/// ownership of the transport so the same socket can be used for the
/// receive loop independently.
pub struct SwimDriver {
    pub config: SwimConfig,
    pub view: MembershipView,
    pub queue: Arc<GossipQueue>,
    pub transport: Arc<dyn Transport>,
    pub clock: Arc<dyn Clock>,
    pub cluster_secret: String,
    /// Cancelled when the cluster is shutting down.
    pub cancel: CancellationToken,
    /// Per-driver RNG, seeded deterministically in tests.
    pub rng: AsyncMutex<SmallRng>,
    /// In-flight probes keyed by sequence number. The probe loop registers a
    /// `oneshot::Sender` before sending a `Ping` (or `PingReq`); the receive
    /// loop fires the matching sender when the corresponding `Ack` /
    /// `IndirectAck` arrives. Internal coordination only — never exposed.
    pub(crate) in_flight: InFlightMap,
    /// Monotonically incrementing probe sequence counter, shared between any
    /// probe-issuing tasks. `AtomicU64` rather than relying on the RNG so
    /// indirect probes can chain a fresh seq without contending for the RNG
    /// lock.
    pub(crate) probe_seq: Arc<std::sync::atomic::AtomicU64>,
}

impl SwimDriver {
    /// Construct a driver. Seeds the RNG from system entropy.
    #[must_use]
    pub fn new(
        config: SwimConfig,
        view: MembershipView,
        queue: Arc<GossipQueue>,
        transport: Arc<dyn Transport>,
        clock: Arc<dyn Clock>,
        cluster_secret: String,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            config,
            view,
            queue,
            transport,
            clock,
            cluster_secret,
            cancel,
            rng: AsyncMutex::new(SmallRng::from_entropy()),
            in_flight: Arc::new(SyncMutex::new(HashMap::new())),
            probe_seq: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Allocate the next probe sequence number.
    pub(crate) fn next_seq(&self) -> u64 {
        self.probe_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Register a oneshot for an in-flight probe and return the receiver. The
    /// caller drops the receiver when the wait completes (or times out) to
    /// release the slot; the receive loop also evicts the entry when it fires.
    pub(crate) fn register_inflight(&self, seq: u64) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.in_flight.lock().insert(seq, tx);
        rx
    }

    /// Forget an in-flight probe (after timeout, on cancel, etc.).
    pub(crate) fn forget_inflight(&self, seq: u64) {
        self.in_flight.lock().remove(&seq);
    }

    /// Fire the oneshot for `seq` if registered. Called by the receive loop.
    pub(crate) fn complete_inflight(&self, seq: u64) {
        let tx = self.in_flight.lock().remove(&seq);
        if let Some(tx) = tx {
            let _ = tx.send(());
        }
    }

    /// Total time a member spends in `Suspect` before promotion to `Dead`:
    /// `suspicion_multiplier * probe_interval` per `docs/03-cluster-management.md`.
    #[must_use]
    pub fn suspicion_timeout(&self) -> Duration {
        self.config.probe_interval * self.config.suspicion_multiplier
    }

    /// Reap suspects whose deadline elapsed; emit `Dead` gossip for any
    /// promotion.
    pub fn tick_reaper(&self) -> Vec<MemberId> {
        let now_ns = self.clock.now_micros().saturating_mul(1_000);
        let promoted = self.view.reap_suspects(now_ns);
        let local_id = self.view.local_id();
        for (id, inc) in &promoted {
            if let Some(from) = local_id {
                self.queue.push(
                    crate::message::GossipEvent::Dead {
                        id: *id,
                        incarnation: *inc,
                        from,
                    },
                    self.view.live_count(),
                );
            }
        }
        promoted.into_iter().map(|(id, _)| id).collect()
    }
}

pub mod loops;
#[cfg(any(test, feature = "mock-transport"))]
pub use loops::probe_once;
pub use loops::{ProbeOutcome, run_probe_loop, run_receive_loop};
