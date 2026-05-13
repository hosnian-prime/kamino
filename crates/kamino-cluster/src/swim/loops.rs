//! Probe + receive loops. Filled in by Phase 3 implementation.
//!
//! Contract (frozen for the parallel agents):
//!
//! * [`run_probe_loop`]: drives one probe per `probe_interval`. Picks a
//!   random `Alive` peer, sends `Ping`, waits up to `probe_timeout`; on
//!   timeout fans out `PingReq` to `indirect_probes` random peers; on
//!   *complete* failure, sets the target to `Suspect`.
//! * [`run_receive_loop`]: consumes envelopes from the transport, validates
//!   `cluster_secret`, dispatches into the membership view, and writes
//!   ack/indirect-ack replies back through the transport.

use std::sync::Arc;

use super::SwimDriver;

/// Outcome reported by a single probe round (used by unit tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// Direct ack received within `probe_timeout`.
    DirectAck,
    /// Indirect ack received via a proxy.
    IndirectAck,
    /// Both phases failed; target was set to `Suspect`.
    Suspected,
    /// No live peers to probe.
    Idle,
}

/// Run the probe loop until `cancel` fires.
///
/// Implementation lives in `Agent A`'s deliverable. The signature is frozen
/// so the receive loop and the cluster runtime can wire it in advance.
pub async fn run_probe_loop(driver: Arc<SwimDriver>) {
    let _ = driver;
    // Placeholder — Phase 3 impl supplies the body.
}

/// Run the receive loop until `cancel` fires. Reads envelopes from the
/// transport, validates the cluster_secret, applies piggybacked gossip,
/// and emits ack / indirect-ack replies.
pub async fn run_receive_loop(driver: Arc<SwimDriver>) {
    let _ = driver;
}
