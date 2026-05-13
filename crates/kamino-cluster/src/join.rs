//! Join sequence: discover peers, send a `Ping` containing our `Alive`
//! gossip, wait for any single ack. Retries up to `max_join_attempts`
//! with `join_retry_interval`, capped overall by `bootstrap_timeout`.
//!
//! Concrete implementation is supplied by Phase 3 Agent B; the public
//! surface is frozen here so `Cluster::bootstrap` can compile.

use std::sync::Arc;
use std::time::Duration;

use kamino_core::config::DiscoveryConfig;

use crate::cluster::Cluster;
use crate::error::ClusterResult;

/// Parameters for a single join attempt.
#[derive(Debug, Clone)]
pub struct JoinParams {
    pub discovery: DiscoveryConfig,
    pub probe_timeout: Duration,
}

/// Execute the join handshake; on success populates `cluster.view` with the
/// seed peers reported by the bootstrap acks. Returns the number of peers
/// successfully contacted.
pub async fn join(_cluster: &Arc<Cluster>, _params: JoinParams) -> ClusterResult<usize> {
    // Agent B's deliverable. The Cluster runtime is otherwise standalone-
    // friendly: assemble() works without any peers, which is sufficient for
    // unit tests of the SWIM driver.
    Ok(0)
}
