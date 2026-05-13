//! Join sequence.
//!
//! Discover peers, send a `Ping` containing our `Alive` gossip, wait for
//! any single ack. Retries up to `max_join_attempts` with
//! `join_retry_interval`, capped overall by `bootstrap_timeout`.
//!
//! Wire contract (Phase 3):
//!
//! * A joining node has *not* yet learned the ids of its bootstrap peers, so
//!   `Ping.target` is set to the sentinel [`JOIN_TARGET_SENTINEL`]
//!   (`MemberId::from_raw(0)`). The receive loop treats a Ping whose target
//!   equals the sentinel as a join request: it replies with an `Ack` and
//!   piggybacks an `Alive` event for itself so the joining node populates
//!   its view.
//! * The piggybacked `Alive { id: self }` is always present on the joiner's
//!   first Ping so the receiver learns about us at the same time.
//! * The first peer we hear an `Alive` for populates our view; we keep
//!   listening until `probe_timeout` elapses to collect any others that
//!   reply in parallel.
//! * If `discovery.discover()` returns an empty list **and** the static
//!   `discovery.peers` config is also empty, we treat this as a single-node
//!   bootstrap and return `Ok(0)` without error.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kamino_core::config::DiscoveryConfig;
use kamino_core::ids::MemberId;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::cluster::Cluster;
use crate::error::{ClusterError, ClusterResult};
use crate::message::{Envelope, SwimMessage, alive_for};

/// Sentinel `target` value used by the join Ping. Real `MemberId`s come
/// from `MemberId::new_random()`, which has a vanishing probability of
/// colliding with zero.
pub const JOIN_TARGET_SENTINEL: MemberId = MemberId::from_raw(0);

/// Parameters for a single join attempt.
#[derive(Debug, Clone)]
pub struct JoinParams {
    pub discovery: DiscoveryConfig,
    pub probe_timeout: Duration,
}

/// Execute the join handshake. On success populates `cluster.view` with the
/// seed peers reported by the bootstrap acks and returns the number of
/// peers from which we received `Alive` gossip.
pub async fn join(cluster: &Arc<Cluster>, params: JoinParams) -> ClusterResult<usize> {
    let max_attempts = params.discovery.max_join_attempts.max(1);
    let retry_interval = params.discovery.join_retry_interval;
    let deadline = Instant::now() + params.discovery.bootstrap_timeout;
    let local_member = cluster.local_member();
    let local_incarnation = cluster.view().local_incarnation();
    let transport = cluster.transport();
    let secret = cluster.cluster_secret().to_string();

    let mut last_error: String = "no attempts made".into();

    for attempt in 1..=max_attempts {
        if Instant::now() >= deadline {
            break;
        }

        let peers = match cluster.discovery().discover().await {
            Ok(p) => p,
            Err(e) => {
                last_error = e.to_string();
                warn!(attempt, error = %e, "discovery returned error; will retry");
                if attempt < max_attempts {
                    tokio::time::sleep(retry_interval).await;
                }
                continue;
            }
        };

        // Single-node bootstrap: both runtime discovery and the configured
        // peer list are empty → no peers to contact, accept as success.
        if peers.is_empty() && params.discovery.peers.is_empty() {
            debug!("join: no peers configured and discovery returned none → single-node bootstrap");
            return Ok(0);
        }

        if peers.is_empty() {
            last_error = "discovery returned no peers".into();
            debug!(attempt, "join: discovery returned no peers; retrying");
            if attempt < max_attempts {
                tokio::time::sleep(retry_interval).await;
            }
            continue;
        }

        // Build a single join envelope per peer. Piggyback an Alive for our
        // local node so the receiver learns about us in the same datagram.
        let envelope = Envelope {
            cluster_secret: secret.clone(),
            from: local_member.id,
            msg: SwimMessage::Ping {
                seq: 0,
                target: JOIN_TARGET_SENTINEL,
            },
            gossip: vec![alive_for(&local_member, local_incarnation)],
        };

        let mut sent = 0usize;
        for peer_addr in &peers {
            if let Err(e) = transport.send_to(&envelope, *peer_addr).await {
                warn!(peer = %peer_addr, error = %e, "join: failed to send ping");
                last_error = e.to_string();
                continue;
            }
            sent += 1;
        }

        if sent == 0 {
            if attempt < max_attempts {
                tokio::time::sleep(retry_interval).await;
            }
            continue;
        }

        // Wait up to `probe_timeout` for at least one peer to respond with
        // an Alive about itself. The receive loop populates the membership
        // view as gossip arrives, so we poll it for new members.
        let observed = wait_for_first_peer(cluster, params.probe_timeout, deadline).await;
        if observed > 0 {
            debug!(
                peers = observed,
                attempt, "join: received Alive from peer(s)"
            );
            return Ok(observed);
        }
        last_error = "no peer replied within probe_timeout".into();
        debug!(attempt, "join: probe_timeout elapsed without a peer reply");

        if attempt < max_attempts && Instant::now() < deadline {
            tokio::time::sleep(retry_interval).await;
        }
    }

    Err(ClusterError::JoinFailed {
        attempts: max_attempts,
        last: last_error,
    })
}

/// Poll the membership view until we observe >= 1 peer that's not us, or
/// until the probe-timeout / overall deadline elapses. The receive loop is
/// responsible for actually applying the gossip; this function just notices.
async fn wait_for_first_peer(
    cluster: &Arc<Cluster>,
    probe_timeout: Duration,
    deadline: Instant,
) -> usize {
    let now = Instant::now();
    let remaining = deadline.saturating_duration_since(now);
    let wait = probe_timeout.min(remaining);
    if wait.is_zero() {
        return count_peers(cluster);
    }
    let local_id = cluster.local_id();
    let cluster = Arc::clone(cluster);
    let poll = async move {
        loop {
            let count = cluster
                .view()
                .snapshot()
                .into_iter()
                .filter(|m| m.id != local_id)
                .count();
            if count > 0 {
                return count;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    timeout(wait, poll).await.unwrap_or(0)
}

fn count_peers(cluster: &Arc<Cluster>) -> usize {
    let local_id = cluster.local_id();
    cluster
        .view()
        .snapshot()
        .into_iter()
        .filter(|m| m.id != local_id)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kamino_core::clock::SystemClock;
    use kamino_core::config::Config;
    use kamino_core::ids::MemberId;
    use kamino_core::member::Member;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use crate::cluster::{Cluster, ClusterDeps};
    use crate::discovery::StaticDiscovery;
    use crate::transport::{Transport, UdpTransport};

    async fn mk_cluster_empty_peers() -> Arc<Cluster> {
        let transport = Arc::new(
            UdpTransport::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
                .await
                .expect("bind udp"),
        );
        let discovery = Arc::new(StaticDiscovery::from_addrs(Vec::new()));
        let local = Member::new(
            MemberId::new_random(),
            "self",
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
            transport.local_addr().unwrap(),
            1,
        );
        let mut config = Config::default();
        config.discovery.peers = Vec::new();
        config.discovery.max_join_attempts = 2;
        config.discovery.join_retry_interval = Duration::from_millis(10);
        config.discovery.bootstrap_timeout = Duration::from_millis(200);
        let deps = ClusterDeps {
            config,
            transport,
            discovery,
            clock: Arc::new(SystemClock),
            local,
        };
        Arc::new(Cluster::assemble(deps))
    }

    #[tokio::test]
    async fn join_with_no_peers_is_single_node_ok() {
        let cluster = mk_cluster_empty_peers().await;
        let params = JoinParams {
            discovery: cluster.discovery_config().clone(),
            probe_timeout: Duration::from_millis(50),
        };
        let n = join(&cluster, params).await.expect("single-node join");
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn join_returns_err_when_peers_unreachable() {
        let transport = Arc::new(
            UdpTransport::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
                .await
                .expect("bind udp"),
        );
        // A localhost port that's almost certainly closed. UDP send may
        // succeed (UDP is connectionless), but no reply ever arrives, so we
        // exercise the timeout / retry path.
        let unreachable = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
        let discovery = Arc::new(StaticDiscovery::from_addrs(vec![unreachable]));
        let local = Member::new(
            MemberId::new_random(),
            "self",
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
            transport.local_addr().unwrap(),
            1,
        );
        let mut config = Config::default();
        config.discovery.peers = vec!["127.0.0.1:1".into()];
        config.discovery.max_join_attempts = 2;
        config.discovery.join_retry_interval = Duration::from_millis(5);
        config.discovery.bootstrap_timeout = Duration::from_millis(120);
        let deps = ClusterDeps {
            config,
            transport,
            discovery,
            clock: Arc::new(SystemClock),
            local,
        };
        let cluster = Arc::new(Cluster::assemble(deps));
        let params = JoinParams {
            discovery: cluster.discovery_config().clone(),
            probe_timeout: Duration::from_millis(30),
        };
        let err = join(&cluster, params)
            .await
            .expect_err("unreachable peer must fail");
        assert!(matches!(err, ClusterError::JoinFailed { .. }));
    }
}
