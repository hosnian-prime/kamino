//! Cluster subsystems: SWIM membership, routing table, replication, balancer,
//! pub/sub, and inter-node forwarding.
//!
//! Phase 3 ships membership only:
//!
//! * `Member` + `MembershipView` (canonical local view, coordinator selection)
//! * `SwimDriver` with probe / indirect-probe / suspect / dead state machine
//! * `GossipQueue` for piggybacked dissemination
//! * `Transport` trait (UDP impl, test-friendly mocks via `turmoil`)
//! * `DiscoveryPlugin` (static + DNS in Phase 3)
//! * `Cluster::shutdown` for graceful leave
//! * `MemberSummary` DTO consumed by the `CLUSTER.MEMBERS` server handler
//!
//! Phases 4–9 add: routing table + forwarder, replication, balancer, pub/sub,
//! locks, additional discovery plugins (Kubernetes, Consul).

pub mod cluster;
pub mod discovery;
pub mod error;
pub mod gossip;
pub mod join;
pub mod membership;
pub mod message;
pub mod swim;
pub mod transport;

pub use cluster::{Cluster, ClusterDeps, MemberProvider, MemberSummary};
#[cfg(feature = "discovery-dns")]
pub use discovery::DnsDiscovery;
pub use discovery::{DiscoveryPlugin, StaticDiscovery};
pub use error::{ClusterError, ClusterResult};
pub use gossip::GossipQueue;
pub use join::{JOIN_TARGET_SENTINEL, JoinParams, join};
pub use membership::{ApplyOutcome, MemberEntry, MemberState, MembershipView};
pub use message::{Envelope, GossipEvent, Incarnation, SwimMessage};
pub use swim::{ProbeOutcome, SwimDriver, run_probe_loop, run_receive_loop};
pub use transport::{Transport, UdpTransport};
