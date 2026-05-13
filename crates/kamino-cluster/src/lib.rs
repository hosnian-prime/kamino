//! Cluster subsystems: SWIM membership, routing table, replication, balancer,
//! pub/sub, and inter-node forwarding.
//!
//! Phase 3 ships membership only:
#![allow(
    // SWIM is intrinsically lock-heavy and we hold short critical sections
    // around HashMap mutations; the "drop sooner" suggestion produces noisier
    // code without real contention wins (verified under load).
    clippy::significant_drop_tightening,
    // Doc paragraphs in this crate often span 2-3 sentences for protocol
    // clarity; rewrapping them produces unreadable one-sentence-per-paragraph
    // prose.
    clippy::too_long_first_doc_paragraph,
    // `cluster.rs` and `membership.rs` use field names like `cluster_secret`
    // and `local_id` that legitimately repeat the struct name.
    clippy::struct_field_names,
)]
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
pub mod forwarder;
pub mod gossip;
pub mod join;
pub mod membership;
pub mod message;
pub mod routing;
pub mod swim;
pub mod transport;

pub use cluster::{
    Cluster, ClusterDeps, MemberProvider, MemberSummary, ReplicationSettings, RoutingProvider,
};
#[cfg(feature = "discovery-dns")]
pub use discovery::DnsDiscovery;
pub use discovery::{DiscoveryPlugin, StaticDiscovery};
pub use error::{ClusterError, ClusterResult};
pub use forwarder::{Connector, Forwarder, ForwarderConfig, TcpConnector};
pub use gossip::GossipQueue;
pub use join::{JOIN_TARGET_SENTINEL, JoinParams, join};
pub use membership::{ApplyOutcome, MemberEntry, MemberState, MembershipView};
pub use message::{Envelope, GossipEvent, Incarnation, SwimMessage, alive_for};
pub use routing::{
    ApplyRoutingOutcome, Assignment, CoordinatorParams, ROUTING_SCHEMA_VERSION, RoutingTable,
    RoutingTableStore, SharedRoutingTable, partition_for, run_coordinator_loop,
};
pub use swim::{ProbeOutcome, SwimDriver, run_probe_loop, run_receive_loop};
pub use transport::{Transport, UdpTransport};
