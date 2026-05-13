//! Cluster subsystems: SWIM membership, routing table, replication, balancer,
//! pub/sub, and inter-node forwarding.
//!
//! Phase 0 ships only the skeleton; SWIM lands in Phase 3, the routing table
//! and forwarder in Phase 4, replication in Phase 5, the balancer in Phase 6,
//! pub/sub in Phase 7, locks in Phase 8, and discovery plugins in Phase 9
//! (see `ROADMAP.md`).
