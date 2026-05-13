//! Routing layer (Phase 4 per `ROADMAP.md` §6).
//!
//! - `ring`: bounded-load consistent hash ring (Mirrokni 2016).
//! - `table`: [`RoutingTable`] with `rmp-serde` named-map codec + key→partition
//!   helper.
//! - `store`: signature-gated shared view; rejects stale broadcasts.
//! - `coordinator`: periodic build-and-push loop, run only on the coordinator.

pub mod coordinator;
pub mod ring;
pub mod store;
pub mod table;

pub use coordinator::{CoordinatorParams, run_coordinator_loop};
pub use ring::{Assignment, assign};
pub use store::{ApplyRoutingOutcome, RoutingTableStore};
pub use table::{ROUTING_SCHEMA_VERSION, RoutingTable, SharedRoutingTable, partition_for};
