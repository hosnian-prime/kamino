//! Core types, configuration, traits and identifiers for Kamino.
//!
//! `kamino-core` sits at the bottom of the dependency graph (see
//! `ROADMAP.md`). It owns:
//!
//! - [`Config`] — the single source of truth for runtime knobs, organised by
//!   section to match `docs/09-configuration.md`.
//! - [`Mode`] — `Standalone`, `EmbeddedSolo` or `EmbeddedClustered`. Mode
//!   selection drives validation; invalid section/mode combinations are
//!   rejected at [`Config::validate`].
//! - [`Profile`] — `Development`, `Production` or `Custom`. Profile hard
//!   guards run alongside the per-section validators (see
//!   `docs/16-config-architecture.md`).
//! - [`Error`] / [`Result`] — typed errors via `thiserror`.
//! - [`Hasher`] + [`XxHasher`] — hash trait for partition assignment.
//! - [`Clock`] + [`SystemClock`] — wall-clock + monotonic abstraction (LWW
//!   timestamps go through this).
//! - [`MemberId`] — random 64-bit per-node identifier.
//! - [`tracing_init`] — opinionated `tracing` subscriber installer.
//!
//! Phase 0 ships the trait surface; Phase 11 fills in the profile hard
//! guards and reload diffing.

pub mod clock;
pub mod config;
pub mod error;
pub mod hasher;
pub mod ids;
pub mod mode;
pub mod profile;
pub mod tracing_init;

pub use clock::{Clock, SystemClock};
pub use config::{
    AuthConfig, BalancerConfig, Config, CoreConfig, DMapConfig, DiscoveryConfig, EventsConfig,
    EvictionConfig, EvictionPolicy, HashConfig, NetworkConfig, ReplicationMode, RoutingConfig,
    StorageConfig, SwimConfig,
};
pub use error::{Error, Result};
pub use hasher::{Hasher, XxHasher};
pub use ids::MemberId;
pub use mode::Mode;
pub use profile::Profile;
