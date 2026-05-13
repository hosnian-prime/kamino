//! Kamino — distributed in-memory cache library and server.
//!
//! See `docs/00-overview.md` for the design and `ROADMAP.md` for the
//! implementation plan. This umbrella crate re-exports the public surface of
//! the individual sub-crates and (from Phase 1 onward) hosts the
//! `Kamino::embedded()` / `embedded_clustered()` / `serve()` builders.

pub use kamino_core::{
    Clock, Config, Error, Hasher, MemberId, Mode, Profile, Result, SystemClock, XxHasher,
    tracing_init,
};

/// Crate version pulled from `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
