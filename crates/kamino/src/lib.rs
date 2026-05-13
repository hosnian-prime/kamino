//! Kamino — distributed in-memory cache library and server.
//!
//! See `docs/00-overview.md` for the design and `ROADMAP.md` for the
//! implementation plan. This umbrella crate re-exports the public surface of
//! the individual sub-crates and (from Phase 1 onward) hosts the
//! `Kamino::embedded()` builder.

pub mod node;

pub use kamino_server::{Server, ServerError, ShutdownHandle};
pub use node::{Kamino, KaminoError};

// Re-exports of the consumer-facing surface.
pub use kamino_client::{
    Client, DMap, DMapOptions, EmbeddedPubSub, Error as ClientError, GetResponse, LockContext,
    Message as PubSubMessage, PubSub, PubSubOptions, PutOptions, Result as ClientResult,
    ScanCursor, ScanOptions, Stats, StatsOptions, Subscription,
};
pub use kamino_core::{
    Clock, Config, Error as CoreError, Hasher, MemberId, Mode, Profile, Result as CoreResult,
    SystemClock, XxHasher, tracing_init,
};
pub use kamino_storage::{
    Entry, Error as StorageError, IdleSweeper, LruSampler, RamBlock, StorageEngine, TtlSweeper,
};

/// Crate version pulled from `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
