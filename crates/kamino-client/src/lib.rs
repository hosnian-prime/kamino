#![allow(clippy::doc_markdown)] // contract doc references are intentionally bare

//! Client surface: `Client`/`DMap` traits and the `EmbeddedClient`.
//!
//! Phase 1 scope: in-process embedded solo. `RemoteClient` and cross-partition
//! `Pipeline` / `ScanCursor` land in later phases (see `ROADMAP.md`).
//!
//! See `docs/08-api-design.md` for the contract.

pub mod cursor;
pub mod dmap;
pub mod embedded;
pub mod error;
pub mod lock;
pub mod remote;
pub mod stats;
pub mod traits;
pub mod types;

pub use cursor::{ScanCursor, ScanOptions};
pub use dmap::DMap;
pub use embedded::{EmbeddedClient, EmbeddedDMap};
pub use error::{Error, Result};
pub use lock::LockContext;
pub use remote::{RemoteClient, RemoteDMap};
pub use stats::{DMapStats, Stats, StatsOptions};
pub use traits::Client;
pub use types::{DMapOptions, GetResponse, PutOptions};
