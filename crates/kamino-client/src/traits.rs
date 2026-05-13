//! Top-level `Client` trait.
//!
//! Phase 1 scopes the trait to embedded solo: `new_dmap`, `stats`,
//! `partition_count`, `close`. Cluster-only methods get an `Unsupported`
//! stub by default; they will become required in Phase 4 once the
//! `RemoteClient` / clustered impls land.

use std::sync::Arc;

use async_trait::async_trait;

use crate::dmap::DMap;
use crate::error::{Error, Result};
use crate::stats::{Stats, StatsOptions};
use crate::types::DMapOptions;

/// Top-level client interface (per `docs/08-api-design.md`).
#[async_trait]
pub trait Client: Send + Sync + std::fmt::Debug {
    /// Create or fetch a DMap handle. Idempotent: a second call with the same
    /// name returns the existing handle and ignores `options` (per
    /// `docs/16-config-architecture.md` reload discipline).
    async fn new_dmap(&self, name: &str, options: DMapOptions) -> Result<Arc<dyn DMap>>;

    /// Collect per-DMap stats.
    async fn stats(&self, options: StatsOptions) -> Result<Stats>;

    /// Number of hash-ring partitions the cluster is configured for.
    fn partition_count(&self) -> u32;

    /// Gracefully shut down the client and any background workers.
    async fn close(&self) -> Result<()>;

    // ---- Cluster-only stubs --------------------------------------------------------------

    /// Ping a specific node. Not supported in embedded solo.
    async fn ping(&self, _addr: &str) -> Result<()> {
        Err(Error::Unsupported("ping"))
    }

    /// Force the client to refresh its cached routing-table metadata. Not
    /// supported in embedded solo.
    async fn refresh_metadata(&self) -> Result<()> {
        Err(Error::Unsupported("refresh_metadata"))
    }
}
