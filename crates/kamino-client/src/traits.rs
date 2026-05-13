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
use crate::pubsub::PubSub;
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

    // ---- Phase 6 fragment migration -------------------------------------------------------

    /// `(dmap_name, partition_id)` pairs that currently hold at least one live
    /// entry on this node. The balancer (see [`kamino-cluster`'s] balancer
    /// loop) calls this to enumerate candidate orphans on each tick.
    ///
    /// Default: empty. The embedded backend overrides with a scan-and-bucket
    /// pass; remote-side clients have no notion of "local storage" and return
    /// empty.
    async fn local_partitions(&self) -> Result<Vec<(String, u32)>> {
        Ok(Vec::new())
    }

    /// Serialise every live entry in `dmap` whose key hashes into
    /// `partition_id` for migration. The wire shape is the
    /// `FragmentPayloadV1` codec defined in this crate's `migration`
    /// module — opaque to the caller; only [`Self::import_partition`] on a
    /// peer should decode it.
    async fn export_partition(&self, _dmap: &str, _partition_id: u32) -> Result<Vec<u8>> {
        Err(Error::Unsupported("export_partition"))
    }

    /// LWW-merge the payload produced by [`Self::export_partition`] into local
    /// storage under `(dmap, partition_id)`. Returns the number of entries
    /// that won the merge (`existing.ts < incoming.ts` or no existing entry).
    async fn import_partition(
        &self,
        _dmap: &str,
        _partition_id: u32,
        _payload: &[u8],
    ) -> Result<u32> {
        Err(Error::Unsupported("import_partition"))
    }

    /// Delete every live entry in `dmap` whose key hashes into `partition_id`.
    /// The balancer calls this after a successful
    /// [`Self::export_partition`] + remote `INTERNAL.NODE.MOVEFRAGMENT` round
    /// to clear the orphan locally. Returns the number of entries removed.
    async fn clear_partition(&self, _dmap: &str, _partition_id: u32) -> Result<u32> {
        Err(Error::Unsupported("clear_partition"))
    }

    /// Sweep storage for fragments emptied by recent `clear_partition`
    /// calls. The Phase 6 single-fragment-per-DMap storage handles this
    /// by running `StorageEngine::compact()` on every registered DMap —
    /// reclaiming deleted-entry bytes after a migration round. Returns
    /// the total bytes reclaimed across all dmaps.
    ///
    /// Phase 6 — `routing.check_empty_fragments_interval` periodic
    /// sweep. The default impl is a no-op (remote clients have no local
    /// storage to compact).
    async fn cleanup_empty_fragments(&self) -> Result<usize> {
        Ok(0)
    }

    // ---- Phase 7 pub/sub ------------------------------------------------------------------
    /// Build a pub/sub handle. The default returns an `Unsupported` error;
    /// in-process `EmbeddedClient` overrides this so app code can
    /// `subscribe` / `publish` against a local registry without going
    /// through the RESP wire.
    fn new_pubsub(&self) -> Result<Arc<dyn PubSub>> {
        Err(Error::Unsupported("new_pubsub"))
    }
}
