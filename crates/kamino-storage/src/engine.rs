//! [`StorageEngine`] trait + shared types.
//!
//! `Fragment` wraps `Box<dyn StorageEngine>` in a `tokio::sync::RwLock`, so
//! the trait must be **object-safe**. That constraint shapes the API:
//!
//! - Scan callbacks are taken by `&mut dyn FnMut(...) -> bool + Send` rather
//!   than a generic `F`.
//! - All methods are explicit `async fn` via `#[async_trait]`.
//! - No GATs, no `Self`-returning helpers, no associated types beyond what
//!   `Send + Sync` requires.

use async_trait::async_trait;

use crate::entry::Entry;
use crate::error::Result;

/// State machine of a [`crate::Table`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableState {
    /// Current table accepting appends.
    ReadWrite,
    /// Full table — reads only; eligible for compaction.
    ReadOnly,
    /// Compacted out and ready for reuse.
    Recycled,
}

/// Self-describing format tag used by [`StorageEngine::export`] /
/// [`StorageEngine::import`]. The byte appears as the first byte of the
/// export payload so importers can pick the right decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExportFormat {
    /// Concatenated `Entry` records in the canonical binary layout, length-prefixed.
    EntriesV1 = 1,
}

/// Callback shape passed to scanners. Returning `false` halts iteration.
///
/// The `&mut dyn FnMut` form keeps [`StorageEngine`] object-safe — a generic
/// `F` would not be callable through `Box<dyn StorageEngine>`.
pub type ScanCallback<'a> = &'a mut (dyn FnMut(u64, &Entry) -> bool + Send);

/// Object-safe storage-engine contract.
///
/// **Concurrency**: implementations may rely on the fact that callers serialise
/// `&mut self` access via `Fragment`'s `RwLock` (write side). Read methods
/// (`&self`) may be called concurrently, so internal state used by reads must
/// be `Sync`.
///
/// **Async**: `RamBlock` is purely in-memory and its futures complete in a
/// single poll. Disk-backed engines may do real I/O; the trait is `async`
/// throughout so they integrate without `spawn_blocking` ceremony at the call
/// site.
#[async_trait]
pub trait StorageEngine: Send + Sync + std::fmt::Debug {
    /// Store an entry, overwriting any prior value for `hkey`.
    async fn put(&mut self, hkey: u64, entry: &Entry) -> Result<()>;

    /// LWW-merge variant of [`Self::put`]. Writes `entry` iff the existing
    /// entry for `hkey` is absent OR has a strictly smaller
    /// `timestamp_nanos`. Returns `true` if the new entry was applied,
    /// `false` if the existing entry won the merge.
    ///
    /// Backup replication uses this so out-of-order replication arrivals
    /// (a later primary write reaching a backup before an earlier one) do
    /// not overwrite a newer entry. Phase 5 — `docs/04-replication.md`
    /// "Conflict Resolution: Last-Write-Wins (LWW)".
    async fn put_lww(&mut self, hkey: u64, entry: &Entry) -> Result<bool> {
        match self.get(hkey).await? {
            Some(existing) if existing.timestamp_nanos >= entry.timestamp_nanos => Ok(false),
            _ => {
                self.put(hkey, entry).await?;
                Ok(true)
            }
        }
    }

    /// Return the live entry for `hkey`, or `None`. Bumps `last_access`
    /// implicitly is the **caller's** responsibility — the engine does not
    /// mutate metadata on read.
    async fn get(&self, hkey: u64) -> Result<Option<Entry>>;

    /// Mark `hkey` as garbage. Returns `true` if a live entry was deleted.
    async fn delete(&mut self, hkey: u64) -> Result<bool>;

    /// Walk every live entry. The callback returns `false` to stop.
    async fn scan(&self, callback: ScanCallback<'_>) -> Result<()>;

    /// Walk every live entry whose key matches `pattern` (regex).
    async fn scan_regex_match(&self, pattern: &str, callback: ScanCallback<'_>) -> Result<()>;

    /// Number of live entries.
    fn len(&self) -> usize;

    /// Total bytes occupied by live entries (excludes garbage and free space).
    fn inuse(&self) -> usize;

    /// `true` if [`Self::len`] is zero. Default impl provided.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Serialise every live entry for migration. First byte is the
    /// [`ExportFormat`] tag.
    async fn export(&self) -> Result<Vec<u8>>;

    /// Inverse of [`Self::export`]. Existing entries are merged with the
    /// imported ones using LWW (`timestamp_nanos`).
    async fn import(&mut self, data: &[u8]) -> Result<()>;

    /// Run a compaction pass. Returns the number of bytes reclaimed.
    /// Default impl returns `0` for engines without garbage (e.g. some
    /// disk-backed engines that compact in the background).
    async fn compact(&mut self) -> Result<usize> {
        Ok(0)
    }
}
