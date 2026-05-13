//! `RamBlock` — default in-memory [`crate::StorageEngine`] implementation.
//!
//! Bitcask-inspired: append-only `ReadWrite` table plus zero-or-more sealed
//! `ReadOnly` tables. Reads walk tables newest-first; writes always land in
//! the current `ReadWrite` table, with a fresh table allocated when capacity
//! is reached.
//!
//! See `docs/05-storage-engine.md`.
//!
//! **Owner**: storage-internals agent.

use async_trait::async_trait;

use crate::engine::{ScanCallback, StorageEngine};
use crate::entry::Entry;
use crate::error::Result;
use crate::table::Table;

/// In-memory append-only engine. Holds a vector of [`Table`]s with the last
/// one in `ReadWrite` state.
#[derive(Debug)]
pub struct RamBlock {
    pub(crate) tables: Vec<Table>,
    pub(crate) table_capacity: usize,
    pub(crate) max_garbage_ratio: f64,
}

impl RamBlock {
    /// Construct an empty engine with the supplied per-table capacity and
    /// garbage-ratio compaction threshold.
    /// **TODO** (storage-internals agent).
    #[must_use]
    pub fn new(table_capacity: usize, max_garbage_ratio: f64) -> Self {
        let _ = (table_capacity, max_garbage_ratio);
        unimplemented!("filled by storage-internals agent")
    }
}

#[async_trait]
impl StorageEngine for RamBlock {
    async fn put(&mut self, hkey: u64, entry: &Entry) -> Result<()> {
        let _ = (hkey, entry);
        unimplemented!("filled by storage-internals agent")
    }
    async fn get(&self, hkey: u64) -> Result<Option<Entry>> {
        let _ = hkey;
        unimplemented!("filled by storage-internals agent")
    }
    async fn delete(&mut self, hkey: u64) -> Result<bool> {
        let _ = hkey;
        unimplemented!("filled by storage-internals agent")
    }
    async fn scan(&self, callback: ScanCallback<'_>) -> Result<()> {
        let _ = callback;
        unimplemented!("filled by storage-internals agent")
    }
    async fn scan_regex_match(&self, pattern: &str, callback: ScanCallback<'_>) -> Result<()> {
        let _ = (pattern, callback);
        unimplemented!("filled by storage-internals agent")
    }
    fn len(&self) -> usize {
        unimplemented!("filled by storage-internals agent")
    }
    fn inuse(&self) -> usize {
        unimplemented!("filled by storage-internals agent")
    }
    async fn export(&self) -> Result<Vec<u8>> {
        unimplemented!("filled by storage-internals agent")
    }
    async fn import(&mut self, data: &[u8]) -> Result<()> {
        let _ = data;
        unimplemented!("filled by storage-internals agent")
    }
    async fn compact(&mut self) -> Result<usize> {
        unimplemented!("filled by storage-internals agent")
    }
}
