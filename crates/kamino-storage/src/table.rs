#![allow(clippy::missing_const_for_fn)] // bodies filled in by storage-internals agent

//! `Table` — append-only memory block backing [`crate::RamBlock`].
//!
//! See `docs/05-storage-engine.md` §"Table Structure" and the state machine
//! `ReadWrite → ReadOnly → Recycled`.
//!
//! **Owner**: storage-internals agent.

use std::collections::HashMap;

use crate::engine::TableState;

/// Pre-allocated, append-only byte block with a hash-key → offset index.
#[derive(Debug)]
pub struct Table {
    /// Pre-allocated byte buffer (`storage.table_size` from config).
    pub(crate) memory: Vec<u8>,
    /// Hash key → byte offset within `memory`.
    pub(crate) hkeys: HashMap<u64, usize>,
    /// Bitmap of live entry start offsets (for fast scan and compaction).
    pub(crate) offset_index: roaring::RoaringBitmap,
    /// Current append offset.
    pub(crate) offset: usize,
    /// Number of garbage (deleted/overwritten) entries.
    pub(crate) garbage_count: usize,
    /// Lifecycle state.
    pub(crate) state: TableState,
}

impl Table {
    /// Allocate a fresh `ReadWrite` table sized for `capacity` bytes.
    /// **TODO** (storage-internals agent).
    pub fn with_capacity(capacity: usize) -> Self {
        let _ = capacity;
        unimplemented!("filled by storage-internals agent")
    }

    /// Current state.
    #[must_use]
    pub fn state(&self) -> TableState {
        self.state
    }
}
