//! `RamBlock` — default in-memory [`crate::StorageEngine`] implementation.
//!
//! Bitcask-inspired: append-only `ReadWrite` table plus zero-or-more sealed
//! `ReadOnly` tables. Reads walk tables newest-first; writes always land in
//! the current `ReadWrite` table, with a fresh table allocated when capacity
//! is reached.
//!
//! See `docs/05-storage-engine.md`.

use async_trait::async_trait;
use regex::bytes::Regex;
use tracing::{debug, trace};

use crate::compaction::compact_table;
use crate::engine::{ExportFormat, ScanCallback, StorageEngine, TableState};
use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::table::Table;

/// In-memory append-only engine. Holds a vector of [`Table`]s with the last
/// non-recycled one in `ReadWrite` state.
#[derive(Debug)]
pub struct RamBlock {
    pub(crate) tables: Vec<Table>,
    pub(crate) table_capacity: usize,
    pub(crate) max_garbage_ratio: f64,
}

impl RamBlock {
    /// Construct an empty engine with the supplied per-table capacity and
    /// garbage-ratio compaction threshold.
    #[must_use]
    pub fn new(table_capacity: usize, max_garbage_ratio: f64) -> Self {
        assert!(table_capacity > 0, "table_capacity must be > 0");
        Self {
            tables: vec![Table::with_capacity(table_capacity)],
            table_capacity,
            max_garbage_ratio,
        }
    }

    /// Iterate the live tables — i.e. anything not in `Recycled`.
    fn live_tables(&self) -> impl DoubleEndedIterator<Item = &Table> {
        self.tables
            .iter()
            .filter(|t| t.state() != TableState::Recycled)
    }

    fn ensure_writable_for(&mut self, need: usize) {
        // The last table is the current ReadWrite head. If it can't fit
        // `need`, seal it and push a fresh one. Oversize entries get a
        // one-shot table sized exactly for them.
        let head_idx = self
            .tables
            .iter()
            .rposition(|t| t.state() == TableState::ReadWrite);

        if let Some(idx) = head_idx {
            let capacity = self.tables[idx].capacity();
            let offset = self.tables[idx].offset;
            if offset + need <= capacity {
                return;
            }
            self.tables[idx].seal();
            trace!(
                capacity,
                need, "sealing ReadWrite head; allocating new table"
            );
        } else {
            trace!("no ReadWrite head; allocating new table");
        }

        let cap = need.max(self.table_capacity);
        if cap > self.table_capacity {
            debug!(
                cap,
                self.table_capacity, "oversize entry — allocating one-shot table"
            );
        }
        self.tables.push(Table::with_capacity(cap));
    }

    /// Number of tables (any state) — useful for tests and metrics.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Find the newest table containing a live entry for `hkey`. Walks the
    /// active tables back-to-front.
    fn find_live(&self, hkey: u64) -> Option<usize> {
        for (idx, t) in self.tables.iter().enumerate().rev() {
            if t.state() == TableState::Recycled {
                continue;
            }
            if t.hkeys.contains_key(&hkey) {
                return Some(idx);
            }
        }
        None
    }
}

#[async_trait]
impl StorageEngine for RamBlock {
    async fn put(&mut self, hkey: u64, entry: &Entry) -> Result<()> {
        // Drop any older live copy in a sealed table so reads stay consistent
        // and garbage-byte accounting is accurate.
        if let Some(idx) = self.find_live(hkey) {
            // If the live entry lives in a sealed table, mark it garbage there;
            // if it lives in the current head, `append` will demote it itself.
            if self.tables[idx].state() == TableState::ReadOnly {
                self.tables[idx].delete(hkey)?;
            }
        }

        let need = entry.encoded_len();
        self.ensure_writable_for(need);
        let head = self
            .tables
            .last_mut()
            .expect("ensure_writable_for guarantees");
        let appended = head.append(hkey, entry)?;
        // If we just allocated a fresh head, append must succeed.
        debug_assert!(appended, "fresh head must accept appends");
        Ok(())
    }

    async fn get(&self, hkey: u64) -> Result<Option<Entry>> {
        for t in self.live_tables().rev() {
            if let Some(e) = t.get(hkey)? {
                return Ok(Some(e));
            }
        }
        Ok(None)
    }

    async fn delete(&mut self, hkey: u64) -> Result<bool> {
        let Some(idx) = self.find_live(hkey) else {
            return Ok(false);
        };
        self.tables[idx].delete(hkey)
    }

    async fn scan(&self, callback: ScanCallback<'_>) -> Result<()> {
        for t in self.live_tables() {
            let cont = t.scan(callback)?;
            if !cont {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn scan_regex_match(&self, pattern: &str, callback: ScanCallback<'_>) -> Result<()> {
        let re = Regex::new(pattern).map_err(|e| Error::Regex(e.to_string()))?;
        let mut wrapped = |hk: u64, e: &Entry| -> bool {
            if re.is_match(&e.key) {
                callback(hk, e)
            } else {
                true
            }
        };
        self.scan(&mut wrapped).await
    }

    fn len(&self) -> usize {
        self.live_tables().map(Table::len).sum()
    }

    fn inuse(&self) -> usize {
        self.live_tables().map(Table::inuse).sum()
    }

    async fn export(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.push(ExportFormat::EntriesV1 as u8);
        // Count first, then payload — keeps the format streamable for future
        // versions but here we just emit a u32 count then the entries.
        let count = self.len();
        let count_u32 = u32::try_from(count).map_err(|_| {
            Error::ImportFormat(format!("export count {count} exceeds u32 capacity"))
        })?;
        buf.extend_from_slice(&count_u32.to_le_bytes());
        let mut err: Option<Error> = None;
        let mut cb = |_hk: u64, e: &Entry| -> bool {
            if let Err(encode_err) = e.encode_into(&mut buf) {
                err = Some(encode_err);
                return false;
            }
            true
        };
        self.scan(&mut cb).await?;
        if let Some(e) = err {
            return Err(e);
        }
        Ok(buf)
    }

    async fn import(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Err(Error::ImportFormat("empty payload".into()));
        }
        let tag = data[0];
        if tag != ExportFormat::EntriesV1 as u8 {
            return Err(Error::ImportFormat(format!("unknown format tag {tag}")));
        }
        if data.len() < 5 {
            return Err(Error::ImportFormat("missing count header".into()));
        }
        let mut count_bytes = [0_u8; 4];
        count_bytes.copy_from_slice(&data[1..5]);
        let count = u32::from_le_bytes(count_bytes);

        let mut cursor = 5;
        for _ in 0..count {
            let (entry, used) = Entry::decode(&data[cursor..])?;
            cursor += used;
            let hkey = hash_key(&entry.key);
            // LWW merge: keep whichever timestamp wins.
            match self.get(hkey).await? {
                Some(existing) if existing.timestamp_nanos >= entry.timestamp_nanos => {
                    trace!(?hkey, "import skipped: existing entry wins LWW");
                }
                _ => {
                    self.put(hkey, &entry).await?;
                }
            }
        }
        Ok(())
    }

    async fn compact(&mut self) -> Result<usize> {
        let mut reclaimed = 0;
        let mut replacements: Vec<(usize, Table)> = Vec::new();
        for (idx, t) in self.tables.iter().enumerate() {
            if t.state() != TableState::ReadOnly {
                continue;
            }
            if t.garbage_ratio() <= self.max_garbage_ratio {
                continue;
            }
            let before = t.inuse() + t.garbage_bytes();
            let new = compact_table(t, t.capacity())?;
            let after = new.inuse();
            reclaimed += before.saturating_sub(after);
            replacements.push((idx, new));
            debug!(
                idx,
                before,
                after,
                ratio = t.garbage_ratio(),
                "compacted table"
            );
        }
        for (idx, mut new) in replacements {
            // New table is in ReadWrite, but it's not the current head: seal it.
            new.seal();
            self.tables[idx].recycle();
            self.tables[idx] = new;
        }
        Ok(reclaimed)
    }
}

/// Tests use this so they can `import` the same `hkey` the engine generates.
/// Kept private — production code resolves `hkey` upstream of the engine.
fn hash_key(key: &[u8]) -> u64 {
    // xxh3 is the workspace's chosen hash (see `Cargo.toml`). Using it here
    // would couple the engine to a hash choice that belongs to a higher
    // layer; for LWW import we just need *some* deterministic hash. Use the
    // std `DefaultHasher` to avoid pulling in an extra dep.
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    fn entry(key: &[u8], value: &[u8], ts: i64) -> Entry {
        Entry {
            key: key.to_vec(),
            ttl_nanos: 0,
            timestamp_nanos: ts,
            last_access_nanos: ts,
            value: value.to_vec(),
        }
    }

    #[tokio::test]
    async fn put_get_delete_basic() {
        let mut rb = RamBlock::new(4096, 0.4);
        let e = entry(b"k", b"v", 1);
        rb.put(1, &e).await.unwrap();
        assert_eq!(rb.get(1).await.unwrap(), Some(e.clone()));
        assert_eq!(rb.len(), 1);
        assert_eq!(rb.inuse(), e.encoded_len());
        assert!(rb.delete(1).await.unwrap());
        assert_eq!(rb.get(1).await.unwrap(), None);
        assert_eq!(rb.len(), 0);
        assert!(!rb.delete(1).await.unwrap());
    }

    #[tokio::test]
    async fn overwrite_returns_latest() {
        let mut rb = RamBlock::new(4096, 0.4);
        rb.put(1, &entry(b"k", b"first", 1)).await.unwrap();
        rb.put(1, &entry(b"k", b"second", 2)).await.unwrap();
        let got = rb.get(1).await.unwrap().unwrap();
        assert_eq!(got.value, b"second");
        assert_eq!(rb.len(), 1);
    }

    #[tokio::test]
    async fn writes_flow_into_new_table_when_full() {
        let e = entry(b"key", b"value", 1);
        // Capacity for exactly two entries.
        let mut rb = RamBlock::new(e.encoded_len() * 2, 0.4);
        for i in 0..5_u64 {
            rb.put(i, &e).await.unwrap();
        }
        assert!(rb.table_count() >= 3);
        for i in 0..5_u64 {
            assert!(rb.get(i).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn cross_table_overwrite_drops_old_value() {
        let small_entry = entry(b"k", b"v", 1);
        let mut rb = RamBlock::new(small_entry.encoded_len(), 0.4);
        rb.put(1, &small_entry).await.unwrap(); // table 0 full, sealed on next.
        rb.put(2, &small_entry).await.unwrap(); // forces a new table.
        assert!(rb.table_count() >= 2);

        // Overwrite hkey 1 (in the old, sealed table) — value goes into the head.
        let updated = entry(b"k", b"v2", 5);
        rb.put(1, &updated).await.unwrap();

        let got = rb.get(1).await.unwrap().unwrap();
        assert_eq!(got.value, b"v2");
        assert_eq!(rb.len(), 2);
    }

    #[tokio::test]
    async fn oversize_entry_lands_in_own_table() {
        let mut rb = RamBlock::new(64, 0.4);
        let big = entry(b"k", &vec![0xaa; 1024], 1);
        rb.put(99, &big).await.unwrap();
        assert_eq!(rb.get(99).await.unwrap().unwrap(), big);
        // The oversize entry must have triggered allocation of a one-shot table.
        assert!(rb.table_count() >= 2);
    }

    #[tokio::test]
    async fn scan_visits_every_live_entry() {
        let mut rb = RamBlock::new(256, 0.4);
        for i in 0_u64..10 {
            rb.put(
                i,
                &entry(format!("k{i}").as_bytes(), b"v", i64::try_from(i).unwrap()),
            )
            .await
            .unwrap();
        }
        rb.delete(3).await.unwrap();
        rb.delete(7).await.unwrap();
        let mut seen: Vec<u64> = Vec::new();
        rb.scan(&mut |hk, _e| {
            seen.push(hk);
            true
        })
        .await
        .unwrap();
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 4, 5, 6, 8, 9]);
    }

    #[tokio::test]
    async fn scan_regex_match_filters_by_key() {
        let mut rb = RamBlock::new(4096, 0.4);
        for &(hk, k) in &[(1_u64, "apple"), (2, "apricot"), (3, "banana")] {
            rb.put(hk, &entry(k.as_bytes(), b"v", 1)).await.unwrap();
        }
        let mut keys: Vec<Vec<u8>> = Vec::new();
        rb.scan_regex_match("^ap", &mut |_hk, e| {
            keys.push(e.key.clone());
            true
        })
        .await
        .unwrap();
        keys.sort();
        assert_eq!(keys, vec![b"apple".to_vec(), b"apricot".to_vec()]);
    }

    #[tokio::test]
    async fn export_import_round_trip() {
        let mut a = RamBlock::new(4096, 0.4);
        let keys: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
        for (i, k) in keys.iter().enumerate() {
            let hk = hash_key(k);
            let ts = i64::try_from(i).unwrap() + 1;
            a.put(hk, &entry(k, b"v", ts)).await.unwrap();
        }
        let payload = a.export().await.unwrap();
        let mut b = RamBlock::new(4096, 0.4);
        b.import(&payload).await.unwrap();
        assert_eq!(b.len(), 3);
        for k in &keys {
            let hk = hash_key(k);
            assert!(b.get(hk).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn import_lww_keeps_higher_timestamp() {
        let mut rb = RamBlock::new(4096, 0.4);
        let hk = hash_key(b"x");
        rb.put(hk, &entry(b"x", b"newer", 10)).await.unwrap();

        // Build an export payload with an older timestamp for the same key.
        let mut payload = Vec::new();
        payload.push(ExportFormat::EntriesV1 as u8);
        payload.extend_from_slice(&1_u32.to_le_bytes());
        entry(b"x", b"older", 5).encode_into(&mut payload).unwrap();
        rb.import(&payload).await.unwrap();

        assert_eq!(rb.get(hk).await.unwrap().unwrap().value, b"newer");

        // Same key with a NEWER timestamp wins.
        let mut payload = Vec::new();
        payload.push(ExportFormat::EntriesV1 as u8);
        payload.extend_from_slice(&1_u32.to_le_bytes());
        entry(b"x", b"newest", 20)
            .encode_into(&mut payload)
            .unwrap();
        rb.import(&payload).await.unwrap();
        assert_eq!(rb.get(hk).await.unwrap().unwrap().value, b"newest");
    }

    #[tokio::test]
    async fn put_lww_rejects_older_timestamp() {
        // Phase 5 — backup-side replication relies on LWW so out-of-order
        // arrivals can't overwrite a fresher entry.
        let mut rb = RamBlock::new(4096, 0.4);
        rb.put(1, &entry(b"k", b"newer", 10)).await.unwrap();
        let applied = rb.put_lww(1, &entry(b"k", b"older", 5)).await.unwrap();
        assert!(!applied, "older timestamp must lose the merge");
        assert_eq!(rb.get(1).await.unwrap().unwrap().value, b"newer");
    }

    #[tokio::test]
    async fn put_lww_applies_newer_timestamp() {
        let mut rb = RamBlock::new(4096, 0.4);
        rb.put(1, &entry(b"k", b"older", 5)).await.unwrap();
        let applied = rb.put_lww(1, &entry(b"k", b"newer", 10)).await.unwrap();
        assert!(applied, "newer timestamp must apply");
        assert_eq!(rb.get(1).await.unwrap().unwrap().value, b"newer");
    }

    #[tokio::test]
    async fn put_lww_writes_when_absent() {
        let mut rb = RamBlock::new(4096, 0.4);
        let applied = rb.put_lww(7, &entry(b"k", b"v", 1)).await.unwrap();
        assert!(applied, "first insert always applies");
        assert_eq!(rb.get(7).await.unwrap().unwrap().value, b"v");
    }

    #[tokio::test]
    async fn put_lww_equal_timestamp_existing_wins() {
        // Equal timestamps: existing entry wins (deterministic — primary's
        // monotonic clock guarantees strict-monotonicity per partition, so a
        // tie can only happen across primaries during split-brain; tie-break
        // by "first writer keeps" matches existing import() semantics).
        let mut rb = RamBlock::new(4096, 0.4);
        rb.put(1, &entry(b"k", b"first", 10)).await.unwrap();
        let applied = rb.put_lww(1, &entry(b"k", b"second", 10)).await.unwrap();
        assert!(!applied);
        assert_eq!(rb.get(1).await.unwrap().unwrap().value, b"first");
    }

    #[tokio::test]
    async fn import_rejects_unknown_format() {
        let mut rb = RamBlock::new(4096, 0.4);
        let bad = vec![0xff, 0, 0, 0, 0];
        assert_matches!(rb.import(&bad).await, Err(Error::ImportFormat(_)));
    }

    #[tokio::test]
    async fn import_rejects_empty_payload() {
        let mut rb = RamBlock::new(4096, 0.4);
        assert_matches!(rb.import(&[]).await, Err(Error::ImportFormat(_)));
    }

    #[tokio::test]
    async fn compact_reclaims_garbage_and_preserves_live() {
        // Make a small capacity so we can fill, overwrite, then compact.
        let e = entry(b"k", b"v", 1);
        let cap = e.encoded_len() * 4;
        let mut rb = RamBlock::new(cap, 0.1);

        // Fill first table with 4 entries.
        for i in 0..4_u64 {
            rb.put(i, &entry(format!("k{i}").as_bytes(), b"v", 1))
                .await
                .unwrap();
        }
        // Force a new head by pushing one more — old table becomes ReadOnly.
        rb.put(99, &entry(b"k99", b"v", 1)).await.unwrap();

        // Now delete entries in the sealed table to create garbage.
        rb.delete(0).await.unwrap();
        rb.delete(1).await.unwrap();
        rb.delete(2).await.unwrap();
        let live_before = rb.len();
        let inuse_before = rb.inuse();

        let reclaimed = rb.compact().await.unwrap();
        assert!(reclaimed > 0, "expected to reclaim some bytes");
        assert_eq!(rb.len(), live_before, "compaction must preserve live count");
        assert!(rb.inuse() <= inuse_before);

        // All surviving entries still readable.
        assert!(rb.get(3).await.unwrap().is_some());
        assert!(rb.get(99).await.unwrap().is_some());
        assert_eq!(rb.get(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn compact_skips_tables_below_threshold() {
        let mut rb = RamBlock::new(4096, 0.99);
        rb.put(1, &entry(b"k1", b"v", 1)).await.unwrap();
        // Force a seal by allocating a new head.
        for i in 2..10_u64 {
            rb.put(i, &entry(format!("k{i}").as_bytes(), b"v", 1))
                .await
                .unwrap();
        }
        // Threshold is 0.99 — nothing should compact.
        let reclaimed = rb.compact().await.unwrap();
        assert_eq!(reclaimed, 0);
    }

    #[tokio::test]
    async fn scan_callback_can_halt_iteration() {
        let mut rb = RamBlock::new(4096, 0.4);
        for i in 0..5_u64 {
            rb.put(i, &entry(format!("k{i}").as_bytes(), b"v", 1))
                .await
                .unwrap();
        }
        let mut n = 0;
        rb.scan(&mut |_hk, _e| {
            n += 1;
            n < 2
        })
        .await
        .unwrap();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn put_rejects_oversize_key() {
        let mut rb = RamBlock::new(4096, 0.4);
        let bad = Entry {
            key: vec![0; crate::entry::MAX_KEY_LEN + 1],
            ttl_nanos: 0,
            timestamp_nanos: 0,
            last_access_nanos: 0,
            value: Vec::new(),
        };
        assert_matches!(rb.put(1, &bad).await, Err(Error::KeyTooLarge { .. }));
    }
}
