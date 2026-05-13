//! `Table` — append-only memory block backing [`crate::RamBlock`].
//!
//! See `docs/05-storage-engine.md` §"Table Structure" and the state machine
//! `ReadWrite → ReadOnly → Recycled`.

use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::engine::TableState;
use crate::entry::Entry;
use crate::error::Result;

/// Pre-allocated, append-only byte block with a hash-key → offset index.
#[derive(Debug)]
pub struct Table {
    pub(crate) memory: Vec<u8>,
    pub(crate) hkeys: HashMap<u64, usize>,
    pub(crate) offset_index: RoaringBitmap,
    pub(crate) offset: usize,
    pub(crate) garbage_bytes: usize,
    pub(crate) live_bytes: usize,
    pub(crate) state: TableState,
}

impl Table {
    /// Allocate a fresh `ReadWrite` table sized for `capacity` bytes.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            memory: Vec::with_capacity(capacity),
            hkeys: HashMap::new(),
            offset_index: RoaringBitmap::new(),
            offset: 0,
            garbage_bytes: 0,
            live_bytes: 0,
            state: TableState::ReadWrite,
        }
    }

    /// Pre-allocated buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    /// Current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> TableState {
        self.state
    }

    /// Live (non-garbage) entries count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.hkeys.len()
    }

    /// `true` if no live entries remain.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hkeys.is_empty()
    }

    /// Live bytes still indexed in this table.
    #[must_use]
    pub const fn inuse(&self) -> usize {
        self.live_bytes
    }

    /// Bytes of garbage (deleted/overwritten entries) accumulated so far.
    #[must_use]
    pub const fn garbage_bytes(&self) -> usize {
        self.garbage_bytes
    }

    /// Ratio of garbage bytes over total bytes appended (0.0 when the table
    /// has never been written to). A high ratio drives compaction.
    #[must_use]
    pub fn garbage_ratio(&self) -> f64 {
        if self.offset == 0 {
            return 0.0;
        }
        // `as` cast: usize → f64 is the conventional way to compute ratios
        // and rounding error is fine for a heuristic threshold.
        #[allow(clippy::cast_precision_loss)]
        let g = self.garbage_bytes as f64;
        #[allow(clippy::cast_precision_loss)]
        let t = self.offset as f64;
        g / t
    }

    /// Append `entry` indexed by `hkey`. Returns `true` on success, `false`
    /// when the table has no room (caller must seal + allocate a new one).
    /// If `hkey` already has a live entry, the previous one is marked garbage.
    pub fn append(&mut self, hkey: u64, entry: &Entry) -> Result<bool> {
        debug_assert_eq!(
            self.state,
            TableState::ReadWrite,
            "append to non-RW table is a programmer error"
        );
        let need = entry.encoded_len();
        if self.offset + need > self.memory.capacity() {
            return Ok(false);
        }

        let start = self.offset;
        entry.encode_into(&mut self.memory)?;
        self.offset += need;

        if let Some(prev_off) = self.hkeys.insert(hkey, start) {
            self.offset_index.remove(offset_to_u32(prev_off));
            let prev = self.decode_at(prev_off)?;
            let prev_len = prev.encoded_len();
            self.garbage_bytes += prev_len;
            self.live_bytes = self.live_bytes.saturating_sub(prev_len);
        }
        self.offset_index.insert(offset_to_u32(start));
        self.live_bytes += need;

        Ok(true)
    }

    /// Look up a live entry by `hkey`.
    pub fn get(&self, hkey: u64) -> Result<Option<Entry>> {
        let Some(&off) = self.hkeys.get(&hkey) else {
            return Ok(None);
        };
        Ok(Some(self.decode_at(off)?))
    }

    /// Mark `hkey`'s live entry as garbage. Returns `true` if a live entry
    /// was actually deleted.
    pub fn delete(&mut self, hkey: u64) -> Result<bool> {
        let Some(off) = self.hkeys.remove(&hkey) else {
            return Ok(false);
        };
        self.offset_index.remove(offset_to_u32(off));
        let prev = self.decode_at(off)?;
        let prev_len = prev.encoded_len();
        self.garbage_bytes += prev_len;
        self.live_bytes = self.live_bytes.saturating_sub(prev_len);
        Ok(true)
    }

    /// Visit every live entry. Callback returns `false` to halt iteration.
    /// Returns `false` if the callback bailed out, `true` if every entry was
    /// visited.
    pub fn scan(&self, callback: &mut (dyn FnMut(u64, &Entry) -> bool + Send)) -> Result<bool> {
        // We need to call back with `hkey`, so invert hkeys into offset → hkey
        // ordering. Building a small temp map keeps the iteration cost linear
        // and avoids quadratic lookups.
        let mut off_to_hkey: HashMap<u32, u64> = HashMap::with_capacity(self.hkeys.len());
        for (&hk, &off) in &self.hkeys {
            off_to_hkey.insert(offset_to_u32(off), hk);
        }
        for off in &self.offset_index {
            let Some(&hk) = off_to_hkey.get(&off) else {
                continue;
            };
            let entry = self.decode_at(off as usize)?;
            if !callback(hk, &entry) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Transition `ReadWrite → ReadOnly`. No-op if already sealed.
    pub fn seal(&mut self) {
        if self.state == TableState::ReadWrite {
            self.state = TableState::ReadOnly;
        }
    }

    /// Transition into `Recycled`.
    pub fn recycle(&mut self) {
        self.state = TableState::Recycled;
    }

    fn decode_at(&self, off: usize) -> Result<Entry> {
        let (entry, _) = Entry::decode(&self.memory[off..])?;
        Ok(entry)
    }
}

/// Narrow a byte offset to `u32` for `RoaringBitmap`. Tables are bounded by
/// `table_capacity` (default 1 MiB, max practical 4 GiB) so this is infallible
/// for valid offsets; on a buggy caller the assertion fires loudly instead of
/// silently truncating.
fn offset_to_u32(off: usize) -> u32 {
    u32::try_from(off).expect("table offsets must fit in u32 (≤ 4 GiB tables)")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &[u8], value: &[u8], ts: i64) -> Entry {
        Entry {
            key: key.to_vec(),
            ttl_nanos: 0,
            timestamp_nanos: ts,
            last_access_nanos: ts,
            value: value.to_vec(),
        }
    }

    #[test]
    fn new_table_is_empty_and_readwrite() {
        let t = Table::with_capacity(1024);
        assert_eq!(t.state(), TableState::ReadWrite);
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
        assert_eq!(t.inuse(), 0);
        assert_eq!(t.garbage_bytes(), 0);
        assert!((t.garbage_ratio() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn append_then_get_round_trips() {
        let mut t = Table::with_capacity(4096);
        let e = entry(b"k", b"v", 1);
        assert!(t.append(42, &e).unwrap());
        assert_eq!(t.len(), 1);
        assert_eq!(t.inuse(), e.encoded_len());
        assert_eq!(t.get(42).unwrap(), Some(e));
        assert_eq!(t.get(43).unwrap(), None);
    }

    #[test]
    fn overwrite_marks_previous_garbage() {
        let mut t = Table::with_capacity(4096);
        let a = entry(b"k", b"v1", 1);
        let b = entry(b"k", b"v2-longer", 2);
        t.append(7, &a).unwrap();
        t.append(7, &b).unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t.garbage_bytes(), a.encoded_len());
        assert_eq!(t.inuse(), b.encoded_len());
        assert_eq!(t.get(7).unwrap(), Some(b));
    }

    #[test]
    fn delete_returns_true_only_on_first_call() {
        let mut t = Table::with_capacity(4096);
        let e = entry(b"k", b"v", 1);
        t.append(1, &e).unwrap();
        assert!(t.delete(1).unwrap());
        assert!(!t.delete(1).unwrap());
        assert_eq!(t.len(), 0);
        assert_eq!(t.garbage_bytes(), e.encoded_len());
        assert_eq!(t.inuse(), 0);
    }

    #[test]
    fn append_returns_false_when_full() {
        let e = entry(b"key", b"value", 1);
        let mut t = Table::with_capacity(e.encoded_len()); // exactly one entry
        assert!(t.append(1, &e).unwrap());
        assert!(!t.append(2, &e).unwrap());
    }

    #[test]
    fn seal_blocks_further_appends_in_debug() {
        let mut t = Table::with_capacity(1024);
        t.seal();
        assert_eq!(t.state(), TableState::ReadOnly);
        // Calling seal again is idempotent.
        t.seal();
        assert_eq!(t.state(), TableState::ReadOnly);
    }

    #[test]
    fn state_transitions_through_recycled() {
        let mut t = Table::with_capacity(1024);
        assert_eq!(t.state(), TableState::ReadWrite);
        t.seal();
        assert_eq!(t.state(), TableState::ReadOnly);
        t.recycle();
        assert_eq!(t.state(), TableState::Recycled);
    }

    #[test]
    fn scan_visits_all_live_entries() {
        let mut t = Table::with_capacity(4096);
        for i in 0_u64..5 {
            t.append(
                i,
                &entry(&[u8::try_from(i).unwrap()], b"x", i64::try_from(i).unwrap()),
            )
            .unwrap();
        }
        t.delete(2).unwrap();
        let mut seen = Vec::new();
        t.scan(&mut |hk, _e| {
            seen.push(hk);
            true
        })
        .unwrap();
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 3, 4]);
    }

    #[test]
    fn scan_callback_can_halt_iteration() {
        let mut t = Table::with_capacity(4096);
        for i in 0_u64..5 {
            t.append(
                i,
                &entry(&[u8::try_from(i).unwrap()], b"x", i64::try_from(i).unwrap()),
            )
            .unwrap();
        }
        let mut visited = 0;
        let finished = t
            .scan(&mut |_hk, _e| {
                visited += 1;
                visited < 2
            })
            .unwrap();
        assert!(!finished);
        assert_eq!(visited, 2);
    }

    #[test]
    fn garbage_ratio_tracks_overwrites() {
        let mut t = Table::with_capacity(4096);
        let a = entry(b"k", b"v", 1);
        let b = entry(b"k", b"v", 2);
        t.append(1, &a).unwrap();
        t.append(1, &b).unwrap();
        // After overwrite, half the appended bytes are garbage.
        assert!((t.garbage_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    #[should_panic(expected = "append to non-RW table")]
    fn debug_panics_when_appending_to_sealed_table() {
        let mut t = Table::with_capacity(1024);
        t.seal();
        let _ = t.append(1, &entry(b"k", b"v", 0));
    }
}
