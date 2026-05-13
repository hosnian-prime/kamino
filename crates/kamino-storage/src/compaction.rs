//! Compaction helpers used by [`crate::RamBlock::compact`].
//!
//! See `docs/05-storage-engine.md` §"Compaction".

use crate::error::Result;
use crate::table::Table;

/// Copy every live entry from `src` into a fresh `ReadWrite` table.
///
/// The returned table is **not** sealed — the caller transitions it to
/// `ReadOnly` once it is wired into the engine. `capacity` is taken as a
/// parameter (rather than `src.capacity()`) so the caller can shrink a
/// sparsely-populated table during compaction.
pub fn compact_table(src: &Table, capacity: usize) -> Result<Table> {
    let live = src.inuse();
    let cap = capacity.max(live);
    let mut dst = Table::with_capacity(cap);
    let mut err: Option<crate::error::Error> = None;
    src.scan(&mut |hk, entry| {
        if let Err(e) = dst.append(hk, entry) {
            err = Some(e);
            return false;
        }
        true
    })?;
    if let Some(e) = err {
        return Err(e);
    }
    Ok(dst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TableState;
    use crate::entry::Entry;

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
    fn copies_live_entries_and_drops_garbage() {
        let mut src = Table::with_capacity(4096);
        for i in 0_u64..5 {
            src.append(
                i,
                &entry(&[u8::try_from(i).unwrap()], b"v", i64::try_from(i).unwrap()),
            )
            .unwrap();
        }
        src.delete(1).unwrap();
        src.delete(3).unwrap();
        assert!(src.garbage_bytes() > 0);

        let dst = compact_table(&src, src.capacity()).unwrap();
        assert_eq!(dst.state(), TableState::ReadWrite);
        assert_eq!(dst.len(), 3);
        assert_eq!(dst.garbage_bytes(), 0);
        for i in [0_u64, 2, 4] {
            assert!(dst.get(i).unwrap().is_some());
        }
        for i in [1_u64, 3] {
            assert!(dst.get(i).unwrap().is_none());
        }
    }

    #[test]
    fn shrinks_when_caller_passes_smaller_capacity() {
        let mut src = Table::with_capacity(4096);
        let e = entry(b"k", b"v", 1);
        src.append(1, &e).unwrap();
        // Caller asks for a tighter capacity; helper still fits the live data.
        let dst = compact_table(&src, 0).unwrap();
        assert!(dst.capacity() >= e.encoded_len());
        assert_eq!(dst.len(), 1);
    }

    #[test]
    fn empty_source_yields_empty_destination() {
        let src = Table::with_capacity(1024);
        let dst = compact_table(&src, 1024).unwrap();
        assert_eq!(dst.len(), 0);
        assert_eq!(dst.state(), TableState::ReadWrite);
    }
}
