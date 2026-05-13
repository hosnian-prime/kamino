//! Fragment-migration wire codec.
//!
//! Phase 6 — `docs/12-failure-handling.md` "Ownership Transfer Protocol".
//!
//! A `FragmentPayloadV1` is the body carried by
//! `INTERNAL.NODE.MOVEFRAGMENT`'s last argument. It is *not* the same shape
//! as `RamBlock`'s engine-level `export`: that one preserves engine state
//! (table boundaries, garbage offsets) while this one is a flat,
//! engine-agnostic list of [`Entry`] records keyed by hash.
//!
//! Layout (little-endian, unaligned):
//!
//! ```text
//! ┌──────────┬───────────┬─────────────┬──────────────────────────┐
//! │ tag (1B) │ part (4B) │ count (4B)  │ count × Entry::encode    │
//! └──────────┴───────────┴─────────────┴──────────────────────────┘
//!     0x01      u32 LE       u32 LE      (key_len|key|...|value)
//! ```
//!
//! The `partition_id` field is informational — it lets the receiver assert
//! the wire payload matches the partition declared in the `INTERNAL.NODE.MOVEFRAGMENT`
//! verb. A mismatch is treated as a programming error and replied to with
//! `-MIGRATION partition_mismatch`.

use kamino_storage::Entry;

use crate::error::{Error, Result};

/// Single-byte version tag at the start of every fragment payload.
///
/// Bumping the tag is reserved for hard-fork wire changes; minor additive
/// evolution reuses the existing tag and gates on remaining buffer length.
pub const FRAGMENT_PAYLOAD_TAG_V1: u8 = 0x01;

/// Encode `entries` into a `FragmentPayloadV1` blob for migration.
///
/// `partition_id` is embedded in the payload so the receiver can sanity-
/// check it against the verb argument before any LWW-merge work begins.
pub fn encode_fragment_payload(partition_id: u32, entries: &[Entry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9 + entries.iter().map(Entry::encoded_len).sum::<usize>());
    buf.push(FRAGMENT_PAYLOAD_TAG_V1);
    buf.extend_from_slice(&partition_id.to_le_bytes());
    let count = u32::try_from(entries.len()).unwrap_or(u32::MAX);
    buf.extend_from_slice(&count.to_le_bytes());
    for e in entries {
        // `Entry::encode_into` only fails on key-length overflow (≥ 256B);
        // upstream code already rejects such writes at the put boundary, so
        // a panic here is genuinely unreachable in well-formed callers.
        e.encode_into(&mut buf)
            .expect("Entry::encode_into never fails on validated entries");
    }
    buf
}

/// Inverse of [`encode_fragment_payload`].
///
/// Returns the embedded `partition_id` (caller cross-checks against the
/// verb argument) and the decoded entries.
pub fn decode_fragment_payload(buf: &[u8]) -> Result<(u32, Vec<Entry>)> {
    if buf.is_empty() {
        return Err(Error::InvalidArgument("empty fragment payload".into()));
    }
    let tag = buf[0];
    if tag != FRAGMENT_PAYLOAD_TAG_V1 {
        return Err(Error::InvalidArgument(format!(
            "unknown fragment payload tag {tag}"
        )));
    }
    if buf.len() < 9 {
        return Err(Error::InvalidArgument(
            "fragment payload header truncated".into(),
        ));
    }
    let mut part_bytes = [0_u8; 4];
    part_bytes.copy_from_slice(&buf[1..5]);
    let partition_id = u32::from_le_bytes(part_bytes);

    let mut count_bytes = [0_u8; 4];
    count_bytes.copy_from_slice(&buf[5..9]);
    let count = u32::from_le_bytes(count_bytes);

    let mut cursor = 9_usize;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (entry, used) = Entry::decode(&buf[cursor..])
            .map_err(|e| Error::InvalidArgument(format!("fragment entry decode failed: {e}")))?;
        cursor += used;
        entries.push(entry);
    }
    Ok((partition_id, entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn entry(key: &[u8], val: &[u8], ts: i64) -> Entry {
        Entry {
            key: key.to_vec(),
            ttl_nanos: 0,
            timestamp_nanos: ts,
            last_access_nanos: ts,
            value: val.to_vec(),
        }
    }

    #[test]
    fn encode_decode_empty_partition() {
        let bytes = encode_fragment_payload(42, &[]);
        let (part, entries) = decode_fragment_payload(&bytes).unwrap();
        assert_eq!(part, 42);
        assert!(entries.is_empty());
    }

    #[test]
    fn encode_decode_single_entry() {
        let e = entry(b"k", b"v", 7);
        let bytes = encode_fragment_payload(13, &[e.clone()]);
        let (part, entries) = decode_fragment_payload(&bytes).unwrap();
        assert_eq!(part, 13);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, e.key);
        assert_eq!(entries[0].value, e.value);
        assert_eq!(entries[0].timestamp_nanos, e.timestamp_nanos);
    }

    #[test]
    fn decode_rejects_empty() {
        assert!(decode_fragment_payload(&[]).is_err());
    }

    #[test]
    fn decode_rejects_wrong_tag() {
        let mut bytes = encode_fragment_payload(0, &[]);
        bytes[0] = 0xFF;
        assert!(decode_fragment_payload(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncated_header() {
        let bytes = vec![FRAGMENT_PAYLOAD_TAG_V1, 0x00, 0x00];
        assert!(decode_fragment_payload(&bytes).is_err());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        #[test]
        fn roundtrip_random_entries(
            partition_id in any::<u32>(),
            entries in prop::collection::vec((
                prop::collection::vec(any::<u8>(), 1..=32),
                prop::collection::vec(any::<u8>(), 0..=64),
                any::<i64>(),
            ), 0..=8),
        ) {
            let typed: Vec<Entry> = entries
                .into_iter()
                .map(|(k, v, ts)| entry(&k, &v, ts))
                .collect();
            let bytes = encode_fragment_payload(partition_id, &typed);
            let (part_back, entries_back) = decode_fragment_payload(&bytes).unwrap();
            prop_assert_eq!(part_back, partition_id);
            prop_assert_eq!(entries_back.len(), typed.len());
            for (a, b) in typed.iter().zip(entries_back.iter()) {
                prop_assert_eq!(&a.key, &b.key);
                prop_assert_eq!(&a.value, &b.value);
                prop_assert_eq!(a.timestamp_nanos, b.timestamp_nanos);
            }
        }
    }
}
