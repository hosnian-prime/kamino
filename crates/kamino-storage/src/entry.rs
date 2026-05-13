//! `Entry` — the on-disk record stored by every `StorageEngine`.
//!
//! Binary layout (per `docs/05-storage-engine.md`):
//!
//! ```text
//! ┌───────────────┬──────────┬─────────┬───────────────┬──────────────────┬────────────────┬──────────────┐
//! │ KEY_LENGTH(1B)│ KEY(var) │ TTL(8B) │ TIMESTAMP(8B) │ LAST_ACCESS(8B)  │ VAL_LEN(4B)    │ VALUE(var)   │
//! └───────────────┴──────────┴─────────┴───────────────┴──────────────────┴────────────────┴──────────────┘
//! ```
//!
//! All integers are little-endian. `ttl_nanos`, `timestamp_nanos` and
//! `last_access_nanos` are signed `i64` so the codec can represent both
//! "before epoch" times (for tests) and the sentinel value `0` ≡ "no TTL".
//!
//! **Contract**:
//! - `ttl_nanos == 0` → entry never expires.
//! - `ttl_nanos > 0` → absolute Unix-nanos expiry deadline. The choice of
//!   absolute (rather than duration-from-write) keeps eviction stateless
//!   under compaction — the deadline doesn't shift when entries are copied
//!   into a new table.
//! - `timestamp_nanos` is the primary's stamp used for LWW conflict
//!   resolution. Monotonic per primary (see `docs/04-replication.md`).
//! - `last_access_nanos` is bumped on every read/write/touch and drives
//!   both idle and LRU eviction.

use crate::error::{Error, Result};

/// Fixed overhead per entry: `key_len(1) + ttl(8) + ts(8) + last_access(8) + val_len(4)`.
pub const HEADER_OVERHEAD: usize = 1 + 8 + 8 + 8 + 4;

/// Maximum key size in bytes (capped by the `u8` length field).
pub const MAX_KEY_LEN: usize = u8::MAX as usize;

/// Maximum value size in bytes (capped by the `u32` length field).
pub const MAX_VALUE_LEN: usize = u32::MAX as usize;

/// Sentinel meaning "no expiry".
pub const NO_TTL: i64 = 0;

/// One stored record, owned (engine clones from its memory block on read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Raw key bytes (max [`MAX_KEY_LEN`]).
    pub key: Vec<u8>,
    /// Absolute expiry deadline in Unix nanoseconds. `0` ≡ never expires.
    pub ttl_nanos: i64,
    /// LWW timestamp in Unix nanoseconds.
    pub timestamp_nanos: i64,
    /// Last access timestamp in Unix nanoseconds (idle + LRU eviction).
    pub last_access_nanos: i64,
    /// Raw value bytes.
    pub value: Vec<u8>,
}

impl Entry {
    /// Serialized length: [`HEADER_OVERHEAD`] + `key.len()` + `value.len()`.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        HEADER_OVERHEAD + self.key.len() + self.value.len()
    }

    /// `true` if `ttl_nanos != 0` and the deadline is in the past.
    #[must_use]
    pub const fn is_expired(&self, now_nanos: i64) -> bool {
        self.ttl_nanos != NO_TTL && now_nanos >= self.ttl_nanos
    }

    /// Encode into `buf` (appended).
    pub fn encode_into(&self, buf: &mut Vec<u8>) -> Result<()> {
        if self.key.len() > MAX_KEY_LEN {
            return Err(Error::KeyTooLarge {
                got: self.key.len(),
                max: MAX_KEY_LEN,
            });
        }
        if self.value.len() > MAX_VALUE_LEN {
            return Err(Error::ValueTooLarge {
                got: self.value.len(),
                max: MAX_VALUE_LEN,
            });
        }

        buf.reserve(self.encoded_len());
        // Cast is safe: bounds-checked above.
        #[allow(clippy::cast_possible_truncation)]
        let key_len = self.key.len() as u8;
        buf.push(key_len);
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.ttl_nanos.to_le_bytes());
        buf.extend_from_slice(&self.timestamp_nanos.to_le_bytes());
        buf.extend_from_slice(&self.last_access_nanos.to_le_bytes());
        // Cast is safe: bounds-checked above.
        #[allow(clippy::cast_possible_truncation)]
        let val_len = self.value.len() as u32;
        buf.extend_from_slice(&val_len.to_le_bytes());
        buf.extend_from_slice(&self.value);
        Ok(())
    }

    /// Decode the entry that starts at `&buf[0]`. Returns the entry and the
    /// number of bytes consumed.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.is_empty() {
            return Err(Error::Truncated { need: 1 });
        }
        let key_len = buf[0] as usize;

        // Header: key_len byte + key + ttl + ts + last_access + val_len.
        let fixed_after_key = 8 + 8 + 8 + 4;
        let header_end = 1 + key_len + fixed_after_key;
        if buf.len() < header_end {
            return Err(Error::Truncated {
                need: header_end - buf.len(),
            });
        }

        let mut cursor = 1;
        let key = buf[cursor..cursor + key_len].to_vec();
        cursor += key_len;

        let ttl_nanos = i64::from_le_bytes(read_8(&buf[cursor..cursor + 8])?);
        cursor += 8;
        let timestamp_nanos = i64::from_le_bytes(read_8(&buf[cursor..cursor + 8])?);
        cursor += 8;
        let last_access_nanos = i64::from_le_bytes(read_8(&buf[cursor..cursor + 8])?);
        cursor += 8;
        let val_len = u32::from_le_bytes(read_4(&buf[cursor..cursor + 4])?) as usize;
        cursor += 4;

        if val_len > MAX_VALUE_LEN {
            return Err(Error::Corruption(format!(
                "value length {val_len} exceeds max {MAX_VALUE_LEN}"
            )));
        }

        let total = cursor + val_len;
        if buf.len() < total {
            return Err(Error::Truncated {
                need: total - buf.len(),
            });
        }
        let value = buf[cursor..total].to_vec();

        Ok((
            Self {
                key,
                ttl_nanos,
                timestamp_nanos,
                last_access_nanos,
                value,
            },
            total,
        ))
    }
}

fn read_8(slice: &[u8]) -> Result<[u8; 8]> {
    slice
        .try_into()
        .map_err(|_| Error::Corruption("expected 8 bytes".into()))
}

fn read_4(slice: &[u8]) -> Result<[u8; 4]> {
    slice
        .try_into()
        .map_err(|_| Error::Corruption("expected 4 bytes".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use proptest::prelude::*;

    fn sample() -> Entry {
        Entry {
            key: b"alpha".to_vec(),
            ttl_nanos: 1_700_000_000_000_000_000,
            timestamp_nanos: 1_700_000_000_111_111_111,
            last_access_nanos: 1_700_000_000_222_222_222,
            value: b"hello world".to_vec(),
        }
    }

    #[test]
    fn round_trip_basic() {
        let entry = sample();
        let mut buf = Vec::new();
        entry.encode_into(&mut buf).unwrap();
        assert_eq!(buf.len(), entry.encoded_len());
        let (decoded, used) = Entry::decode(&buf).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(decoded, entry);
    }

    #[test]
    fn empty_value_allowed() {
        let entry = Entry {
            key: b"k".to_vec(),
            ttl_nanos: 0,
            timestamp_nanos: 1,
            last_access_nanos: 1,
            value: Vec::new(),
        };
        let mut buf = Vec::new();
        entry.encode_into(&mut buf).unwrap();
        let (decoded, used) = Entry::decode(&buf).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(decoded, entry);
    }

    #[test]
    fn empty_key_allowed_by_codec() {
        // The codec permits a zero-byte key; rejection (if any) is a higher
        // layer's policy. Round-trip must still hold.
        let entry = Entry {
            key: Vec::new(),
            ttl_nanos: 0,
            timestamp_nanos: 0,
            last_access_nanos: 0,
            value: b"v".to_vec(),
        };
        let mut buf = Vec::new();
        entry.encode_into(&mut buf).unwrap();
        let (decoded, _) = Entry::decode(&buf).unwrap();
        assert_eq!(decoded, entry);
    }

    #[test]
    fn key_at_max() {
        let entry = Entry {
            key: vec![0xab; MAX_KEY_LEN],
            ttl_nanos: 0,
            timestamp_nanos: 0,
            last_access_nanos: 0,
            value: b"v".to_vec(),
        };
        let mut buf = Vec::new();
        entry.encode_into(&mut buf).unwrap();
        let (decoded, _) = Entry::decode(&buf).unwrap();
        assert_eq!(decoded, entry);
    }

    #[test]
    fn key_too_large_rejected() {
        let entry = Entry {
            key: vec![0; MAX_KEY_LEN + 1],
            ttl_nanos: 0,
            timestamp_nanos: 0,
            last_access_nanos: 0,
            value: Vec::new(),
        };
        let mut buf = Vec::new();
        assert_matches!(
            entry.encode_into(&mut buf),
            Err(Error::KeyTooLarge { got, max }) if got == MAX_KEY_LEN + 1 && max == MAX_KEY_LEN
        );
    }

    #[test]
    fn truncated_buffer_reports_need() {
        let entry = sample();
        let mut buf = Vec::new();
        entry.encode_into(&mut buf).unwrap();
        // Drop the final value byte.
        let short = &buf[..buf.len() - 1];
        assert_matches!(Entry::decode(short), Err(Error::Truncated { need: 1 }));
    }

    #[test]
    fn truncated_header_reports_need() {
        let entry = sample();
        let mut buf = Vec::new();
        entry.encode_into(&mut buf).unwrap();
        // Cut into the header (after key, before val_len fully read).
        let short = &buf[..1 + entry.key.len() + 8];
        assert_matches!(Entry::decode(short), Err(Error::Truncated { .. }));
    }

    #[test]
    fn empty_input_is_truncated() {
        assert_matches!(Entry::decode(&[]), Err(Error::Truncated { need: 1 }));
    }

    #[test]
    fn corruption_when_val_len_overflows_buffer() {
        // Build a header by hand whose val_len is bigger than the rest of buf.
        let mut buf = Vec::new();
        buf.push(1); // key_len
        buf.push(b'a'); // key
        buf.extend_from_slice(&0_i64.to_le_bytes()); // ttl
        buf.extend_from_slice(&0_i64.to_le_bytes()); // ts
        buf.extend_from_slice(&0_i64.to_le_bytes()); // last_access
        buf.extend_from_slice(&999_u32.to_le_bytes()); // val_len lies
        // No value bytes follow.
        assert_matches!(Entry::decode(&buf), Err(Error::Truncated { need: 999 }));
    }

    #[test]
    fn is_expired_semantics() {
        let mut e = sample();
        e.ttl_nanos = 100;
        assert!(e.is_expired(100));
        assert!(e.is_expired(150));
        assert!(!e.is_expired(99));
        e.ttl_nanos = NO_TTL;
        assert!(!e.is_expired(i64::MAX));
    }

    #[test]
    fn extra_bytes_after_entry_ignored() {
        let entry = sample();
        let mut buf = Vec::new();
        entry.encode_into(&mut buf).unwrap();
        let consumed = buf.len();
        buf.extend_from_slice(b"trailing-noise");
        let (decoded, used) = Entry::decode(&buf).unwrap();
        assert_eq!(decoded, entry);
        assert_eq!(used, consumed);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn round_trip_random(
            key in prop::collection::vec(any::<u8>(), 0..=MAX_KEY_LEN),
            value in prop::collection::vec(any::<u8>(), 0..=65_536),
            ttl in any::<i64>(),
            ts in any::<i64>(),
            la in any::<i64>(),
        ) {
            let entry = Entry {
                key,
                ttl_nanos: ttl,
                timestamp_nanos: ts,
                last_access_nanos: la,
                value,
            };
            let mut buf = Vec::new();
            entry.encode_into(&mut buf).unwrap();
            let (decoded, used) = Entry::decode(&buf).unwrap();
            prop_assert_eq!(used, buf.len());
            prop_assert_eq!(decoded, entry);
        }
    }
}
