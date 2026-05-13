//! Hash abstraction used by the consistent-hash partitioner.
//!
//! `kamino-cluster` consumes [`Hasher`] when assigning keys to partitions.
//! The default impl is [`XxHasher`] (xxh3 64-bit), per `CLAUDE.md` and
//! `docs/02-consistent-hashing.md`. Custom hash functions stay possible by
//! injecting a different `Hasher` implementation at config time.

/// Pluggable 64-bit byte hasher.
///
/// Implementations must be deterministic across process restarts — partition
/// assignment depends on it. Implementations are also `Send + Sync` so a
/// single instance can be shared across cluster tasks without locking.
pub trait Hasher: Send + Sync + 'static {
    /// Hash `bytes` to a 64-bit value.
    fn hash64(&self, bytes: &[u8]) -> u64;

    /// Stable name for logging / metric labels.
    fn name(&self) -> &'static str;
}

/// Default `xxh3_64` implementation. Stateless and `Copy`.
#[derive(Debug, Default, Clone, Copy)]
pub struct XxHasher;

impl Hasher for XxHasher {
    fn hash64(&self, bytes: &[u8]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(bytes)
    }

    fn name(&self) -> &'static str {
        "xxh3"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xxhash_is_deterministic() {
        let h = XxHasher;
        let a = h.hash64(b"user:1");
        let b = h.hash64(b"user:1");
        assert_eq!(a, b, "same input must yield same hash across calls");
    }

    #[test]
    fn xxhash_separates_inputs() {
        let h = XxHasher;
        assert_ne!(h.hash64(b"user:1"), h.hash64(b"user:2"));
    }

    #[test]
    fn xxhash_name_is_xxh3() {
        assert_eq!(XxHasher.name(), "xxh3");
    }

    #[test]
    fn empty_input_hashes() {
        let h = XxHasher;
        // Must not panic and must be deterministic.
        let a = h.hash64(b"");
        let b = h.hash64(b"");
        assert_eq!(a, b);
    }

    /// `Hasher` must be object-safe so it can live behind `Arc<dyn Hasher>`.
    #[test]
    fn hasher_is_object_safe() {
        let boxed: Box<dyn Hasher> = Box::new(XxHasher);
        assert_eq!(boxed.name(), "xxh3");
    }
}
