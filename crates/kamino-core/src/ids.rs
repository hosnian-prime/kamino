//! Stable per-process identifiers.

use std::fmt;

use rand::Rng;
use serde::{Deserialize, Serialize};

/// Random 64-bit identifier assigned at boot.
///
/// Used as the tiebreaker in coordinator selection (see
/// `docs/03-cluster-management.md`). The implementation deliberately uses a
/// random u64 (not a UUID) — `Member` ordering and routing-table signatures
/// rely on cheap totally-ordered comparison of integer IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemberId(pub u64);

impl MemberId {
    /// Allocate a fresh random ID. Each process should call this once at
    /// startup and keep it for the process lifetime.
    #[must_use]
    pub fn new_random() -> Self {
        Self(rand::thread_rng().r#gen::<u64>())
    }

    /// Construct from raw bits (mostly for tests).
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Raw u64 value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for MemberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_ids_differ() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1024 {
            assert!(seen.insert(MemberId::new_random()));
        }
    }

    #[test]
    fn display_is_hex() {
        let id = MemberId::from_raw(0xdead_beef_cafe_f00d);
        assert_eq!(id.to_string(), "deadbeefcafef00d");
    }

    #[test]
    fn order_matches_u64() {
        assert!(MemberId::from_raw(1) < MemberId::from_raw(2));
    }
}
