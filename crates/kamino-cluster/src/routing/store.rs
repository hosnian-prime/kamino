//! `RoutingTableStore` — signature-gated shared view of the current routing
//! table.
//!
//! Receivers reject any incoming table whose signature is `<=` their local
//! one. This is the transient-dual-coordinator resolution mechanism described
//! in `docs/02-consistent-hashing.md` and `docs/03-cluster-management.md`
//! ("Election — Eventually Consistent"):
//!
//! - Coordinator increments `signature` on every topology change.
//! - During SWIM convergence two nodes may briefly believe they are
//!   coordinator. Only one's view has the correct, latest member set, so
//!   only that node's table accumulates new signatures. Stale broadcasts get
//!   filtered here.
//!
//! For sustained partitions the scalar clock alone is not enough — see
//! `member_count_quorum` (Phase 5 wires the read/write gate).

use std::sync::Arc;

use parking_lot::RwLock;

use crate::routing::table::{RoutingTable, SharedRoutingTable};

/// Outcome of applying a candidate table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyRoutingOutcome {
    /// Local signature was strictly less than the candidate; the new table
    /// is now the local view.
    Accepted,
    /// Candidate's signature was `<=` local — rejected as stale.
    Stale,
    /// Schema version comes from a future MAJOR we cannot speak; the table
    /// was rejected and the local view is unchanged.
    UnsupportedSchema,
}

/// Thread-safe holder for the current routing table.
///
/// Cheap to clone: internally an `Arc<RwLock<Option<Arc<RoutingTable>>>>`.
/// Reads do not block writers in `parking_lot`, but writes are infrequent
/// (one per push) so contention is negligible in practice.
#[derive(Debug, Clone, Default)]
pub struct RoutingTableStore {
    inner: Arc<RwLock<Option<SharedRoutingTable>>>,
}

impl RoutingTableStore {
    /// Build a fresh empty store (no table seen yet).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a candidate table. Returns the outcome so callers can log /
    /// metric stale rejections.
    pub fn apply(&self, candidate: RoutingTable) -> ApplyRoutingOutcome {
        // Within the same MAJOR a higher schema_version is still acceptable
        // (additive). Cross-MAJOR enforcement lives at HELLO; that wiring
        // lands in Phase 11. For Phase 4 we accept any schema_version.
        let mut slot = self.inner.write();
        if let Some(current) = slot.as_ref() {
            if candidate.signature <= current.signature {
                return ApplyRoutingOutcome::Stale;
            }
        }
        *slot = Some(Arc::new(candidate));
        ApplyRoutingOutcome::Accepted
    }

    /// Snapshot the current table (cheap `Arc` clone) or `None` if no table
    /// has been seen yet.
    #[must_use]
    pub fn snapshot(&self) -> Option<SharedRoutingTable> {
        self.inner.read().clone()
    }

    /// Current signature, or `0` if no table has been seen.
    #[must_use]
    pub fn signature(&self) -> u64 {
        self.inner.read().as_ref().map_or(0, |rt| rt.signature)
    }

    /// True once a routing table has been observed.
    #[must_use]
    pub fn is_populated(&self) -> bool {
        self.inner.read().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kamino_core::hasher::XxHasher;
    use kamino_core::ids::MemberId;
    use kamino_core::member::Member;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn mk(id: u64) -> Member {
        Member::new(
            MemberId::from_raw(id),
            format!("n{id}"),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3322),
            100,
        )
    }

    fn table(signature: u64) -> RoutingTable {
        RoutingTable::build(vec![mk(1)], &XxHasher, 271, 20, 1.25, 1, signature).unwrap()
    }

    #[test]
    fn first_push_accepted() {
        let s = RoutingTableStore::new();
        assert_eq!(s.signature(), 0);
        assert_eq!(s.apply(table(1)), ApplyRoutingOutcome::Accepted);
        assert_eq!(s.signature(), 1);
    }

    #[test]
    fn higher_signature_replaces() {
        let s = RoutingTableStore::new();
        s.apply(table(1));
        assert_eq!(s.apply(table(2)), ApplyRoutingOutcome::Accepted);
        assert_eq!(s.signature(), 2);
    }

    #[test]
    fn equal_or_lower_signature_rejected() {
        let s = RoutingTableStore::new();
        s.apply(table(5));
        assert_eq!(s.apply(table(5)), ApplyRoutingOutcome::Stale);
        assert_eq!(s.apply(table(4)), ApplyRoutingOutcome::Stale);
        assert_eq!(s.signature(), 5);
    }

    #[test]
    fn snapshot_is_none_until_first_apply() {
        let s = RoutingTableStore::new();
        assert!(s.snapshot().is_none());
        assert!(!s.is_populated());
        s.apply(table(1));
        assert!(s.snapshot().is_some());
        assert!(s.is_populated());
    }
}
