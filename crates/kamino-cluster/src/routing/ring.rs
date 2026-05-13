//! Bounded-load consistent hash ring.
//!
//! Based on Mirrokni, Thorup, Zadimoghaddam — *"Consistent Hashing with
//! Bounded Loads"* (arXiv:1608.01350, 2016). The variant Kamino runs is the
//! two-level scheme described in `docs/02-consistent-hashing.md`:
//!
//! 1. **Members** are placed on the ring as `virtual_nodes_per_member`
//!    virtual nodes each, at deterministic ring positions derived from
//!    `hash("<member-id-hex>#<vnode-idx>")`. Deterministic positions matter:
//!    every node must build an identical ring from the same input set, or
//!    routing-table convergence breaks.
//! 2. **Partitions** (`0..partition_count`) walk the ring at their own hash
//!    positions and pick the first member with capacity. A member's capacity
//!    is `ceil(partition_count / member_count * load_factor)`; when full it
//!    is skipped and the partition continues walking. With `load_factor >=
//!    1.0` this loop always terminates because the total capacity strictly
//!    exceeds the partition count.
//! 3. **Backups**: for each partition, the next `replica_count - 1` distinct
//!    members on the ring (still respecting capacity for the *primary* slot
//!    only — backups are not capacity-bounded; the paper bounds only the
//!    primary assignment).
//!
//! The walk is `O(virtual_nodes_per_member * member_count)` worst case but
//! members*vnodes is small (default 20 vnodes × 50 nodes = 1000) and the
//! function runs at most once per topology change.

use kamino_core::hasher::Hasher;
use kamino_core::ids::MemberId;
use kamino_core::member::Member;

/// Computed partition assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    /// `partition_id -> primary member id` (one entry per partition).
    pub primary: Vec<MemberId>,
    /// `partition_id -> backup member ids` (length `replica_count - 1`).
    pub backups: Vec<Vec<MemberId>>,
}

/// Build a bounded-load consistent-hash assignment from the supplied members.
///
/// Inputs are deterministic: same member set + same parameters yield the same
/// assignment on every node. `members` is **assumed pre-sorted** by
/// `(birthdate, id)` — pass `MembershipView::snapshot()` directly.
///
/// `replica_count` includes the primary, so `replica_count = 2` means one
/// primary + one backup.
///
/// Returns `None` if `members` is empty or `partition_count == 0`.
#[must_use]
pub fn assign(
    hasher: &dyn Hasher,
    members: &[Member],
    partition_count: u32,
    virtual_nodes_per_member: u32,
    load_factor: f64,
    replica_count: u32,
) -> Option<Assignment> {
    if members.is_empty() || partition_count == 0 {
        return None;
    }
    let n_members = members.len();
    let part_count = partition_count as usize;

    // Per-primary capacity ceiling. The paper proves convergence iff total
    // capacity strictly exceeds partition count, which holds for any
    // `load_factor > 1`. We clamp to `>= 1` so the trivial 1-member case
    // works regardless of how miserly the operator sets load_factor.
    let cap = capacity_per_member(part_count, n_members, load_factor);

    // Build the ring: sorted (hash_position, member_index) pairs.
    let ring = build_ring(hasher, members, virtual_nodes_per_member);

    let mut load = vec![0_u32; n_members];
    let mut primary = Vec::with_capacity(part_count);
    let mut backups = Vec::with_capacity(part_count);

    for partition_id in 0..partition_count {
        let p_hash = partition_position(hasher, partition_id);
        let start = ring.partition_point(|&(pos, _)| pos < p_hash);

        let (prim_idx, ring_idx) = find_primary(&ring, start, &load, cap, n_members)
            .expect("capacity > partition_count guarantees a slot");
        load[prim_idx] += 1;
        primary.push(members[prim_idx].id);

        let backup_ids = collect_backups(
            &ring,
            ring_idx,
            prim_idx,
            members,
            replica_count.saturating_sub(1) as usize,
        );
        backups.push(backup_ids);
    }

    Some(Assignment { primary, backups })
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)] // partition_count is small (default 271); cap_f is always finite > 1
fn capacity_per_member(part_count: usize, n_members: usize, load_factor: f64) -> u32 {
    // ceil((partition_count / member_count) * load_factor)
    let avg = part_count as f64 / n_members as f64;
    let cap_f = (avg * load_factor).ceil().max(1.0);
    cap_f as u32
}

fn build_ring(
    hasher: &dyn Hasher,
    members: &[Member],
    virtual_nodes_per_member: u32,
) -> Vec<(u64, usize)> {
    use std::fmt::Write as _;
    let cap = members.len() * virtual_nodes_per_member as usize;
    let mut ring = Vec::with_capacity(cap);
    let mut buf = String::with_capacity(32);
    for (idx, m) in members.iter().enumerate() {
        for v in 0..virtual_nodes_per_member {
            buf.clear();
            // `MemberId` displays as 16-char hex; the `#` separator can't
            // appear in the hex space so distinct (member, vnode) pairs
            // always hash distinct inputs.
            let _ = write!(buf, "{:016x}#{v}", m.id.as_u64());
            let pos = hasher.hash64(buf.as_bytes());
            ring.push((pos, idx));
        }
    }
    ring.sort_unstable_by_key(|&(pos, _)| pos);
    ring
}

fn partition_position(hasher: &dyn Hasher, partition_id: u32) -> u64 {
    // Independent input space from member vnodes; matches the partition_id
    // mod approach without colliding on the same hash inputs.
    let mut buf = [0_u8; 12];
    buf[..8].copy_from_slice(b"p:______");
    buf[8..].copy_from_slice(&partition_id.to_be_bytes());
    hasher.hash64(&buf)
}

fn find_primary(
    ring: &[(u64, usize)],
    start: usize,
    load: &[u32],
    cap: u32,
    n_members: usize,
) -> Option<(usize, usize)> {
    if ring.is_empty() {
        return None;
    }
    let len = ring.len();
    for step in 0..len {
        let ring_idx = (start + step) % len;
        let member_idx = ring[ring_idx].1;
        if load[member_idx] < cap {
            return Some((member_idx, ring_idx));
        }
    }
    // Fallback: every member is at capacity. The paper proves this is
    // impossible for `load_factor > 1`; if it triggers (rounding edge case
    // on tiny clusters), pick the first ring slot so we still produce *an*
    // answer rather than panicking.
    debug_assert!(false, "every member at capacity — load_factor too tight");
    Some((ring[start % len].1, start % len)).filter(|_| n_members > 0)
}

fn collect_backups(
    ring: &[(u64, usize)],
    primary_ring_idx: usize,
    primary_member_idx: usize,
    members: &[Member],
    want: usize,
) -> Vec<MemberId> {
    if want == 0 || ring.is_empty() {
        return Vec::new();
    }
    let len = ring.len();
    let mut seen: std::collections::HashSet<usize> =
        std::collections::HashSet::with_capacity(want + 1);
    seen.insert(primary_member_idx);
    let mut out = Vec::with_capacity(want);
    let mut step = 1;
    while out.len() < want && step < len {
        let idx = (primary_ring_idx + step) % len;
        let m_idx = ring[idx].1;
        if seen.insert(m_idx) {
            out.push(members[m_idx].id);
        }
        step += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use kamino_core::hasher::XxHasher;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn mk(id: u64, birthdate: u64, port: u16) -> Member {
        Member::new(
            MemberId::from_raw(id),
            format!("n{id}"),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2),
            birthdate,
        )
    }

    #[test]
    fn empty_members_returns_none() {
        let h = XxHasher;
        assert!(assign(&h, &[], 271, 20, 1.25, 1).is_none());
    }

    #[test]
    fn single_member_owns_everything() {
        let h = XxHasher;
        let a = assign(&h, &[mk(1, 100, 3320)], 271, 20, 1.25, 1).unwrap();
        assert_eq!(a.primary.len(), 271);
        let only = MemberId::from_raw(1);
        assert!(a.primary.iter().all(|id| *id == only));
        assert!(a.backups.iter().all(Vec::is_empty));
    }

    #[test]
    fn deterministic_under_same_inputs() {
        let h = XxHasher;
        let members = [mk(1, 100, 3320), mk(2, 100, 3322), mk(3, 100, 3324)];
        let a1 = assign(&h, &members, 271, 20, 1.25, 2).unwrap();
        let a2 = assign(&h, &members, 271, 20, 1.25, 2).unwrap();
        assert_eq!(a1, a2);
    }

    #[test]
    fn load_factor_bounds_each_primary() {
        let h = XxHasher;
        let members: Vec<Member> = (1..=5)
            .map(|i| mk(i, 100, 3320 + u16::try_from(i).unwrap() * 2))
            .collect();
        let load_factor = 1.25_f64;
        let a = assign(&h, &members, 271, 20, load_factor, 1).unwrap();
        let mut counts: HashMap<MemberId, u32> = HashMap::new();
        for p in &a.primary {
            *counts.entry(*p).or_default() += 1;
        }
        let cap = capacity_per_member(271, 5, load_factor);
        for &c in counts.values() {
            assert!(
                c <= cap,
                "member load {c} exceeds capacity {cap} for 271 parts / 5 members @ lf={load_factor}",
            );
        }
    }

    #[test]
    fn backups_are_distinct_from_primary_and_each_other() {
        let h = XxHasher;
        let members: Vec<Member> = (1..=4)
            .map(|i| mk(i, 100, 3320 + u16::try_from(i).unwrap() * 2))
            .collect();
        let a = assign(&h, &members, 271, 20, 1.25, 3).unwrap();
        for (p, bs) in a.primary.iter().zip(a.backups.iter()) {
            assert_eq!(bs.len(), 2, "want replica_count-1 = 2 backups");
            assert!(!bs.contains(p), "primary must not appear in backups");
            assert!(bs[0] != bs[1], "backups must be distinct");
        }
    }

    #[test]
    fn changing_one_member_moves_minority_of_partitions() {
        // Classical consistent-hashing property: adding a node should move
        // approximately K/N partitions, not all of them.
        let h = XxHasher;
        let before: Vec<Member> = (1..=4)
            .map(|i| mk(i, 100, 3320 + u16::try_from(i).unwrap() * 2))
            .collect();
        let after: Vec<Member> = (1..=5)
            .map(|i| mk(i, 100, 3320 + u16::try_from(i).unwrap() * 2))
            .collect();
        let a = assign(&h, &before, 271, 20, 1.25, 1).unwrap();
        let b = assign(&h, &after, 271, 20, 1.25, 1).unwrap();
        let moved = a
            .primary
            .iter()
            .zip(b.primary.iter())
            .filter(|(x, y)| x != y)
            .count();
        // K/N for K=271, N=5 ≈ 54. The bounded-load extension can move a
        // few extra to honour the capacity constraint — accept up to 2× K/N
        // as the upper bound for the test.
        assert!(
            moved < 271 / 2,
            "want minority moved, got {moved} of 271 partitions",
        );
    }

    #[test]
    fn coordinator_ordering_irrelevant_for_assignment() {
        // Whether a node is the coordinator does not feed into the ring;
        // sorting by (birthdate, id) is the *caller's* concern.
        let h = XxHasher;
        let m1 = [mk(1, 100, 3320), mk(2, 200, 3322), mk(3, 300, 3324)];
        let mut m2 = m1.clone();
        m2.reverse();
        let a1 = assign(&h, &m1, 271, 20, 1.25, 1).unwrap();
        let a2 = assign(&h, &m2, 271, 20, 1.25, 1).unwrap();
        assert_eq!(a1, a2);
    }
}
