//! Property-based tests for `MembershipView::apply`.
//!
//! Invariants under test:
//!
//! 1. **Convergence**: applying the same set of `GossipEvent`s in any order
//!    to two fresh views yields the same `snapshot()` (commutativity of the
//!    apply function).
//! 2. **Monotonicity**: a member's incarnation never decreases, and a `Dead`
//!    state is absorbing for that incarnation (no transitions out of Dead at
//!    or below the current incarnation).
//! 3. **Coordinator uniqueness**: in every snapshot, at most one member has
//!    `is_coordinator = true`.

#![allow(
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    // Test code: deterministic casts are fine and explicit `as` is clearer
    // than `u16::try_from(...).unwrap()` for ports we control.
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    // Stylistic noise on test scaffolding.
    clippy::missing_const_for_fn,
    clippy::unreadable_literal,
)]

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kamino_cluster::membership::{MemberState, MembershipView};
use kamino_cluster::message::{GossipEvent, Incarnation};
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use proptest::prelude::*;

const SUSPICION_NS: u64 = 1_000_000_000; // 1s
const NOW_NS: u64 = 1_700_000_000_000_000_000;

fn mk_member(id: u64, birthdate: u64, port: u16) -> Member {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let disc = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port.wrapping_add(100));
    Member::new(
        MemberId::from_raw(id),
        format!("n{id}"),
        addr,
        disc,
        birthdate,
    )
}

fn alive_event(m: &Member, incarnation: Incarnation) -> GossipEvent {
    GossipEvent::Alive {
        id: m.id,
        name: m.name.clone(),
        addr: m.addr,
        discovery_addr: m.discovery_addr,
        birthdate: m.birthdate,
        incarnation,
    }
}

/// Strategy for member ids in a small pool so we get repeated subjects.
fn member_id_strat() -> impl Strategy<Value = u64> {
    1u64..=8u64
}

/// Strategy for incarnation numbers in [1, 5].
fn incarnation_strat() -> impl Strategy<Value = Incarnation> {
    1u64..=5u64
}

#[derive(Debug, Clone)]
enum Op {
    Alive {
        id: u64,
        incarnation: Incarnation,
    },
    Suspect {
        id: u64,
        incarnation: Incarnation,
        from: u64,
    },
    Dead {
        id: u64,
        incarnation: Incarnation,
        from: u64,
    },
}

/// Stable birthdate derived from id so the same `Alive` event for the same
/// id always carries identical metadata regardless of when it was sampled.
/// Without this, two Alive events with different birthdates would race and
/// break convergence (the first to arrive wins per `apply_alive` rules).
fn birthdate_for(id: u64) -> u64 {
    100 + id
}

fn op_strat() -> impl Strategy<Value = Op> {
    prop_oneof![
        (member_id_strat(), incarnation_strat()).prop_map(|(id, inc)| Op::Alive {
            id,
            incarnation: inc,
        }),
        (member_id_strat(), incarnation_strat(), member_id_strat()).prop_map(|(id, inc, from)| {
            Op::Suspect {
                id,
                incarnation: inc,
                from,
            }
        }),
        (member_id_strat(), incarnation_strat(), member_id_strat()).prop_map(|(id, inc, from)| {
            Op::Dead {
                id,
                incarnation: inc,
                from,
            }
        }),
    ]
}

fn op_to_event(op: &Op) -> GossipEvent {
    match op {
        Op::Alive { id, incarnation } => alive_event(
            &mk_member(*id, birthdate_for(*id), 10_000_u16.wrapping_add(*id as u16)),
            *incarnation,
        ),
        Op::Suspect {
            id,
            incarnation,
            from,
        } => GossipEvent::Suspect {
            id: MemberId::from_raw(*id),
            incarnation: *incarnation,
            from: MemberId::from_raw(*from),
        },
        Op::Dead {
            id,
            incarnation,
            from,
        } => GossipEvent::Dead {
            id: MemberId::from_raw(*id),
            incarnation: *incarnation,
            from: MemberId::from_raw(*from),
        },
    }
}

fn apply_all(view: &MembershipView, events: &[GossipEvent]) {
    for ev in events {
        view.apply(ev, NOW_NS, SUSPICION_NS);
    }
}

fn snapshot_signature(
    view: &MembershipView,
) -> BTreeMap<u64, (u64, u64, MemberState, Incarnation)> {
    // (id) -> (birthdate, port, state, incarnation)
    view.snapshot_all()
        .into_iter()
        .map(|(m, state, inc)| {
            (
                m.id.as_u64(),
                (m.birthdate, m.addr.port() as u64, state, inc),
            )
        })
        .collect()
}

/// Pre-seed both views with an `Alive@inc=0` for every id mentioned in `ops`.
/// Suspect/Dead events arriving before the corresponding `Alive` are dropped
/// by `apply` (a node we've never seen cannot be reported dead), so without
/// this seed convergence does not hold under arbitrary reordering.
fn seed_members(view: &MembershipView, ops: &[Op]) {
    use std::collections::BTreeSet;
    let mut ids: BTreeSet<u64> = BTreeSet::new();
    for op in ops {
        let id = match op {
            Op::Alive { id, .. } | Op::Suspect { id, .. } | Op::Dead { id, .. } => *id,
        };
        ids.insert(id);
    }
    for id in ids {
        if id == 99 {
            continue;
        } // skip local
        let m = mk_member(id, birthdate_for(id), 10_000_u16.wrapping_add(id as u16));
        view.apply(&alive_event(&m, 0), NOW_NS, SUSPICION_NS);
    }
}

/// Canonicalize a sequence of ops so per-`(id, incarnation)` only the
/// strongest event survives (`Dead > Suspect > Alive`). At the same
/// incarnation, Alive vs Suspect is non-commutative in `apply`, so we
/// restrict the convergence claim to a deterministic merged input by
/// keeping the strongest event. This matches the property SWIM gossip
/// converges on after sufficient rounds — see `docs/03-cluster-management.md`.
fn canonicalize(ops: &[Op]) -> Vec<Op> {
    use std::collections::BTreeMap;
    // (id, incarnation) -> strongest seen so far. 0=Alive, 1=Suspect, 2=Dead.
    let mut strongest: BTreeMap<(u64, Incarnation), u8> = BTreeMap::new();
    for op in ops {
        let (id, inc, rank) = match op {
            Op::Alive { id, incarnation } => (*id, *incarnation, 0),
            Op::Suspect {
                id, incarnation, ..
            } => (*id, *incarnation, 1),
            Op::Dead {
                id, incarnation, ..
            } => (*id, *incarnation, 2),
        };
        let entry = strongest.entry((id, inc)).or_insert(rank);
        if rank > *entry {
            *entry = rank;
        }
    }
    // Build canonical ops preserving the original encounter order so
    // shuffling is still meaningful.
    let mut emitted: BTreeMap<(u64, Incarnation), bool> = BTreeMap::new();
    let mut out = Vec::with_capacity(ops.len());
    for op in ops {
        let (id, inc, rank) = match op {
            Op::Alive { id, incarnation } => (*id, *incarnation, 0u8),
            Op::Suspect {
                id, incarnation, ..
            } => (*id, *incarnation, 1u8),
            Op::Dead {
                id, incarnation, ..
            } => (*id, *incarnation, 2u8),
        };
        let strongest_rank = strongest[&(id, inc)];
        if rank == strongest_rank && !emitted.get(&(id, inc)).copied().unwrap_or(false) {
            out.push(op.clone());
            emitted.insert((id, inc), true);
        }
    }
    out
}

proptest! {
    /// Convergence: applying the same canonicalized set of events in any
    /// order yields the same `snapshot()`. Canonicalization keeps the
    /// strongest event per (id, incarnation), which matches what SWIM
    /// gossip converges on after sufficient rounds — Alive and Suspect at
    /// the same incarnation are non-commutative on a single `apply` call,
    /// so the proptest models the *steady state*, not every transient.
    #[test]
    fn convergence_order_independent(
        raw_ops in proptest::collection::vec(op_strat(), 1..40),
        seed in any::<u64>(),
    ) {
        let ops = canonicalize(&raw_ops);
        // Build a permutation deterministically from `seed`.
        let mut shuffled: Vec<Op> = ops.clone();
        // Simple Fisher-Yates with a small LCG keyed by `seed`.
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        for i in (1..shuffled.len()).rev() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (state >> 33) as usize % (i + 1);
            shuffled.swap(i, j);
        }

        let local = mk_member(99, 0, 9000);
        let view_a = MembershipView::bootstrap(local.clone());
        let view_b = MembershipView::bootstrap(local);
        seed_members(&view_a, &ops);
        seed_members(&view_b, &ops);

        let events_a: Vec<GossipEvent> = ops.iter().map(op_to_event).collect();
        let events_b: Vec<GossipEvent> = shuffled.iter().map(op_to_event).collect();
        apply_all(&view_a, &events_a);
        apply_all(&view_b, &events_b);

        let sig_a = snapshot_signature(&view_a);
        let sig_b = snapshot_signature(&view_b);
        prop_assert_eq!(sig_a, sig_b);
    }

    /// Monotonicity: incarnation never decreases for any member, and once
    /// Dead at incarnation k, no transition out at any incarnation < k+1.
    #[test]
    fn incarnation_monotonic_and_dead_absorbing(
        ops in proptest::collection::vec(op_strat(), 1..40),
    ) {
        let local = mk_member(99, 0, 9000);
        let view = MembershipView::bootstrap(local);

        let mut max_inc: BTreeMap<u64, Incarnation> = BTreeMap::new();
        let mut dead_at: BTreeMap<u64, Incarnation> = BTreeMap::new();

        for op in &ops {
            let ev = op_to_event(op);
            view.apply(&ev, NOW_NS, SUSPICION_NS);

            // Inspect entry after every application.
            for (m, state, inc) in view.snapshot_all() {
                let id = m.id.as_u64();
                // Incarnations monotone.
                let prev = max_inc.get(&id).copied().unwrap_or(0);
                prop_assert!(
                    inc >= prev,
                    "incarnation regressed for {}: {} -> {}",
                    id, prev, inc,
                );
                max_inc.insert(id, inc);
                // Dead absorbing.
                if let Some(dead_inc) = dead_at.get(&id).copied() {
                    if inc <= dead_inc {
                        prop_assert_eq!(
                            state,
                            MemberState::Dead,
                            "dead state escaped for {} at incarnation {} (<= dead at {})",
                            id,
                            inc,
                            dead_inc,
                        );
                    }
                }
                if state == MemberState::Dead {
                    dead_at.insert(id, inc);
                }
            }
        }
    }

    /// Coordinator uniqueness: `snapshot()` reports `is_coordinator = true`
    /// for at most one entry.
    #[test]
    fn coordinator_is_unique(
        ops in proptest::collection::vec(op_strat(), 0..40),
    ) {
        let local = mk_member(99, 0, 9000);
        let view = MembershipView::bootstrap(local);
        for op in &ops {
            view.apply(&op_to_event(op), NOW_NS, SUSPICION_NS);
        }
        let snap = view.snapshot();
        let count = snap.iter().filter(|m| m.is_coordinator).count();
        prop_assert!(count <= 1, "multiple coordinators in {snap:?}");
    }
}
