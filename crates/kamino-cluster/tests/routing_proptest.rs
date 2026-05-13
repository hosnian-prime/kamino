//! Property tests for the routing layer.
//!
//! Three invariants from `docs/02-consistent-hashing.md` +
//! `docs/03-cluster-management.md` "Election":
//!
//! 1. **Determinism**: same member set + parameters → byte-identical
//!    assignment.
//! 2. **Bounded load**: no member's primary count exceeds the capacity
//!    ceiling derived from `load_factor`.
//! 3. **Signature monotonicity**: a [`RoutingTableStore`] never accepts a
//!    table whose signature is `<=` its current one, regardless of the
//!    arrival order (the transient-dual-coordinator resolution mechanism).

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use kamino_cluster::routing::assign;
use kamino_cluster::{ApplyRoutingOutcome, RoutingTable, RoutingTableStore};
use kamino_core::hasher::XxHasher;
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use proptest::prelude::*;

fn mk_members(ids: &[u64]) -> Vec<Member> {
    ids.iter()
        .enumerate()
        .map(|(i, &id)| {
            let port = 3320 + u16::try_from(i).unwrap() * 2;
            Member::new(
                MemberId::from_raw(id),
                format!("n{id}"),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2),
                u64::from(u32::try_from(i).unwrap()) + 1,
            )
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Same inputs → same outputs, every time. Failure here means the ring
    /// has snuck in nondeterminism (random seeds, hashmap iteration order,
    /// etc.) — fatal for cluster-wide convergence.
    #[test]
    fn ring_assignment_is_deterministic(
        ids in prop::collection::vec(any::<u64>(), 1..=8).prop_map(|mut v| {
            v.sort_unstable();
            v.dedup();
            v
        }),
        partitions in prop::sample::select(vec![17_u32, 31, 71, 271]),
        vnodes in 1_u32..=32,
        load_factor in (1.0_f64..=2.5),
        replica_count in 1_u32..=3,
    ) {
        let members = mk_members(&ids);
        let a1 = assign(&XxHasher, &members, partitions, vnodes, load_factor, replica_count);
        let a2 = assign(&XxHasher, &members, partitions, vnodes, load_factor, replica_count);
        prop_assert_eq!(a1, a2);
    }

    /// No primary owner gets more than `ceil(parts/members * load_factor)`
    /// partitions — the bounded-load invariant the Mirrokni 2016 paper
    /// proves.
    #[test]
    fn bounded_load_invariant(
        ids in prop::collection::vec(any::<u64>(), 2..=8).prop_map(|mut v| {
            v.sort_unstable();
            v.dedup();
            v
        }),
        partitions in prop::sample::select(vec![31_u32, 71, 271]),
        load_factor in (1.05_f64..=2.0),
    ) {
        let members = mk_members(&ids);
        let asg = assign(&XxHasher, &members, partitions, 16, load_factor, 1)
            .expect("non-empty members must assign");
        let mut counts: HashMap<MemberId, u32> = HashMap::new();
        for p in &asg.primary {
            *counts.entry(*p).or_default() += 1;
        }
        #[allow(clippy::cast_precision_loss)]
        let avg = f64::from(partitions) / members.len() as f64;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
        )]
        let cap = (avg * load_factor).ceil().max(1.0) as u32;
        for &c in counts.values() {
            prop_assert!(
                c <= cap,
                "primary count {} exceeded capacity {} (parts={partitions}, members={}, lf={load_factor})",
                c,
                cap,
                members.len(),
            );
        }
    }

    /// Signatures applied in arbitrary order can only ever monotonically
    /// increase the local signature. Models the transient-dual-coordinator
    /// scenario where competing coordinators push tables with overlapping
    /// signature ranges; the store must converge on the highest one.
    #[test]
    fn signature_clock_is_monotonic_under_reordering(
        sigs in prop::collection::vec(1_u64..=10_000, 1..=32),
    ) {
        let members = mk_members(&[1, 2, 3]);
        let store = RoutingTableStore::new();
        let mut max_seen = 0_u64;
        for &s in &sigs {
            let table = RoutingTable::build(
                members.clone(),
                &XxHasher,
                271,
                20,
                1.25,
                1,
                s,
            ).unwrap();
            let outcome = store.apply(table);
            if s > max_seen {
                prop_assert_eq!(outcome, ApplyRoutingOutcome::Accepted);
                max_seen = s;
            } else {
                prop_assert_eq!(outcome, ApplyRoutingOutcome::Stale);
            }
            prop_assert_eq!(store.signature(), max_seen);
        }
    }
}
