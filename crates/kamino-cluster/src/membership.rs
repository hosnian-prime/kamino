//! In-memory cluster membership view.
//!
//! Owns the canonical `Vec<Member>` along with each member's SWIM state
//! (`Alive`, `Suspect`, `Dead`, `Left`) and incarnation. The view is held
//! behind a `parking_lot::RwLock` so SWIM probes (frequent reads) don't
//! contend with the comparatively rare topology mutations.
//!
//! Coordinator selection: members are kept sorted by `(birthdate ASC, id ASC)`
//! per `docs/03-cluster-management.md`. Index `0` is the coordinator.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use parking_lot::RwLock;

use crate::message::{GossipEvent, Incarnation};

/// SWIM lifecycle state for a single member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    /// Reachable. Probes have been acked recently (or no probes yet).
    Alive,
    /// Direct + indirect probes have failed within the last suspicion
    /// window. Will be promoted to `Dead` after `suspicion_timeout` elapses
    /// unless an `Alive` refutation with `incarnation >= suspect.incarnation`
    /// arrives.
    Suspect,
    /// Either declared dead by SWIM or announced a graceful leave.
    Dead,
}

/// Per-member tracking metadata kept inside the view.
#[derive(Debug, Clone)]
pub struct MemberEntry {
    pub member: Member,
    pub state: MemberState,
    pub incarnation: Incarnation,
    /// Wall-clock ns at which a `Suspect` state expires. `0` while alive.
    pub suspect_deadline_ns: u64,
}

/// Cluster-wide membership view shared across the SWIM tasks.
///
/// Cheap to clone: internally an `Arc<RwLock<_>>`.
#[derive(Debug, Clone, Default)]
pub struct MembershipView {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    /// All known members, indexed by id for O(1) lookup.
    by_id: HashMap<MemberId, MemberEntry>,
    /// Sorted ids by `(birthdate, id)` — index 0 is the coordinator.
    sorted: Vec<MemberId>,
    /// The locally generated id of this node.
    local_id: Option<MemberId>,
    /// Local incarnation number. Bumped when we observe a suspicion against
    /// ourselves and want to refute it.
    local_incarnation: Incarnation,
}

/// Outcome of applying a gossip event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// No change (stale, redundant, or already in a more advanced state).
    Ignored,
    /// New member learned.
    NewMember,
    /// State or incarnation of an existing member advanced.
    Updated,
    /// We learned that a remote node thinks we are suspect/dead — caller
    /// must bump local_incarnation and broadcast a refuting Alive.
    SelfRefutationNeeded,
}

impl MembershipView {
    /// Build a fresh view containing only the local node as `Alive`.
    pub fn bootstrap(local: Member) -> Self {
        let view = Self::default();
        {
            let mut inner = view.inner.write();
            inner.local_id = Some(local.id);
            inner.local_incarnation = 1;
            inner.by_id.insert(
                local.id,
                MemberEntry {
                    member: local.clone(),
                    state: MemberState::Alive,
                    incarnation: 1,
                    suspect_deadline_ns: 0,
                },
            );
            inner.sorted = vec![local.id];
        }
        view
    }

    /// Snapshot of all members, sorted by `(birthdate, id)`. The first entry
    /// is the coordinator. Each `Member.is_coordinator` reflects index 0.
    pub fn snapshot(&self) -> Vec<Member> {
        let inner = self.inner.read();
        inner
            .sorted
            .iter()
            .enumerate()
            .filter_map(|(idx, id)| {
                let entry = inner.by_id.get(id)?;
                if entry.state == MemberState::Dead {
                    return None;
                }
                let mut m = entry.member.clone();
                m.is_coordinator = idx == 0;
                Some(m)
            })
            .collect()
    }

    /// Snapshot including dead members (for diagnostics).
    pub fn snapshot_all(&self) -> Vec<(Member, MemberState, Incarnation)> {
        let inner = self.inner.read();
        inner
            .sorted
            .iter()
            .filter_map(|id| {
                let e = inner.by_id.get(id)?;
                Some((e.member.clone(), e.state, e.incarnation))
            })
            .collect()
    }

    /// Number of `Alive` + `Suspect` members.
    pub fn live_count(&self) -> usize {
        let inner = self.inner.read();
        inner
            .by_id
            .values()
            .filter(|e| e.state != MemberState::Dead)
            .count()
    }

    /// Local member id (returns `None` only before `bootstrap`).
    pub fn local_id(&self) -> Option<MemberId> {
        self.inner.read().local_id
    }

    /// Current local incarnation number.
    pub fn local_incarnation(&self) -> Incarnation {
        self.inner.read().local_incarnation
    }

    /// Bump the local incarnation, returning the new value. Used when we
    /// learn a remote node suspects us and we want to refute.
    pub fn bump_local_incarnation(&self) -> Incarnation {
        let mut inner = self.inner.write();
        inner.local_incarnation += 1;
        let new_inc = inner.local_incarnation;
        if let Some(local_id) = inner.local_id {
            if let Some(entry) = inner.by_id.get_mut(&local_id) {
                entry.incarnation = new_inc;
                entry.state = MemberState::Alive;
                entry.suspect_deadline_ns = 0;
            }
        }
        new_inc
    }

    /// Look up a member by id.
    pub fn get(&self, id: MemberId) -> Option<MemberEntry> {
        self.inner.read().by_id.get(&id).cloned()
    }

    /// Look up a member by SWIM discovery address.
    pub fn find_by_discovery_addr(&self, addr: SocketAddr) -> Option<MemberEntry> {
        let inner = self.inner.read();
        inner
            .by_id
            .values()
            .find(|e| e.member.discovery_addr == addr)
            .cloned()
    }

    /// Apply a gossip event. Returns the outcome so callers can decide
    /// whether to re-broadcast.
    pub fn apply(&self, event: &GossipEvent, now_ns: u64, suspicion_timeout_ns: u64) -> ApplyOutcome {
        let mut inner = self.inner.write();
        match *event {
            GossipEvent::Alive {
                id,
                ref name,
                addr,
                discovery_addr,
                birthdate,
                incarnation,
            } => apply_alive(
                &mut inner,
                id,
                name,
                addr,
                discovery_addr,
                birthdate,
                incarnation,
            ),
            GossipEvent::Suspect { id, incarnation, .. } => {
                apply_suspect(&mut inner, id, incarnation, now_ns, suspicion_timeout_ns)
            }
            GossipEvent::Dead { id, incarnation, .. } => apply_dead(&mut inner, id, incarnation),
            GossipEvent::Leave { id, incarnation } => apply_dead(&mut inner, id, incarnation),
        }
    }

    /// Promote any `Suspect` members whose deadline has elapsed to `Dead`.
    /// Returns the ids that transitioned so the caller can emit Dead gossip.
    pub fn reap_suspects(&self, now_ns: u64) -> Vec<(MemberId, Incarnation)> {
        let mut inner = self.inner.write();
        let mut promoted = Vec::new();
        for entry in inner.by_id.values_mut() {
            if entry.state == MemberState::Suspect && entry.suspect_deadline_ns <= now_ns {
                entry.state = MemberState::Dead;
                promoted.push((entry.member.id, entry.incarnation));
            }
        }
        if !promoted.is_empty() {
            re_sort(&mut inner);
        }
        promoted
    }

    /// Garbage-collect `Dead` entries older than the supplied tombstone TTL.
    pub fn evict_tombstones(&self, older_than_ns: u64, now_ns: u64) {
        let mut inner = self.inner.write();
        let to_remove: Vec<MemberId> = inner
            .by_id
            .values()
            .filter(|e| {
                e.state == MemberState::Dead
                    && e.suspect_deadline_ns != 0
                    && now_ns.saturating_sub(e.suspect_deadline_ns) >= older_than_ns
            })
            .map(|e| e.member.id)
            .collect();
        for id in &to_remove {
            inner.by_id.remove(id);
        }
        if !to_remove.is_empty() {
            re_sort(&mut inner);
        }
    }
}

fn apply_alive(
    inner: &mut Inner,
    id: MemberId,
    name: &str,
    addr: SocketAddr,
    discovery_addr: SocketAddr,
    birthdate: u64,
    incarnation: Incarnation,
) -> ApplyOutcome {
    if Some(id) == inner.local_id {
        // Someone has the same id as us — only update if it's an explicit
        // alive for our own incarnation (idempotent). Never demote.
        return ApplyOutcome::Ignored;
    }
    let entry = inner.by_id.get(&id).cloned();
    match entry {
        Some(existing) if incarnation < existing.incarnation => ApplyOutcome::Ignored,
        Some(existing) if incarnation == existing.incarnation && existing.state == MemberState::Alive => {
            ApplyOutcome::Ignored
        }
        Some(_) => {
            let member = Member {
                id,
                name: name.to_string(),
                addr,
                discovery_addr,
                birthdate,
                is_coordinator: false,
            };
            inner.by_id.insert(
                id,
                MemberEntry {
                    member,
                    state: MemberState::Alive,
                    incarnation,
                    suspect_deadline_ns: 0,
                },
            );
            re_sort(inner);
            ApplyOutcome::Updated
        }
        None => {
            let member = Member {
                id,
                name: name.to_string(),
                addr,
                discovery_addr,
                birthdate,
                is_coordinator: false,
            };
            inner.by_id.insert(
                id,
                MemberEntry {
                    member,
                    state: MemberState::Alive,
                    incarnation,
                    suspect_deadline_ns: 0,
                },
            );
            re_sort(inner);
            ApplyOutcome::NewMember
        }
    }
}

fn apply_suspect(
    inner: &mut Inner,
    id: MemberId,
    incarnation: Incarnation,
    now_ns: u64,
    suspicion_timeout_ns: u64,
) -> ApplyOutcome {
    if Some(id) == inner.local_id {
        return ApplyOutcome::SelfRefutationNeeded;
    }
    let Some(entry) = inner.by_id.get_mut(&id) else {
        return ApplyOutcome::Ignored;
    };
    if incarnation < entry.incarnation {
        return ApplyOutcome::Ignored;
    }
    match entry.state {
        MemberState::Dead => ApplyOutcome::Ignored,
        MemberState::Suspect if incarnation == entry.incarnation => ApplyOutcome::Ignored,
        _ => {
            entry.state = MemberState::Suspect;
            entry.incarnation = incarnation;
            entry.suspect_deadline_ns = now_ns.saturating_add(suspicion_timeout_ns);
            ApplyOutcome::Updated
        }
    }
}

fn apply_dead(inner: &mut Inner, id: MemberId, incarnation: Incarnation) -> ApplyOutcome {
    if Some(id) == inner.local_id {
        // Cannot ack our own death; instead force a refutation.
        return ApplyOutcome::SelfRefutationNeeded;
    }
    let Some(entry) = inner.by_id.get_mut(&id) else {
        return ApplyOutcome::Ignored;
    };
    if incarnation < entry.incarnation {
        return ApplyOutcome::Ignored;
    }
    if entry.state == MemberState::Dead {
        return ApplyOutcome::Ignored;
    }
    entry.state = MemberState::Dead;
    entry.incarnation = incarnation;
    entry.suspect_deadline_ns = 0;
    re_sort(inner);
    ApplyOutcome::Updated
}

fn re_sort(inner: &mut Inner) {
    let mut sorted: Vec<MemberId> = inner.by_id.keys().copied().collect();
    sorted.sort_by(|a, b| {
        let ea = &inner.by_id[a];
        let eb = &inner.by_id[b];
        match ea.member.birthdate.cmp(&eb.member.birthdate) {
            Ordering::Equal => a.as_u64().cmp(&b.as_u64()),
            other => other,
        }
    });
    inner.sorted = sorted;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn mk_member(id: u64, birthdate: u64, port: u16) -> Member {
        Member::new(
            MemberId::from_raw(id),
            format!("n{id}"),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2),
            birthdate,
        )
    }

    #[test]
    fn bootstrap_self_is_coordinator() {
        let view = MembershipView::bootstrap(mk_member(1, 100, 3320));
        let snap = view.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap[0].is_coordinator);
    }

    #[test]
    fn alive_event_adds_member() {
        let view = MembershipView::bootstrap(mk_member(1, 100, 3320));
        let other = mk_member(2, 50, 3324);
        let outcome = view.apply(
            &GossipEvent::Alive {
                id: other.id,
                name: other.name.clone(),
                addr: other.addr,
                discovery_addr: other.discovery_addr,
                birthdate: other.birthdate,
                incarnation: 1,
            },
            0,
            1_000_000_000,
        );
        assert_eq!(outcome, ApplyOutcome::NewMember);
        let snap = view.snapshot();
        assert_eq!(snap.len(), 2);
        // Older birthdate wins coordinator slot.
        assert_eq!(snap[0].id.as_u64(), 2);
        assert!(snap[0].is_coordinator);
        assert!(!snap[1].is_coordinator);
    }

    #[test]
    fn suspect_then_dead_reaps() {
        let view = MembershipView::bootstrap(mk_member(1, 100, 3320));
        let other = mk_member(2, 50, 3324);
        view.apply(
            &GossipEvent::Alive {
                id: other.id,
                name: other.name.clone(),
                addr: other.addr,
                discovery_addr: other.discovery_addr,
                birthdate: other.birthdate,
                incarnation: 3,
            },
            0,
            1000,
        );
        view.apply(
            &GossipEvent::Suspect {
                id: other.id,
                incarnation: 3,
                from: MemberId::from_raw(1),
            },
            0,
            1000,
        );
        let promoted = view.reap_suspects(5000);
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].0.as_u64(), 2);
        assert_eq!(view.snapshot().len(), 1);
    }

    #[test]
    fn alive_refutes_suspect_with_higher_incarnation() {
        let view = MembershipView::bootstrap(mk_member(1, 100, 3320));
        let other = mk_member(2, 50, 3324);
        view.apply(
            &GossipEvent::Alive {
                id: other.id,
                name: other.name.clone(),
                addr: other.addr,
                discovery_addr: other.discovery_addr,
                birthdate: other.birthdate,
                incarnation: 3,
            },
            0,
            1000,
        );
        view.apply(
            &GossipEvent::Suspect {
                id: other.id,
                incarnation: 3,
                from: MemberId::from_raw(1),
            },
            0,
            1000,
        );
        view.apply(
            &GossipEvent::Alive {
                id: other.id,
                name: other.name.clone(),
                addr: other.addr,
                discovery_addr: other.discovery_addr,
                birthdate: other.birthdate,
                incarnation: 4,
            },
            0,
            1000,
        );
        let entry = view.get(other.id).unwrap();
        assert_eq!(entry.state, MemberState::Alive);
        assert_eq!(entry.incarnation, 4);
    }

    #[test]
    fn self_suspect_triggers_refutation_signal() {
        let view = MembershipView::bootstrap(mk_member(1, 100, 3320));
        let outcome = view.apply(
            &GossipEvent::Suspect {
                id: MemberId::from_raw(1),
                incarnation: 1,
                from: MemberId::from_raw(2),
            },
            0,
            1000,
        );
        assert_eq!(outcome, ApplyOutcome::SelfRefutationNeeded);
    }

    #[test]
    fn coordinator_tiebreaker_uses_id() {
        let view = MembershipView::bootstrap(mk_member(5, 100, 3320));
        for id in [3_u64, 7, 1, 9] {
            let m = mk_member(id, 100, 3320 + id as u16 * 2);
            view.apply(
                &GossipEvent::Alive {
                    id: m.id,
                    name: m.name.clone(),
                    addr: m.addr,
                    discovery_addr: m.discovery_addr,
                    birthdate: m.birthdate,
                    incarnation: 1,
                },
                0,
                1000,
            );
        }
        let snap = view.snapshot();
        assert_eq!(snap[0].id.as_u64(), 1);
        assert!(snap[0].is_coordinator);
    }
}
