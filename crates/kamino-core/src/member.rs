//! Cluster member identity.
//!
//! Phase 3 surface (per `ROADMAP.md` §6 / `docs/03-cluster-management.md`).
//! The `Member` struct is owned by `kamino-core` so that every crate above the
//! core can talk about cluster membership without depending on
//! `kamino-cluster`. Membership transport, gossip, and SWIM live in
//! `kamino-cluster`.

use std::cmp::Ordering;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::ids::MemberId;

/// A single cluster member as seen by the local node.
///
/// Two members compare equal iff their [`MemberId`] matches; ordering is
/// `(birthdate ASC, id ASC)` per `docs/03-cluster-management.md` so that all
/// nodes converge on the same coordinator (index `0`) once their member lists
/// agree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    /// Unique identifier, used as the tiebreaker when birthdates collide.
    pub id: MemberId,
    /// Display name (typically `host:port` or a K8s pod name).
    pub name: String,
    /// RESP server address (Phase 2 server).
    pub addr: SocketAddr,
    /// SWIM protocol bind address (Phase 3 transport).
    pub discovery_addr: SocketAddr,
    /// Monotonic join timestamp, measured by the local clock at first sighting.
    /// Values are unix nanoseconds.
    pub birthdate: u64,
    /// Local view of whether this member is the coordinator. Computed from
    /// the sorted member list; do not serialise into routing tables.
    #[serde(default, skip_serializing)]
    pub is_coordinator: bool,
}

impl Member {
    /// Build a new member entry.
    #[must_use]
    pub fn new(
        id: MemberId,
        name: impl Into<String>,
        addr: SocketAddr,
        discovery_addr: SocketAddr,
        birthdate: u64,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            addr,
            discovery_addr,
            birthdate,
            is_coordinator: false,
        }
    }

    /// Ordering key for coordinator selection: `(birthdate, id)` ascending.
    #[must_use]
    pub const fn coord_key(&self) -> (u64, u64) {
        (self.birthdate, self.id.as_u64())
    }
}

impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Member {}

impl PartialOrd for Member {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Member {
    fn cmp(&self, other: &Self) -> Ordering {
        self.coord_key().cmp(&other.coord_key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn mk(id: u64, birthdate: u64) -> Member {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320);
        let disc = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3322);
        Member::new(
            MemberId::from_raw(id),
            format!("n{id}"),
            addr,
            disc,
            birthdate,
        )
    }

    #[test]
    fn sort_by_birthdate_then_id() {
        let mut v = vec![mk(2, 100), mk(1, 100), mk(3, 50), mk(0, 200)];
        v.sort();
        let ids: Vec<u64> = v.iter().map(|m| m.id.as_u64()).collect();
        assert_eq!(ids, vec![3, 1, 2, 0]);
    }

    #[test]
    fn eq_by_id_only() {
        let mut a = mk(7, 100);
        let b = mk(7, 999);
        assert_eq!(a, b);
        a.is_coordinator = true;
        assert_eq!(a, b);
    }
}
