//! `RoutingTable` data model + `MessagePack` codec.
//!
//! The on-wire form is described in `docs/02-consistent-hashing.md` ("Routing
//! Table Structure") and `docs/15-compatibility.md` ("Routing Table Schema
//! Evolution"):
//!
//! - **Named-map `MessagePack`**: every field is keyed by its `serde`
//!   identifier. Adding fields in later phases is forward-compatible because
//!   decoders default missing fields to their type's zero value.
//! - **`schema_version: u16`** as the first field. Receivers that see an
//!   unknown `schema_version` within the same MAJOR log a warning and
//!   continue with best-effort decoding; a different MAJOR aborts (Phase 11
//!   wires the MAJOR check at HELLO time).
//! - **`signature: u64`** — scalar Lamport clock for transient
//!   dual-coordinator resolution.
//! - Per `docs/02-consistent-hashing.md`, `primary` and `backup` are keyed by
//!   partition id (`u32`). They store `Vec<Member>` so receivers know how to
//!   route without a separate member lookup.

use std::collections::HashMap;
use std::sync::Arc;

use kamino_core::hasher::Hasher;
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use serde::{Deserialize, Serialize};

use crate::error::{ClusterError, ClusterResult};
use crate::routing::ring::{Assignment, assign};

/// Current routing-table schema. Bumped on additive changes for diagnostics;
/// a different MAJOR (Phase 11 introduces the MAJOR check at HELLO time)
/// blocks cross-version chatter entirely.
pub const ROUTING_SCHEMA_VERSION: u16 = 1;

/// Cluster-wide partition map. Serialised with `rmp-serde` named-map encoding
/// for forward-compatibility (see `docs/15-compatibility.md`).
///
/// The decoder is intentionally tolerant: missing fields take their default
/// value, and unknown fields are dropped. That's the exact contract that
/// lets MINOR releases add fields without bumping MAJOR.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RoutingTable {
    /// First field — decoders inspect before reading the rest.
    pub schema_version: u16,
    /// Monotonic clock incremented by the coordinator on every topology
    /// change. Receivers reject tables with `signature <= local_signature`
    /// (see [`crate::routing::store::RoutingTableStore`]).
    pub signature: u64,
    /// Cluster members, sorted by `(birthdate, id)` — index 0 is the
    /// coordinator at the time this table was built.
    pub members: Vec<Member>,
    /// `partition_id -> Vec<Member>` (head = current primary, tail =
    /// previous owners during fragmented-partition transitions per
    /// `docs/02-consistent-hashing.md`).
    pub primary: HashMap<u32, Vec<Member>>,
    /// `partition_id -> Vec<Member>` (closest `replica_count - 1` distinct
    /// nodes on the ring, primary excluded).
    pub backup: HashMap<u32, Vec<Member>>,
}

impl RoutingTable {
    /// Build the canonical, non-fragmented table for `members`.
    ///
    /// Returns `None` if `members` is empty.
    #[must_use]
    pub fn build(
        members: Vec<Member>,
        hasher: &dyn Hasher,
        partition_count: u32,
        virtual_nodes_per_member: u32,
        load_factor: f64,
        replica_count: u32,
        signature: u64,
    ) -> Option<Self> {
        let Assignment {
            primary: prim_ids,
            backups: backup_ids,
        } = assign(
            hasher,
            &members,
            partition_count,
            virtual_nodes_per_member,
            load_factor,
            replica_count,
        )?;

        let by_id: HashMap<MemberId, Member> = members.iter().map(|m| (m.id, m.clone())).collect();

        let mut primary = HashMap::with_capacity(prim_ids.len());
        let mut backup = HashMap::with_capacity(prim_ids.len());
        for (idx, prim_id) in prim_ids.into_iter().enumerate() {
            let part_id = u32::try_from(idx).expect("partition_count fits in u32");
            let prim = by_id
                .get(&prim_id)
                .expect("ring assignment yields a known member id")
                .clone();
            primary.insert(part_id, vec![prim]);
            let backup_members: Vec<Member> = backup_ids[idx]
                .iter()
                .filter_map(|id| by_id.get(id).cloned())
                .collect();
            backup.insert(part_id, backup_members);
        }

        Some(Self {
            schema_version: ROUTING_SCHEMA_VERSION,
            signature,
            members,
            primary,
            backup,
        })
    }

    /// Number of partitions described by this table.
    #[must_use]
    pub fn partition_count(&self) -> u32 {
        u32::try_from(self.primary.len()).unwrap_or(u32::MAX)
    }

    /// Primary owner for `partition_id`, or `None` if the partition is
    /// unknown to this table.
    #[must_use]
    pub fn primary_for(&self, partition_id: u32) -> Option<&Member> {
        self.primary.get(&partition_id).and_then(|v| v.first())
    }

    /// All current owners (primary + previous primaries during fragmented
    /// transitions). Head is current.
    #[must_use]
    pub fn owners_for(&self, partition_id: u32) -> &[Member] {
        self.primary
            .get(&partition_id)
            .map_or(&[][..], Vec::as_slice)
    }

    /// Backup owners for `partition_id`.
    #[must_use]
    pub fn backups_for(&self, partition_id: u32) -> &[Member] {
        self.backup
            .get(&partition_id)
            .map_or(&[][..], Vec::as_slice)
    }

    /// Serialise to `MessagePack` named-map bytes for the wire.
    pub fn to_msgpack(&self) -> ClusterResult<Vec<u8>> {
        rmp_serde::to_vec_named(self)
            .map_err(|e| ClusterError::Codec(format!("encode routing table: {e}")))
    }

    /// Deserialise from `MessagePack` bytes. Unknown fields are silently
    /// dropped (additive-changes contract).
    pub fn from_msgpack(bytes: &[u8]) -> ClusterResult<Self> {
        rmp_serde::from_slice::<Self>(bytes)
            .map_err(|e| ClusterError::Codec(format!("decode routing table: {e}")))
    }
}

/// Compute the partition id for `(dmap_name, key)` per
/// `docs/02-consistent-hashing.md`:
///
/// ```text
/// partition_id = hash(dmap_name + key) % partition_count
/// ```
///
/// Concatenates the bytes directly — the hasher consumes the joined buffer
/// so collisions across `(dmap, key)` boundaries are not a concern any more
/// than for any string concatenation.
#[must_use]
pub fn partition_for(
    hasher: &dyn Hasher,
    dmap_name: &[u8],
    key: &[u8],
    partition_count: u32,
) -> u32 {
    let mut buf = Vec::with_capacity(dmap_name.len() + key.len());
    buf.extend_from_slice(dmap_name);
    buf.extend_from_slice(key);
    let h = hasher.hash64(&buf);
    // `h % partition_count` always fits in u32 because partition_count is u32.
    u32::try_from(h % u64::from(partition_count)).expect("modulo of u32 fits in u32")
}

/// Cheap shared handle to a routing table.
pub type SharedRoutingTable = Arc<RoutingTable>;

#[cfg(test)]
mod tests {
    use super::*;
    use kamino_core::hasher::XxHasher;
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
    fn build_populates_every_partition() {
        let h = XxHasher;
        let members = vec![mk(1, 100, 3320), mk(2, 200, 3322), mk(3, 300, 3324)];
        let rt = RoutingTable::build(members, &h, 271, 20, 1.25, 2, 1).unwrap();
        assert_eq!(rt.signature, 1);
        assert_eq!(rt.partition_count(), 271);
        for p in 0..271 {
            assert!(rt.primary_for(p).is_some(), "missing primary for {p}");
            assert!(!rt.backups_for(p).is_empty(), "missing backups for {p}");
        }
    }

    #[test]
    fn empty_members_rejected() {
        let h = XxHasher;
        assert!(RoutingTable::build(vec![], &h, 271, 20, 1.25, 1, 1).is_none());
    }

    #[test]
    fn roundtrip_msgpack_preserves_table() {
        let h = XxHasher;
        let members = vec![mk(1, 100, 3320), mk(2, 200, 3322)];
        let rt = RoutingTable::build(members, &h, 271, 20, 1.25, 1, 42).unwrap();
        let bytes = rt.to_msgpack().unwrap();
        let back = RoutingTable::from_msgpack(&bytes).unwrap();
        assert_eq!(back, rt);
    }

    #[test]
    fn decoder_tolerates_unknown_fields() {
        // Encode a table, then decode it after wrapping the bytes in a
        // larger named map with an extra unknown field. We do this by
        // serialising a sibling struct with one extra field and confirming
        // the routing fields survive the trip.
        #[derive(Serialize)]
        #[allow(dead_code)] // serialised, not consumed
        struct Plus {
            schema_version: u16,
            signature: u64,
            members: Vec<Member>,
            primary: HashMap<u32, Vec<Member>>,
            backup: HashMap<u32, Vec<Member>>,
            phase_6_field: String,
        }
        let h = XxHasher;
        let members = vec![mk(1, 100, 3320)];
        let rt = RoutingTable::build(members, &h, 271, 20, 1.25, 1, 7).unwrap();
        let extended = Plus {
            schema_version: rt.schema_version,
            signature: rt.signature,
            members: rt.members.clone(),
            primary: rt.primary.clone(),
            backup: rt.backup.clone(),
            phase_6_field: "future use".into(),
        };
        let bytes = rmp_serde::to_vec_named(&extended).unwrap();
        let back = RoutingTable::from_msgpack(&bytes).unwrap();
        // Unknown field dropped; routing payload intact.
        assert_eq!(back.signature, rt.signature);
        assert_eq!(back.partition_count(), rt.partition_count());
    }

    #[test]
    fn partition_for_is_stable() {
        let h = XxHasher;
        let a = partition_for(&h, b"sessions", b"user:1", 271);
        let b = partition_for(&h, b"sessions", b"user:1", 271);
        assert_eq!(a, b);
        assert!(a < 271);
    }

    #[test]
    fn partition_for_separates_dmaps() {
        let h = XxHasher;
        // Distinct dmaps shouldn't always collide on the same partition for
        // the same key (statistical assertion — at least one differs in 8).
        let mut differ = 0;
        for i in 0..8 {
            let k = format!("k{i}");
            let a = partition_for(&h, b"dm1", k.as_bytes(), 271);
            let b = partition_for(&h, b"dm2", k.as_bytes(), 271);
            if a != b {
                differ += 1;
            }
        }
        assert!(differ > 0, "expected some independence across dmaps");
    }
}
