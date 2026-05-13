//! SWIM wire messages.
//!
//! Frame format (MessagePack-encoded, fits in a single UDP datagram for all
//! Phase 3 message types — anti-entropy / routing-table push lands on TCP in
//! Phase 4):
//!
//! ```text
//! Header (constant 4 bytes):
//!   magic   = 0x4B 0x6D   ("Km" — SWIM)
//!   version = 0x01
//!   reserved = 0x00
//!
//! Body: rmp-serde encoded `Envelope` (named-map encoding so adding fields is
//! forward-compatible per docs/15-compatibility.md).
//! ```

use std::net::SocketAddr;

use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use serde::{Deserialize, Serialize};

use crate::error::{ClusterError, ClusterResult};

/// 4-byte frame prefix: `b"Km"`, version byte, reserved.
pub const HEADER: [u8; 4] = [b'K', b'm', 0x01, 0x00];

/// Maximum permitted UDP payload size (header + body). Conservative ceiling
/// well below the typical 1500-byte MTU after IP+UDP overhead.
pub const MAX_DATAGRAM: usize = 1400;

/// Monotonically increasing per-member incarnation number. Bumped by a node
/// when it learns it has been suspected so that the alive refutation wins.
pub type Incarnation = u64;

/// A SWIM event piggybacked on protocol messages (see
/// `docs/03-cluster-management.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GossipEvent {
    /// `id` is alive at `incarnation` and is reachable at `addr` /
    /// `discovery_addr`. Carries enough information for first-time learners
    /// to construct a complete `Member` entry.
    Alive {
        id: MemberId,
        name: String,
        addr: SocketAddr,
        discovery_addr: SocketAddr,
        birthdate: u64,
        incarnation: Incarnation,
    },
    /// `id` is suspected at the given incarnation. The receiver should adopt
    /// the suspicion if `incarnation >= local incarnation`.
    Suspect {
        id: MemberId,
        incarnation: Incarnation,
        from: MemberId,
    },
    /// `id` has been declared dead at the given incarnation. Final state for
    /// that incarnation.
    Dead {
        id: MemberId,
        incarnation: Incarnation,
        from: MemberId,
    },
    /// `id` is leaving the cluster voluntarily.
    Leave {
        id: MemberId,
        incarnation: Incarnation,
    },
}

/// The kind of SWIM message; the wire `Envelope` wraps this with shared
/// metadata (sender + piggybacked gossip).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SwimMessage {
    /// Direct probe (`A -> B`).
    Ping {
        /// Monotonic per-conversation sequence; the responder echoes it back.
        seq: u64,
        /// The id `from` believes `to` has. Receivers reject `Ping`s that
        /// don't match their own id to avoid stale-address misdelivery.
        target: MemberId,
    },
    /// Direct ack (`B -> A`).
    Ack { seq: u64, from: MemberId },
    /// Indirect probe (`A -> C` asking C to probe B).
    PingReq {
        seq: u64,
        target: MemberId,
        target_addr: SocketAddr,
    },
    /// Indirect ack (`C -> A` after receiving B's ack).
    IndirectAck {
        seq: u64,
        target: MemberId,
        from: MemberId,
    },
}

/// Wire envelope: identifies the sender, carries the SWIM payload, and
/// piggybacks gossip entries. `cluster_secret` is set to the configured
/// value for the cluster and validated by the receiver before processing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// Shared secret from `auth.cluster_secret`. Empty string disables the
    /// check.
    pub cluster_secret: String,
    /// Sender member id.
    pub from: MemberId,
    /// SWIM payload.
    pub msg: SwimMessage,
    /// Gossip piggyback batch.
    pub gossip: Vec<GossipEvent>,
}

impl Envelope {
    /// Serialise the envelope into a single UDP datagram.
    pub fn encode(&self) -> ClusterResult<Vec<u8>> {
        let body = rmp_serde::to_vec_named(self)
            .map_err(|e| ClusterError::Codec(format!("encode envelope: {e}")))?;
        let total = HEADER.len() + body.len();
        if total > MAX_DATAGRAM {
            return Err(ClusterError::Codec(format!(
                "envelope too large: {total} > {MAX_DATAGRAM}"
            )));
        }
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&HEADER);
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Decode a datagram. Rejects unknown magic / version bytes early so a
    /// stray packet from another protocol can never deserialise into garbage.
    pub fn decode(bytes: &[u8]) -> ClusterResult<Self> {
        if bytes.len() < HEADER.len() {
            return Err(ClusterError::Codec("short datagram".into()));
        }
        if bytes[..2] != HEADER[..2] {
            return Err(ClusterError::Codec(format!(
                "bad magic: {:02x}{:02x}",
                bytes[0], bytes[1]
            )));
        }
        if bytes[2] != HEADER[2] {
            return Err(ClusterError::Codec(format!(
                "unsupported swim version: {}",
                bytes[2]
            )));
        }
        rmp_serde::from_slice::<Self>(&bytes[HEADER.len()..])
            .map_err(|e| ClusterError::Codec(format!("decode envelope: {e}")))
    }

    /// Validate the `cluster_secret` against the local expectation.
    pub fn check_secret(&self, expected: &str) -> ClusterResult<()> {
        if self.cluster_secret == expected {
            Ok(())
        } else {
            Err(ClusterError::Handshake("cluster_secret mismatch".into()))
        }
    }
}

/// Convenience constructor for an `Alive` gossip event from a `Member`.
pub fn alive_for(member: &Member, incarnation: Incarnation) -> GossipEvent {
    GossipEvent::Alive {
        id: member.id,
        name: member.name.clone(),
        addr: member.addr,
        discovery_addr: member.discovery_addr,
        birthdate: member.birthdate,
        incarnation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn mk_envelope() -> Envelope {
        Envelope {
            cluster_secret: "s3cret".into(),
            from: MemberId::from_raw(0xdead_beef),
            msg: SwimMessage::Ping {
                seq: 42,
                target: MemberId::from_raw(0xc0de),
            },
            gossip: vec![GossipEvent::Alive {
                id: MemberId::from_raw(0xc0de),
                name: "n1".into(),
                addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
                discovery_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3322),
                birthdate: 12345,
                incarnation: 7,
            }],
        }
    }

    #[test]
    fn roundtrip() {
        let env = mk_envelope();
        let bytes = env.encode().unwrap();
        assert_eq!(&bytes[..4], &HEADER);
        let back = Envelope::decode(&bytes).unwrap();
        assert_eq!(env, back);
    }

    #[test]
    fn bad_magic_rejected() {
        let mut bytes = mk_envelope().encode().unwrap();
        bytes[0] = b'X';
        assert!(matches!(
            Envelope::decode(&bytes),
            Err(ClusterError::Codec(_))
        ));
    }

    #[test]
    fn unknown_version_rejected() {
        let mut bytes = mk_envelope().encode().unwrap();
        bytes[2] = 0xFF;
        assert!(matches!(
            Envelope::decode(&bytes),
            Err(ClusterError::Codec(_))
        ));
    }

    #[test]
    fn secret_mismatch() {
        let env = mk_envelope();
        assert!(env.check_secret("nope").is_err());
        assert!(env.check_secret("s3cret").is_ok());
    }
}
