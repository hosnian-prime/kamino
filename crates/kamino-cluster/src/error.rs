//! Errors raised by the cluster subsystems.

use std::io;
use std::net::SocketAddr;

use thiserror::Error;

/// Failures from the SWIM transport, discovery, or membership orchestrator.
#[derive(Debug, Error)]
pub enum ClusterError {
    /// Underlying socket I/O.
    #[error("transport I/O error: {0}")]
    Io(#[from] io::Error),

    /// Codec / wire-format error.
    #[error("malformed SWIM message: {0}")]
    Codec(String),

    /// `cluster_secret` mismatch on inter-node handshake.
    #[error("cluster handshake rejected: {0}")]
    Handshake(String),

    /// Discovery plugin returned no peers and the call to `init` failed.
    #[error("discovery failed: {0}")]
    Discovery(String),

    /// Could not join any peer before `bootstrap_timeout`.
    #[error("join failed after {attempts} attempts (last error: {last})")]
    JoinFailed { attempts: u32, last: String },

    /// Tried to reach a peer that is not yet known to the local view.
    #[error("unknown peer: {0}")]
    UnknownPeer(SocketAddr),

    /// Configuration is invalid for the requested operation.
    #[error("invalid cluster configuration: {0}")]
    Config(String),

    /// The runtime has shut down.
    #[error("cluster runtime shut down")]
    Shutdown,
}

/// Convenience alias.
pub type ClusterResult<T> = Result<T, ClusterError>;
