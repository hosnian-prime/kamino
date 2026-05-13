//! SWIM transport abstraction.
//!
//! The state machine in [`crate::swim`] is transport-agnostic: it consumes a
//! [`Transport`] trait. The default implementation is a UDP socket
//! ([`UdpTransport`]); tests can substitute an in-memory transport that lets
//! `turmoil` drive deterministic simulation.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::net::UdpSocket;

use crate::error::{ClusterError, ClusterResult};
use crate::message::{Envelope, MAX_DATAGRAM};

/// Datagram-oriented transport used by the SWIM state machine.
///
/// Implementations must be safe to share across tasks (`Send + Sync`) so the
/// receiver and sender halves can run on independent tokio tasks. Both methods
/// are cancellation-safe with respect to `tokio::select!`.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Block until a datagram arrives, returning it together with the
    /// sender's address.
    async fn recv(&self) -> ClusterResult<(Envelope, SocketAddr)>;

    /// Send an envelope to the given address. Implementations should drop
    /// packets that exceed [`MAX_DATAGRAM`] rather than fragmenting.
    async fn send_to(&self, env: &Envelope, dst: SocketAddr) -> ClusterResult<()>;

    /// Local bind address (useful for advertising `discovery_addr`).
    fn local_addr(&self) -> ClusterResult<SocketAddr>;
}

/// Production transport over a single `tokio::net::UdpSocket`.
#[derive(Debug)]
pub struct UdpTransport {
    socket: Arc<UdpSocket>,
}

impl UdpTransport {
    /// Bind a UDP socket on the supplied address.
    pub async fn bind(addr: SocketAddr) -> ClusterResult<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self {
            socket: Arc::new(socket),
        })
    }

    /// Construct from an already-bound socket (useful in tests).
    #[must_use]
    pub fn from_socket(socket: Arc<UdpSocket>) -> Self {
        Self { socket }
    }
}

#[async_trait]
impl Transport for UdpTransport {
    async fn recv(&self) -> ClusterResult<(Envelope, SocketAddr)> {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        let (len, src) = self.socket.recv_from(&mut buf).await?;
        let env = Envelope::decode(&buf[..len])?;
        Ok((env, src))
    }

    async fn send_to(&self, env: &Envelope, dst: SocketAddr) -> ClusterResult<()> {
        let bytes = env.encode()?;
        let n = self.socket.send_to(&bytes, dst).await?;
        if n != bytes.len() {
            return Err(ClusterError::Codec(format!(
                "short UDP write: {n}/{}",
                bytes.len()
            )));
        }
        Ok(())
    }

    fn local_addr(&self) -> ClusterResult<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }
}
