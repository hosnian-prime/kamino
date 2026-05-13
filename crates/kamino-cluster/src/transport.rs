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

#[cfg(any(test, feature = "mock-transport"))]
pub use mock::{MockHub, MockTransport};

#[cfg(any(test, feature = "mock-transport"))]
mod mock {
    //! In-memory transport for deterministic SWIM tests.
    //!
    //! Two or more [`MockTransport`] instances share a [`MockHub`] that routes
    //! `Envelope`s by destination [`SocketAddr`]. The receive side is a
    //! per-instance unbounded channel — sufficient for unit tests where the
    //! receive loop drains promptly.

    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use tokio::sync::Mutex as AsyncMutex;
    use tokio::sync::mpsc;

    use super::Transport;
    use crate::error::{ClusterError, ClusterResult};
    use crate::message::Envelope;

    type Inbox = mpsc::UnboundedSender<(Envelope, SocketAddr)>;

    /// Shared routing table for a set of paired [`MockTransport`] endpoints.
    #[derive(Debug, Clone, Default)]
    pub struct MockHub {
        inner: Arc<Mutex<HubInner>>,
    }

    #[derive(Debug, Default)]
    struct HubInner {
        routes: HashMap<SocketAddr, Inbox>,
        /// Addresses that have been partitioned off. Sends from / to these
        /// addresses are silently dropped, modelling network failure.
        partitioned: std::collections::HashSet<SocketAddr>,
        /// Directional link drops `(from, to)` — useful when modelling
        /// asymmetric reachability (A can't reach B but C can still reach B).
        dropped_links: std::collections::HashSet<(SocketAddr, SocketAddr)>,
    }

    impl MockHub {
        /// Construct an empty hub.
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Register a new endpoint and return the matching [`MockTransport`].
        pub fn endpoint(&self, addr: SocketAddr) -> MockTransport {
            let (tx, rx) = mpsc::unbounded_channel();
            self.inner.lock().routes.insert(addr, tx);
            MockTransport {
                local: addr,
                hub: self.clone(),
                rx: AsyncMutex::new(rx),
            }
        }

        /// Mark an endpoint as partitioned: send-to and recv-from it are
        /// silently dropped.
        pub fn partition(&self, addr: SocketAddr) {
            self.inner.lock().partitioned.insert(addr);
        }

        /// Restore an endpoint to normal routing.
        pub fn heal(&self, addr: SocketAddr) {
            self.inner.lock().partitioned.remove(&addr);
        }

        /// Drop packets going from `from` to `to`. The reverse direction is
        /// unaffected — model asymmetric reachability.
        pub fn drop_link(&self, from: SocketAddr, to: SocketAddr) {
            self.inner.lock().dropped_links.insert((from, to));
        }

        /// Restore a previously dropped link.
        pub fn restore_link(&self, from: SocketAddr, to: SocketAddr) {
            self.inner.lock().dropped_links.remove(&(from, to));
        }

        fn route(&self, env: Envelope, from: SocketAddr, to: SocketAddr) -> ClusterResult<()> {
            let inner = self.inner.lock();
            if inner.partitioned.contains(&from) || inner.partitioned.contains(&to) {
                return Ok(());
            }
            if inner.dropped_links.contains(&(from, to)) {
                return Ok(());
            }
            let inbox = inner.routes.get(&to).cloned();
            drop(inner);
            let Some(inbox) = inbox else {
                // Drop silently — UDP semantics for an unbound peer.
                return Ok(());
            };
            inbox
                .send((env, from))
                .map_err(|_| ClusterError::Io(std::io::Error::other("mock transport: peer gone")))
        }
    }

    /// In-memory `Transport` implementation backed by a [`MockHub`].
    #[allow(missing_debug_implementations)]
    pub struct MockTransport {
        local: SocketAddr,
        hub: MockHub,
        rx: AsyncMutex<mpsc::UnboundedReceiver<(Envelope, SocketAddr)>>,
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn recv(&self) -> ClusterResult<(Envelope, SocketAddr)> {
            let mut rx = self.rx.lock().await;
            rx.recv()
                .await
                .ok_or_else(|| ClusterError::Io(std::io::Error::other("mock transport closed")))
        }

        async fn send_to(&self, env: &Envelope, dst: SocketAddr) -> ClusterResult<()> {
            self.hub.route(env.clone(), self.local, dst)
        }

        fn local_addr(&self) -> ClusterResult<SocketAddr> {
            Ok(self.local)
        }
    }
}
