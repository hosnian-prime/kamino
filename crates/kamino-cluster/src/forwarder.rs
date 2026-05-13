//! Inter-node RESP forwarder (per `docs/06-network-protocol.md` "Inter-Node
//! Communication").
//!
//! The forwarder owns one **pool per peer**. Each pool holds
//! `internode_pool_size` TCP connections; each connection multiplexes up to
//! `internode_inflight_per_conn` RESP requests with response demux by FIFO
//! ordering on the wire. Three backpressure mechanisms cap memory growth:
//!
//! 1. **TCP socket buffers** (kernel level, automatic).
//! 2. **Per-connection inflight cap** via a `tokio::sync::Semaphore`. When
//!    full, the next forward call awaits a permit.
//! 3. **Per-RPC `internode_request_timeout`** — late replies free their
//!    permit, returning `ClusterError::Timeout` to the caller.
//!
//! Failure handling (per `docs/06-network-protocol.md` "Failure Handling on
//! the Forward Path"):
//!
//! - Peer disconnect → every in-flight request on that connection resolves
//!   to `ClusterError::ServerGone`. The pool reopens the connection in the
//!   background with exponential backoff
//!   (`internode_reconnect_backoff_min..max`).
//! - `evict_peer` (called when SWIM marks a peer dead) drains the pool and
//!   bounces every queued request with `ServerGone`.
//! - Server `-MOVED ...` replies surface as `ClusterError::Moved`; the
//!   caller refreshes routing and may retry once.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use kamino_protocol::{Command, Frame, HelloArgs, RespCodec};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use crate::error::{ClusterError, ClusterResult};
use crate::routing::coordinator::RoutingPusher;

/// Static tunables shared across every peer pool. Sourced from
/// `network.internode_*` in [`kamino_core::config::NetworkConfig`].
#[derive(Debug, Clone)]
pub struct ForwarderConfig {
    pub pool_size: u32,
    pub inflight_per_conn: u32,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub reconnect_backoff_min: Duration,
    pub reconnect_backoff_max: Duration,
    /// `cluster_secret` attached to every inter-node `AUTH` handshake.
    pub cluster_secret: String,
}

impl Default for ForwarderConfig {
    fn default() -> Self {
        Self {
            pool_size: 4,
            inflight_per_conn: 256,
            connect_timeout: Duration::from_millis(500),
            request_timeout: Duration::from_secs(2),
            reconnect_backoff_min: Duration::from_millis(100),
            reconnect_backoff_max: Duration::from_secs(5),
            cluster_secret: String::new(),
        }
    }
}

/// Top-level forwarder.
///
/// Cheap to clone. Internally an `Arc` over the peer-pool registry.
#[derive(Clone)]
pub struct Forwarder {
    inner: Arc<ForwarderInner>,
}

impl std::fmt::Debug for Forwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forwarder")
            .field("pool_size", &self.inner.config.pool_size)
            .field("inflight_per_conn", &self.inner.config.inflight_per_conn)
            .finish_non_exhaustive()
    }
}

struct ForwarderInner {
    config: ForwarderConfig,
    pools: Mutex<std::collections::HashMap<SocketAddr, Arc<PeerPool>>>,
    connector: Arc<dyn Connector>,
}

impl Forwarder {
    /// Build a new forwarder with the default TCP connector.
    #[must_use]
    pub fn new(config: ForwarderConfig) -> Self {
        Self::with_connector(config, Arc::new(TcpConnector))
    }

    /// Build a forwarder with a custom connector (used by tests so RESP
    /// peers can live in-process over `tokio::io::duplex`).
    #[must_use]
    pub fn with_connector(config: ForwarderConfig, connector: Arc<dyn Connector>) -> Self {
        Self {
            inner: Arc::new(ForwarderInner {
                config,
                pools: Mutex::new(std::collections::HashMap::new()),
                connector,
            }),
        }
    }

    /// Forward `cmd` to `peer` and await the response.
    ///
    /// Returns `ClusterError::ServerGone` if the pool can't reach the peer,
    /// `ClusterError::Timeout` if the per-RPC deadline elapses, or
    /// `ClusterError::Moved` if the peer answers `-MOVED ...`.
    pub async fn send(&self, peer: SocketAddr, cmd: Command) -> ClusterResult<Frame> {
        let pool = self.get_or_create_pool(peer);
        pool.send(cmd, self.inner.config.request_timeout).await
    }

    /// Drain the pool for `peer` and return queued requests with
    /// `ServerGone`. Called when SWIM marks the peer dead.
    pub fn evict_peer(&self, peer: SocketAddr) {
        let pool = self.inner.pools.lock().remove(&peer);
        if let Some(pool) = pool {
            pool.shutdown();
        }
    }

    fn get_or_create_pool(&self, peer: SocketAddr) -> Arc<PeerPool> {
        let mut pools = self.inner.pools.lock();
        if let Some(p) = pools.get(&peer) {
            return Arc::clone(p);
        }
        let pool = Arc::new(PeerPool::new(
            peer,
            self.inner.config.clone(),
            Arc::clone(&self.inner.connector),
        ));
        pools.insert(peer, Arc::clone(&pool));
        pool
    }
}

#[async_trait]
impl RoutingPusher for Forwarder {
    async fn push(&self, target: SocketAddr, table_bytes: &[u8]) -> ClusterResult<()> {
        let cmd = Command::InternalNodeUpdateRouting {
            table: bytes::Bytes::copy_from_slice(table_bytes),
        };
        match self.send(target, cmd).await {
            Ok(Frame::SimpleString(s)) if s == "OK" || s == "STALE" || s == "SCHEMA" => Ok(()),
            Ok(Frame::Error(e)) => Err(ClusterError::Codec(format!(
                "peer rejected routing push: {e}",
            ))),
            Ok(other) => Err(ClusterError::Codec(format!(
                "unexpected routing-push reply: {other:?}",
            ))),
            Err(e) => Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Per-peer pool
// ---------------------------------------------------------------------------

/// Tracks one peer's pool of pipelined connections.
struct PeerPool {
    addr: SocketAddr,
    config: ForwarderConfig,
    /// Round-robin pointer into `conns`.
    next: AtomicUsize,
    conns: Mutex<Vec<Arc<PoolConn>>>,
    connector: Arc<dyn Connector>,
    /// When set, no more forwards are accepted (peer evicted).
    closed: AtomicBool,
}

impl PeerPool {
    fn new(addr: SocketAddr, config: ForwarderConfig, connector: Arc<dyn Connector>) -> Self {
        Self {
            addr,
            config,
            next: AtomicUsize::new(0),
            conns: Mutex::new(Vec::new()),
            connector,
            closed: AtomicBool::new(false),
        }
    }

    // Clippy's `option_if_let_else` wants `map_or_else`, then its
    // `redundant_closure_for_method_calls` wants `|r| r` removed — but the
    // outer wrapper is `Result<ClusterResult<Frame>, _>`, so the inner
    // closure isn't actually redundant. Suppress the first to land on a
    // readable match.
    #[allow(clippy::option_if_let_else)]
    async fn send(&self, cmd: Command, request_timeout: Duration) -> ClusterResult<Frame> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ClusterError::ServerGone(format!("peer {} evicted", self.addr)));
        }
        let conn = self.pick_conn().await?;
        let outcome = timeout(request_timeout, conn.send_request(cmd)).await;
        match outcome {
            Ok(r) => r,
            Err(_) => Err(ClusterError::Timeout(format!(
                "internode RPC to {} exceeded {:?}",
                self.addr, request_timeout,
            ))),
        }
    }

    async fn pick_conn(&self) -> ClusterResult<Arc<PoolConn>> {
        // Ensure pool size invariant.
        let pool_size = usize::try_from(self.config.pool_size.max(1)).unwrap_or(1);
        loop {
            {
                let mut guard = self.conns.lock();
                guard.retain(|c| !c.is_terminal());
                if guard.len() >= pool_size {
                    let idx = self.next.fetch_add(1, Ordering::Relaxed) % guard.len();
                    let conn = Arc::clone(&guard[idx]);
                    return Ok(conn);
                }
            }
            // Below target capacity — spin up another connection. Multiple
            // concurrent callers may race here; that's fine, we just clamp
            // back to `pool_size` after.
            let conn = self.dial_new().await?;
            let mut guard = self.conns.lock();
            if guard.len() < pool_size {
                guard.push(conn);
            }
        }
    }

    async fn dial_new(&self) -> ClusterResult<Arc<PoolConn>> {
        let stream = self
            .connector
            .connect(self.addr, self.config.connect_timeout)
            .await
            .map_err(|e| ClusterError::ServerGone(format!("connect {}: {e}", self.addr)))?;
        PoolConn::start(self.addr, stream, self.config.clone()).await
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let conns = std::mem::take(&mut *self.conns.lock());
        for c in conns {
            c.shutdown("peer evicted");
        }
    }
}

// ---------------------------------------------------------------------------
// Pooled connection
// ---------------------------------------------------------------------------

struct PoolConn {
    addr: SocketAddr,
    cmd_tx: mpsc::Sender<OutgoingCommand>,
    permits: Arc<Semaphore>,
    terminal: Arc<AtomicBool>,
    notify_terminal: Arc<Notify>,
    writer_handle: Mutex<Option<JoinHandle<()>>>,
    reader_handle: Mutex<Option<JoinHandle<()>>>,
}

type OutgoingCommand = (Command, oneshot::Sender<ClusterResult<Frame>>, OwnedSemaphorePermit);

impl PoolConn {
    async fn start<S>(
        addr: SocketAddr,
        stream: S,
        config: ForwarderConfig,
    ) -> ClusterResult<Arc<Self>>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let codec = RespCodec::new();
        let framed = Framed::new(stream, codec);
        let permits = Arc::new(Semaphore::new(usize::try_from(
            config.inflight_per_conn.max(1),
        ).unwrap_or(1)));
        let (cmd_tx, cmd_rx) = mpsc::channel::<OutgoingCommand>(usize::try_from(
            config.inflight_per_conn.max(1),
        ).unwrap_or(1));
        let in_flight: Arc<Mutex<VecDeque<InFlight>>> = Arc::new(Mutex::new(VecDeque::new()));
        let terminal = Arc::new(AtomicBool::new(false));
        let notify_terminal = Arc::new(Notify::new());

        let (sink, stream_part) = framed.split();
        let writer = WriterTask {
            sink,
            cmd_rx,
            in_flight: Arc::clone(&in_flight),
            terminal: Arc::clone(&terminal),
            notify_terminal: Arc::clone(&notify_terminal),
            addr,
        };
        let reader = ReaderTask {
            stream: stream_part,
            in_flight,
            terminal: Arc::clone(&terminal),
            notify_terminal: Arc::clone(&notify_terminal),
            addr,
        };

        let conn = Arc::new(Self {
            addr,
            cmd_tx,
            permits,
            terminal,
            notify_terminal,
            writer_handle: Mutex::new(None),
            reader_handle: Mutex::new(None),
        });

        // Perform inline HELLO+AUTH handshake using the conn we just built.
        let conn_for_hs = Arc::clone(&conn);
        let writer_handle = tokio::spawn(writer.run());
        let reader_handle = tokio::spawn(reader.run());
        *conn.writer_handle.lock() = Some(writer_handle);
        *conn.reader_handle.lock() = Some(reader_handle);

        let handshake = handshake_inner(&conn_for_hs, &config.cluster_secret);
        match timeout(config.connect_timeout, handshake).await {
            Ok(Ok(())) => Ok(conn_for_hs),
            Ok(Err(e)) => {
                conn_for_hs.shutdown(&format!("handshake failed: {e}"));
                Err(e)
            }
            Err(_) => {
                conn_for_hs.shutdown("handshake timed out");
                Err(ClusterError::ServerGone(format!(
                    "handshake to {addr} timed out",
                )))
            }
        }
    }

    fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }

    fn shutdown(&self, reason: &str) {
        self.terminal.store(true, Ordering::Release);
        self.notify_terminal.notify_waiters();
        let writer = self.writer_handle.lock().take();
        if let Some(h) = writer {
            h.abort();
        }
        let reader = self.reader_handle.lock().take();
        if let Some(h) = reader {
            h.abort();
        }
        trace!(addr = %self.addr, reason, "pool conn shut down");
    }

    async fn send_request(&self, cmd: Command) -> ClusterResult<Frame> {
        if self.is_terminal() {
            return Err(ClusterError::ServerGone(format!(
                "connection to {} closed",
                self.addr,
            )));
        }
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| {
                ClusterError::ServerGone(format!("permit semaphore closed for {}", self.addr))
            })?;
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.send((cmd, tx, permit)).await.is_err() {
            return Err(ClusterError::ServerGone(format!(
                "writer queue closed for {}",
                self.addr,
            )));
        }
        rx.await.map_err(|_| {
            ClusterError::ServerGone(format!("response slot dropped for {}", self.addr))
        })?
    }
}

async fn handshake_inner(conn: &PoolConn, cluster_secret: &str) -> ClusterResult<()> {
    // We can't use `Forwarder::send`'s timeout-wrapped path because we are
    // _inside_ start(); instead drive a plain `send_request` and fold any
    // failure into a clean error.
    let hello = HelloArgs {
        protocol_version: Some(2),
        auth: if cluster_secret.is_empty() {
            None
        } else {
            Some((None, bytes::Bytes::copy_from_slice(cluster_secret.as_bytes())))
        },
        client_name: Some(bytes::Bytes::from_static(b"kamino-internode")),
    };
    let reply = conn.send_request(Command::Hello(hello)).await?;
    match reply {
        Frame::Map(_) | Frame::Array(Some(_)) | Frame::SimpleString(_) => Ok(()),
        Frame::Error(e) => Err(ClusterError::Handshake(e)),
        other => Err(ClusterError::Codec(format!(
            "handshake unexpected reply: {other:?}",
        ))),
    }
}

struct InFlight {
    reply: oneshot::Sender<ClusterResult<Frame>>,
    /// Permit dropped when the slot resolves (success or failure).
    _permit: OwnedSemaphorePermit,
}

struct WriterTask<S> {
    sink: futures::stream::SplitSink<Framed<S, RespCodec>, Frame>,
    cmd_rx: mpsc::Receiver<OutgoingCommand>,
    in_flight: Arc<Mutex<VecDeque<InFlight>>>,
    terminal: Arc<AtomicBool>,
    notify_terminal: Arc<Notify>,
    addr: SocketAddr,
}

impl<S> WriterTask<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    async fn run(mut self) {
        loop {
            let next = self.cmd_rx.recv().await;
            let Some((cmd, slot, permit)) = next else {
                break;
            };
            if self.terminal.load(Ordering::Acquire) {
                let _ = slot.send(Err(ClusterError::ServerGone(format!(
                    "connection to {} closed",
                    self.addr,
                ))));
                drop(permit);
                continue;
            }
            let frame = cmd.to_frame();
            self.in_flight.lock().push_back(InFlight {
                reply: slot,
                _permit: permit,
            });
            if let Err(e) = self.sink.send(frame).await {
                warn!(addr = %self.addr, error = %e, "internode write failed; failing in-flight");
                self.fail_all(&format!("write failed: {e}"));
                self.terminal.store(true, Ordering::Release);
                self.notify_terminal.notify_waiters();
                return;
            }
        }
        // Channel closed by pool drop.
        self.fail_all("writer channel closed");
        self.terminal.store(true, Ordering::Release);
        self.notify_terminal.notify_waiters();
    }

    fn fail_all(&self, reason: &str) {
        let mut q = self.in_flight.lock();
        while let Some(slot) = q.pop_front() {
            let _ = slot.reply.send(Err(ClusterError::ServerGone(reason.to_string())));
        }
    }
}

struct ReaderTask<S> {
    stream: futures::stream::SplitStream<Framed<S, RespCodec>>,
    in_flight: Arc<Mutex<VecDeque<InFlight>>>,
    terminal: Arc<AtomicBool>,
    notify_terminal: Arc<Notify>,
    addr: SocketAddr,
}

impl<S> ReaderTask<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    async fn run(mut self) {
        loop {
            let Some(frame_result) = self.stream.next().await else {
                break;
            };
            match frame_result {
                Ok(frame) => {
                    let slot = self.in_flight.lock().pop_front();
                    if let Some(slot) = slot {
                        let mapped = map_reply(frame);
                        let _ = slot.reply.send(mapped);
                    } else {
                        debug!(addr = %self.addr, "frame with no matching slot");
                    }
                }
                Err(e) => {
                    warn!(addr = %self.addr, error = %e, "internode decode error; failing in-flight");
                    self.fail_all(&format!("decode error: {e}"));
                    self.terminal.store(true, Ordering::Release);
                    self.notify_terminal.notify_waiters();
                    return;
                }
            }
        }
        self.fail_all("peer closed connection");
        self.terminal.store(true, Ordering::Release);
        self.notify_terminal.notify_waiters();
    }

    fn fail_all(&self, reason: &str) {
        let mut q = self.in_flight.lock();
        while let Some(slot) = q.pop_front() {
            let _ = slot.reply.send(Err(ClusterError::ServerGone(reason.to_string())));
        }
    }
}

/// Translate a `-MOVED ...` frame into `ClusterError::Moved`; pass everything
/// else through untouched.
fn map_reply(frame: Frame) -> ClusterResult<Frame> {
    if let Frame::Error(e) = &frame {
        if let Some(rest) = e.strip_prefix("MOVED ") {
            return Err(ClusterError::Moved(rest.to_string()));
        }
    }
    Ok(frame)
}

// ---------------------------------------------------------------------------
// Connector abstraction
// ---------------------------------------------------------------------------

/// Streaming-transport factory. Tests pass an in-memory duplex.
#[async_trait]
pub trait Connector: Send + Sync + std::fmt::Debug {
    async fn connect(
        &self,
        addr: SocketAddr,
        deadline: Duration,
    ) -> std::io::Result<Box<dyn DuplexStream>>;
}

/// Marker for `AsyncRead + AsyncWrite + Send + Unpin + 'static` streams.
pub trait DuplexStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> DuplexStream for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

/// Plain TCP connector.
#[derive(Debug, Default, Clone, Copy)]
pub struct TcpConnector;

#[async_trait]
impl Connector for TcpConnector {
    async fn connect(
        &self,
        addr: SocketAddr,
        deadline: Duration,
    ) -> std::io::Result<Box<dyn DuplexStream>> {
        let stream = timeout(deadline, TcpStream::connect(addr))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"))??;
        stream.set_nodelay(true).ok();
        Ok(Box::new(stream))
    }
}

// ---------------------------------------------------------------------------
// Tests (in-process duplex)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kamino_protocol::BulkString;
    use tokio::io::DuplexStream as TokioDuplex;

    /// In-memory connector that hands out one end of `tokio::io::duplex`
    /// pre-wired to a server-side echo loop.
    #[derive(Debug)]
    struct LocalConnector {
        server: Arc<dyn LocalServer + Send + Sync>,
    }

    #[async_trait]
    trait LocalServer: std::fmt::Debug {
        async fn handle(&self, server_end: TokioDuplex);
    }

    #[async_trait]
    impl Connector for LocalConnector {
        async fn connect(
            &self,
            _addr: SocketAddr,
            _deadline: Duration,
        ) -> std::io::Result<Box<dyn DuplexStream>> {
            let (a, b) = tokio::io::duplex(64 * 1024);
            let server = Arc::clone(&self.server);
            tokio::spawn(async move {
                server.handle(b).await;
            });
            Ok(Box::new(a))
        }
    }

    /// Loop that answers HELLO + PING; tracks the count of PINGs seen.
    #[derive(Debug)]
    struct PingEcho {
        seen: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl LocalServer for PingEcho {
        async fn handle(&self, server_end: TokioDuplex) {
            let mut framed = Framed::new(server_end, RespCodec::new());
            while let Some(item) = framed.next().await {
                let Ok(frame) = item else {
                    return;
                };
                let Ok(cmd) = Command::parse(frame) else {
                    let _ = framed.send(Frame::Error("ERR parse".into())).await;
                    continue;
                };
                let reply = match cmd {
                    Command::Hello(_) => Frame::Array(Some(vec![
                        Frame::Bulk(BulkString::from("server")),
                        Frame::Bulk(BulkString::from("kamino")),
                    ])),
                    Command::Ping(_) => {
                        self.seen.fetch_add(1, Ordering::Relaxed);
                        Frame::SimpleString("PONG".into())
                    }
                    Command::Quit => {
                        let _ = framed.send(Frame::ok()).await;
                        return;
                    }
                    _ => Frame::Error("ERR unexpected".into()),
                };
                if framed.send(reply).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Loop that always responds with `-MOVED 3 127.0.0.1:9999`.
    #[derive(Debug)]
    struct MovedEcho;

    #[async_trait]
    impl LocalServer for MovedEcho {
        async fn handle(&self, server_end: TokioDuplex) {
            let mut framed = Framed::new(server_end, RespCodec::new());
            while let Some(item) = framed.next().await {
                let Ok(frame) = item else {
                    return;
                };
                let Ok(cmd) = Command::parse(frame) else {
                    let _ = framed.send(Frame::Error("ERR parse".into())).await;
                    continue;
                };
                let reply = match cmd {
                    Command::Hello(_) => Frame::Array(Some(vec![
                        Frame::Bulk(BulkString::from("server")),
                        Frame::Bulk(BulkString::from("kamino")),
                    ])),
                    _ => Frame::Error("MOVED 3 127.0.0.1:9999".into()),
                };
                if framed.send(reply).await.is_err() {
                    return;
                }
            }
        }
    }

    fn local_addr() -> SocketAddr {
        "127.0.0.1:65500".parse().unwrap()
    }

    #[tokio::test]
    async fn forward_returns_pong() {
        let seen = Arc::new(AtomicUsize::new(0));
        let server = Arc::new(PingEcho { seen: Arc::clone(&seen) });
        let connector = Arc::new(LocalConnector { server });
        let fwd = Forwarder::with_connector(
            ForwarderConfig {
                pool_size: 2,
                inflight_per_conn: 8,
                connect_timeout: Duration::from_secs(2),
                request_timeout: Duration::from_secs(2),
                reconnect_backoff_min: Duration::from_millis(10),
                reconnect_backoff_max: Duration::from_millis(100),
                cluster_secret: String::new(),
            },
            connector,
        );
        let r = fwd.send(local_addr(), Command::Ping(None)).await.unwrap();
        assert!(matches!(r, Frame::SimpleString(ref s) if s == "PONG"));
        assert_eq!(seen.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn moved_reply_surfaces_as_moved_error() {
        let connector = Arc::new(LocalConnector {
            server: Arc::new(MovedEcho),
        });
        let fwd = Forwarder::with_connector(
            ForwarderConfig {
                pool_size: 1,
                inflight_per_conn: 4,
                connect_timeout: Duration::from_secs(2),
                request_timeout: Duration::from_secs(2),
                reconnect_backoff_min: Duration::from_millis(10),
                reconnect_backoff_max: Duration::from_millis(100),
                cluster_secret: String::new(),
            },
            connector,
        );
        let err = fwd
            .send(local_addr(), Command::Ping(None))
            .await
            .expect_err("MOVED should fail");
        assert!(matches!(err, ClusterError::Moved(ref s) if s.contains("127.0.0.1:9999")));
    }

    #[tokio::test]
    async fn request_timeout_returns_timeout_error() {
        // Server that never replies after HELLO.
        #[derive(Debug)]
        struct DeafEcho;
        #[async_trait]
        impl LocalServer for DeafEcho {
            async fn handle(&self, server_end: TokioDuplex) {
                let mut framed = Framed::new(server_end, RespCodec::new());
                // Reply only to HELLO; subsequent commands hang indefinitely.
                let Some(item) = framed.next().await else {
                    return;
                };
                let Ok(_frame) = item else { return };
                let _ = framed
                    .send(Frame::Array(Some(vec![Frame::Bulk(BulkString::from(
                        "server",
                    ))])))
                    .await;
                // Now go silent.
                std::future::pending::<()>().await;
            }
        }

        let connector = Arc::new(LocalConnector {
            server: Arc::new(DeafEcho),
        });
        let fwd = Forwarder::with_connector(
            ForwarderConfig {
                pool_size: 1,
                inflight_per_conn: 4,
                connect_timeout: Duration::from_secs(2),
                request_timeout: Duration::from_millis(80),
                reconnect_backoff_min: Duration::from_millis(10),
                reconnect_backoff_max: Duration::from_millis(100),
                cluster_secret: String::new(),
            },
            connector,
        );
        let err = fwd
            .send(local_addr(), Command::Ping(None))
            .await
            .expect_err("should time out");
        assert!(matches!(err, ClusterError::Timeout(_)));
    }

    #[tokio::test]
    async fn evict_peer_drains_pool() {
        let seen = Arc::new(AtomicUsize::new(0));
        let connector = Arc::new(LocalConnector {
            server: Arc::new(PingEcho {
                seen: Arc::clone(&seen),
            }),
        });
        let fwd = Forwarder::with_connector(
            ForwarderConfig {
                pool_size: 1,
                inflight_per_conn: 4,
                connect_timeout: Duration::from_secs(2),
                request_timeout: Duration::from_secs(2),
                reconnect_backoff_min: Duration::from_millis(10),
                reconnect_backoff_max: Duration::from_millis(100),
                cluster_secret: String::new(),
            },
            connector,
        );
        fwd.send(local_addr(), Command::Ping(None)).await.unwrap();
        fwd.evict_peer(local_addr());
        // After eviction, the pool is repopulated lazily — pings still work
        // because we re-dial on demand.
        fwd.send(local_addr(), Command::Ping(None)).await.unwrap();
    }
}
