//! `RemoteClient` — RESP-speaking implementation of the [`Client`] trait.
//!
//! Architecture:
//!
//! - A single multiplexed TCP connection per [`RemoteClient`].
//! - A background "writer" loop owns the outbound half of the [`Framed`]
//!   stream and pulls (command, response-slot) pairs from an
//!   [`tokio::sync::mpsc`] queue.
//! - A background "reader" loop owns the inbound half and matches each
//!   incoming frame to the head of an in-flight FIFO queue.
//! - Public API methods send a `(Command, oneshot::Sender<Frame>)` into the
//!   writer queue and `.await` the oneshot.
//!
//! Errors:
//! - If the connection dies mid-flight, every queued / in-flight slot is
//!   resolved with [`Error::ServerGone`] (synthesised here in this module
//!   because the canonical `kamino_client::Error` is frozen at Phase 1).
//! - If the server returns a `-ERR ...` frame, it surfaces as
//!   [`Error::Serialization`] or [`Error::InvalidArgument`] depending on
//!   pattern.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use kamino_protocol::{
    BulkString, Command, Frame, HelloArgs, PutCommandOptions, RespCodec, ScanCommandOptions,
};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use crate::cursor::{BufferedCursor, ScanCursor, ScanOptions};
use crate::dmap::DMap;
use crate::error::{Error, Result};
use crate::lock::LockContext;
use crate::stats::{DMapStats, Stats, StatsOptions};
use crate::traits::Client;
use crate::types::{DMapOptions, GetResponse, PutOptions};

/// Channel size for the writer queue. Bounded so a stalled connection doesn't
/// accept unbounded backlog.
const COMMAND_QUEUE_SIZE: usize = 1024;

/// Slot tracked for one in-flight command.
type ResponseSlot = oneshot::Sender<Result<Frame, ConnError>>;

/// Internal connection-level error. Mapped onto `kamino_client::Error` at the
/// public boundary so the remote module can describe transport failures
/// without modifying the frozen `error.rs`.
#[derive(Debug, thiserror::Error)]
enum ConnError {
    #[error("connection closed: {0}")]
    Closed(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl From<ConnError> for Error {
    fn from(e: ConnError) -> Self {
        match e {
            ConnError::Closed(reason) => Self::ServerGone(reason),
            ConnError::Protocol(msg) => Self::Protocol(msg),
            ConnError::Io(err) => Self::ServerGone(err.to_string()),
        }
    }
}

/// Remote RESP client.
pub struct RemoteClient {
    inner: Arc<RemoteInner>,
}

impl std::fmt::Debug for RemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteClient")
            .field("addr", &self.inner.addr)
            .finish_non_exhaustive()
    }
}

struct RemoteInner {
    addr: String,
    cmd_tx: mpsc::Sender<(Command, ResponseSlot)>,
    /// Joined when the client is closed.
    writer_handle: Mutex<Option<JoinHandle<()>>>,
    reader_handle: Mutex<Option<JoinHandle<()>>>,
    /// `Mutex<()>` here is only used to provide `Default`; not held across
    /// await points so `parking_lot` is fine.
    _phantom: std::marker::PhantomData<()>,
}

impl std::fmt::Debug for RemoteInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteInner")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl RemoteClient {
    /// Connect to `addr`, send `HELLO 3` (and optional inline `AUTH`),
    /// return a ready client.
    pub async fn connect(addr: &str, auth: Option<&str>) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::ServerGone(format!("connect {addr}: {e}")))?;
        Self::from_stream(stream, addr.to_string(), auth).await
    }

    /// Connect using a caller-supplied `AsyncRead + AsyncWrite` stream. Used
    /// by tests with `tokio::io::duplex`.
    pub async fn from_stream<S>(stream: S, label: String, auth: Option<&str>) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let codec = RespCodec::new();
        let framed = Framed::new(stream, codec);
        let (cmd_tx, cmd_rx) = mpsc::channel::<(Command, ResponseSlot)>(COMMAND_QUEUE_SIZE);
        let in_flight: Arc<Mutex<VecDeque<ResponseSlot>>> = Arc::new(Mutex::new(VecDeque::new()));

        let (writer, reader) = split_framed(framed, cmd_rx, Arc::clone(&in_flight));
        let writer_handle = tokio::spawn(writer.run());
        let reader_handle = tokio::spawn(reader.run());

        let inner = Arc::new(RemoteInner {
            addr: label,
            cmd_tx,
            writer_handle: Mutex::new(Some(writer_handle)),
            reader_handle: Mutex::new(Some(reader_handle)),
            _phantom: std::marker::PhantomData,
        });
        let client = Self { inner };

        // Send `HELLO 2` with optional inline auth. We stay on RESP2 in
        // Phase 2 because RESP3-only payloads (Map / Push) require both
        // sides to flip their codec mid-stream; that coordination lands
        // with the pub/sub work in Phase 7 (which needs push frames).
        let auth_arg = auth.map(|p| (None::<Bytes>, Bytes::copy_from_slice(p.as_bytes())));
        let hello = HelloArgs {
            protocol_version: Some(2),
            auth: auth_arg,
            client_name: None,
        };
        let frame = client.send(Command::Hello(hello)).await?;
        check_not_error(&frame)?;
        Ok(client)
    }

    /// Send a `PING [msg]` and return `()` on `+PONG` / matching echo.
    pub async fn ping_once(&self, message: Option<&[u8]>) -> Result<()> {
        let frame = self
            .send(Command::Ping(message.map(Bytes::copy_from_slice)))
            .await?;
        check_not_error(&frame)?;
        Ok(())
    }

    async fn send(&self, cmd: Command) -> Result<Frame> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send((cmd, tx))
            .await
            .map_err(|_| Error::ServerGone("writer queue closed".into()))?;
        let frame = rx
            .await
            .map_err(|_| Error::ServerGone("response slot dropped".into()))??;
        Ok(frame)
    }
}

#[async_trait]
impl Client for RemoteClient {
    async fn new_dmap(&self, name: &str, _options: DMapOptions) -> Result<Arc<dyn DMap>> {
        // DMap registration is server-side implicit (every `DM.*` command
        // names the DMap), so this is a pure factory.
        let dmap: Arc<dyn DMap> = Arc::new(RemoteDMap {
            client: Arc::clone(&self.inner),
            name: name.to_string(),
        });
        Ok(dmap)
    }

    async fn stats(&self, _options: StatsOptions) -> Result<Stats> {
        let frame = self.send(Command::Stats).await?;
        check_not_error(&frame)?;
        // `STATS` returns an Array of bulk key/value pairs.
        let Frame::Array(Some(items)) = frame else {
            return Err(Error::Protocol("STATS expected Array(Some(_))".into()));
        };
        let mut map = std::collections::BTreeMap::<String, String>::new();
        let mut it = items.into_iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            if let (Frame::Bulk(BulkString(Some(kb))), Frame::Bulk(BulkString(Some(vb)))) = (k, v) {
                if let (Ok(ks), Ok(vs)) = (std::str::from_utf8(&kb), std::str::from_utf8(&vb)) {
                    map.insert(ks.to_string(), vs.to_string());
                }
            }
        }
        // Phase 2's `STATS` doesn't carry per-DMap data; surface a single
        // synthetic entry so callers get a non-empty `Stats` they can render.
        let mut out = Stats::default();
        if !map.is_empty() {
            let len = map
                .get("connections")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let inuse = map
                .get("commands_total")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            out.dmaps
                .insert("__server__".to_string(), DMapStats { len, inuse });
        }
        Ok(out)
    }

    fn partition_count(&self) -> u32 {
        // Phase 2: the server doesn't expose partition_count over RESP yet.
        // Match the default Kamino config so the surface is stable.
        271
    }

    async fn close(&self) -> Result<()> {
        // Drop the command channel: writer exits, then reader sees EOF.
        let _ = self.send(Command::Quit).await;
        let writer = self.inner.writer_handle.lock().take();
        if let Some(h) = writer {
            h.abort();
        }
        let reader = self.inner.reader_handle.lock().take();
        if let Some(h) = reader {
            h.abort();
        }
        Ok(())
    }

    async fn ping(&self, _addr: &str) -> Result<()> {
        // `addr` is for cluster routing in Phase 4; in Phase 2 the only
        // server is the one we're connected to. Forward as a plain PING.
        self.ping_once(None).await
    }

    async fn refresh_metadata(&self) -> Result<()> {
        // No routing table yet; nothing to refresh.
        Ok(())
    }
}

/// Remote DMap handle. Stateless: every method issues a single round-trip.
pub struct RemoteDMap {
    client: Arc<RemoteInner>,
    name: String,
}

impl std::fmt::Debug for RemoteDMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteDMap")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl RemoteDMap {
    async fn send(&self, cmd: Command) -> Result<Frame> {
        let (tx, rx) = oneshot::channel();
        self.client
            .cmd_tx
            .send((cmd, tx))
            .await
            .map_err(|_| Error::ServerGone("writer queue closed".into()))?;
        let frame = rx
            .await
            .map_err(|_| Error::ServerGone("response slot dropped".into()))??;
        Ok(frame)
    }

    fn dmap_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(self.name.as_bytes())
    }
}

#[async_trait]
impl DMap for RemoteDMap {
    fn name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: &str, value: &[u8], options: PutOptions) -> Result<()> {
        options.validate()?;
        let frame = self
            .send(Command::DmPut {
                dmap: self.dmap_bytes(),
                key: Bytes::copy_from_slice(key.as_bytes()),
                value: Bytes::copy_from_slice(value),
                options: PutCommandOptions {
                    ex: options.ex,
                    px: options.px,
                    exat: options.exat,
                    pxat: options.pxat,
                    nx: options.nx,
                    xx: options.xx,
                    timestamp: options.timestamp,
                },
            })
            .await?;
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Bulk(BulkString(None)) | Frame::Null => {
                // NX/XX rejection — surface the predicate that fired.
                if options.nx {
                    Err(Error::KeyAlreadyExists)
                } else if options.xx {
                    Err(Error::KeyNotExists)
                } else {
                    Err(Error::Protocol("null reply for unconditional PUT".into()))
                }
            }
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.PUT unexpected reply: {other:?}"
            ))),
        }
    }

    async fn get(&self, key: &str) -> Result<GetResponse> {
        let frame = self
            .send(Command::DmGet {
                dmap: self.dmap_bytes(),
                key: Bytes::copy_from_slice(key.as_bytes()),
            })
            .await?;
        match frame {
            Frame::Bulk(BulkString(Some(b))) => Ok(GetResponse {
                value: b.to_vec(),
                timestamp: 0,
                ttl: None,
            }),
            Frame::Bulk(BulkString(None)) | Frame::Null => Err(Error::KeyNotFound),
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.GET unexpected reply: {other:?}"
            ))),
        }
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let frame = self
            .send(Command::DmDel {
                dmap: self.dmap_bytes(),
                keys: vec![Bytes::copy_from_slice(key.as_bytes())],
            })
            .await?;
        match frame {
            Frame::Integer(n) => Ok(n > 0),
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.DEL unexpected reply: {other:?}"
            ))),
        }
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64> {
        let frame = self
            .send(Command::DmIncr {
                dmap: self.dmap_bytes(),
                key: Bytes::copy_from_slice(key.as_bytes()),
                delta,
            })
            .await?;
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.INCR unexpected reply: {other:?}"
            ))),
        }
    }

    async fn decr(&self, key: &str, delta: i64) -> Result<i64> {
        let frame = self
            .send(Command::DmDecr {
                dmap: self.dmap_bytes(),
                key: Bytes::copy_from_slice(key.as_bytes()),
                delta,
            })
            .await?;
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.DECR unexpected reply: {other:?}"
            ))),
        }
    }

    async fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64> {
        let frame = self
            .send(Command::DmIncrByFloat {
                dmap: self.dmap_bytes(),
                key: Bytes::copy_from_slice(key.as_bytes()),
                delta,
            })
            .await?;
        match frame {
            Frame::Bulk(BulkString(Some(b))) => std::str::from_utf8(&b)
                .map_err(|e| Error::Serialization(e.to_string()))?
                .parse::<f64>()
                .map_err(|e| Error::Serialization(e.to_string())),
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.INCRBYFLOAT unexpected reply: {other:?}"
            ))),
        }
    }

    async fn get_put(&self, key: &str, value: &[u8]) -> Result<Option<GetResponse>> {
        let frame = self
            .send(Command::DmGetPut {
                dmap: self.dmap_bytes(),
                key: Bytes::copy_from_slice(key.as_bytes()),
                value: Bytes::copy_from_slice(value),
            })
            .await?;
        match frame {
            Frame::Bulk(BulkString(Some(b))) => Ok(Some(GetResponse {
                value: b.to_vec(),
                timestamp: 0,
                ttl: None,
            })),
            Frame::Bulk(BulkString(None)) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.GETPUT unexpected reply: {other:?}"
            ))),
        }
    }

    async fn expire(&self, key: &str, duration: Duration) -> Result<()> {
        let ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let frame = self
            .send(Command::DmPexpire {
                dmap: self.dmap_bytes(),
                key: Bytes::copy_from_slice(key.as_bytes()),
                milliseconds: ms,
            })
            .await?;
        match frame {
            Frame::Integer(1) => Ok(()),
            Frame::Integer(0) => Err(Error::KeyNotFound),
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.PEXPIRE unexpected reply: {other:?}"
            ))),
        }
    }

    async fn lock(self: Arc<Self>, _key: &str, _deadline: Duration) -> Result<LockContext> {
        Err(Error::Unsupported("DM.LOCK (Phase 7)"))
    }

    async fn lock_with_timeout(
        self: Arc<Self>,
        _key: &str,
        _lease: Duration,
        _deadline: Duration,
    ) -> Result<LockContext> {
        Err(Error::Unsupported("DM.LOCK (Phase 7)"))
    }

    async fn scan(&self, partition_id: u32, options: ScanOptions) -> Result<Box<dyn ScanCursor>> {
        let scan_opts = ScanCommandOptions {
            match_pattern: options.match_pattern.map(|s| Bytes::from(s.into_bytes())),
            count: options.count.and_then(|c| u32::try_from(c).ok()),
        };
        let frame = self
            .send(Command::DmScan {
                partition_id,
                dmap: self.dmap_bytes(),
                cursor: 0,
                options: scan_opts,
            })
            .await?;
        let items = match frame {
            Frame::Array(Some(parts)) if parts.len() == 2 => {
                if let Frame::Array(Some(kv)) = parts.into_iter().nth(1).unwrap() {
                    kv
                } else {
                    return Err(Error::Protocol("DM.SCAN payload not an array".into()));
                }
            }
            Frame::Error(e) => return Err(translate_error(&e)),
            other => {
                return Err(Error::Protocol(format!(
                    "DM.SCAN unexpected reply: {other:?}"
                )));
            }
        };
        let mut collected: Vec<(String, Vec<u8>)> = Vec::with_capacity(items.len() / 2);
        let mut it = items.into_iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            if let (Frame::Bulk(BulkString(Some(kb))), Frame::Bulk(BulkString(Some(vb)))) = (k, v) {
                if let Ok(s) = std::str::from_utf8(&kb) {
                    collected.push((s.to_string(), vb.to_vec()));
                }
            }
        }
        Ok(Box::new(BufferedCursor::new(collected)))
    }

    async fn destroy(&self) -> Result<()> {
        let frame = self
            .send(Command::DmDestroy {
                dmap: self.dmap_bytes(),
            })
            .await?;
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(translate_error(&e)),
            other => Err(Error::Protocol(format!(
                "DM.DESTROY unexpected reply: {other:?}"
            ))),
        }
    }

    async fn unlock_internal(&self, _key: &str, _token: &[u8]) -> Result<()> {
        Err(Error::Unsupported("DM.LOCK (Phase 7)"))
    }

    async fn lease_internal(&self, _key: &str, _token: &[u8], _duration: Duration) -> Result<()> {
        Err(Error::Unsupported("DM.LOCK (Phase 7)"))
    }
}

// ---------------------------------------------------------------------------
// I/O background tasks
// ---------------------------------------------------------------------------

fn split_framed<S>(
    framed: Framed<S, RespCodec>,
    cmd_rx: mpsc::Receiver<(Command, ResponseSlot)>,
    in_flight: Arc<Mutex<VecDeque<ResponseSlot>>>,
) -> (WriterTask<S>, ReaderTask<S>)
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (sink, stream) = framed.split();
    // Reader cancels this once the socket EOFs / errors; the writer's main
    // loop selects on it so new commands get rejected immediately instead of
    // hanging on a response that will never come.
    let cancel = tokio_util::sync::CancellationToken::new();
    (
        WriterTask {
            sink,
            cmd_rx,
            in_flight: Arc::clone(&in_flight),
            cancel: cancel.clone(),
        },
        ReaderTask {
            stream,
            in_flight,
            cancel,
        },
    )
}

struct WriterTask<S> {
    sink: futures::stream::SplitSink<Framed<S, RespCodec>, Frame>,
    cmd_rx: mpsc::Receiver<(Command, ResponseSlot)>,
    in_flight: Arc<Mutex<VecDeque<ResponseSlot>>>,
    cancel: tokio_util::sync::CancellationToken,
}

impl<S> WriterTask<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    async fn run(mut self) {
        loop {
            let next = tokio::select! {
                biased;
                () = self.cancel.cancelled() => None,
                v = self.cmd_rx.recv() => v,
            };
            let Some((cmd, slot)) = next else { break };

            // Reader is gone — fail this command immediately rather than
            // pushing it into a queue that will never be drained.
            if self.cancel.is_cancelled() {
                let _ = slot.send(Err(ConnError::Closed("server closed connection".into())));
                continue;
            }

            let frame = cmd.to_frame();
            self.in_flight.lock().push_back(slot);
            if let Err(e) = self.sink.send(frame).await {
                warn!(?e, "remote-client send error; failing in-flight");
                self.fail_all(&ConnError::Protocol(e.to_string()));
                self.cancel.cancel();
                return;
            }
        }
        // Channel closed by client drop / close(), or reader signalled EOF.
        let reason = if self.cancel.is_cancelled() {
            ConnError::Closed("server closed connection".into())
        } else {
            ConnError::Closed("client dropped".into())
        };
        self.fail_all(&reason);
        debug!("remote-client writer exiting");
    }

    fn fail_all(&self, err: &ConnError) {
        let mut q = self.in_flight.lock();
        while let Some(slot) = q.pop_front() {
            let _ = slot.send(Err(clone_err(err)));
        }
    }
}

struct ReaderTask<S> {
    stream: futures::stream::SplitStream<Framed<S, RespCodec>>,
    in_flight: Arc<Mutex<VecDeque<ResponseSlot>>>,
    cancel: tokio_util::sync::CancellationToken,
}

impl<S> ReaderTask<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    async fn run(mut self) {
        while let Some(frame_result) = self.stream.next().await {
            let frame = match frame_result {
                Ok(f) => f,
                Err(e) => {
                    warn!(?e, "remote-client decode error; failing in-flight");
                    self.fail_all(&ConnError::Protocol(e.to_string()));
                    self.cancel.cancel();
                    return;
                }
            };
            let slot = self.in_flight.lock().pop_front();
            if let Some(slot) = slot {
                let _ = slot.send(Ok(frame));
            } else {
                debug!(?frame, "frame with no matching slot; discarding");
            }
        }
        self.fail_all(&ConnError::Closed("server closed connection".into()));
        self.cancel.cancel();
        debug!("remote-client reader exiting");
    }

    fn fail_all(&self, err: &ConnError) {
        let mut q = self.in_flight.lock();
        while let Some(slot) = q.pop_front() {
            let _ = slot.send(Err(clone_err(err)));
        }
    }
}

fn clone_err(e: &ConnError) -> ConnError {
    match e {
        ConnError::Closed(s) => ConnError::Closed(s.clone()),
        ConnError::Protocol(s) => ConnError::Protocol(s.clone()),
        ConnError::Io(io) => ConnError::Closed(io.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn check_not_error(frame: &Frame) -> Result<()> {
    if let Frame::Error(msg) = frame {
        return Err(translate_error(msg));
    }
    Ok(())
}

/// Map an `-ERR ...` server reply into a `kamino_client::Error`.
fn translate_error(msg: &str) -> Error {
    if let Some(rest) = msg.strip_prefix("MOVED ") {
        let mut parts = rest.splitn(2, ' ');
        let partition = parts.next().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
        let addr = parts.next().unwrap_or("").to_string();
        return Error::Moved { partition, addr };
    }
    if let Some(rest) = msg.strip_prefix("NOAUTH ") {
        return Error::Auth(rest.to_string());
    }
    if let Some(rest) = msg.strip_prefix("WRONGPASS ") {
        return Error::Auth(rest.to_string());
    }
    if let Some(rest) = msg.strip_prefix("CURSOR ") {
        let _ = rest;
        return Error::InvalidCursor;
    }
    if let Some(rest) = msg.strip_prefix("CROSSPARTITION ") {
        return Error::InvalidArgument(format!("CROSSPARTITION {rest}"));
    }
    if let Some(rest) = msg.strip_prefix("ERR ") {
        // Heuristic pattern matching for well-known sub-errors.
        let lower = rest.to_ascii_lowercase();
        if lower.contains("not an integer") || lower.contains("not a number") {
            return Error::NotANumber {
                expected: "integer",
                got: rest.to_string(),
            };
        }
        if lower.contains("operation timed out") {
            return Error::Timeout;
        }
        if lower.contains("no such lock") {
            return Error::NoSuchLock;
        }
        if lower.contains("lock not acquired") {
            return Error::LockNotAcquired;
        }
        return Error::Server(rest.to_string());
    }
    Error::Server(msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error as ClientError;

    #[test]
    fn translates_noauth() {
        let e = translate_error("NOAUTH Authentication required");
        assert!(matches!(e, ClientError::Auth(_)));
    }

    #[test]
    fn translates_wrongpass() {
        let e = translate_error("WRONGPASS invalid creds");
        assert!(matches!(e, ClientError::Auth(_)));
    }

    #[test]
    fn translates_cursor() {
        let e = translate_error("CURSOR partition migrated");
        assert!(matches!(e, ClientError::InvalidCursor));
    }

    #[test]
    fn translates_timeout() {
        let e = translate_error("ERR operation timed out");
        assert!(matches!(e, ClientError::Timeout));
    }

    #[test]
    fn translates_no_such_lock() {
        let e = translate_error("ERR no such lock");
        assert!(matches!(e, ClientError::NoSuchLock));
    }

    #[test]
    fn translates_lock_not_acquired() {
        let e = translate_error("ERR lock not acquired");
        assert!(matches!(e, ClientError::LockNotAcquired));
    }

    #[test]
    fn translates_unknown_err_to_server() {
        let e = translate_error("ERR something else");
        assert!(matches!(e, ClientError::Server(ref s) if s.contains("something")));
    }

    #[test]
    fn translates_bare_message_to_server() {
        let e = translate_error("BADTYPE wrong shape");
        assert!(matches!(e, ClientError::Server(_)));
    }

    #[test]
    fn translates_not_an_integer() {
        let e = translate_error("ERR value is not an integer or out of range");
        assert!(matches!(e, ClientError::NotANumber { .. }));
    }
}
