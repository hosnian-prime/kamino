//! Per-connection async loop. Owns the `Framed<TcpStream, RespCodec>` and
//! drives one connection from accept through close.

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use futures::StreamExt;
use kamino_cluster::DeliveredMessage;
use kamino_protocol::{Command, Frame, ProtocolVersion, RespCodec};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use crate::dispatch::{self, DispatchResult, ServerContext};
use crate::handlers::{self, HandlerOutcome};
use crate::state::ConnState;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnSettings {
    pub(crate) idle_close: Duration,
    pub(crate) keep_alive_period: Duration,
}

/// Bounded per-connection pub/sub queue. Once full, `try_send` drops new
/// messages — that's the at-most-once contract per `docs/11-pubsub.md`.
/// Sized for ~64 in-flight messages — large enough to absorb typical
/// bursts, small enough that a misbehaving subscriber doesn't pin
/// arbitrary memory.
const PUBSUB_QUEUE_DEPTH: usize = 64;

fn enable_keep_alive(stream: &TcpStream, period: Duration) {
    if period.is_zero() {
        return;
    }
    let sock_ref = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new().with_time(period);
    if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
        debug!(?e, "failed to set TCP keep-alive (non-fatal)");
    }
}

pub(crate) async fn run_connection(
    stream: TcpStream,
    ctx: Arc<ServerContext>,
    settings: ConnSettings,
    mut shutdown: broadcast::Receiver<()>,
) {
    enable_keep_alive(&stream, settings.keep_alive_period);
    let codec = RespCodec::new();
    let mut framed = Framed::new(stream, codec);
    let auth_required = !ctx.password.is_empty();
    let mut state = ConnState::new(auth_required);

    // Pub/sub queue. Always created so the dispatcher can hand the
    // `Sender` to the registry the first time SUBSCRIBE arrives.
    let (pubsub_tx, mut pubsub_rx) = mpsc::channel::<DeliveredMessage>(PUBSUB_QUEUE_DEPTH);

    ctx.metrics.on_connect();

    let idle = settings.idle_close;
    let use_idle = !idle.is_zero();

    loop {
        let next_frame = framed.next();
        tokio::pin!(next_frame);
        let outcome = if use_idle {
            tokio::select! {
                biased;
                _ = shutdown.recv() => RunStep::Shutdown,
                frame = &mut next_frame => RunStep::Frame(frame),
                Some(msg) = pubsub_rx.recv(), if state.in_pubsub_mode() => RunStep::PubSub(msg),
                () = tokio::time::sleep(idle) => RunStep::IdleTimeout,
            }
        } else {
            tokio::select! {
                biased;
                _ = shutdown.recv() => RunStep::Shutdown,
                frame = &mut next_frame => RunStep::Frame(frame),
                Some(msg) = pubsub_rx.recv(), if state.in_pubsub_mode() => RunStep::PubSub(msg),
            }
        };

        match outcome {
            RunStep::Shutdown => {
                trace!("connection draining: shutdown signalled");
                break;
            }
            RunStep::IdleTimeout => {
                debug!("connection idle-closed after {:?}", idle);
                break;
            }
            RunStep::PubSub(msg) => {
                let frame = handlers::deliver_message_frame(&msg, state.version);
                if let Err(e) = SinkExt::<Frame>::send(&mut framed, frame).await {
                    warn!(?e, "send failed for pub/sub delivery");
                    break;
                }
            }
            RunStep::Frame(None) => {
                trace!("client closed connection");
                break;
            }
            RunStep::Frame(Some(Err(e))) => {
                warn!(?e, "protocol error on frame decode; closing connection");
                let _ =
                    SinkExt::<Frame>::send(&mut framed, Frame::Error(format!("ERR protocol: {e}")))
                        .await;
                break;
            }
            RunStep::Frame(Some(Ok(frame))) => {
                let cmd = match Command::parse(frame) {
                    Ok(c) => c,
                    Err(err) => {
                        let f = dispatch::parse_error_frame(&err);
                        if let Err(e) = SinkExt::<Frame>::send(&mut framed, f).await {
                            warn!(?e, "send failed for parse-error frame");
                            break;
                        }
                        continue;
                    }
                };

                let resp = dispatch::dispatch(&ctx, &mut state, &pubsub_tx, cmd).await;
                let outcome = resp.outcome();
                let send_err = match resp {
                    DispatchResult::Single(r) => {
                        SinkExt::<Frame>::send(&mut framed, r.frame).await.err()
                    }
                    DispatchResult::Multi(m) => {
                        let mut last_err = None;
                        for f in m.frames {
                            if let Err(e) = SinkExt::<Frame>::send(&mut framed, f).await {
                                last_err = Some(e);
                                break;
                            }
                        }
                        last_err
                    }
                };
                if let Some(e) = send_err {
                    warn!(?e, "send failed for response frame");
                    break;
                }
                match outcome {
                    HandlerOutcome::Continue => {}
                    HandlerOutcome::UpgradeToResp3 => {
                        framed.codec_mut().upgrade_to_resp3();
                        state.version = ProtocolVersion::Resp3;
                    }
                    HandlerOutcome::Close => break,
                }
            }
        }
    }

    // Pub/sub cleanup before the codec is dropped: tell the registry to
    // forget this connection's subscriptions and sender so future
    // publishes don't try to deliver into a closed queue.
    if let Some(id) = state.pub_sub_id {
        if let Some(p) = &ctx.pubsub_provider {
            p.cleanup_conn(id);
        }
    }

    let _ = <_ as SinkExt<Frame>>::close(&mut framed).await;
    ctx.metrics.on_disconnect();
}

enum RunStep {
    Frame(Option<Result<Frame, kamino_protocol::ProtocolError>>),
    PubSub(DeliveredMessage),
    IdleTimeout,
    Shutdown,
}
