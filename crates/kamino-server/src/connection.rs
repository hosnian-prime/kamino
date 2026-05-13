//! Per-connection async loop. Owns the `Framed<TcpStream, RespCodec>` and
//! drives one connection from accept through close.

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use futures::StreamExt;
use kamino_protocol::{Command, Frame, ProtocolVersion, RespCodec};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use crate::dispatch::{self, ServerContext};
use crate::handlers::HandlerOutcome;
use crate::state::ConnState;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnSettings {
    pub(crate) idle_close: Duration,
    pub(crate) keep_alive_period: Duration,
}

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
                () = tokio::time::sleep(idle) => RunStep::IdleTimeout,
            }
        } else {
            tokio::select! {
                biased;
                _ = shutdown.recv() => RunStep::Shutdown,
                frame = &mut next_frame => RunStep::Frame(frame),
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

                let resp = dispatch::dispatch(&ctx, &mut state, cmd).await;
                let outcome = resp.outcome;
                if let Err(e) = SinkExt::<Frame>::send(&mut framed, resp.frame).await {
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

    let _ = <_ as SinkExt<Frame>>::close(&mut framed).await;
    ctx.metrics.on_disconnect();
}

enum RunStep {
    Frame(Option<Result<Frame, kamino_protocol::ProtocolError>>),
    IdleTimeout,
    Shutdown,
}
