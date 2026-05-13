//! Command dispatcher.
//!
//! The dispatcher is plumbing: it takes a parsed [`Command`], checks the
//! auth gate, hands the request to the right handler in `crate::handlers`,
//! and returns the [`Response`].

use std::sync::Arc;
use std::time::Duration;

use kamino_client::Client;
use kamino_cluster::MemberProvider;
use kamino_protocol::{Command, Frame};

use crate::handlers::{self, Response};
use crate::metrics::ServerMetrics;
use crate::state::ConnState;

/// Static server-side context shared by all connections.
pub(crate) struct ServerContext {
    pub(crate) client: Arc<dyn Client>,
    pub(crate) password: String,
    pub(crate) metrics: Arc<ServerMetrics>,
    pub(crate) version: &'static str,
    pub(crate) id: u64,
    /// Source for `CLUSTER.MEMBERS`. `None` when running standalone-without-
    /// cluster (Phase 2 compatibility); in that case the handler returns an
    /// empty array per `docs/06-network-protocol.md`.
    pub(crate) member_provider: Option<Arc<dyn MemberProvider>>,
}

impl std::fmt::Debug for ServerContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `client` and `metrics` are `dyn` / `Arc` and have no useful Debug
        // payload at this level; skipping them keeps the formatter cheap and
        // log-safe.
        f.debug_struct("ServerContext")
            .field("password_set", &!self.password.is_empty())
            .field("version", &self.version)
            .field("id", &self.id)
            .field("member_provider", &self.member_provider.is_some())
            .finish_non_exhaustive()
    }
}

const NOAUTH: &str = "NOAUTH Authentication required";

pub(crate) async fn dispatch(ctx: &ServerContext, state: &mut ConnState, cmd: Command) -> Response {
    ctx.metrics.on_command();

    if !state.is_authed() && !is_pre_auth_command(&cmd) {
        return Response::ok(Frame::Error(NOAUTH.into()));
    }

    match cmd {
        Command::Ping(msg) => handlers::ping(msg.as_ref()),
        Command::Auth { username, password } => {
            handlers::auth(state, &ctx.password, username.as_ref(), &password)
        }
        Command::Hello(args) => handlers::hello(state, &ctx.password, &args, ctx.version, ctx.id),
        Command::Quit => handlers::quit(),
        Command::Stats => handlers::stats(ctx.metrics.snapshot(), ctx.version),

        Command::DmPut {
            dmap,
            key,
            value,
            options,
        } => handlers::dm_put(&ctx.client, &dmap, &key, &value, options).await,
        Command::DmGet { dmap, key } => handlers::dm_get(&ctx.client, &dmap, &key).await,
        Command::DmDel { dmap, keys } => handlers::dm_del(&ctx.client, &dmap, &keys).await,
        Command::DmExpire { dmap, key, seconds } => {
            handlers::dm_expire(&ctx.client, &dmap, &key, Duration::from_secs(seconds)).await
        }
        Command::DmPexpire {
            dmap,
            key,
            milliseconds,
        } => {
            handlers::dm_expire(
                &ctx.client,
                &dmap,
                &key,
                Duration::from_millis(milliseconds),
            )
            .await
        }
        Command::DmIncr { dmap, key, delta } => {
            handlers::dm_incr(&ctx.client, &dmap, &key, delta).await
        }
        Command::DmDecr { dmap, key, delta } => {
            handlers::dm_decr(&ctx.client, &dmap, &key, delta).await
        }
        Command::DmGetPut { dmap, key, value } => {
            handlers::dm_get_put(&ctx.client, &dmap, &key, &value).await
        }
        Command::DmIncrByFloat { dmap, key, delta } => {
            handlers::dm_incr_by_float(&ctx.client, &dmap, &key, delta).await
        }
        Command::DmDestroy { dmap } => handlers::dm_destroy(&ctx.client, &dmap).await,
        Command::DmScan {
            partition_id,
            dmap,
            cursor,
            options,
        } => handlers::dm_scan(&ctx.client, &dmap, partition_id, cursor, options).await,

        Command::ClusterMembers => handlers::cluster_members(ctx.member_provider.as_ref()),
    }
}

/// Commands accepted before the connection has authenticated.
const fn is_pre_auth_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Ping(_) | Command::Auth { .. } | Command::Hello(_) | Command::Quit
    )
}

/// Translate a parser error into an `-ERR ...` frame.
pub(crate) fn parse_error_frame(err: &kamino_protocol::CommandError) -> Frame {
    Frame::Error(format!("ERR {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kamino_protocol::{BulkString, HelloArgs};

    fn ctx_no_auth() -> ServerContext {
        ServerContext {
            client: dummy_client(),
            password: String::new(),
            metrics: Arc::new(ServerMetrics::new()),
            version: "0.0.0",
            id: 1,
            member_provider: None,
        }
    }

    fn dummy_client() -> Arc<dyn Client> {
        use async_trait::async_trait;
        use kamino_client::{DMap, DMapOptions, Stats, StatsOptions};

        #[derive(Debug)]
        struct NullClient;
        #[async_trait]
        impl Client for NullClient {
            async fn new_dmap(
                &self,
                _name: &str,
                _options: DMapOptions,
            ) -> kamino_client::Result<Arc<dyn DMap>> {
                Err(kamino_client::Error::Unsupported("new_dmap"))
            }
            async fn stats(&self, _options: StatsOptions) -> kamino_client::Result<Stats> {
                Ok(Stats::default())
            }
            fn partition_count(&self) -> u32 {
                1
            }
            async fn close(&self) -> kamino_client::Result<()> {
                Ok(())
            }
        }
        Arc::new(NullClient)
    }

    #[tokio::test]
    async fn ping_returns_pong() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(&ctx, &mut st, Command::Ping(None)).await;
        assert!(matches!(resp.frame, Frame::SimpleString(ref s) if s == "PONG"));
    }

    #[tokio::test]
    async fn ping_with_message_echoes() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let msg = Bytes::from_static(b"hi");
        let resp = dispatch(&ctx, &mut st, Command::Ping(Some(msg))).await;
        if let Frame::Bulk(BulkString(Some(b))) = resp.frame {
            assert_eq!(&b[..], b"hi");
        } else {
            panic!("expected bulk");
        }
    }

    #[tokio::test]
    async fn pre_auth_gate_blocks_dm() {
        let ctx = ServerContext {
            password: "p".into(),
            ..ctx_no_auth()
        };
        let mut st = ConnState::new(true);
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmGet {
                dmap: Bytes::from_static(b"d"),
                key: Bytes::from_static(b"k"),
            },
        )
        .await;
        assert!(matches!(resp.frame, Frame::Error(ref e) if e.starts_with("NOAUTH")));
    }

    #[tokio::test]
    async fn auth_with_password_authenticates() {
        let ctx = ServerContext {
            password: "p".into(),
            ..ctx_no_auth()
        };
        let mut st = ConnState::new(true);
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::Auth {
                username: None,
                password: Bytes::from_static(b"p"),
            },
        )
        .await;
        assert!(matches!(resp.frame, Frame::SimpleString(ref s) if s == "OK"));
        assert!(st.is_authed());
    }

    #[tokio::test]
    async fn auth_without_password_set_rejects() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::Auth {
                username: Some(Bytes::from_static(b"default")),
                password: Bytes::from_static(b"x"),
            },
        )
        .await;
        assert!(matches!(resp.frame, Frame::Error(ref e) if e.contains("no password is set")));
    }

    #[tokio::test]
    async fn hello_returns_map_in_resp3() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(&ctx, &mut st, Command::Hello(HelloArgs::versioned(3))).await;
        assert!(matches!(resp.frame, Frame::Map(_)));
        assert_eq!(
            resp.outcome,
            crate::handlers::HandlerOutcome::UpgradeToResp3
        );
    }

    #[tokio::test]
    async fn hello_returns_array_in_resp2() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(&ctx, &mut st, Command::Hello(HelloArgs::versioned(2))).await;
        assert!(matches!(resp.frame, Frame::Array(Some(_))));
        assert_eq!(resp.outcome, crate::handlers::HandlerOutcome::Continue);
    }

    #[tokio::test]
    async fn quit_signals_close() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(&ctx, &mut st, Command::Quit).await;
        assert_eq!(resp.outcome, crate::handlers::HandlerOutcome::Close);
    }

    #[tokio::test]
    async fn stats_is_array() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(&ctx, &mut st, Command::Stats).await;
        assert!(matches!(resp.frame, Frame::Array(Some(_))));
    }

    #[tokio::test]
    async fn cluster_members_empty_without_provider() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(&ctx, &mut st, Command::ClusterMembers).await;
        let Frame::Array(Some(items)) = resp.frame else {
            panic!("expected array");
        };
        assert!(items.is_empty(), "no provider → empty array");
    }

    #[test]
    fn pre_auth_classification() {
        assert!(is_pre_auth_command(&Command::Quit));
        assert!(is_pre_auth_command(&Command::Ping(None)));
        assert!(is_pre_auth_command(&Command::Hello(HelloArgs::default())));
        assert!(is_pre_auth_command(&Command::Auth {
            username: None,
            password: Bytes::new(),
        }));
        assert!(!is_pre_auth_command(&Command::Stats));
    }

    #[tokio::test]
    async fn auth_unauth_path_still_allows_ping() {
        let ctx = ServerContext {
            password: "p".into(),
            ..ctx_no_auth()
        };
        let mut st = ConnState::new(true);
        let resp = dispatch(&ctx, &mut st, Command::Ping(None)).await;
        assert!(matches!(resp.frame, Frame::SimpleString(_)));
    }

    #[test]
    fn parse_error_translates() {
        let f = parse_error_frame(&kamino_protocol::CommandError::NotAnArray);
        assert!(matches!(f, Frame::Error(ref e) if e.starts_with("ERR")));
    }
}
