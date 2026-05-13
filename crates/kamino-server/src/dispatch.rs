//! Command dispatcher.
//!
//! The dispatcher is plumbing: it takes a parsed [`Command`], checks the
//! auth gate, hands the request to the right handler in `crate::handlers`,
//! and returns the [`Response`].

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kamino_client::Client;
use kamino_cluster::{MemberProvider, RoutingProvider};
use kamino_protocol::{Command, Frame};

use crate::handlers::{self, Response};
use crate::metrics::ServerMetrics;
use crate::state::ConnState;

/// Static server-side context shared by all connections.
pub(crate) struct ServerContext {
    pub(crate) client: Arc<dyn Client>,
    pub(crate) password: String,
    /// Inter-node shared secret. Empty ⇒ inter-node auth disabled (the
    /// `INTERNAL.NODE.*` gate then falls back to the local-config check:
    /// `routing_provider.is_some()`).
    pub(crate) cluster_secret: String,
    pub(crate) metrics: Arc<ServerMetrics>,
    pub(crate) version: &'static str,
    pub(crate) id: u64,
    /// Source for `CLUSTER.MEMBERS`. `None` when running standalone-without-
    /// cluster (Phase 2 compatibility); in that case the handler returns an
    /// empty array per `docs/06-network-protocol.md`.
    pub(crate) member_provider: Option<Arc<dyn MemberProvider>>,
    /// Source for `CLUSTER.ROUTINGTABLE`, `CLUSTER.READY`,
    /// `INTERNAL.NODE.UPDATEROUTING`. `None` ⇒ standalone-without-cluster.
    pub(crate) routing_provider: Option<Arc<dyn RoutingProvider>>,
}

impl std::fmt::Debug for ServerContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `client` and `metrics` are `dyn` / `Arc` and have no useful Debug
        // payload at this level; skipping them keeps the formatter cheap and
        // log-safe.
        f.debug_struct("ServerContext")
            .field("password_set", &!self.password.is_empty())
            .field("cluster_secret_set", &!self.cluster_secret.is_empty())
            .field("version", &self.version)
            .field("id", &self.id)
            .field("member_provider", &self.member_provider.is_some())
            .field("routing_provider", &self.routing_provider.is_some())
            .finish_non_exhaustive()
    }
}

const NOAUTH: &str = "NOAUTH Authentication required";
const NOPERM_INTERNAL: &str = "NOPERM INTERNAL.NODE.* requires cluster_secret auth";

pub(crate) async fn dispatch(ctx: &ServerContext, state: &mut ConnState, cmd: Command) -> Response {
    ctx.metrics.on_command();

    if !state.is_authed() && !is_pre_auth_command(&cmd) {
        return Response::ok(Frame::Error(NOAUTH.into()));
    }

    if !is_internal_allowed(ctx, state, &cmd) {
        return Response::ok(Frame::Error(NOPERM_INTERNAL.into()));
    }

    if let Some(moved) = check_routing(ctx, &cmd) {
        return moved;
    }

    match cmd {
        Command::Ping(msg) => handlers::ping(msg.as_ref()),
        Command::Auth { username, password } => handlers::auth(
            state,
            &ctx.password,
            &ctx.cluster_secret,
            username.as_ref(),
            &password,
        ),
        Command::Hello(args) => handlers::hello(
            state,
            &ctx.password,
            &ctx.cluster_secret,
            &args,
            ctx.version,
            ctx.id,
        ),
        Command::Quit => handlers::quit(),
        Command::Stats => handlers::stats(ctx.metrics.snapshot(), ctx.version),

        Command::DmPut {
            dmap,
            key,
            value,
            options,
        } => handlers::dm_put(&ctx.client, &dmap, &key, &value, options).await,
        Command::DmGet { dmap, key } => handlers::dm_get(&ctx.client, &dmap, &key).await,
        Command::DmDel { dmap, keys } => {
            handlers::dm_del(&ctx.client, ctx.routing_provider.as_ref(), &dmap, &keys).await
        }
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
        Command::ClusterRoutingTable => {
            handlers::cluster_routing_table(ctx.routing_provider.as_ref())
        }
        Command::ClusterReady => handlers::cluster_ready(ctx.routing_provider.as_ref()),
        Command::InternalNodeUpdateRouting { table } => {
            handlers::internal_node_update_routing(ctx.routing_provider.as_ref(), &table)
        }
        Command::InternalNodeLengthOfPart { partition_id } => {
            handlers::internal_node_length_of_part(partition_id)
        }
    }
}

/// Commands accepted before the connection has authenticated.
const fn is_pre_auth_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Ping(_) | Command::Auth { .. } | Command::Hello(_) | Command::Quit
    )
}

/// `INTERNAL.NODE.*` commands are restricted to peers that authenticated
/// with `cluster_secret` (per `docs/06-network-protocol.md` "Inter-Node
/// Authentication"). When the deployment configures no `cluster_secret`
/// (empty string), inter-node auth is disabled and any authenticated
/// client passes — that matches the existing single-node trust model.
fn is_internal_allowed(ctx: &ServerContext, state: &ConnState, cmd: &Command) -> bool {
    let is_internal = matches!(
        cmd,
        Command::InternalNodeUpdateRouting { .. } | Command::InternalNodeLengthOfPart { .. }
    );
    if !is_internal {
        return true;
    }
    if ctx.cluster_secret.is_empty() {
        return true;
    }
    state.internode
}

/// Per `docs/02-consistent-hashing.md`, a node that receives a single-key
/// DM.* command for a partition it does not own returns `-MOVED <part>
/// <addr>`. The client then refreshes routing and retries (Phase 4).
///
/// `DM.DEL` is special because it may carry many keys; the multi-key
/// fan-out path lives in `handlers::dm_del` so we let it through here.
/// `DM.SCAN` carries an explicit `partition_id` and is allowed against any
/// node — the scan source treats it as an explicit pin.
fn check_routing(ctx: &ServerContext, cmd: &Command) -> Option<Response> {
    let provider = ctx.routing_provider.as_ref()?;
    let (dmap, key): (&Bytes, &Bytes) = match cmd {
        Command::DmPut { dmap, key, .. }
        | Command::DmGet { dmap, key }
        | Command::DmExpire { dmap, key, .. }
        | Command::DmPexpire { dmap, key, .. }
        | Command::DmIncr { dmap, key, .. }
        | Command::DmDecr { dmap, key, .. }
        | Command::DmGetPut { dmap, key, .. }
        | Command::DmIncrByFloat { dmap, key, .. } => (dmap, key),
        _ => return None,
    };
    let target = provider.route_key(dmap, key)?;
    let part = provider.partition_for_key(dmap, key);
    Some(Response::ok(Frame::Error(format!("MOVED {part} {target}"))))
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
            cluster_secret: String::new(),
            metrics: Arc::new(ServerMetrics::new()),
            version: "0.0.0",
            id: 1,
            member_provider: None,
            routing_provider: None,
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

    #[tokio::test]
    async fn cluster_routing_table_norent_without_provider() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(&ctx, &mut st, Command::ClusterRoutingTable).await;
        assert!(matches!(resp.frame, Frame::SimpleString(ref s) if s == "NORT"));
    }

    #[tokio::test]
    async fn cluster_ready_notready_without_provider() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(&ctx, &mut st, Command::ClusterReady).await;
        assert!(matches!(resp.frame, Frame::Error(ref e) if e.starts_with("NOTREADY")));
    }

    #[tokio::test]
    async fn internal_update_routing_errors_without_provider() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::InternalNodeUpdateRouting {
                table: Bytes::from_static(b"\x80"),
            },
        )
        .await;
        assert!(matches!(resp.frame, Frame::Error(ref e) if e.starts_with("ERR")));
    }

    #[tokio::test]
    async fn internal_length_of_part_returns_zero_for_now() {
        let ctx = ctx_no_auth();
        let mut st = ConnState::new(false);
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::InternalNodeLengthOfPart { partition_id: 0 },
        )
        .await;
        assert!(matches!(resp.frame, Frame::Integer(0)));
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

    // ---- Phase 4 routing-dispatch tests --------------------------------

    use kamino_cluster::{ApplyRoutingOutcome, ClusterError, RoutingProvider};

    /// `RoutingProvider` stub: every single-key DM.* is "owned by" the
    /// `forced` socket addr; `multi_key_strict` is toggleable.
    #[derive(Debug)]
    struct StubRouter {
        forced: Option<std::net::SocketAddr>,
        strict: bool,
    }

    impl RoutingProvider for StubRouter {
        fn routing_table_bytes(&self) -> Option<Vec<u8>> {
            None
        }
        fn apply_routing_update(
            &self,
            _bytes: &[u8],
        ) -> Result<ApplyRoutingOutcome, ClusterError> {
            Ok(ApplyRoutingOutcome::Accepted)
        }
        fn is_ready(&self) -> bool {
            true
        }
        fn routing_signature(&self) -> u64 {
            1
        }
        fn route_key(
            &self,
            _dmap_name: &[u8],
            _key: &[u8],
        ) -> Option<std::net::SocketAddr> {
            self.forced
        }
        fn partition_for_key(&self, _dmap_name: &[u8], _key: &[u8]) -> u32 {
            7
        }
        fn multi_key_strict(&self) -> bool {
            self.strict
        }
        fn forward_dm_del<'a>(
            &'a self,
            _peer: std::net::SocketAddr,
            _dmap: bytes::Bytes,
            keys: Vec<bytes::Bytes>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<i64, ClusterError>>
                    + Send
                    + 'a,
            >,
        > {
            // Stub: pretend every forwarded key was deleted successfully.
            let n = i64::try_from(keys.len()).unwrap_or(0);
            Box::pin(async move { Ok(n) })
        }
    }

    fn ctx_with_router(router: Arc<dyn RoutingProvider>) -> ServerContext {
        ServerContext {
            client: dummy_client(),
            password: String::new(),
            cluster_secret: String::new(),
            metrics: Arc::new(ServerMetrics::new()),
            version: "0.0.0",
            id: 1,
            member_provider: None,
            routing_provider: Some(router),
        }
    }

    #[tokio::test]
    async fn dm_get_to_wrong_owner_emits_moved() {
        let forced = "127.0.0.1:9999".parse().unwrap();
        let router: Arc<dyn RoutingProvider> = Arc::new(StubRouter {
            forced: Some(forced),
            strict: false,
        });
        let ctx = ctx_with_router(router);
        let mut st = ConnState::new(false);
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmGet {
                dmap: Bytes::from_static(b"dm"),
                key: Bytes::from_static(b"k"),
            },
        )
        .await;
        let Frame::Error(msg) = resp.frame else {
            panic!("expected -MOVED error frame");
        };
        assert!(
            msg.starts_with("MOVED 7 127.0.0.1:9999"),
            "got {msg:?}",
        );
    }

    #[tokio::test]
    async fn dm_get_to_local_owner_proceeds() {
        let router: Arc<dyn RoutingProvider> = Arc::new(StubRouter {
            forced: None,
            strict: false,
        });
        let ctx = ctx_with_router(router);
        let mut st = ConnState::new(false);
        // The null client returns Unsupported("new_dmap") — that's fine, we
        // only care that the dispatcher did not short-circuit with MOVED.
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmGet {
                dmap: Bytes::from_static(b"dm"),
                key: Bytes::from_static(b"k"),
            },
        )
        .await;
        match resp.frame {
            Frame::Error(ref e) => {
                assert!(!e.starts_with("MOVED"), "expected non-MOVED, got {e:?}");
            }
            _ => {} // any non-MOVED reply is fine here
        }
    }

    #[tokio::test]
    async fn dm_del_multi_key_strict_rejects_cross_partition() {
        let forced = "127.0.0.1:9999".parse().unwrap();
        let router: Arc<dyn RoutingProvider> = Arc::new(StubRouter {
            forced: Some(forced),
            strict: true,
        });
        let ctx = ctx_with_router(router);
        let mut st = ConnState::new(false);
        // Two keys both route to the remote owner under our stub; with
        // `strict = true` that crosses-partitions (router != local) so
        // the handler must respond with `-CROSSPARTITION`.
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmDel {
                dmap: Bytes::from_static(b"dm"),
                keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            },
        )
        .await;
        let Frame::Error(msg) = resp.frame else {
            panic!("expected -CROSSPARTITION");
        };
        assert!(msg.starts_with("CROSSPARTITION"), "got {msg:?}");
    }

    #[tokio::test]
    async fn internal_node_requires_cluster_secret() {
        // Server has cluster_secret set; a connection authed with the *client*
        // password (not the cluster secret) must NOT pass the INTERNAL gate.
        let mut ctx = ctx_no_auth();
        ctx.password = "clientpass".into();
        ctx.cluster_secret = "topsecret".into();
        ctx.routing_provider = Some(Arc::new(StubRouter {
            forced: None,
            strict: false,
        }));

        let mut st = ConnState::new(true);
        // Authenticate with the client password.
        let auth_resp = dispatch(
            &ctx,
            &mut st,
            Command::Auth {
                username: None,
                password: Bytes::from_static(b"clientpass"),
            },
        )
        .await;
        assert!(matches!(auth_resp.frame, Frame::SimpleString(ref s) if s == "OK"));
        assert!(st.is_authed());
        assert!(!st.internode, "client-password auth must NOT set internode");

        let resp = dispatch(
            &ctx,
            &mut st,
            Command::InternalNodeUpdateRouting {
                table: Bytes::from_static(b"\x80"),
            },
        )
        .await;
        let Frame::Error(msg) = resp.frame else {
            panic!("expected NOPERM");
        };
        assert!(msg.starts_with("NOPERM"), "got {msg:?}");
    }

    #[tokio::test]
    async fn internal_node_passes_with_cluster_secret_auth() {
        let mut ctx = ctx_no_auth();
        ctx.password = "clientpass".into();
        ctx.cluster_secret = "topsecret".into();
        ctx.routing_provider = Some(Arc::new(StubRouter {
            forced: None,
            strict: false,
        }));

        let mut st = ConnState::new(true);
        // Authenticate with the cluster_secret.
        let auth_resp = dispatch(
            &ctx,
            &mut st,
            Command::Auth {
                username: None,
                password: Bytes::from_static(b"topsecret"),
            },
        )
        .await;
        assert!(matches!(auth_resp.frame, Frame::SimpleString(ref s) if s == "OK"));
        assert!(st.internode, "cluster_secret auth must set internode");

        let resp = dispatch(
            &ctx,
            &mut st,
            Command::InternalNodeUpdateRouting {
                table: Bytes::from_static(b"\x80"),
            },
        )
        .await;
        // StubRouter says `Ok(Accepted)` so the handler returns +OK. The
        // key assertion is the *absence* of NOPERM — i.e. the cluster-
        // secret gate let us through.
        match resp.frame {
            Frame::Error(ref m) => assert!(
                !m.starts_with("NOPERM"),
                "expected non-NOPERM, got {m:?}",
            ),
            Frame::SimpleString(_) => {} // accepted
            other => panic!("unexpected reply: {other:?}"),
        }
    }

    #[tokio::test]
    async fn dm_del_lenient_forwards_cross_partition() {
        // StubRouter::forward_dm_del returns the supplied key count, so
        // sending 2 cross-partition keys should yield Integer(2).
        let forced = "127.0.0.1:9999".parse().unwrap();
        let router: Arc<dyn RoutingProvider> = Arc::new(StubRouter {
            forced: Some(forced),
            strict: false,
        });
        let ctx = ctx_with_router(router);
        let mut st = ConnState::new(false);
        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmDel {
                dmap: Bytes::from_static(b"dm"),
                keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            },
        )
        .await;
        // Stub treats forwarded keys as successfully deleted.
        match resp.frame {
            Frame::Integer(n) => assert_eq!(n, 2),
            other => panic!("expected Integer(2), got {other:?}"),
        }
    }
}
