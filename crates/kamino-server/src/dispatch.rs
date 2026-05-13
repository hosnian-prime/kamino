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
use crate::replication::TimestampSource;
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
    /// Node-wide LWW timestamp source. Phase 5 — every write op on the
    /// primary stamps its entry from here before fan-out, so backups
    /// receive a single canonical timestamp.
    pub(crate) ts_source: Arc<TimestampSource>,
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
const QUORUM_NOT_MET: &str = "QUORUM cluster has insufficient members";

pub(crate) async fn dispatch(ctx: &ServerContext, state: &mut ConnState, cmd: Command) -> Response {
    ctx.metrics.on_command();

    if !state.is_authed() && !is_pre_auth_command(&cmd) {
        return Response::ok(Frame::Error(NOAUTH.into()));
    }

    if !is_internal_allowed(ctx, state, &cmd) {
        return Response::ok(Frame::Error(NOPERM_INTERNAL.into()));
    }

    // Phase 5: `member_count_quorum` is enforced before every DMap op.
    // Internode forwards bypass the gate because the primary already
    // checked it; refusing here would break replication during a quorum
    // dip that the primary already decided to absorb.
    if !state.internode && is_dmap_op(&cmd) && !ctx_member_quorum_ok(ctx) {
        return Response::ok(Frame::Error(QUORUM_NOT_MET.into()));
    }

    // Internode replication arrivals never re-route — the primary already
    // routed them to us, and re-checking would emit a spurious MOVED back
    // at the primary.
    if !state.internode {
        if let Some(moved) = check_routing(ctx, &cmd) {
            return moved;
        }
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
        } => {
            handlers::dm_put(
                &ctx.client,
                ctx.routing_provider.as_ref(),
                ctx.ts_source.as_ref(),
                state.internode,
                &dmap,
                &key,
                &value,
                options,
            )
            .await
        }
        Command::DmGet { dmap, key } => {
            handlers::dm_get(
                &ctx.client,
                ctx.routing_provider.as_ref(),
                ctx.ts_source.as_ref(),
                state.internode,
                &dmap,
                &key,
            )
            .await
        }
        Command::DmDel { dmap, keys } => {
            handlers::dm_del(
                &ctx.client,
                ctx.routing_provider.as_ref(),
                state.internode,
                &dmap,
                &keys,
            )
            .await
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
        Command::InternalNodeGetWithTs { dmap, key } => {
            handlers::internal_node_get_with_ts(&ctx.client, &dmap, &key).await
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

/// True for any DM.* mutating or read op — used to gate Phase 5
/// `member_count_quorum` enforcement.
const fn is_dmap_op(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::DmPut { .. }
            | Command::DmGet { .. }
            | Command::DmDel { .. }
            | Command::DmExpire { .. }
            | Command::DmPexpire { .. }
            | Command::DmIncr { .. }
            | Command::DmDecr { .. }
            | Command::DmGetPut { .. }
            | Command::DmIncrByFloat { .. }
            | Command::DmDestroy { .. }
            | Command::DmScan { .. }
    )
}

fn ctx_member_quorum_ok(ctx: &ServerContext) -> bool {
    ctx.routing_provider
        .as_ref()
        .is_none_or(|p| p.member_quorum_satisfied())
}

/// `INTERNAL.NODE.*` commands are restricted to peers that authenticated
/// with `cluster_secret` (per `docs/06-network-protocol.md` "Inter-Node
/// Authentication"). When the deployment configures no `cluster_secret`
/// (empty string), inter-node auth is disabled and any authenticated
/// client passes — that matches the existing single-node trust model.
fn is_internal_allowed(ctx: &ServerContext, state: &ConnState, cmd: &Command) -> bool {
    let is_internal = matches!(
        cmd,
        Command::InternalNodeUpdateRouting { .. }
            | Command::InternalNodeLengthOfPart { .. }
            | Command::InternalNodeGetWithTs { .. }
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
            ts_source: Arc::new(TimestampSource::new()),
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
        fn apply_routing_update(&self, _bytes: &[u8]) -> Result<ApplyRoutingOutcome, ClusterError> {
            Ok(ApplyRoutingOutcome::Accepted)
        }
        fn is_ready(&self) -> bool {
            true
        }
        fn routing_signature(&self) -> u64 {
            1
        }
        fn route_key(&self, _dmap_name: &[u8], _key: &[u8]) -> Option<std::net::SocketAddr> {
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
            Box<dyn std::future::Future<Output = Result<i64, ClusterError>> + Send + 'a>,
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
            ts_source: Arc::new(TimestampSource::new()),
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
        assert!(msg.starts_with("MOVED 7 127.0.0.1:9999"), "got {msg:?}",);
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
            Frame::Error(ref m) => {
                assert!(!m.starts_with("NOPERM"), "expected non-NOPERM, got {m:?}");
            }
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

    // ---- Phase 5 replication-dispatch tests ----------------------------

    use kamino_cluster::ReplicationSettings;
    use kamino_core::ReplicationMode;
    use std::sync::Mutex;

    /// Stub router with full Phase 5 surface — backups list, replication
    /// settings, member quorum, and a captured forward-command log.
    #[derive(Debug)]
    struct ReplStubRouter {
        /// Returned by `backup_addrs_for_key`.
        backups: Vec<std::net::SocketAddr>,
        /// Returned by `member_quorum_satisfied`.
        quorum_ok: bool,
        /// Returned by `replication_settings`.
        settings: ReplicationSettings,
        /// Captures every forwarded command for assertions.
        forwarded: Mutex<Vec<(std::net::SocketAddr, Command)>>,
        /// Reply each forwarded command returns. `None` means simulate a
        /// transport failure.
        forward_reply: Option<Frame>,
    }

    impl ReplStubRouter {
        fn new(backups: Vec<std::net::SocketAddr>, settings: ReplicationSettings) -> Self {
            Self {
                backups,
                quorum_ok: true,
                settings,
                forwarded: Mutex::new(Vec::new()),
                forward_reply: Some(Frame::ok()),
            }
        }
    }

    impl RoutingProvider for ReplStubRouter {
        fn routing_table_bytes(&self) -> Option<Vec<u8>> {
            None
        }
        fn apply_routing_update(&self, _bytes: &[u8]) -> Result<ApplyRoutingOutcome, ClusterError> {
            Ok(ApplyRoutingOutcome::Accepted)
        }
        fn is_ready(&self) -> bool {
            true
        }
        fn routing_signature(&self) -> u64 {
            1
        }
        fn route_key(&self, _dmap_name: &[u8], _key: &[u8]) -> Option<std::net::SocketAddr> {
            None // local primary for every key
        }
        fn partition_for_key(&self, _dmap_name: &[u8], _key: &[u8]) -> u32 {
            7
        }
        fn multi_key_strict(&self) -> bool {
            false
        }
        fn backup_addrs_for_key(
            &self,
            _dmap_name: &[u8],
            _key: &[u8],
        ) -> Vec<std::net::SocketAddr> {
            self.backups.clone()
        }
        fn member_quorum_satisfied(&self) -> bool {
            self.quorum_ok
        }
        fn replication_settings(&self) -> ReplicationSettings {
            self.settings
        }
        fn forward_command<'a>(
            &'a self,
            peer: std::net::SocketAddr,
            cmd: Command,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Frame, ClusterError>> + Send + 'a>,
        > {
            self.forwarded.lock().unwrap().push((peer, cmd));
            let reply = self.forward_reply.clone();
            Box::pin(async move {
                reply.map_or_else(
                    || {
                        Err(ClusterError::ServerGone(
                            "stub forward configured to fail".into(),
                        ))
                    },
                    Ok,
                )
            })
        }
        fn forward_dm_del<'a>(
            &'a self,
            _peer: std::net::SocketAddr,
            _dmap: bytes::Bytes,
            keys: Vec<bytes::Bytes>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<i64, ClusterError>> + Send + 'a>,
        > {
            let n = i64::try_from(keys.len()).unwrap_or(0);
            Box::pin(async move { Ok(n) })
        }
    }

    fn embedded_ctx_with_router(router: Arc<ReplStubRouter>) -> ServerContext {
        use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
        use kamino_core::{Clock, Hasher, SystemClock, XxHasher};
        use kamino_storage::{Locker, RamBlock, StorageEngine};

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
        let factory: EngineFactory =
            Arc::new(|| -> Box<dyn StorageEngine> { Box::new(RamBlock::new(4096, 0.4)) });
        let deps = EmbeddedDeps {
            clock,
            hasher,
            locker: Locker::new(),
            engine_factory: factory,
            partition_count: 1,
        };
        let embedded: Arc<dyn Client> = EmbeddedClient::new(deps);
        ServerContext {
            client: embedded,
            password: String::new(),
            cluster_secret: String::new(),
            metrics: Arc::new(ServerMetrics::new()),
            version: "0.0.0",
            id: 1,
            member_provider: None,
            routing_provider: Some(router as Arc<dyn RoutingProvider>),
            ts_source: Arc::new(TimestampSource::new()),
        }
    }

    const fn settings_with(replica_count: u32, write_quorum: u32) -> ReplicationSettings {
        ReplicationSettings {
            replica_count,
            write_quorum,
            read_quorum: 1,
            read_repair: false,
            mode: ReplicationMode::Sync,
        }
    }

    #[tokio::test]
    async fn dm_put_fans_out_to_backups_with_assigned_timestamp() {
        let backup: std::net::SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let router = Arc::new(ReplStubRouter::new(vec![backup], settings_with(2, 2)));
        let ctx = embedded_ctx_with_router(Arc::clone(&router));
        let mut st = ConnState::new(false);

        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmPut {
                dmap: Bytes::from_static(b"d"),
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                options: kamino_protocol::PutCommandOptions::default(),
            },
        )
        .await;
        assert!(matches!(resp.frame, Frame::SimpleString(ref s) if s == "OK"));

        let log = router.forwarded.lock().unwrap();
        assert_eq!(log.len(), 1, "primary must fan out to one backup");
        match &log[0] {
            (
                peer,
                Command::DmPut {
                    options, dmap, key, ..
                },
            ) => {
                assert_eq!(*peer, backup);
                assert_eq!(dmap.as_ref(), b"d");
                assert_eq!(key.as_ref(), b"k");
                assert!(
                    options.timestamp.is_some(),
                    "primary must stamp replicated writes",
                );
            }
            other => panic!("expected DM.PUT fan-out, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dm_put_returns_quorum_when_backup_acks_missing() {
        let backup: std::net::SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let router = Arc::new(ReplStubRouter {
            backups: vec![backup],
            forward_reply: None, // simulate transport failure
            ..ReplStubRouter::new(vec![backup], settings_with(2, 2))
        });
        let ctx = embedded_ctx_with_router(Arc::clone(&router));
        let mut st = ConnState::new(false);

        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmPut {
                dmap: Bytes::from_static(b"d"),
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                options: kamino_protocol::PutCommandOptions::default(),
            },
        )
        .await;
        let Frame::Error(msg) = resp.frame else {
            panic!("expected -QUORUM error frame");
        };
        assert!(msg.starts_with("QUORUM"), "got {msg:?}");
    }

    #[tokio::test]
    async fn dm_put_skips_fanout_in_async_mode_after_local_commit() {
        // Async mode: primary returns OK without waiting for backups. The
        // tokio task fires-and-forgets; assertions focus on the immediate
        // reply, not the eventual log state.
        let backup: std::net::SocketAddr = "127.0.0.1:9002".parse().unwrap();
        let mut settings = settings_with(2, 2);
        settings.mode = ReplicationMode::Async;
        let router = Arc::new(ReplStubRouter::new(vec![backup], settings));
        let ctx = embedded_ctx_with_router(Arc::clone(&router));
        let mut st = ConnState::new(false);

        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmPut {
                dmap: Bytes::from_static(b"d"),
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                options: kamino_protocol::PutCommandOptions::default(),
            },
        )
        .await;
        assert!(
            matches!(resp.frame, Frame::SimpleString(ref s) if s == "OK"),
            "async mode must return OK before fan-out completes",
        );
    }

    #[tokio::test]
    async fn dm_put_replica_arrival_takes_lww_path_and_skips_fanout() {
        // A peer-authenticated connection delivering a DM.PUT (state.internode
        // = true) must apply LWW merge and NOT fan back out.
        let backup: std::net::SocketAddr = "127.0.0.1:9003".parse().unwrap();
        let router = Arc::new(ReplStubRouter::new(vec![backup], settings_with(2, 2)));
        let ctx = embedded_ctx_with_router(Arc::clone(&router));
        let mut st = ConnState::new(false);
        st.internode = true;

        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmPut {
                dmap: Bytes::from_static(b"d"),
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                options: kamino_protocol::PutCommandOptions {
                    timestamp: Some(123_456),
                    ..Default::default()
                },
            },
        )
        .await;
        assert!(matches!(resp.frame, Frame::SimpleString(ref s) if s == "OK"));
        assert!(
            router.forwarded.lock().unwrap().is_empty(),
            "backup must not re-fan-out replicated writes",
        );
    }

    #[tokio::test]
    async fn dm_put_quorum_unmet_rejects() {
        // member_count_quorum gate: when the cluster has too few live
        // members, every DM.* op short-circuits with -QUORUM.
        let mut router_inner = ReplStubRouter::new(vec![], settings_with(1, 1));
        router_inner.quorum_ok = false;
        let router = Arc::new(router_inner);
        let ctx = embedded_ctx_with_router(Arc::clone(&router));
        let mut st = ConnState::new(false);

        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmPut {
                dmap: Bytes::from_static(b"d"),
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                options: kamino_protocol::PutCommandOptions::default(),
            },
        )
        .await;
        let Frame::Error(msg) = resp.frame else {
            panic!("expected -QUORUM");
        };
        assert!(msg.starts_with("QUORUM"), "got {msg:?}");
    }

    #[tokio::test]
    async fn dm_put_replica_arrival_bypasses_quorum_gate() {
        // The primary already decided to accept the write; the backup must
        // mirror that decision even if its own live-member view is below
        // member_count_quorum (transient SWIM dip).
        let mut router_inner = ReplStubRouter::new(vec![], settings_with(2, 2));
        router_inner.quorum_ok = false;
        let router = Arc::new(router_inner);
        let ctx = embedded_ctx_with_router(Arc::clone(&router));
        let mut st = ConnState::new(false);
        st.internode = true;

        let resp = dispatch(
            &ctx,
            &mut st,
            Command::DmPut {
                dmap: Bytes::from_static(b"d"),
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                options: kamino_protocol::PutCommandOptions {
                    timestamp: Some(42),
                    ..Default::default()
                },
            },
        )
        .await;
        assert!(
            matches!(resp.frame, Frame::SimpleString(ref s) if s == "OK"),
            "internode write must bypass quorum gate, got {:?}",
            resp.frame,
        );
    }
}
