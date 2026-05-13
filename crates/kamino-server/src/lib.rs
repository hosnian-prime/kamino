//! Kamino server runtime: TCP listener, RESP dispatch, graceful shutdown.
//!
//! Phase 2 scope: single-node, no SWIM, no routing. Wraps an embedded
//! `kamino_client::EmbeddedClient` and dispatches RESP commands against it.

// `pub(crate)` is the right visibility for the cross-module wiring used by
// the connection / dispatch / handlers split. Clippy's `redundant_pub_crate`
// (nursery) flags it because everything is in private modules, but the
// alternative — `pub` — would actually be wider. Suppress crate-wide.
#![allow(clippy::redundant_pub_crate)]

mod connection;
mod dispatch;
mod handlers;
mod metrics;
mod replication;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;

use kamino_client::Client;
use kamino_cluster::{Cluster, MemberProvider, RoutingProvider};
use kamino_core::{Config, Mode};
use rand::RngCore;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tracing::{debug, info, warn};

use crate::connection::ConnSettings;
use crate::dispatch::ServerContext;
use crate::metrics::ServerMetrics;

/// Server-construction or runtime error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServerError {
    /// `Mode` is not `Standalone` — the Phase 2 server only handles that case.
    #[error(
        "kamino-server requires Mode::Standalone, got {0}; use Kamino::embedded \
         for in-process modes"
    )]
    WrongMode(&'static str),

    /// Listening on the configured port failed.
    #[error("failed to bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    /// Configuration validation failed.
    #[error(transparent)]
    Config(#[from] kamino_core::Error),

    /// Generic I/O error during run.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Running Kamino server. Hold this and call [`Server::run`] to drive the
/// accept loop.
pub struct Server {
    listener: TcpListener,
    addr: SocketAddr,
    ctx: Arc<ServerContext>,
    settings: ConnSettings,
    shutdown_tx: broadcast::Sender<()>,
    drain_grace: Duration,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("addr", &self.addr)
            .field("idle_close", &self.settings.idle_close)
            .field("keep_alive_period", &self.settings.keep_alive_period)
            .finish_non_exhaustive()
    }
}

/// Clonable handle for triggering a graceful shutdown.
#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    tx: broadcast::Sender<()>,
}

impl ShutdownHandle {
    /// Signal the server to stop accepting and drain existing connections.
    pub fn trigger(&self) {
        let _ = self.tx.send(());
    }
}

impl Server {
    /// Build and bind the server from a `Config` + an embedded client.
    /// `Mode` must be `Standalone`.
    pub async fn bind(config: &Config, client: Arc<dyn Client>) -> Result<Self, ServerError> {
        if config.mode != Mode::Standalone {
            return Err(ServerError::WrongMode(config.mode.as_str()));
        }
        Self::bind_internal(config, client, None, None).await
    }

    /// Like [`Server::bind`] but registers `cluster` as both the
    /// `MemberProvider` (`CLUSTER.MEMBERS`) and the `RoutingProvider`
    /// (`CLUSTER.ROUTINGTABLE`, `CLUSTER.READY`, `INTERNAL.NODE.UPDATEROUTING`).
    ///
    /// The server takes a reference-counted handle to the running cluster
    /// runtime; lifetime management remains the caller's responsibility
    /// (typically the umbrella `Kamino` builder).
    pub async fn bind_with_cluster(
        config: &Config,
        client: Arc<dyn Client>,
        cluster: Arc<Cluster>,
    ) -> Result<Self, ServerError> {
        if config.mode != Mode::Standalone {
            return Err(ServerError::WrongMode(config.mode.as_str()));
        }
        let member_provider: Arc<dyn MemberProvider> = Arc::clone(&cluster) as _;
        let routing_provider: Arc<dyn RoutingProvider> = Arc::clone(&cluster) as _;
        // Phase 6: hand the cluster a `MigrationSource` so its balancer loop
        // can drive `INTERNAL.NODE.MOVEFRAGMENT` against this server's
        // storage. The cluster owns the JoinHandle and the cancel token.
        let migration_adapter = Arc::new(crate::replication::ClientMigrationSource::new(
            Arc::clone(&client),
        ));
        let source: Arc<dyn kamino_cluster::MigrationSource> =
            Arc::clone(&migration_adapter) as Arc<dyn kamino_cluster::MigrationSource>;
        let cleaner: Arc<dyn kamino_cluster::FragmentCleaner> =
            Arc::clone(&migration_adapter) as Arc<dyn kamino_cluster::FragmentCleaner>;
        cluster.spawn_balancer(source, None);
        cluster.spawn_fragment_cleanup(cleaner);
        Self::bind_internal(
            config,
            client,
            Some(member_provider),
            Some(routing_provider),
        )
        .await
    }

    /// Bind with arbitrary `MemberProvider` / `RoutingProvider` impls. Used
    /// by integration tests that need to inject a stub routing view
    /// (Phase 4 MOVED-retry end-to-end coverage).
    pub async fn bind_with_providers(
        config: &Config,
        client: Arc<dyn Client>,
        member_provider: Option<Arc<dyn MemberProvider>>,
        routing_provider: Option<Arc<dyn RoutingProvider>>,
    ) -> Result<Self, ServerError> {
        if config.mode != Mode::Standalone {
            return Err(ServerError::WrongMode(config.mode.as_str()));
        }
        Self::bind_internal(config, client, member_provider, routing_provider).await
    }

    async fn bind_internal(
        config: &Config,
        client: Arc<dyn Client>,
        member_provider: Option<Arc<dyn MemberProvider>>,
        routing_provider: Option<Arc<dyn RoutingProvider>>,
    ) -> Result<Self, ServerError> {
        let addr = SocketAddr::new(config.network.bind_addr, config.network.bind_port);
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|source| ServerError::Bind { addr, source })?;
        let local = listener.local_addr()?;

        let id = rand::thread_rng().next_u64();
        let ctx = Arc::new(ServerContext {
            client,
            password: config.auth.password.clone(),
            cluster_secret: config.auth.cluster_secret.clone(),
            metrics: Arc::new(ServerMetrics::new()),
            version: env!("CARGO_PKG_VERSION"),
            id,
            member_provider,
            routing_provider,
            ts_source: Arc::new(crate::replication::TimestampSource::new()),
        });
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let settings = ConnSettings {
            idle_close: config.network.idle_close,
            keep_alive_period: config.network.keep_alive_period,
        };
        info!(addr = %local, "kamino-server listening");
        Ok(Self {
            listener,
            addr: local,
            ctx,
            settings,
            shutdown_tx,
            drain_grace: Duration::from_secs(5),
        })
    }

    /// Local address the server is bound to (useful for tests on port 0).
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Handle to trigger graceful shutdown from another task.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            tx: self.shutdown_tx.clone(),
        }
    }

    /// Override the drain grace period applied during shutdown.
    pub const fn set_drain_grace(&mut self, grace: Duration) {
        self.drain_grace = grace;
    }

    /// Run the accept loop until shutdown.
    pub async fn run(self) -> Result<(), ServerError> {
        let Self {
            listener,
            addr,
            ctx,
            settings,
            shutdown_tx,
            drain_grace,
        } = self;
        let mut shutdown_rx = shutdown_tx.subscribe();
        let mut conns: JoinSet<()> = JoinSet::new();

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => {
                    info!(addr = %addr, "shutdown signalled; draining {} connection(s)", conns.len());
                    break;
                }
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, peer)) => {
                            debug!(%peer, "accepted connection");
                            let ctx = Arc::clone(&ctx);
                            let conn_shutdown = shutdown_tx.subscribe();
                            conns.spawn(connection::run_connection(
                                stream,
                                ctx,
                                settings,
                                conn_shutdown,
                            ));
                        }
                        Err(e) => {
                            warn!(?e, "accept error; continuing");
                        }
                    }
                }
            }
        }

        let drain = async { while conns.join_next().await.is_some() {} };
        if tokio::time::timeout(drain_grace, drain).await.is_err() {
            warn!(
                "drain grace expired with {} connection(s) still active; aborting",
                conns.len()
            );
            conns.shutdown().await;
        } else {
            info!("all connections drained cleanly");
        }
        Ok(())
    }
}
