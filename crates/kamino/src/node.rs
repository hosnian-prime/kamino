//! `Kamino` builder.
//!
//! Wires up an [`EmbeddedClient`], a [`Locker`], and the eviction workers
//! from a [`Config`]. In standalone mode it additionally owns the RESP
//! listener exposed via [`kamino_server::Server`].

use std::sync::Arc;
use std::time::Duration;

use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_client::{Client, DMapOptions};
use kamino_core::{Clock, Config, Hasher, Mode, SystemClock, XxHasher};
use kamino_server::{Server, ServerError, ShutdownHandle};
use kamino_storage::{IdleSweeper, Locker, RamBlock, StorageEngine, TtlSweeper};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Errors raised by [`Kamino::embedded`] or [`Kamino::serve`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KaminoError {
    /// Builder called with a config in the wrong mode.
    #[error(
        "Kamino::embedded requires Mode::EmbeddedSolo / serve requires Mode::Standalone, \
         got {0}"
    )]
    WrongMode(&'static str),

    /// Configuration validation failed.
    #[error(transparent)]
    Config(#[from] kamino_core::Error),

    /// `Kamino::serve` failed to bind / drive the server.
    #[error(transparent)]
    Server(#[from] ServerError),
}

/// Running Kamino node. Hold this to keep the eviction workers alive; drop
/// or call [`Kamino::shutdown`] to tear them down.
///
/// When built via [`Kamino::serve`], the handle additionally owns the RESP
/// listener — call [`Kamino::run`] to drive the accept loop and
/// [`Kamino::shutdown_handle`] to get a clonable cancellation handle.
pub struct Kamino {
    client: Arc<EmbeddedClient>,
    cancel: CancellationToken,
    workers: Vec<JoinHandle<()>>,
    /// `Some` only when produced by [`Kamino::serve`].
    server: Option<Server>,
}

impl std::fmt::Debug for Kamino {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Kamino")
            .field("client", &self.client)
            .field("workers", &self.workers.len())
            .finish_non_exhaustive()
    }
}

impl Kamino {
    /// Build an embedded solo Kamino node using the default `RamBlock` engine.
    pub async fn embedded(config: Config) -> Result<Self, KaminoError> {
        let engine_factory = ramblock_factory(&config);
        Self::embedded_with_engine(config, engine_factory).await
    }

    /// Build an embedded solo Kamino node using a caller-supplied engine
    /// factory. Useful for tests that want to inject a mock engine and for
    /// pre-Phase-1-A bring-up while `RamBlock` is still being filled in.
    ///
    /// Marked `async` to match the future shape (Phase 4 will need to await
    /// SWIM bring-up here for `embedded_clustered`).
    #[allow(clippy::unused_async)]
    pub async fn embedded_with_engine(
        config: Config,
        engine_factory: EngineFactory,
    ) -> Result<Self, KaminoError> {
        config.validate()?;
        if config.mode != Mode::EmbeddedSolo {
            return Err(KaminoError::WrongMode(config.mode.as_str()));
        }

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
        let locker = Locker::new();

        let deps = EmbeddedDeps {
            clock: Arc::clone(&clock),
            hasher,
            locker,
            engine_factory,
            partition_count: config.core.partition_count,
        };
        let client = EmbeddedClient::new(deps);

        // Pre-create the DMaps declared in config so they have eviction workers
        // attached immediately.
        for (name, dmap_cfg) in &config.dmaps {
            client.get_or_create(name, DMapOptions::from_config(dmap_cfg));
        }

        let cancel = CancellationToken::new();
        let workers = spawn_workers(&client, &cancel, &clock, &config);

        Ok(Self {
            client,
            cancel,
            workers,
            server: None,
        })
    }

    /// Build a standalone Kamino node: an [`EmbeddedClient`] wrapped behind
    /// a RESP TCP listener. `config.mode` must be `Mode::Standalone`. Returns
    /// a handle ready to be driven via [`Kamino::run`].
    pub async fn serve(config: Config) -> Result<Self, KaminoError> {
        let engine_factory = ramblock_factory(&config);
        Self::serve_with_engine(config, engine_factory).await
    }

    /// Same as [`Kamino::serve`] but with a caller-supplied engine factory.
    pub async fn serve_with_engine(
        config: Config,
        engine_factory: EngineFactory,
    ) -> Result<Self, KaminoError> {
        config.validate()?;
        if config.mode != Mode::Standalone {
            return Err(KaminoError::WrongMode(config.mode.as_str()));
        }

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
        let locker = Locker::new();

        let deps = EmbeddedDeps {
            clock: Arc::clone(&clock),
            hasher,
            locker,
            engine_factory,
            partition_count: config.core.partition_count,
        };
        let client = EmbeddedClient::new(deps);
        for (name, dmap_cfg) in &config.dmaps {
            client.get_or_create(name, DMapOptions::from_config(dmap_cfg));
        }
        let cancel = CancellationToken::new();
        let workers = spawn_workers(&client, &cancel, &clock, &config);

        let erased: Arc<dyn Client> = Arc::clone(&client) as Arc<dyn Client>;
        let server = Server::bind(&config, erased).await?;
        Ok(Self {
            client,
            cancel,
            workers,
            server: Some(server),
        })
    }

    /// Drive the standalone listener until shutdown. Tears down the eviction
    /// workers on the way out.
    pub async fn run(mut self) -> Result<(), KaminoError> {
        let Some(server) = self.server.take() else {
            return Err(KaminoError::WrongMode("embedded (no server)"));
        };
        let res = server.run().await.map_err(KaminoError::from);
        self.cancel.cancel();
        for w in self.workers.drain(..) {
            if let Err(err) = tokio::time::timeout(Duration::from_secs(5), w).await {
                warn!(?err, "eviction worker did not exit promptly");
            }
        }
        if let Err(err) = self.client.close().await {
            debug!(?err, "client close error after run");
        }
        res
    }

    /// Get a `ShutdownHandle` for the standalone listener. Returns `None`
    /// for embedded handles.
    #[must_use]
    pub fn shutdown_handle(&self) -> Option<ShutdownHandle> {
        self.server.as_ref().map(Server::shutdown_handle)
    }

    /// Local listener address (server mode only).
    #[must_use]
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.server.as_ref().map(Server::local_addr)
    }

    /// Erased client handle.
    #[must_use]
    pub fn client(&self) -> Arc<dyn Client> {
        Arc::clone(&self.client) as Arc<dyn Client>
    }

    /// Typed embedded-client handle (escape hatch for tests + advanced use).
    #[must_use]
    pub fn embedded_client(&self) -> Arc<EmbeddedClient> {
        Arc::clone(&self.client)
    }

    /// Gracefully shut down the eviction workers and drop state.
    pub async fn shutdown(mut self) -> Result<(), KaminoError> {
        self.cancel.cancel();
        for w in self.workers.drain(..) {
            // Workers exit on cancel; await them with a generous timeout so a
            // misbehaving worker doesn't block forever.
            if let Err(err) = tokio::time::timeout(Duration::from_secs(5), w).await {
                warn!(?err, "eviction worker did not exit promptly");
            }
        }
        if let Err(err) = self.client.close().await {
            debug!(?err, "client close error during shutdown");
        }
        Ok(())
    }
}

impl Drop for Kamino {
    fn drop(&mut self) {
        // Be defensive: if the caller forgot `shutdown`, at least signal the
        // workers so they don't tick forever after we're gone.
        if !self.cancel.is_cancelled() {
            self.cancel.cancel();
        }
    }
}

fn ramblock_factory(config: &Config) -> EngineFactory {
    let table_size = usize::try_from(config.storage.table_size.0).unwrap_or(usize::MAX);
    let max_garbage_ratio = config.storage.max_garbage_ratio;
    Arc::new(move || -> Box<dyn StorageEngine> {
        Box::new(RamBlock::new(table_size, max_garbage_ratio))
    })
}

fn spawn_workers(
    client: &Arc<EmbeddedClient>,
    cancel: &CancellationToken,
    clock: &Arc<dyn Clock>,
    _config: &Config,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    let ttl_interval = Duration::from_secs(1);
    let idle_interval = Duration::from_secs(1);

    for dmap in client.registered_dmaps() {
        let fragment = dmap.fragment();
        let cancel = cancel.clone();
        let clock = Arc::clone(clock);

        // TTL sweeper always runs (no-op for entries without TTL).
        handles.push(tokio::spawn(TtlSweeper::run(
            fragment.clone(),
            clock.clone(),
            cancel.clone(),
            ttl_interval,
        )));

        // Idle sweeper only runs when the DMap has a max_idle_duration set.
        if let Some(max_idle) = dmap.options().max_idle_duration {
            if !max_idle.is_zero() {
                handles.push(tokio::spawn(IdleSweeper::run(
                    fragment.clone(),
                    clock,
                    cancel,
                    idle_interval,
                    max_idle,
                )));
            }
        }
        // LRU eviction is inline at write time (see `EmbeddedDMap`).
    }
    handles
}
