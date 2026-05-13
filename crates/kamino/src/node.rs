//! `Kamino` builder — wires up an [`EmbeddedClient`], a [`Locker`], and the
//! eviction workers from a [`Config`].

use std::sync::Arc;
use std::time::Duration;

use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_client::{Client, DMapOptions};
use kamino_core::{Clock, Config, Hasher, Mode, SystemClock, XxHasher};
use kamino_storage::{IdleSweeper, Locker, RamBlock, StorageEngine, TtlSweeper};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Errors raised by [`Kamino::embedded`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KaminoError {
    /// `Kamino::embedded` was called with a non-`EmbeddedSolo` config.
    #[error(
        "Kamino::embedded requires Mode::EmbeddedSolo, got {0}; use \
         embedded_clustered/serve for other modes"
    )]
    WrongMode(&'static str),

    /// Configuration validation failed.
    #[error(transparent)]
    Config(#[from] kamino_core::Error),
}

/// Running Kamino node. Hold this to keep the eviction workers alive; drop
/// or call [`Kamino::shutdown`] to tear them down.
pub struct Kamino {
    client: Arc<EmbeddedClient>,
    cancel: CancellationToken,
    workers: Vec<JoinHandle<()>>,
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
        })
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
