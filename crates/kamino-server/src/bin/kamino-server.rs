//! Kamino server binary entry point.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_client::{Client, DMapOptions};
use kamino_core::{Clock, Config, Hasher, SystemClock, XxHasher};
use kamino_server::Server;
use kamino_storage::{IdleSweeper, Locker, RamBlock, StorageEngine, TtlSweeper};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "kamino-server",
    version,
    about = "Kamino distributed cache server"
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    kamino_core::tracing_init::install_default();

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("kamino-server: failed to build tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        match run(cli).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                error!(error = %e, "kamino-server exiting with error");
                ExitCode::FAILURE
            }
        }
    })
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = match &cli.config {
        Some(path) => Config::from_toml_file(path)?,
        None => Config::default(),
    };
    kamino_core::config::env::overlay_from_env(&mut config)?;

    if config.mode != kamino_core::Mode::Standalone {
        return Err(format!(
            "kamino-server requires Mode::Standalone, got {}; set mode = \"standalone\" \
             in the TOML config or KAMINO_MODE=standalone",
            config.mode.as_str()
        )
        .into());
    }

    info!(
        version = env!("CARGO_PKG_VERSION"),
        config = ?cli.config,
        bind_addr = %config.network.bind_addr,
        bind_port = config.network.bind_port,
        "kamino-server starting",
    );

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
    let locker = Locker::new();
    let engine_factory = ramblock_factory(&config);
    let deps = EmbeddedDeps {
        clock: Arc::clone(&clock),
        hasher,
        locker,
        engine_factory,
        partition_count: config.core.partition_count,
    };
    let embedded = EmbeddedClient::new(deps);
    for (name, dmap_cfg) in &config.dmaps {
        embedded.get_or_create(name, DMapOptions::from_config(dmap_cfg));
    }
    let cancel = CancellationToken::new();
    let workers = spawn_workers(&embedded, &cancel, &clock);

    let client: Arc<dyn Client> = Arc::clone(&embedded) as Arc<dyn Client>;
    let server = Server::bind(&config, client).await?;
    let shutdown = server.shutdown_handle();

    let signal_task = tokio::spawn(async move {
        wait_for_signal().await;
        info!("signal received; triggering graceful shutdown");
        shutdown.trigger();
    });

    let run_result = server.run().await;
    signal_task.abort();
    cancel.cancel();
    for w in workers {
        if let Err(err) = tokio::time::timeout(Duration::from_secs(5), w).await {
            warn!(?err, "worker did not exit promptly");
        }
    }
    if let Err(err) = embedded.close().await {
        debug!(?err, "embedded client close error");
    }
    run_result.map_err(Into::into)
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
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    let ttl_interval = Duration::from_secs(1);
    let idle_interval = Duration::from_secs(1);
    for dmap in client.registered_dmaps() {
        let fragment = dmap.fragment();
        let cancel = cancel.clone();
        let clock = Arc::clone(clock);
        handles.push(tokio::spawn(TtlSweeper::run(
            fragment.clone(),
            clock.clone(),
            cancel.clone(),
            ttl_interval,
        )));
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
    }
    handles
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => return,
    };
    tokio::select! {
        _ = sigint.recv() => {},
        _ = sigterm.recv() => {},
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
