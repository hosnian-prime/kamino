//! Admin CLI binary.
//!
//! Phase 2 wires `ping` and `stats` against the [`RemoteClient`]. The other
//! subcommands (members, routing, scan, drain) are stubbed until the
//! corresponding server surface lands in later phases.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use kamino_client::{Client, RemoteClient, StatsOptions};

#[derive(Parser, Debug)]
#[command(name = "kamino-cli", version, about = "Kamino admin CLI")]
struct Cli {
    /// Server address to connect to.
    #[arg(short, long, default_value = "127.0.0.1:3320", global = true)]
    addr: String,

    /// Optional client password (matches `auth.password` server-side).
    #[arg(long, global = true)]
    auth: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Send PING and report success/failure.
    Ping,
    /// Print server stats.
    Stats,
    /// List cluster members (Phase 3+ — not wired in Phase 2).
    Members,
    /// Print the routing table (Phase 4+ — not wired in Phase 2).
    Routing,
    /// Scan keys in a `DMap` (Phase 2 future work).
    Scan {
        /// `DMap` name.
        dmap: String,
        /// Optional glob pattern.
        #[arg(short, long)]
        pattern: Option<String>,
    },
    /// Drain this node before shutdown (Phase 8+).
    Drain,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    kamino_core::tracing_init::install_default();

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("kamino-cli: failed to start tokio: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        match run(cli).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("kamino-cli: {e}");
                ExitCode::FAILURE
            }
        }
    })
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Ping => {
            let client = RemoteClient::connect(&cli.addr, cli.auth.as_deref()).await?;
            client.ping(&cli.addr).await?;
            println!("PONG");
            let _ = client.close().await;
            Ok(())
        }
        Command::Stats => {
            let client = RemoteClient::connect(&cli.addr, cli.auth.as_deref()).await?;
            let stats = client.stats(StatsOptions).await?;
            println!("{stats:#?}");
            let _ = client.close().await;
            Ok(())
        }
        other @ (Command::Members | Command::Routing | Command::Scan { .. } | Command::Drain) => {
            Err(format!("kamino-cli: {other:?} is not wired in Phase 2").into())
        }
    }
}
