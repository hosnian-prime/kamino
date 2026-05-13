//! Admin CLI binary.
//!
//! Phase 0 wires `clap` skeletons for the commands listed in `ROADMAP.md`
//! Phase 2 (`ping`, `stats`) and later phases; the actual handlers are
//! filled in as the server grows.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "kamino-cli", version, about = "Kamino admin CLI")]
struct Cli {
    /// Server address to connect to.
    #[arg(short, long, default_value = "127.0.0.1:3320", global = true)]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Send PING and report the round-trip time.
    Ping,
    /// Print server stats.
    Stats,
    /// List cluster members.
    Members,
    /// Print the routing table.
    Routing,
    /// Scan keys in a `DMap`.
    Scan {
        /// `DMap` name.
        dmap: String,
        /// Optional glob pattern.
        #[arg(short, long)]
        pattern: Option<String>,
    },
    /// Drain this node before shutdown.
    Drain,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    kamino_core::tracing_init::install_default();

    eprintln!(
        "kamino-cli: Phase 0 skeleton — command {:?} against {} is not wired yet.",
        cli.command, cli.addr,
    );
    ExitCode::SUCCESS
}
