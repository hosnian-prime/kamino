//! Kamino server binary entry point.
//!
//! Wires `clap` to the server runtime. The dispatcher itself lands in Phase 2;
//! Phase 0 only stands up the binary so the workspace produces a buildable
//! artifact end-to-end.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tracing::info;

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

    info!(
        version = env!("CARGO_PKG_VERSION"),
        config = ?cli.config,
        "kamino-server starting (Phase 0 skeleton)",
    );

    // Phase 2 will replace this with the actual server runtime.
    eprintln!(
        "kamino-server: Phase 0 skeleton — the dispatcher lands in Phase 2 (see ROADMAP.md)."
    );
    ExitCode::SUCCESS
}
