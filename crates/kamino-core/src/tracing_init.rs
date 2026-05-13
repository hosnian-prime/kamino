//! Opinionated default tracing-subscriber installer.
//!
//! Binaries call [`install_default`] at startup. Library consumers that
//! already have their own subscriber should ignore this module.

use std::io::IsTerminal;
use std::sync::Once;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

static INIT: Once = Once::new();

/// Install the default tracing subscriber idempotently.
///
/// - Reads `RUST_LOG` if set, otherwise defaults to `info`.
/// - Picks pretty terminal format when stderr is a TTY, JSON otherwise.
/// - Emits `NEW` + `CLOSE` span events for latency measurement.
///
/// Calling this more than once is a no-op.
pub fn install_default() {
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        let is_tty = std::io::stderr().is_terminal();
        if is_tty {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_target(true)
                .with_writer(std::io::stderr)
                .init();
        } else {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_target(true)
                .with_writer(std::io::stderr)
                .json()
                .init();
        }
    });
}
