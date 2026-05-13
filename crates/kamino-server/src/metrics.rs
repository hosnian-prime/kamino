//! Lightweight counters surfaced by `STATS` until Phase 10 wires Prometheus.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Aggregated server-wide counters.
#[derive(Debug)]
pub(crate) struct ServerMetrics {
    started_at: Instant,
    current_connections: AtomicUsize,
    total_connections: AtomicU64,
    commands_total: AtomicU64,
}

impl ServerMetrics {
    pub(crate) fn new() -> Self {
        Self {
            started_at: Instant::now(),
            current_connections: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            commands_total: AtomicU64::new(0),
        }
    }

    pub(crate) fn on_connect(&self) {
        self.current_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn on_disconnect(&self) {
        self.current_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn on_command(&self) {
        self.commands_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            uptime_secs: self.started_at.elapsed().as_secs(),
            current_connections: self.current_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
            commands_total: self.commands_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MetricsSnapshot {
    pub(crate) uptime_secs: u64,
    pub(crate) current_connections: usize,
    pub(crate) total_connections: u64,
    pub(crate) commands_total: u64,
}
