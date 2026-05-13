//! Stats request/response shapes.

/// Stats request shape. Empty for Phase 1 — added knobs land later.
#[derive(Debug, Default, Clone)]
pub struct StatsOptions;

/// Aggregated per-process statistics.
#[derive(Debug, Default, Clone)]
pub struct Stats {
    /// One stat block per DMap, keyed by name.
    pub dmaps: std::collections::BTreeMap<String, DMapStats>,
}

/// Per-DMap counters.
#[derive(Debug, Default, Clone)]
pub struct DMapStats {
    /// Live entry count.
    pub len: usize,
    /// Bytes occupied by live entries.
    pub inuse: usize,
}
