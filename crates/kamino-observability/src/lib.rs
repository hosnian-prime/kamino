//! Observability surface for Kamino: Prometheus + OTLP + slow log + healthz.
//!
//! Phase 0 ships only the skeleton; the `MetricRegistry` trait and exporters
//! land in Phase 10 (see `ROADMAP.md`), with hooks added incrementally from
//! Phase 2 onward.
