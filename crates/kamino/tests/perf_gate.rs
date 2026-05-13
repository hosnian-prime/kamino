//! CI hard-fail perf gate.
//!
//! Asserts the embedded `DMap` PUT/GET path stays above the targets in
//! `ROADMAP.md` §6 Phase 1 acceptance.
//!
//! The aspirational ROADMAP numbers are **100k PUT/s and 250k GET/s** on a
//! single core, P99 ≤ 1 ms. The dev-box measurement is roughly 1.4M PUT/s
//! and 0.9M GET/s, so the aspirational targets carry 5–15× headroom.
//!
//! The CI floors below sit at the ROADMAP PUT target and at 80 % of the
//! ROADMAP GET target. GET is bounded by an in-line write (we bump
//! `last_access` on every read per `docs/05-storage-engine.md`); shaving
//! that off is a Phase 1 follow-up and will let us tighten the floor.
//!
//! Set `KAMINO_PERF_GATE=skip` in the environment to skip (debug-mode
//! coverage runs, RR/sanitizer runs).

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::time::{Duration, Instant};

use kamino::{Config, Kamino, Mode, PutOptions};
use kamino_client::{DMap, DMapOptions};

/// PUT floor (ops/s). Matches the ROADMAP target with ~14× dev-box headroom.
const MIN_PUT_PER_SEC: f64 = 100_000.0;
/// GET floor (ops/s). 80 % of the ROADMAP target — see module-level note on
/// `last_access` writes. ~4.5× dev-box headroom.
const MIN_GET_PER_SEC: f64 = 200_000.0;
/// P99 ceiling (microseconds). ROADMAP says ≤ 1 ms; dev box is ~3µs.
const MAX_P99_US: u128 = 1_000;

/// Total iterations measured. Big enough for a stable mean on a noisy CI
/// runner, small enough that the test runs in under a second on a dev box.
const ITERATIONS: usize = 50_000;
/// Warm-up rounds before we start measuring.
const WARMUP: usize = 5_000;

/// Skip if explicitly disabled, or if we are running an unoptimised debug
/// build (where every measurement is 10–50× slower than release).
fn gate_disabled() -> bool {
    if cfg!(debug_assertions) {
        return true;
    }
    std::env::var("KAMINO_PERF_GATE")
        .map(|v| v == "skip")
        .unwrap_or(false)
}

/// Pre-build all keys before measurement so the hot loop only does the
/// operation, not string formatting.
fn prebuilt_keys(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i:08}")).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_put_throughput() {
    if gate_disabled() {
        eprintln!("perf gate skipped via KAMINO_PERF_GATE=skip");
        return;
    }
    let (_node, dmap) = build_node().await;
    let value = vec![0u8; 64];
    let warm_keys = prebuilt_keys("warm", WARMUP);
    let keys = prebuilt_keys("k", ITERATIONS);

    for k in &warm_keys {
        dmap.put(k, &value, PutOptions::default()).await.unwrap();
    }

    let mut samples: Vec<Duration> = Vec::with_capacity(ITERATIONS);
    let total_start = Instant::now();
    for k in &keys {
        let op_start = Instant::now();
        dmap.put(k, &value, PutOptions::default()).await.unwrap();
        samples.push(op_start.elapsed());
    }
    let total = total_start.elapsed();

    let per_sec = (ITERATIONS as f64) / total.as_secs_f64();
    let p99 = percentile(&mut samples, 0.99);
    eprintln!("PUT: {ITERATIONS} ops in {total:?}  -> {per_sec:.0} ops/s, P99 {p99:?}");

    assert!(
        per_sec >= MIN_PUT_PER_SEC,
        "PUT throughput {per_sec:.0}/s below floor {MIN_PUT_PER_SEC}/s — \
         set KAMINO_PERF_GATE=skip to bypass",
    );
    assert!(
        p99.as_micros() <= MAX_P99_US,
        "PUT P99 {p99:?} above ceiling {MAX_P99_US}us",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_get_throughput() {
    if gate_disabled() {
        eprintln!("perf gate skipped via KAMINO_PERF_GATE=skip");
        return;
    }
    let (_node, dmap) = build_node().await;
    let value = vec![0u8; 64];
    let keys = prebuilt_keys("k", ITERATIONS);

    for k in &keys {
        dmap.put(k, &value, PutOptions::default()).await.unwrap();
    }
    for k in keys.iter().take(WARMUP) {
        let _ = dmap.get(k).await.unwrap();
    }

    let mut samples: Vec<Duration> = Vec::with_capacity(ITERATIONS);
    let total_start = Instant::now();
    for k in &keys {
        let op_start = Instant::now();
        let _ = dmap.get(k).await.unwrap();
        samples.push(op_start.elapsed());
    }
    let total = total_start.elapsed();

    let per_sec = (ITERATIONS as f64) / total.as_secs_f64();
    let p99 = percentile(&mut samples, 0.99);
    eprintln!("GET: {ITERATIONS} ops in {total:?}  -> {per_sec:.0} ops/s, P99 {p99:?}");

    assert!(
        per_sec >= MIN_GET_PER_SEC,
        "GET throughput {per_sec:.0}/s below floor {MIN_GET_PER_SEC}/s — \
         set KAMINO_PERF_GATE=skip to bypass",
    );
    assert!(
        p99.as_micros() <= MAX_P99_US,
        "GET P99 {p99:?} above ceiling {MAX_P99_US}us",
    );
}

async fn build_node() -> (Kamino, std::sync::Arc<dyn DMap>) {
    let config = Config {
        mode: Mode::EmbeddedSolo,
        ..Config::default()
    };
    let node = Kamino::embedded(config).await.expect("embedded");
    let client = node.client();
    let dmap = client
        .new_dmap("perf", DMapOptions::default())
        .await
        .expect("new_dmap");
    (node, dmap)
}

fn percentile(samples: &mut [Duration], q: f64) -> Duration {
    samples.sort_unstable();
    let n = samples.len();
    let idx = ((n as f64) * q) as usize;
    let idx = idx.min(n - 1);
    samples[idx]
}
