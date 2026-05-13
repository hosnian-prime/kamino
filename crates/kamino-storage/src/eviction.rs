//! Background eviction workers.
//!
//! Per `docs/05-storage-engine.md`:
//!
//! - [`TtlSweeper`] — 20-sample probabilistic expiry: sample 20 keys with TTL
//!   set, delete the expired ones, repeat aggressively when >25% of the
//!   sample was expired, otherwise sleep.
//! - [`IdleSweeper`] — entries whose `last_access + max_idle_duration` is in
//!   the past get evicted.
//! - [`LruSampler`] — sample `lru_samples` keys, evict the one with the
//!   oldest `last_access`. Called inline by writers when `max_keys` /
//!   `max_inuse` is exceeded.

use std::sync::Arc;
use std::time::Duration;

use kamino_core::Clock;
use rand::seq::IteratorRandom;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::entry::Entry;
use crate::fragment::Fragment;

/// Number of keys sampled per TTL sweep (per `docs/05`).
pub const TTL_SAMPLE_SIZE: usize = 20;

/// Fraction of expired keys in the sample that triggers another immediate
/// pass before sleeping (per `docs/05`).
const TTL_AGGRESSIVE_RATIO: f64 = 0.25;

fn now_nanos(clock: &dyn Clock) -> i64 {
    // `Clock::now_micros` returns u64 micros; convert to i64 nanos. Saturate
    // on overflow — at u64::MAX micros this happens well after the year 2554,
    // long past the lifetime of any deployed cluster.
    let micros = clock.now_micros();
    i64::try_from(micros).map_or(i64::MAX, |m| m.saturating_mul(1_000))
}

/// TTL sweeper: probabilistic 20-sample expiry loop.
#[derive(Debug)]
pub struct TtlSweeper;

impl TtlSweeper {
    /// Run the sweep loop until `cancel` fires. `interval` is the cool-down
    /// between non-aggressive cycles.
    pub async fn run(
        fragment: Arc<Fragment>,
        clock: Arc<dyn Clock>,
        cancel: CancellationToken,
        interval: Duration,
    ) {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            Self::run_until_quiet(&fragment, clock.as_ref()).await;
        }
    }

    async fn run_until_quiet(fragment: &Fragment, clock: &dyn Clock) {
        loop {
            let expired_ratio = Self::sweep_once(fragment, clock).await;
            if expired_ratio <= TTL_AGGRESSIVE_RATIO {
                break;
            }
        }
    }

    /// One pass: sample, delete expired, return `expired / sampled` ratio.
    async fn sweep_once(fragment: &Fragment, clock: &dyn Clock) -> f64 {
        let now = now_nanos(clock);
        let mut ttl_entries: Vec<(u64, Entry)> = Vec::new();
        // Collect candidates with TTL set; the scan callback is sync.
        let res = fragment
            .scan(|h, e| {
                if e.ttl_nanos != 0 {
                    ttl_entries.push((h, e.clone()));
                }
                true
            })
            .await;
        if let Err(err) = res {
            warn!(?err, "ttl sweep scan failed");
            return 0.0;
        }
        if ttl_entries.is_empty() {
            return 0.0;
        }
        let sample: Vec<&(u64, Entry)> = {
            let mut rng = rand::thread_rng();
            ttl_entries
                .iter()
                .choose_multiple(&mut rng, TTL_SAMPLE_SIZE)
        };
        let sampled = sample.len();
        let mut expired = 0_usize;
        for (h, e) in sample {
            if e.is_expired(now) && matches!(fragment.delete(*h).await, Ok(true)) {
                expired += 1;
            }
        }
        #[allow(clippy::cast_precision_loss)]
        let ratio = expired as f64 / sampled as f64;
        debug!(sampled, expired, ratio, "ttl sweep pass");
        ratio
    }
}

/// Idle sweeper: evicts entries whose `last_access + max_idle` is in the past.
#[derive(Debug)]
pub struct IdleSweeper;

impl IdleSweeper {
    /// Run the idle sweep loop until `cancel` fires.
    pub async fn run(
        fragment: Arc<Fragment>,
        clock: Arc<dyn Clock>,
        cancel: CancellationToken,
        interval: Duration,
        max_idle: Duration,
    ) {
        if max_idle.is_zero() {
            return;
        }
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            Self::sweep_once(&fragment, clock.as_ref(), max_idle).await;
        }
    }

    async fn sweep_once(fragment: &Fragment, clock: &dyn Clock, max_idle: Duration) {
        let now = now_nanos(clock);
        let idle_nanos = i64::try_from(max_idle.as_nanos()).unwrap_or(i64::MAX);
        let mut victims: Vec<u64> = Vec::new();
        let res = fragment
            .scan(|h, e| {
                if now.saturating_sub(e.last_access_nanos) > idle_nanos {
                    victims.push(h);
                }
                true
            })
            .await;
        if let Err(err) = res {
            warn!(?err, "idle sweep scan failed");
            return;
        }
        for h in victims {
            let _ = fragment.delete(h).await;
        }
    }
}

/// Sampled-LRU evictor — synchronous helper invoked by writers.
#[derive(Debug)]
pub struct LruSampler;

impl LruSampler {
    /// Sample `samples` random entries from `fragment` and evict the one with
    /// the oldest `last_access`. Returns `true` if an entry was evicted.
    pub async fn evict_one(fragment: &Fragment, samples: usize) -> bool {
        if samples == 0 {
            return false;
        }
        let mut keys: Vec<(u64, i64)> = Vec::new();
        let _ = fragment
            .scan(|h, e| {
                keys.push((h, e.last_access_nanos));
                true
            })
            .await;
        if keys.is_empty() {
            return false;
        }
        let chosen: Vec<&(u64, i64)> = {
            let mut rng = rand::thread_rng();
            keys.iter().choose_multiple(&mut rng, samples)
        };
        if let Some(victim) = chosen.into_iter().min_by_key(|(_, ts)| *ts) {
            fragment.delete(victim.0).await.unwrap_or(false)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::Entry;
    use crate::fragment::tests::MockEngine;

    #[derive(Debug)]
    struct FakeClock {
        now_micros: AtomicU64,
    }
    impl FakeClock {
        const fn new(micros: u64) -> Self {
            Self {
                now_micros: AtomicU64::new(micros),
            }
        }
    }
    impl Clock for FakeClock {
        fn now_micros(&self) -> u64 {
            self.now_micros.load(Ordering::SeqCst)
        }
        fn now_monotonic(&self) -> std::time::Instant {
            std::time::Instant::now()
        }
    }

    fn entry_with_ttl(key: &str, ttl_nanos: i64, ts: i64) -> Entry {
        Entry {
            key: key.as_bytes().to_vec(),
            ttl_nanos,
            timestamp_nanos: ts,
            last_access_nanos: ts,
            value: b"v".to_vec(),
        }
    }

    fn mock_fragment() -> Arc<Fragment> {
        Fragment::new(Box::new(MockEngine::default()))
    }

    #[tokio::test]
    async fn ttl_sweep_deletes_expired() {
        let frag = mock_fragment();
        // entry with absolute ttl_nanos=1_000 should be expired at now=10_000 ns
        for i in 0..5_u64 {
            frag.put(i, &entry_with_ttl(&format!("k{i}"), 1_000, 0))
                .await
                .unwrap();
        }
        // a fresh non-expired entry with TTL well in the future
        frag.put(99, &entry_with_ttl("fresh", 1_000_000_000_000, 0))
            .await
            .unwrap();

        let clock = Arc::new(FakeClock::new(10)); // 10 micros -> 10_000 nanos
        TtlSweeper::run_until_quiet(&frag, clock.as_ref()).await;
        // All five expired ones should be gone; the fresh one stays.
        assert_eq!(frag.len().await, 1);
        assert!(frag.get(99).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn ttl_sweep_skips_entries_without_ttl() {
        let frag = mock_fragment();
        frag.put(1, &entry_with_ttl("k", 0, 0)).await.unwrap();
        let clock = Arc::new(FakeClock::new(1_000_000));
        TtlSweeper::run_until_quiet(&frag, clock.as_ref()).await;
        assert_eq!(frag.len().await, 1);
    }

    #[tokio::test]
    async fn idle_sweep_deletes_old_entries() {
        let frag = mock_fragment();
        let old = Entry {
            key: b"old".to_vec(),
            ttl_nanos: 0,
            timestamp_nanos: 0,
            last_access_nanos: 0,
            value: b"v".to_vec(),
        };
        let fresh = Entry {
            key: b"fresh".to_vec(),
            ttl_nanos: 0,
            timestamp_nanos: 0,
            // 10 ms in nanos
            last_access_nanos: 10_000_000,
            value: b"v".to_vec(),
        };
        frag.put(1, &old).await.unwrap();
        frag.put(2, &fresh).await.unwrap();
        // now = 20 ms in nanos
        let clock = FakeClock::new(20_000);
        IdleSweeper::sweep_once(&frag, &clock, Duration::from_millis(15)).await;
        // Old entry: 20ms - 0 = 20ms > 15ms idle threshold, evicted.
        // Fresh entry: 20ms - 10ms = 10ms < 15ms, kept.
        assert!(frag.get(1).await.unwrap().is_none());
        assert!(frag.get(2).await.unwrap().is_some());
    }

    #[tokio::test]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
    async fn lru_sampler_evicts_oldest_when_sampling_all() {
        let frag = mock_fragment();
        for i in 0..10_i64 {
            let mut e = entry_with_ttl(&format!("k{i}"), 0, 0);
            e.last_access_nanos = i;
            frag.put(i as u64, &e).await.unwrap();
        }
        // sample size >= total ensures the oldest (hkey=0) is chosen.
        let evicted = LruSampler::evict_one(&frag, 100).await;
        assert!(evicted);
        assert!(frag.get(0).await.unwrap().is_none());
        assert_eq!(frag.len().await, 9);
    }

    #[tokio::test]
    async fn cancellation_stops_sweeper() {
        let frag = mock_fragment();
        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(0));
        let cancel = CancellationToken::new();
        let task = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                TtlSweeper::run(frag, clock, cancel, Duration::from_secs(60)).await;
            })
        };
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("sweeper exits promptly after cancel")
            .unwrap();
    }
}
