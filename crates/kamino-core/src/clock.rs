//! Time abstraction.
//!
//! LWW conflict resolution uses [`Clock::now_micros`] as the timestamp
//! source — see `docs/04-replication.md`. Tests and turmoil sims inject
//! a mock clock instead of [`SystemClock`].

use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Wall-clock + monotonic time, injectable for tests and simulation.
///
/// Implementations must be `Send + Sync` so a single instance can be shared
/// across cluster tasks.
pub trait Clock: Send + Sync + 'static {
    /// Wall-clock microseconds since the Unix epoch.
    ///
    /// Saturates at 0 on pre-epoch system time, which is a configuration
    /// error — see `docs/09-configuration.md#ntp-requirement`.
    fn now_micros(&self) -> u64;

    /// Monotonic time for measuring elapsed durations.
    fn now_monotonic(&self) -> Instant;
}

/// Production `Clock` backed by `std::time`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_micros(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
    }

    fn now_monotonic(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn wall_clock_is_after_epoch() {
        let clock = SystemClock;
        let micros = clock.now_micros();
        // 1700000000s == 2023-11-14, well in the past of any plausible run.
        assert!(micros > 1_700_000_000_u64 * 1_000_000, "{micros}");
    }

    #[test]
    fn monotonic_clock_advances() {
        let clock = SystemClock;
        let a = clock.now_monotonic();
        thread::sleep(Duration::from_millis(1));
        let b = clock.now_monotonic();
        assert!(b > a);
    }

    /// Custom `Clock` impls are useful for turmoil and jepsen sims.
    #[test]
    fn custom_clock_impl() {
        #[derive(Debug)]
        struct FakeClock {
            now: Arc<AtomicU64>,
        }
        impl Clock for FakeClock {
            fn now_micros(&self) -> u64 {
                self.now.load(Ordering::SeqCst)
            }
            fn now_monotonic(&self) -> Instant {
                Instant::now()
            }
        }

        let now = Arc::new(AtomicU64::new(42));
        let clock = FakeClock { now: now.clone() };
        assert_eq!(clock.now_micros(), 42);
        now.store(100, Ordering::SeqCst);
        assert_eq!(clock.now_micros(), 100);
    }

    #[test]
    fn clock_is_object_safe() {
        let boxed: Box<dyn Clock> = Box::new(SystemClock);
        let _ = boxed.now_micros();
    }
}
