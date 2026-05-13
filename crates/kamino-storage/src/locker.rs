#![allow(clippy::type_complexity)] // the registry shape is the public contract

//! `Locker` — fine-grained per-key lock primitive.
//!
//! Per `docs/07-concurrency.md`:
//!
//! - The outer `HashMap<(dmap, key), Arc<LockEntry>>` is guarded by a
//!   `parking_lot::Mutex` (sync, **never held across `.await`**).
//! - Each `LockEntry` holds a `tokio::sync::Mutex<()>` that is the actual
//!   contention point and *is* held across `.await`.
//! - Refcount-based cleanup: when the last waiter releases an entry the slot
//!   is reaped from the outer map under the same sync mutex.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex as SyncMutex;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Errors raised by [`Locker::acquire`].
#[derive(Debug, thiserror::Error)]
pub enum LockerError {
    /// Failed to acquire the per-key lock within the supplied deadline.
    #[error("lock acquisition timed out after {0:?}")]
    Timeout(Duration),
}

/// One contended lock — the inner async mutex is what waiters serialise on.
#[derive(Debug)]
pub struct LockEntry {
    /// Async mutex held across `.await` by the lock holder.
    pub(crate) mutex: Arc<AsyncMutex<()>>,
    /// Number of tasks that have a reference to this entry: holder + waiters.
    /// Used by the drop path to know when to reap the slot.
    pub(crate) refcount: AtomicUsize,
}

/// Registry of currently-held locks per `(dmap, key)`.
#[derive(Debug, Default)]
pub struct Locker {
    pub(crate) locks: SyncMutex<HashMap<(String, Vec<u8>), Arc<LockEntry>>>,
}

/// RAII guard returned by [`Locker::acquire`]. Releases the lock when dropped
/// and removes the registry entry once the last waiter is gone.
#[derive(Debug)]
pub struct LockGuard {
    // Drop order is field-declaration order in Rust: the OwnedMutexGuard drops
    // first (releasing the inner mutex), then the cleanup runs.
    _inner: OwnedMutexGuard<()>,
    cleanup: Option<Cleanup>,
}

#[derive(Debug)]
struct Cleanup {
    locker: Arc<Locker>,
    key: (String, Vec<u8>),
    entry: Arc<LockEntry>,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup.reap();
        }
    }
}

impl Cleanup {
    fn reap(self) {
        // Decrement the refcount; if we were the last reference, lock the
        // outer map and remove the entry. Holding the outer mutex during the
        // removal check is what prevents a racing acquire from grabbing a
        // stale `Arc<LockEntry>` after we've decided to remove it.
        let prev = self.entry.refcount.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev >= 1, "refcount underflow");
        if prev != 1 {
            return;
        }
        let mut locks = self.locker.locks.lock();
        // Double-check under the lock — a racing `acquire` might have bumped
        // the refcount back up between our decrement and now.
        if self.entry.refcount.load(Ordering::Acquire) != 0 {
            return;
        }
        if let Some(found) = locks.get(&self.key) {
            if Arc::ptr_eq(found, &self.entry) {
                locks.remove(&self.key);
            }
        }
    }
}

impl Locker {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Acquire the lock for `(dmap, key)`. Waits up to `deadline` before
    /// returning [`LockerError::Timeout`].
    ///
    /// The returned [`LockGuard`] holds the lock until dropped.
    pub async fn acquire(
        self: &Arc<Self>,
        dmap: &str,
        key: &[u8],
        deadline: Duration,
    ) -> Result<LockGuard, LockerError> {
        let map_key = (dmap.to_string(), key.to_vec());
        let entry = self.get_or_create(map_key.clone());

        // `lock_owned` is `async` so we acquire the outer parking_lot mutex
        // only above (sync), then drop it before awaiting here.
        let mutex_handle = Arc::clone(&entry.mutex);
        let timeout_result = tokio::time::timeout(deadline, mutex_handle.lock_owned()).await;
        let Ok(owned) = timeout_result else {
            // Lock acquisition gave up; drop our refcount and bail out.
            Cleanup {
                locker: Arc::clone(self),
                key: map_key,
                entry,
            }
            .reap();
            return Err(LockerError::Timeout(deadline));
        };

        Ok(LockGuard {
            _inner: owned,
            cleanup: Some(Cleanup {
                locker: Arc::clone(self),
                key: map_key,
                entry,
            }),
        })
    }

    fn get_or_create(&self, map_key: (String, Vec<u8>)) -> Arc<LockEntry> {
        let entry = {
            let mut locks = self.locks.lock();
            locks
                .entry(map_key)
                .or_insert_with(|| {
                    Arc::new(LockEntry {
                        mutex: Arc::new(AsyncMutex::new(())),
                        refcount: AtomicUsize::new(0),
                    })
                })
                .clone()
        };
        entry.refcount.fetch_add(1, Ordering::AcqRel);
        entry
    }

    /// Number of distinct `(dmap, key)` slots currently held or contended.
    /// Exposed for tests + observability — the value is racy in production.
    pub fn registry_len(&self) -> usize {
        self.locks.lock().len()
    }
}

#[cfg(test)]
#[allow(clippy::significant_drop_tightening)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn acquire_and_release_cleans_up() {
        let locker = Locker::new();
        {
            let _g = locker
                .acquire("d", b"k", Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(locker.registry_len(), 1);
        }
        assert_eq!(locker.registry_len(), 0);
    }

    #[tokio::test]
    async fn second_acquire_blocks_until_first_drops() {
        let locker = Locker::new();
        let g1 = locker
            .acquire("d", b"k", Duration::from_secs(1))
            .await
            .unwrap();

        // Spawn a second acquirer; it should not complete until we drop g1.
        let l = Arc::clone(&locker);
        let handle =
            tokio::spawn(
                async move { l.acquire("d", b"k", Duration::from_secs(1)).await.unwrap() },
            );

        // Give the spawned task a chance to enter the wait state.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!handle.is_finished());
        drop(g1);
        let g2 = handle.await.unwrap();
        drop(g2);
        assert_eq!(locker.registry_len(), 0);
    }

    #[tokio::test]
    async fn timeout_returns_err() {
        let locker = Locker::new();
        let _g1 = locker
            .acquire("d", b"k", Duration::from_secs(1))
            .await
            .unwrap();
        let err = locker
            .acquire("d", b"k", Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(matches!(err, LockerError::Timeout(_)));
        // Failed waiter must not leak a registry slot — only the holder remains.
        assert_eq!(locker.registry_len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_acquires_on_same_key_are_serialized_and_leave_no_leak() {
        let locker = Locker::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            let l = Arc::clone(&locker);
            let c = Arc::clone(&counter);
            let m = Arc::clone(&max_observed);
            handles.push(tokio::spawn(async move {
                let _g = l
                    .acquire("d", b"contended", Duration::from_secs(5))
                    .await
                    .unwrap();
                let current = c.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(current, Ordering::SeqCst);
                // Tiny pause so a buggy implementation would let another in.
                tokio::time::sleep(Duration::from_micros(50)).await;
                c.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            max_observed.load(Ordering::SeqCst),
            1,
            "lock must serialise holders"
        );
        assert_eq!(locker.registry_len(), 0, "no leaked entries");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disjoint_keys_do_not_block_each_other() {
        let locker = Locker::new();
        let mut handles = Vec::new();
        let start = std::time::Instant::now();
        for i in 0..32 {
            let l = Arc::clone(&locker);
            handles.push(tokio::spawn(async move {
                let key = format!("k{i}");
                let _g = l
                    .acquire("d", key.as_bytes(), Duration::from_secs(5))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = start.elapsed();
        // Sequential would be 32 * 20ms = 640ms; loose bound at 250ms.
        assert!(
            elapsed < Duration::from_millis(250),
            "disjoint keys serialised: {elapsed:?}"
        );
        assert_eq!(locker.registry_len(), 0);
    }
}
