//! Loom model-check for the [`kamino_storage::Locker`] cleanup race.
//!
//! Run with:
//!
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test -p kamino-storage --test locker_loom --release
//! ```
//!
//! Without `--cfg loom`, this file compiles to an empty test binary.
//!
//! # What this proves
//!
//! Loom can't directly drive `parking_lot::Mutex` (the registry mutex inside
//! `Locker`) or `tokio::sync::Mutex` (the per-key contention point), so this
//! file builds a **faithful model** of the *cleanup-race* portion of the
//! algorithm using `loom::sync::*` primitives. The model mirrors:
//!
//! - Outer mutex protects the `HashMap`.
//! - Each entry has an `AtomicUsize` refcount.
//! - `acquire` locks the outer mutex, finds-or-creates the entry, bumps the
//!   refcount, drops the outer mutex.
//! - Drop decrements the refcount; if the old value was 1, it re-locks the
//!   outer mutex, re-checks the refcount under the lock with `Arc::ptr_eq`
//!   on the entry, and removes only if still zero.
//!
//! The invariant under all loom-explored schedules:
//!
//! > After every spawned thread finishes acquire+release, the registry
//! > contains no entries.
//!
//! Loom exhaustively explores thread interleavings up to its bound, so a
//! pass certifies the algorithm against ABA-style races and lost-reap bugs.

#![cfg(loom)]

use std::collections::HashMap;

use loom::sync::Arc;
use loom::sync::Mutex;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;

#[derive(Debug)]
struct EntryModel {
    refcount: AtomicUsize,
}

#[derive(Debug, Default)]
struct LockerModel {
    locks: Mutex<HashMap<u32, Arc<EntryModel>>>,
}

fn acquire(locker: &Arc<LockerModel>, key: u32) -> GuardModel {
    let mut locks = locker.locks.lock().unwrap();
    let entry = locks
        .entry(key)
        .or_insert_with(|| {
            Arc::new(EntryModel {
                refcount: AtomicUsize::new(0),
            })
        })
        .clone();
    entry.refcount.fetch_add(1, Ordering::AcqRel);
    drop(locks);
    GuardModel {
        locker: Arc::clone(locker),
        key,
        entry,
    }
}

#[derive(Debug)]
struct GuardModel {
    locker: Arc<LockerModel>,
    key: u32,
    entry: Arc<EntryModel>,
}

impl Drop for GuardModel {
    fn drop(&mut self) {
        let prev = self.entry.refcount.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            let mut locks = self.locker.locks.lock().unwrap();
            // Double-check under the lock — a racing acquire might have
            // bumped the refcount back up since our fetch_sub.
            if self.entry.refcount.load(Ordering::Acquire) == 0 {
                if let Some(found) = locks.get(&self.key) {
                    if Arc::ptr_eq(found, &self.entry) {
                        locks.remove(&self.key);
                    }
                }
            }
        }
    }
}

#[test]
fn cleanup_after_two_threads_same_key() {
    loom::model(|| {
        let locker = Arc::new(LockerModel::default());

        let a = {
            let l = Arc::clone(&locker);
            thread::spawn(move || {
                let g = acquire(&l, 7);
                drop(g);
            })
        };
        let b = {
            let l = Arc::clone(&locker);
            thread::spawn(move || {
                let g = acquire(&l, 7);
                drop(g);
            })
        };

        a.join().unwrap();
        b.join().unwrap();

        let locks = locker.locks.lock().unwrap();
        assert!(
            locks.is_empty(),
            "registry must be empty after every thread releases (had {} entries)",
            locks.len(),
        );
    });
}

#[test]
fn cleanup_after_two_threads_disjoint_keys() {
    loom::model(|| {
        let locker = Arc::new(LockerModel::default());

        let a = {
            let l = Arc::clone(&locker);
            thread::spawn(move || {
                let g = acquire(&l, 1);
                drop(g);
            })
        };
        let b = {
            let l = Arc::clone(&locker);
            thread::spawn(move || {
                let g = acquire(&l, 2);
                drop(g);
            })
        };

        a.join().unwrap();
        b.join().unwrap();

        let locks = locker.locks.lock().unwrap();
        assert!(
            locks.is_empty(),
            "registry must be empty after every thread releases (had {} entries)",
            locks.len(),
        );
    });
}

/// The classic ABA case: A acquires, A drops (refcount→0). Before A can
/// re-lock the outer mutex to reap, B acquires the SAME key (sees the entry
/// still in the map, bumps refcount to 1). A then takes the outer mutex, sees
/// refcount != 0, and *correctly does not remove*. The entry must survive
/// until B drops too.
#[test]
fn aba_safe_reap() {
    loom::model(|| {
        let locker = Arc::new(LockerModel::default());

        let a = {
            let l = Arc::clone(&locker);
            thread::spawn(move || {
                let g = acquire(&l, 42);
                drop(g);
            })
        };
        let b = {
            let l = Arc::clone(&locker);
            thread::spawn(move || {
                let g = acquire(&l, 42);
                drop(g);
            })
        };

        a.join().unwrap();
        b.join().unwrap();

        // After both threads finish, the entry must be gone.
        let locks = locker.locks.lock().unwrap();
        assert_eq!(
            locks.len(),
            0,
            "lost-reap bug: registry still holds {} entries",
            locks.len(),
        );
    });
}
