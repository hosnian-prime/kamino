//! Background eviction workers.
//!
//! Per `docs/05-storage-engine.md`:
//!
//! - **TTL** — 20-sample probabilistic expiry. Sample 20 keys with TTL set,
//!   delete the expired ones, repeat aggressively when >25% of the sample
//!   was expired.
//! - **Idle** — entries whose `last_access + max_idle_duration` is in the
//!   past get evicted.
//! - **LRU** — sample `lru_samples` keys, evict the one with the oldest
//!   `last_access`. Triggered when `max_keys` / `max_inuse` is exceeded.
//!
//! **Owner**: concurrency-layer agent.
