//! Compaction driver — periodic loop that asks each [`crate::Fragment`] to
//! run a compaction pass when its garbage ratio crosses the configured
//! threshold.
//!
//! See `docs/05-storage-engine.md` §"Compaction".
//!
//! **Owner**: storage-internals agent.
