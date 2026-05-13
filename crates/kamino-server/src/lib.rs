//! Kamino server runtime: TCP listener, RESP dispatch and SIGHUP reload.
//!
//! Phase 0 ships only the skeleton; the dispatcher lands in Phase 2 and the
//! reload pipeline in Phase 11 (see `ROADMAP.md`).
