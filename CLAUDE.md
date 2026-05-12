# Kamino

Distributed in-memory cache library written in Rust.

## Project Structure

- `docs/` — Technical documentation (architecture, protocol, storage engine, K8s, etc.)
- `.claude/skills/` — Custom skills: `/engineer`, `/reviewer`, `/architect`

## Language & Standards

- Language: Rust (latest stable)
- Async runtime: tokio
- Serialization: RESP (wire), MessagePack (routing table), custom binary (storage)
- Config format: TOML
- No `unsafe` without explicit justification
- All public APIs must be `Send + Sync`

## Architecture Decisions

- SWIM gossip for cluster membership (no Raft/Paxos)
- Bounded-load consistent hashing (271 partitions, xxHash)
- Primary-backup replication with LWW conflict resolution
- RESP wire protocol (Redis compatible)
- RamBlock append-only storage engine
- Coordinator = oldest node (no election protocol)

## Conventions

- Keep code minimal — no speculative abstractions
- Prefer `thiserror` for error types, `tracing` for logging
- Tests go in the same file (`#[cfg(test)]` module), integration tests in `tests/`
- Commit messages: imperative mood, concise, explain "why" not "what"
- Documentation lives in `docs/`, not in README
