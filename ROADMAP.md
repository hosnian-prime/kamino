# Implementation Roadmap

This is the plan for going from `docs/` to a working Kamino. It does three things:

1. Fixes the workspace layout and dependency direction so the code stays modular as it grows.
2. Phases the work so every phase ships a usable artifact (no big-bang).
3. Cross-references every implementable surface in `docs/` against a phase, so nothing is silently skipped.

The plan is fluid; treat it as the current best guess, not a contract. The docs in `docs/` are the contract.

---

## 1. Guiding Principles

- **Module first, crate last.** A crate boundary is a compile-time wall. Don't pay for one until a piece of code earns it (≥ 500 LOC and ≥ 2 dependents). The flat `crates/` layout follows matklad's "Large Rust Workspaces" recommendation.
- **Traits define seams; impls live next to where they're used.** Following TiKV's `engine_traits` pattern: `kamino-storage` exposes the trait and ships the default `RamBlock` impl. Future RocksDB / LMDB engines become their own crates only when they actually exist.
- **Features are additive only.** No feature gate ever removes or changes an API; it only enables an optional impl. Feature creep is the workspace's slow death.
- **Strict dependency direction.** Lower-level crates never depend on higher-level ones. Enforced by a CI check (`cargo deny` plus a custom workspace lint).
- **Async correctness over async fashion.** Mixed sync/async lock discipline (per `docs/07-concurrency.md`) is reviewed at every PR. No `parking_lot::Mutex` across `.await`, ever.
- **Every phase ships.** Each phase below produces a runnable binary or a usable library — never "almost done, will finish next phase."

---

## 2. Workspace Layout

```
kamino/
├── Cargo.toml                # virtual workspace manifest, no [package]
├── ROADMAP.md                # this file
├── README.md
├── CLAUDE.md
├── docs/                     # contract (see docs/00-overview.md)
├── crates/
│   ├── kamino-core/          # Config, Mode, Profile, errors, Hasher trait, Clock, ids
│   ├── kamino-storage/       # StorageEngine trait + RamBlock + Entry + Fragment + Locker + eviction
│   ├── kamino-protocol/      # RESP2/3 codec + command parsing types (no handlers)
│   ├── kamino-cluster/       # SWIM + RoutingTable + Discovery + Coordinator + Balancer
│   │                         # + Replication + DMapService + PubSubService + Forwarder
│   ├── kamino-observability/ # Prometheus + OTLP + slow log + HTTP healthz + metric hooks
│   ├── kamino-client/        # Client trait + RemoteClient + EmbeddedClient + Pipeline + ScanCursor
│   ├── kamino-server/        # TCP listener + RESP handlers + AUTH + dispatch + SIGHUP reload
│   │                         # + bin/kamino-server.rs (the server binary)
│   ├── kamino/               # Umbrella crate: re-exports + Kamino::embedded()/serve() builders
│   └── kamino-cli/           # Admin CLI binary (drain, stats, scan, members)
├── xtask/                    # Build automation binary (`cargo xtask <subcommand>`)
├── tests/                    # Multi-crate integration tests
│   ├── e2e/                  # End-to-end against a real binary
│   ├── turmoil/              # Deterministic SWIM simulation
│   ├── jepsen/               # Network-partition + LWW convergence
│   └── compat/               # Cross-version wire smoke
└── benches/                  # criterion benches that span crates
```

**Naming**: folder name == crate name. `kamino-storage`, not `storage`. matklad's rule: don't strip common prefixes — reverse-dependency search and `cargo` output stay clear.

**Versioning**: every internal crate uses `version = "0.0.0"` until 1.0. The umbrella `kamino` crate is what the public consumes; sub-crates are implementation surface.

---

## 3. Crate Responsibilities and Dependency Graph

```
kamino-core                                      (no internal deps)
   ▲
   │
kamino-storage   kamino-protocol   kamino-observability
   ▲                  ▲                    ▲
   │                  │                    │
   └─────► kamino-cluster ◄─────────────────┤
                ▲                            │
                │                            │
        kamino-client     kamino-server ─────┘
                ▲                ▲
                │                │
                └──── kamino ◄───┘
                         ▲
                         │
                     kamino-cli
```

| Crate | Responsibility | Allowed deps |
|-------|----------------|--------------|
| `kamino-core` | `Config`, `Mode` enum, `Profile`, error types, `Hasher` trait + xxHash, `Clock` trait, `MemberId`, time/duration helpers | std + `thiserror`, `tracing`, `xxhash-rust`, `serde` |
| `kamino-storage` | `StorageEngine` trait, `RamBlock` impl, `Entry` binary codec, `Table` state machine, `Fragment`, `Locker`, eviction (TTL/idle/LRU), compaction | `kamino-core`, `tokio`, `parking_lot`, `roaring`, `bytes` |
| `kamino-protocol` | RESP2/3 parser+encoder, command AST, `HELLO` negotiation type, wire error variants | `kamino-core`, `tokio`, `bytes`, `winnow` |
| `kamino-cluster` | SWIM (probe/indirect/suspect/dead), `Member`, `RoutingTable` (MessagePack with `schema_version`), `DiscoveryPlugin` trait + impls (Static/DNS default, K8s/Consul feature-gated), Coordinator, Balancer, Replication primitives, `DMapService`, `PubSubService`, inter-node `Forwarder` with pool + backpressure | `kamino-core`, `kamino-storage`, `kamino-protocol`, `kamino-observability`, `tokio`, `rmp-serde` |
| `kamino-observability` | Prometheus exposition, OTLP exporter (feature-gated), `MetricRegistry` trait that other crates instrument against, slow command log, HTTP `/healthz/*` and `/metrics` | `kamino-core`, `tokio`, `prometheus-client`, optional `opentelemetry-otlp` |
| `kamino-client` | `Client` trait, `RemoteClient` (TCP), `EmbeddedClient` (in-process wrapper around cluster services), `Pipeline`, `ScanCursor` impls (single-partition + cross-partition aggregation) | `kamino-core`, `kamino-cluster`, `kamino-protocol`, `tokio` |
| `kamino-server` | TCP listener, RESP framing, command dispatch, pub/sub mode state machine, AUTH/HELLO handling, SIGHUP-driven reload, server binary entry point | `kamino-cluster`, `kamino-protocol`, `kamino-observability`, `kamino-core`, `tokio` |
| `kamino` | Umbrella: `Kamino::embedded()` / `embedded_clustered()` / `serve()` builders, ergonomic re-exports | all of the above |
| `kamino-cli` | `kamino-cli drain`, `stats`, `scan`, `members`, `routing` admin commands | `kamino-client`, `kamino-protocol`, `clap` |

**Why discovery isn't its own crate.** Static + DNS impls are ~200 lines. K8s impl is ~400 lines but only requires `kube` + `k8s-openapi` behind a feature flag. Below the 500-line "earns its own crate" threshold; staying in `kamino-cluster` avoids a useless boundary.

**Why client + cluster aren't merged.** `EmbeddedClient` needs the cluster services in-process, but `RemoteClient` only needs the protocol. Splitting lets consumers of `RemoteClient` (small wire-protocol clients, language bindings later) avoid pulling SWIM and replication code.

---

## 4. Feature Flag Policy

Features are **additive only** — they enable optional implementations, never gate API surface differently.

| Crate | Feature | Default? | Pulls in |
|-------|---------|----------|----------|
| `kamino-cluster` | `discovery-static` | yes | nothing extra |
| `kamino-cluster` | `discovery-dns` | yes | `hickory-resolver` |
| `kamino-cluster` | `discovery-kubernetes` | no | `kube`, `k8s-openapi` |
| `kamino-cluster` | `discovery-consul` | no | `reqwest`, `consul-api` types |
| `kamino-observability` | `prometheus` | yes | `prometheus-client` |
| `kamino-observability` | `otlp` | no | `opentelemetry`, `opentelemetry-otlp`, `tonic` |
| `kamino` | `kubernetes` | no | `kamino-cluster/discovery-kubernetes` |
| `kamino` | `otlp` | no | `kamino-observability/otlp` |
| `kamino` | `full` | no | all of the above |

**Rule**: every feature added requires a justification in the PR description: *what optional dep does this gate, why is it optional.* No "convenience" features.

**Workspace dependency table** (`[workspace.dependencies]` in root `Cargo.toml`) pins every external crate version exactly once. Sub-crates use `tokio.workspace = true` form. Avoids version skew on `tokio`, `serde`, `tracing`.

---

## 5. Build, Test, Tooling

- **`cargo xtask`** for build automation. Subcommands: `xtask check-deps` (verifies dependency direction), `xtask schema-check` (validates the MessagePack `RoutingTable` schema for forward/backward compat), `xtask gen-manifests` (generates the K8s manifest examples in `docs/13-kubernetes.md` from a single source of truth), `xtask bench-report` (runs benches and writes a summary).
- **CI matrix**: stable Rust + MSRV (decide at Phase 0), Linux + macOS, default features + `--all-features` + `--no-default-features`.
- **Lints**: `clippy::pedantic` opt-in per crate (start strict in `kamino-core`, expand outward), `cargo deny` for license + advisory + duplicate-version enforcement.
- **Test layers**:
  - Unit tests in `#[cfg(test)]` modules (per `CLAUDE.md` convention).
  - Crate integration tests in each crate's `tests/`.
  - Workspace-level `tests/e2e/` driving a real binary.
  - `tests/turmoil/` for deterministic SWIM (cluster sizes 3, 5, 10, 50; injected partitions and packet loss).
  - `tests/jepsen/` for partition + LWW convergence (Docker-compose + iptables).
  - `tests/compat/` for rolling-upgrade wire compat smoke (old binary ↔ new binary).
- **Benches**: criterion in `benches/`, with hard P99 budgets that fail CI if regressed beyond a threshold.

---

## 6. Phased Delivery

Each phase lists: **goal**, **crates touched**, **surfaces delivered** (from the cross-check in §7), **acceptance**, **risks**.

### Phase 0 — Workspace skeleton (1 week)

**Goal**: a buildable empty workspace with the layout, CI, and tooling locked in.

**Crates touched**: all (skeleton only).

**Surfaces**:
- Virtual workspace `Cargo.toml`, `[workspace.dependencies]` table, MSRV decision.
- `kamino-core`: `Config` struct, `Mode` enum (variants empty), `Profile` enum, `Error` types with `thiserror`, `Hasher` trait with xxHash impl, `Clock` trait with system impl.
- `xtask` skeleton with `check-deps` subcommand.
- CI pipeline (build + test + clippy + cargo-deny).
- `tracing` setup with an opinionated default format.
- Lints policy committed to `clippy.toml`.

**Acceptance**: `cargo build && cargo test && cargo xtask check-deps` is green on a fresh clone.

**Risks**: MSRV creep. Pin and document; require explicit PR justification to bump.

---

### Phase 1 — Embedded solo cache (5–6 weeks)

**Goal**: a library that drops into a single binary as a local cache. `Mode::EmbeddedSolo` works.

**Crates touched**: `kamino-storage`, `kamino-core`, `kamino-client`, `kamino`.

**Surfaces** (from §7):
- `Entry` binary codec (29-byte header) with property-tested roundtrip.
- `Table` state machine: ReadWrite → ReadOnly → Recycled.
- `RamBlock` engine implementing `StorageEngine` (put/get/delete/scan/scan_regex_match/len/inuse/export/import).
- Compaction (`max_garbage_ratio`, `trigger_compaction_interval`).
- `Fragment` (`tokio::sync::RwLock<Box<dyn StorageEngine>>`).
- `Locker` with `parking_lot::Mutex<HashMap>` map + `tokio::sync::Mutex` `LockEntry`, cleanup via refcount.
- Eviction workers: TTL (probabilistic 20-sample), idle (`max_idle_duration`), LRU (`lru_samples`).
- Per-DMap config overrides.
- `DMap` ops: `put` (with `PutOptions` — ex/px/exat/pxat/nx/xx/ts), `get`, `delete`, `incr`, `decr`, `incr_by_float`, `get_put`, `expire`, `destroy`, `scan` (single-partition cursor).
- `Kamino::embedded()` builder.
- `Client` and `DMap` traits.
- Property tests for LWW timestamp monotonicity, eviction correctness, locker race-freedom.
- criterion benches for PUT/GET throughput and P99.

**Acceptance**:
1. A 100-line example program uses Kamino as a local cache.
2. Bench targets: ≥ 100k PUT/s and ≥ 250k GET/s on a single core, P99 ≤ 1 ms in steady state. Hard fail in CI if regressed.
3. Loom or `shuttle` test for the `Locker` cleanup race.

**Risks**:
- RamBlock perf is the foundation for everything. If it's slow, every later phase pays the tax. Mitigation: benches block the phase; perf regressions block PRs.
- Compaction pause measurement — must be sub-millisecond for typical table size.

---

### Phase 2 — Standalone single-node server (3–4 weeks)

**Goal**: `kamino-server` binary that speaks RESP to `redis-cli`. Single-node, no SWIM yet.

**Crates touched**: `kamino-protocol`, `kamino-server`, `kamino-client` (`RemoteClient`), `kamino-core` (TOML loader).

**Surfaces**:
- RESP2 parser+encoder, RESP3 push-frame upgrade via `HELLO 3`.
- Command dispatch framework.
- DM.* handlers for everything Phase 1 added: PUT/GET/DEL/EXPIRE/PEXPIRE/INCR/DECR/GETPUT/INCRBYFLOAT/DESTROY/SCAN.
- Utility commands: PING, AUTH, HELLO (basic; version-feature negotiation deferred to Phase 11), QUIT, STATS.
- TOML config loader, env-var overrides (`KAMINO_FOO__BAR` double-underscore), `Mode::Standalone` validation.
- `kamino-server` binary with `--config` flag.
- `RemoteClient`: connection, AUTH, basic commands.
- Per-connection state machine (auth state, RESP version, pub/sub mode reservation).
- `kamino-cli` first commands: `ping`, `stats`.

**Acceptance**:
1. `redis-cli -p 3320 DM.PUT sessions u1 hello` works.
2. RemoteClient passes the same DMap conformance suite Phase 1 wrote.
3. TOML round-trip: load → modify → save preserves field order and comments where reasonable.

**Risks**:
- RESP edge cases (inline vs multi-bulk, binary-safe values, RESP3 map/set/push). Mitigation: fuzz the parser with `cargo-fuzz`.

---

### Phase 3 — SWIM cluster membership (4–5 weeks)

**Goal**: multi-node cluster that knows about itself. No data replication, no routing yet.

**Crates touched**: `kamino-cluster` (SWIM module), `kamino-server` (CLUSTER.MEMBERS), `kamino-core` (Member + tiebreaker).

**Surfaces**:
- `Member` struct (id, name, addr, discovery_addr, birthdate, is_coordinator).
- Member ID generation (random u64 at boot).
- Coordinator selection: `(birthdate ASC, id ASC)`, index 0.
- SWIM probe/indirect-probe/suspect/dead state machine.
- Gossip dissemination (piggyback on protocol messages).
- Configurable: `probe_interval`, `probe_timeout`, `indirect_probes`, `suspicion_multiplier`.
- Static peer + DNS discovery plugins; `DiscoveryPlugin` trait.
- Join sequence with `max_join_attempts`, `join_retry_interval`, `bootstrap_timeout`.
- Graceful leave (broadcast + `leave_timeout`).
- Automatic coordinator failover on death.
- `CLUSTER.MEMBERS` command.

**Acceptance**:
1. `tests/turmoil/` simulation: 5-node cluster converges within 3 probe intervals after a node death.
2. Cluster sizes 3, 5, 10, 50 all reach steady state under default config; metrics captured.
3. Coordinator agreement under simultaneous-birthdate stress test (deterministic seed).

**Risks**:
- Suspicion threshold tuning. Mitigation: parameterize and measure under `turmoil` with packet loss 0%, 1%, 5%.
- Indirect-probe peer selection bias. Mitigation: deterministic randomness in tests + statistical assertions.

---

### Phase 4 — Consistent hashing + routing (3–4 weeks)

**Goal**: multi-node cluster routes requests by hash. Still no replication; primary-only.

**Crates touched**: `kamino-cluster` (hash ring + routing + forwarder), `kamino-protocol` (`MOVED` error, internal commands), `kamino-server` (forward path), `kamino-client` (`MOVED` retry).

**Surfaces**:
- Bounded-load consistent hash ring (Mirrokni 2016), `virtual_nodes_per_member`, `load_factor`.
- `RoutingTable` struct with `schema_version: u16`, `signature: u64`, primary + backup maps, sorted members.
- MessagePack named-map encoding (per `docs/15-compatibility.md` discipline).
- Signature versioning + stale-table rejection.
- `INTERNAL.NODE.UPDATEROUTING` and `INTERNAL.NODE.LENGTHOFPART` commands.
- Periodic routing-table push (`routing.push_interval`), parallel push bounded by CPU count.
- Inter-node `Forwarder`: per-peer pool (`internode_pool_size`), pipelined connections, in-flight cap (`internode_inflight_per_conn`), TCP-backpressure-aware, timeouts (`internode_connect_timeout`, `internode_request_timeout`), exponential reconnect backoff.
- Forward failure handling: `ErrServerGone`, `ErrTimeout`, `ErrMoved` (single retry after refresh).
- `MOVED` error returned by server when routing is stale; client refreshes and retries.
- Cross-partition `DM.DEL` fan-out semantics (`docs/06-network-protocol.md`).
- `multi_key_strict` config knob.
- `CLUSTER.ROUTINGTABLE`, `CLUSTER.READY` commands.
- `kamino-client` cross-partition `Pipeline::execute`.
- `kamino-client` cross-partition `ScanCursor` aggregator.
- Inter-node `cluster_secret` enforcement at handshake.

**Acceptance**:
1. 3-node cluster: 1k keys distributed roughly evenly (within load_factor bounds).
2. Kill node, routing table converges, client retries succeed.
3. Fan-out `DM.DEL` across 5 partitions returns correct count under partial failure.
4. Forwarder load test: per-peer backpressure engages under controlled slowness, no head-of-line blocking for unrelated clients.
5. `xtask schema-check` validates routing-table MessagePack adds-only.

**Risks**:
- Dual-coordinator window during SWIM convergence. Mitigation: property test the `signature` clock under random ordering; explicit acceptance test for the transient case described in `docs/03-cluster-management.md`.
- Forwarder memory growth under sustained slow peer. Mitigation: bounded queue is mandatory; load test asserts a memory ceiling.

---

### Phase 5 — Replication (4 weeks)

**Goal**: writes survive a node loss. `replica_count >= 2` works end to end.

**Crates touched**: `kamino-cluster` (replicator), `kamino-storage` (entry timestamp source), `kamino-server` (quorum check at op boundary).

**Surfaces**:
- Backup owner selection: `get_closest_n_for_partition(part, replica_count)` skipping primary.
- Synchronous replication path; `write_quorum` enforcement.
- Asynchronous replication path (fire-and-forget).
- `read_quorum` > 1 path: read from primary + backups, return highest-timestamp.
- Read repair (`read_repair = true`): propagate winning version to stale replicas.
- LWW merge function with primary-assigned timestamp (simplified HLC: `max(prev_ts + 1, wall_time)`).
- `member_count_quorum` check before every DMap op; returns `ErrClusterQuorum`.
- `PutOptions.timestamp` client override path.

**Acceptance**:
1. `tests/jepsen/`: kill a primary, reads served from backup, no data loss.
2. Property test: under random concurrent writes + reads, LWW convergence reached after partition heal.
3. Quorum stress: `member_count_quorum = 2` cluster of 3 nodes correctly rejects writes when 2 nodes are unreachable.
4. Bench: sync replication adds at most 1× RTT_p99 over single-node PUT latency.

**Risks**:
- Cross-primary clock skew (LWW silent loss). Documented and accepted in `docs/04-replication.md`. Mitigation: jepsen test must demonstrate the failure mode, not paper over it — the test asserts *which* write is preserved deterministically, not that no write is lost.

---

### Phase 6 — Rebalancer and anti-entropy (3 weeks)

**Goal**: topology changes move data correctly; fragmented partitions resolve.

**Crates touched**: `kamino-cluster` (balancer + fragment migration), `kamino-protocol` (`INTERNAL.NODE.MOVEFRAGMENT`).

**Surfaces**:
- Balancer periodic loop (`balancer.trigger_interval`).
- `FragmentPack` (partition_id, partition_type, dmap_name, payload).
- `INTERNAL.NODE.MOVEFRAGMENT` command.
- Storage `export`/`import` with LWW merge.
- Fragmented-partition read path: new primary first, fallback to previous owners.
- `LeftOverDataReport` exchanged on routing-table push; coordinator directs migrations.
- Empty-fragment cleanup (`check_empty_fragments_interval`).
- Read-repair integration with previous-owner list.
- Cluster event publishing (`fragment-migration`, `fragment-received`).

**Acceptance**:
1. Add a 4th node to a 3-node cluster; ~25% of partitions migrate, no data lost.
2. Remove a node mid-write; reads continue serving from backups, balancer reconciles within 2 cycles.
3. Property test: under random join/leave sequences, eventual convergence to a balanced state.

**Risks**:
- Migration ordering vs concurrent writes. Mitigation: writes during migration always go to the new primary; reads merge with LWW.

---

### Phase 7 — Pub/Sub (2 weeks, parallelizable with 5–6)

**Goal**: cluster-wide pub/sub messaging.

**Crates touched**: `kamino-cluster` (`PubSubService`), `kamino-server` (subscribe state machine), `kamino-client` (`PubSub` trait).

**Surfaces**:
- Subscription registry: `BTreeMap<(bool /* is_pattern */, String, u64), Subscriber>`.
- SUBSCRIBE / UNSUBSCRIBE (exact channels).
- PSUBSCRIBE / PUNSUBSCRIBE (glob patterns: `*`, `?`, `[abc]`, `[^abc]`).
- PUBLISH with cluster-wide forward.
- PUBSUB CHANNELS / NUMSUB / NUMPAT.
- Pub/sub mode restrictions (allowed-command set on subscribed connections).
- Message and pmessage RESP frame shapes.
- RESP3 push frames when `HELLO 3` negotiated.
- `cluster.events` channel (gated on `enable_cluster_events_channel`): node-join, node-left, fragment-migration, fragment-received.
- At-most-once delivery; documented limitations enforced by absence of retry.

**Acceptance**:
1. 3-node cluster, publish on node A, both subscribers (on B and C) receive within 10ms P99 under no load.
2. Pattern subscription correctness suite (every glob edge case).

**Risks**:
- O(N²) broadcast at large N. Out of scope for the v1; recorded as a known limit in `docs/11-pubsub.md`.

---

### Phase 8 — Distributed locks (1 week, parallelizable with 5–7)

**Goal**: `DM.LOCK` works both variants.

**Crates touched**: `kamino-cluster` (uses existing DMap NX semantics), `kamino-server`, `kamino-client`.

**Surfaces**:
- `DM.LOCK <dmap> <key> <deadline_ms> [timeout_ms]` (both variants).
- `DM.UNLOCK <dmap> <key> <token>` with byte-equal token check.
- `DM.LOCKLEASE` / `DM.PLOCKLEASE`.
- Random 16-byte token.
- `LockContext` with `unlock()` and `lease()`.
- 10ms retry loop honoring deadline.
- `lock(key, deadline)` flagged as unsafe in API docs (already done).

**Acceptance**:
1. Concurrent acquisition test: 100 clients race for one key, exactly one wins, others see `ErrLockNotAcquired` after deadline.
2. Crash test: holder dies, lease-expiry variant releases on schedule; no-lease variant remains held (correct per spec).

---

### Phase 9 — Discovery plugins + Kubernetes (2 weeks, parallelizable with 5–8)

**Goal**: K8s deployment works end to end.

**Crates touched**: `kamino-cluster` (k8s discovery feature), `kamino` (feature wiring), `docs/13-kubernetes.md` (manifest examples generated by `xtask gen-manifests`).

**Surfaces**:
- `DiscoveryPlugin` async trait.
- Kubernetes Endpoints API plugin reading both `addresses` and `notReadyAddresses` (gated by `include_not_ready`).
- In-cluster config: service account token + API server env vars.
- Namespace resolution order: env var → config → `/var/run/secrets/.../namespace`.
- RBAC manifest (Role + RoleBinding + ServiceAccount): minimal `get` on `endpoints`.
- StatefulSet manifest with `podManagementPolicy: Parallel`.
- Headless Service manifest with `publishNotReadyAddresses: true`, TCP + UDP for 3322.
- `xtask gen-manifests` produces the example YAML in `docs/` from one source.
- Consul discovery (optional `consul` feature) — stub for now, full impl in a later iteration.

**Acceptance**:
1. `kubectl apply -f` against a kind cluster: 3 pods form a cluster, `CLUSTER.READY` passes within 10s.
2. Scale to 5, then 2; cluster stays consistent through both transitions.
3. RBAC: deliberately remove the Role, pods fail discovery with a clear error.

**Risks**:
- First-pod bootstrap deadlock if any of the three required settings is missing. Mitigation: explicit acceptance test for member_count_quorum=2 fresh-bootstrap case.

---

### Phase 10 — Observability (2 weeks + ongoing, instrumented from Phase 2 onward)

**Goal**: every metric and trace in `docs/14-observability.md` is wired.

**Crates touched**: `kamino-observability`, every other crate (instrumentation call sites).

**Surfaces**:
- `MetricRegistry` trait in `kamino-observability` that other crates call into.
- Prometheus exposition at `:9128/metrics`.
- HTTP `/healthz/live` (process responsive) and `/healthz/ready` (same as `CLUSTER.READY`).
- OTLP gRPC exporter (feature `otlp`), `otlp_endpoint`, `otlp_interval`, `service_name`.
- All metrics from §7's catalog: RESP, SWIM, routing, replication, balancer, storage, internode. **Cardinality reviewed in PR.**
- W3C `traceparent` propagation via `HELLO` extension and `DM.TRACECTX` command.
- Span hierarchy: client.command → routing.lookup → forward.peer → storage.read|write / replication.backup.
- Parent-based 1% head sampling default.
- Slow command log: JSON-lines, `slow_command_threshold_ms`, optional `cluster.slowlog` pub/sub channel.
- Slow log entry shape (ts, command, dmap, key_hash, partition, duration_us, stages, peer).

**Acceptance**:
1. Grafana dashboard renders RED metrics for DM.PUT/DM.GET correctly.
2. Trace from `redis-cli` ends up in Jaeger with the correct span tree across 3 nodes.
3. Slow log captures a 200ms artificial slowness; no other commands logged.

**Risks**:
- Cardinality explosion. Mitigation: enforced label whitelist; `xtask check-metrics` validates no per-key labels.

---

### Phase 11 — Production hardening (3–4 weeks)

**Goal**: every guarantee `docs/15-compatibility.md` and `docs/16-config-architecture.md` makes is real.

**Crates touched**: `kamino-server` (SIGHUP), `kamino-cluster` (HELLO features, schema check), `kamino-core` (Profile guards, reload diff), `tests/compat/`, `tests/jepsen/`.

**Surfaces**:
- `HELLO` version negotiation with `kamino_protocol` integer and `features` array.
- `INTERNAL.NODE.*` compatibility checks (sender gates new commands on receiver features).
- `RoutingTable` schema_version enforcement: same MAJOR → tolerant decode; different MAJOR → reject.
- `xtask schema-check`: routing-table MessagePack schema is append-only since the last release.
- Reload pipeline: SIGHUP (standalone) and `Kamino::reload_config()` (embedded) with `ReloadReport`. All-or-nothing on bootstrap-only changes.
- `Profile::Production` hard guards: `ErrProductionReplicaCount`, `ErrProductionQuorum`.
- TOML irrelevant-section error: `ErrIrrelevantSection { section, mode }`.
- Cross-version smoke (`tests/compat/`): release N-1 binary forms a cluster with release N binary; basic DM operations succeed.
- Jepsen-style suite expanded: network partition + clock skew + LWW convergence assertion; minority-quorum write rejection.
- Bench suite at scale: 3, 5, 10, 50 nodes on `m5.large`-class instances (or equivalent local sim); P99 budgets locked in.
- Failure-injection in `turmoil`: packet drop, reorder, delay; assert SWIM still converges.
- `deprecated_use_total{name}` metric and one-per-minute throttled log.

**Acceptance**:
1. Rolling upgrade smoke: pod-by-pod restart on a new MINOR, no client errors over the upgrade window.
2. Production profile rejects single-replica config with a clear error, not a warning.
3. SIGHUP changes `slow_command_threshold_ms` live; SIGHUP attempts to change `partition_count` and gets a clean `ErrBootstrapImmutable`.
4. `xtask schema-check` is part of release CI; release blocked if the routing-table schema is backwards-incompatible.

**Risks**:
- Compatibility tests are expensive to maintain. Mitigation: pin them to MAJOR boundaries; PATCH/MINOR don't need exhaustive cross-version coverage.

---

## 7. Cross-Reference: docs → phases

This table makes sure every implementable surface from `docs/` lands somewhere. Audited against the exhaustive checklist produced from `docs/00`–`docs/16`.

| Doc | Major surfaces | Phase |
|-----|----------------|-------|
| `00-overview.md` | DMap / Pub-Sub / Locks data structures; SWIM + bounded hashing + RESP pillars | 1, 3, 4, 7, 8 |
| `01-architecture.md` | KaminoNode components; coordinator + tiebreaker; write/read path; partition layout; embedded vs standalone | 0 (Config/Mode), 1, 2, 3, 4, 5 |
| `02-consistent-hashing.md` | Bounded-load ring, virtual nodes, xxHash, partition mapping, routing table struct + signature, client routing, MOVED retry | 4 |
| `02 immutability** of partition_count | enforced at `Config::build()`; documented warning | 0 (validation), 1 (no migrate path) |
| `03-cluster-management.md` | SWIM 3-phase, gossip, ports, discovery (Static/DNS/K8s/Plugin trait), join + leave, coordinator + failover, signature limitation, member fields, cluster events | 3, 7 (events), 9 (K8s) |
| `04-replication.md` | Primary-backup, sync/async, write/read quorum, read repair, member_count_quorum, LWW + HLC, timestamp source, clock-skew failure mode, INCR lost-update | 5, 11 (Jepsen) |
| `05-storage-engine.md` | RamBlock, Entry format, Table state machine, eviction (TTL/idle/LRU), compaction, StorageEngine trait, per-DMap config, limitations | 1 |
| `06-network-protocol.md` | RESP2/3, HELLO, AUTH, cluster_secret, no-TLS warning, full DM.* set, multi-key fan-out + `multi_key_strict`, pub/sub commands, cluster commands, internal commands, inter-node pool + backpressure + failure handling, metrics list | 2, 4, 7, 10 |
| `07-concurrency.md` | Lock hierarchy, RoutingTable RwLock, DMap registry RwLock, Fragment RwLock, Locker (parking_lot + tokio), sync-vs-async discipline, deadlock prevention | 1, 4 |
| `08-api-design.md` | Client trait, DMap trait (all ops), PutOptions, GetResponse, LockContext, ScanCursor, Pipeline + options, PubSub trait, Subscription, Error enum, examples | 1, 2 (Client trait), 4 (Pipeline cross-partition), 7, 8 |
| `09-configuration.md` | All knobs and defaults; Reload column; production-recommended defaults | 0 (Config), 2 (TOML loader), 11 (Profile hard guards, reload) |
| `10-distributed-locking.md` | `lock_with_timeout` vs `lock`, NX algorithm, token comparison, lease, partition routing, failure scenarios | 8 |
| `11-pubsub.md` | Subscription registry, exact + pattern, propagation, commands, mode restrictions, RESP frames, cluster.events, delivery + ordering caveats | 7 |
| `12-failure-handling.md` | Detection, routing rebuild, backup promotion, replica_count=1 loss, balancer, fragmented partitions, ownership transfer, merge, split-brain, anti-entropy mechanisms | 3, 5, 6 |
| `13-kubernetes.md` | StatefulSet, headless Service, K8s discovery, `publishNotReadyAddresses`, `podManagementPolicy: Parallel`, `include_not_ready`, RBAC, probes, lifecycle, scaling | 9 |
| `14-observability.md` | Prometheus + OTLP exposition, metric catalog (RESP/SWIM/routing/replication/balancer/storage/internode), traces with traceparent, slow log, healthz | 10 |
| `15-compatibility.md` | SemVer at cluster level, HELLO negotiation + features, routing-table schema discipline, internal command compat, rolling upgrade, deprecation policy | 11 |
| `16-config-architecture.md` | Config + Mode enum, source precedence, env-var encoding, profile defaults, reload discipline (bootstrap-only vs reloadable), validation rules, Kamino::embedded() builder | 0, 2, 11 |

Any item from `docs/` not anchored here is a plan defect — file an issue and we'll fix the roadmap.

---

## 8. Critical Path and Parallelism

```
0 ──► 1 ──► 2 ──► 3 ──► 4 ──► 5 ──► 6 ──► 11
                          │
                          ├──► 7 (parallel with 5–8)
                          │
                          ├──► 8 (parallel with 5–7)
                          │
                          └──► 9 (parallel with 5–10)
                          
10 starts at Phase 2 (instrumentation as code is written) and finishes alongside 11.
```

**Critical-path duration**: ~30 weeks for a single-developer scenario. With 2–3 developers, ~24 weeks because 7, 8, 9 can move in parallel once 4 ships.

**Best leverage points**: instrument observability from Phase 2 (not Phase 10), and start `tests/turmoil/` and `tests/jepsen/` skeletons in Phase 3 so we have somewhere to land tests as features land.

---

## 9. Risks and Validation Strategy

| Risk | Phase | Mitigation |
|------|-------|------------|
| RamBlock perf insufficient | 1 | Benches with hard P99 budgets gate CI from Phase 1 onward |
| SWIM convergence pathology | 3 | `turmoil` deterministic sim with packet loss, reorder, partition |
| Transient dual-coordinator | 3, 4 | Property test signature-clock under random ordering; explicit transient case |
| Forward-path memory growth under slow peer | 4 | Bounded queue mandatory; load test asserts ceiling |
| LWW silent data loss | 5 | Jepsen test asserts *deterministic* winner, not "no loss" |
| Routing-table schema break across versions | 4, 11 | `xtask schema-check` in release CI |
| K8s bootstrap deadlock with quorum > 1 | 9 | Explicit acceptance test for fresh-bootstrap with `member_count_quorum=2` |
| Cardinality explosion in metrics | 10 | Label whitelist + `xtask check-metrics` |
| Compatibility regression in rolling upgrade | 11 | `tests/compat/` runs N-1 ↔ N smoke on every release branch |

**Continuous validation**:
- Every phase delivers a working binary or library; no integration debt accumulates.
- Bench budgets are hard. Regression beyond threshold blocks merge.
- `tests/turmoil/` and `tests/jepsen/` skeletons exist by Phase 3 even if empty — Phase N adds the relevant scenarios.

---

## 10. What This Plan Doesn't Cover

- **Disk-backed storage engines.** RocksDB / LMDB impls become separate crates when there's a documented use case. Not in v1.
- **CRDT counters.** A PN-Counter would solve the INCR lost-update problem under partition (per `docs/04-replication.md`). Noted as future work; v1 ships the LWW caveat.
- **Cross-DC replication.** Single-DC v1; cross-DC requires WAN-aware quorum + bounded HLC skew handling.
- **Authentication beyond password + cluster_secret.** mTLS, OIDC, SASL — proxy concerns for v1 per `docs/06-network-protocol.md`.
- **Persistence / WAL.** Out of scope (`docs/05-storage-engine.md` explicit).
- **Hot partition migration acceleration.** Today's balancer migrates whole fragments. Streaming migration is a future optimization.

These deliberately stay out of v1 so the critical path is finite. Each is a candidate for its own milestone post-v1.
