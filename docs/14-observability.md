# Observability

## Overview

Kamino exposes three observability surfaces, each addressing a different operator question:

- **Metrics** (Prometheus + OTLP) — *Is the cluster healthy? Where is the bottleneck?*
- **Tracing** (OpenTelemetry) — *What happened to this specific request?*
- **Slow command log** — *Which queries are the long-tail latency cost?*

The cluster-events pub/sub channel ([Pub/Sub](11-pubsub.md#cluster-event-channel)) is **not** an observability surface — it is best-effort and unsuitable for SLO measurement. Pair it with a poll loop or, better, consume the metrics below.

## Metrics

### Exposition

| Endpoint | Default | Format | Purpose |
|----------|---------|--------|---------|
| `:9128/metrics` | enabled | Prometheus text exposition | Scrape via Prometheus / Grafana Agent / OpenTelemetry Collector |
| OTLP/gRPC `:4317` | disabled (opt-in) | OpenTelemetry Protocol | Push to vendor backends without an intermediate scraper |

```toml
[observability]
prometheus_addr = "0.0.0.0:9128"
otlp_endpoint = ""               # e.g. "http://otel-collector:4317" to enable OTLP push
otlp_interval = "10s"
service_name = "kamino"          # populates `service.name` resource attribute
```

The Prometheus endpoint is bound to a separate port from the RESP server so scrape traffic is isolated from data-plane traffic — a noisy scraper cannot stall client requests.

### Metric Design Principles

- **RED at the surface, USE underneath.** Client-facing latencies follow the Rate/Error/Duration model. Resource utilization (memory, queue depth, CPU per stage) follows Utilization/Saturation/Errors.
- **Cardinality is bounded.** Per-command labels (~25 commands) and per-partition labels (271) are allowed. Per-key labels are forbidden. Per-peer labels are bounded by cluster size.
- **Histograms over averages.** Latency metrics are histograms with explicit bucket boundaries tuned to cache workloads (`[0.1, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000] ms`).
- **No metric is added without a question it answers.** Vanity counters get dropped at review.

### Metric Catalog

#### RESP server (RED)

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `kamino_commands_total` | counter | `command`, `status` | Throughput and error rate per command |
| `kamino_command_duration_seconds` | histogram | `command` | Per-command latency distribution |
| `kamino_connections_total` | counter | — | Lifetime accept count |
| `kamino_connections_active` | gauge | — | Currently open connections |
| `kamino_bytes_read_total` | counter | — | Inbound bytes from clients |
| `kamino_bytes_written_total` | counter | — | Outbound bytes to clients |

#### Cluster membership (SWIM)

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `kamino_swim_members` | gauge | `state` ∈ {alive, suspect, dead} | Membership health |
| `kamino_swim_probes_total` | counter | `result` ∈ {ack, timeout} | Direct probe success rate |
| `kamino_swim_indirect_probes_total` | counter | `result` ∈ {ack, timeout} | Indirect probe success rate |
| `kamino_swim_gossip_msgs_sent_total` | counter | — | Gossip message volume |
| `kamino_swim_gossip_msgs_received_total` | counter | — | Gossip message volume |
| `kamino_swim_suspicion_duration_seconds` | histogram | — | Time in suspect state before alive/dead resolution |

A rising `kamino_swim_members{state="suspect"}` with low `dead` is the canonical signal of a network flap.

#### Routing table

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `kamino_routing_signature` | gauge | — | Current routing table signature (monotonic) |
| `kamino_routing_age_seconds` | gauge | — | Seconds since last accepted routing table push |
| `kamino_routing_pushes_total` | counter | `result` | Coordinator push attempts |
| `kamino_routing_rejected_total` | counter | `reason` ∈ {stale_signature, auth} | Pushes rejected by this node |

A flat `kamino_routing_signature` for many minutes on a multi-node cluster suggests the coordinator is wedged.

#### Replication

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `kamino_replication_duration_seconds` | histogram | `mode` ∈ {sync, async}, `result` | Backup acknowledge latency |
| `kamino_replication_quorum_failures_total` | counter | — | Writes that missed `write_quorum` |
| `kamino_replication_lag_bytes` | gauge | `partition` | Unreplicated bytes (async mode only) |

#### Balancer & migration

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `kamino_balancer_migrations_total` | counter | `result` ∈ {ok, conflict, error} | Fragment migration attempts |
| `kamino_balancer_migration_bytes_total` | counter | — | Bytes shipped between nodes |
| `kamino_balancer_pending_migrations` | gauge | — | Queue depth |
| `kamino_balancer_run_duration_seconds` | histogram | — | Time per balancer cycle |

#### Storage engine

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `kamino_storage_tables` | gauge | `state` ∈ {read_write, read_only, recycled} | RamBlock table counts |
| `kamino_storage_inuse_bytes` | gauge | `dmap` | Per-DMap live data size |
| `kamino_storage_keys` | gauge | `dmap` | Per-DMap live key count |
| `kamino_storage_garbage_ratio` | gauge | `dmap` | Compaction trigger signal |
| `kamino_storage_compactions_total` | counter | `result` | Compaction outcomes |
| `kamino_storage_evictions_total` | counter | `policy` ∈ {ttl, idle, lru} | Why entries were dropped |

#### Inter-node forward path

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `kamino_internode_inflight` | gauge | `peer` | Per-peer in-flight RPC count |
| `kamino_internode_pool_size` | gauge | `peer` | Per-peer open connections |
| `kamino_internode_forward_duration_seconds` | histogram | `peer` | Forwarded RPC latency |
| `kamino_internode_forward_errors_total` | counter | `peer`, `reason` ∈ {timeout, conn_error, moved} | Forward failure attribution |

A persistent rise in `kamino_internode_inflight{peer="X"}` to the cap is the signal that the per-peer backpressure (see [Network Protocol](06-network-protocol.md#backpressure)) is engaged.

## Tracing

Kamino emits OpenTelemetry traces when `otlp_endpoint` is configured. Span hierarchy:

```
client.command (root, command + dmap as attributes)
├── routing.lookup
├── forward.peer (if not owner)         ← span per hop
│   └── ... (continues on the receiving node)
├── storage.read | storage.write
└── replication.backup (per backup)     ← parallel spans, one per backup
```

Trace propagation uses the W3C `traceparent` field. Clients pass it via the RESP `HELLO` extension (see [Compatibility](15-compatibility.md#hello-negotiation)) or via an explicit `DM.TRACECTX` command. The internal RESP client always forwards `traceparent` so a single request's spans stitch across nodes.

Sampling defaults to **parent-based, 1% head sampling at the client edge.** Internal spans (forward, replication) inherit the sampling decision. Operators can override via `otlp_sampler` in config.

## Slow Command Log

The slow command log captures any command whose total server-side time exceeds `slow_command_threshold_ms` (default `100`).

```toml
[observability]
slow_command_threshold_ms = 100
slow_command_log_path = "/var/log/kamino/slowlog.json"  # JSON-lines; rotate with logrotate
slow_command_channel = false                            # also publish to `cluster.slowlog` pub/sub
```

Entry format:

```json
{
  "ts": "2026-05-13T04:35:01.234Z",
  "command": "DM.PUT",
  "dmap": "sessions",
  "key_hash": "0x7A3F...",
  "partition": 142,
  "duration_us": 187_432,
  "stages": {"route": 12, "forward": 4_211, "replication": 182_900, "storage": 309},
  "peer": "10.0.1.3:3320"
}
```

Keys themselves are never logged — they may carry PII. `key_hash` is the same hash the routing layer used, sufficient to correlate with metrics and traces.

## Health Checks

| Check | Endpoint | Use |
|-------|----------|-----|
| `CLUSTER.READY` (RESP) | RESP `:3320` | Kubernetes readiness — gates client traffic |
| `PING` (RESP) | RESP `:3320` | Liveness — process is responsive |
| `GET /healthz/live` (HTTP) | `:9128/healthz/live` | Liveness over HTTP for environments that prefer it |
| `GET /healthz/ready` (HTTP) | `:9128/healthz/ready` | Readiness over HTTP — same logic as `CLUSTER.READY` |

The HTTP endpoints share the Prometheus port so a single port-forward is enough for incident response.

## What Kamino Does NOT Emit (and Why)

| Telemetry | Why not |
|-----------|---------|
| Per-key counters | Cardinality explosion. Use the slow log for per-request investigation. |
| Per-client-IP labels | Cardinality, and clients are often NATed behind proxies. Use connection logs instead. |
| Audit log of every command | Volume and PII risk. Slow log + sampled tracing covers the same forensic ground. |
| OS-level metrics (CPU, RSS, fd count) | Out of scope — that is `node_exporter`'s job; do not re-implement it. |
