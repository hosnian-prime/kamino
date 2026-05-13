# Wire Compatibility and Rolling Upgrades

## Overview

Kamino is a distributed system: at any moment during a rolling upgrade, mixed versions are talking to each other over the wire. This document defines the compatibility contract that makes that safe.

The constraint is real, not theoretical. The routing table is serialized with MessagePack — a format that, by its own maintainers' admission, does not natively solve schema evolution (msgpack/msgpack#142). The internal command set (`INTERNAL.NODE.*`) is part of the public surface as far as a rolling cluster is concerned. Without explicit rules, every release risks silent data corruption.

## Versioning Scheme

Kamino follows semantic versioning at the **cluster compatibility** level (not the API level):

| Bump | Meaning | Cross-version traffic |
|------|---------|------------------------|
| **PATCH** (1.4.2 → 1.4.3) | Bug fixes only. No wire change. | Always safe. |
| **MINOR** (1.4 → 1.5) | Additive wire changes (new commands, new optional fields). | Safe: old reads new, new reads old. |
| **MAJOR** (1.x → 2.x) | Breaking wire change. | **Not safe.** Requires a planned migration. |

A node refuses to join a cluster whose advertised version differs in the MAJOR component. MINOR/PATCH differences are accepted.

## HELLO Negotiation

`HELLO` is extended to carry Kamino's protocol version alongside RESP's:

```
HELLO 2|3 [AUTH user pass] [SETNAME name] [KAMINO version]
```

Server reply (RESP3 map):

```
server: "kamino"
version: "1.5.2"
kamino_protocol: 3              ; bumped on every MAJOR
features: ["read_repair", "otlp", "multi_key_strict"]
```

- **Same `kamino_protocol`** → handshake succeeds, full command surface available.
- **Different `kamino_protocol`** → server replies with `ErrVersionMismatch`. The internal RESP client refuses to forward to peers it cannot speak to.
- **`features`** is a forward-compatibility hint. Old clients ignore unknown features; new clients gate optional behavior on the flag.

## Routing Table Schema Evolution

The routing table crosses the wire on every coordinator push. Its MessagePack encoding follows a strict discipline:

1. **Named-map encoding only.** Every field is keyed by name, never by positional index. New fields can be added; old fields can be deprecated but never removed within a MAJOR.
2. **All fields are optional on decode.** Missing fields take their default value (zero-equivalent for the type). This is what makes additive changes safe.
3. **The first field is `schema_version: u16`.** Decoders inspect it before reading the rest. A decoder that sees a `schema_version` it does not recognize within its MAJOR logs a warning and continues with best-effort decoding; a decoder that sees a `schema_version` from a different MAJOR aborts.
4. **No field type ever changes.** A `u64` stays a `u64`. A new representation requires a new field name and a deprecation cycle.
5. **No field name is ever reused.** Once `member_count_quorum: u32` exists, that name is reserved forever, even if removed.

```rust
#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields = false)]  // tolerant decode
pub struct RoutingTable {
    schema_version: u16,            // 1 in MAJOR 1.x
    signature: u64,
    members: Vec<Member>,
    primary: HashMap<u32, Vec<Member>>,
    backup: HashMap<u32, Vec<Member>>,
    // New optional fields added here in MINOR releases, never reordered.
}
```

Unknown fields are silently dropped on decode; this is the cost of MessagePack's permissive nature, and it is the correct trade for forward compatibility.

## Internal Command Compatibility

`INTERNAL.NODE.*` commands are part of the wire contract:

| Rule | Rationale |
|------|-----------|
| New commands may be added in any MINOR. | Older peers respond with `ErrUnknownCommand`; the sender treats it as "feature not yet rolled out" and falls back. |
| Argument lists are extensible *at the tail* only. | Older parsers stop reading after the arguments they know about; trailing arguments are ignored. |
| Reply shapes are additive. | A new field in a reply is an optional MessagePack key; old clients ignore it. |
| Removing a command requires a MAJOR. | And a deprecation window of at least one MINOR. |

The `features` array from `HELLO` is the gate for new commands: a node only sends `INTERNAL.NODE.NEW_THING` to peers that advertise it.

## Rolling Upgrade Procedure

### MINOR or PATCH (the normal case)

```
For each pod in the StatefulSet (highest ordinal first):
  1. kubectl delete pod kamino-N
     → SIGTERM → graceful leave → terminationGracePeriodSeconds expires → SIGKILL
  2. K8s restarts the pod with the new image.
  3. New pod joins via SWIM; coordinator pushes routing table.
  4. CLUSTER.READY passes; K8s moves on to kamino-(N-1).
```

Mixed-version traffic flows the whole time. Pre-flight checklist:

- `leave_timeout` ≥ time needed to migrate this pod's data.
- `terminationGracePeriodSeconds` ≥ `leave_timeout + 5s`.
- `member_count_quorum` is set such that draining one pod still satisfies it.
- The new version's release notes do not flag any field renames or wire-format changes (those should not happen in a MINOR; if release notes flag them, treat it as a MAJOR even if the version number says otherwise).

### MAJOR

A MAJOR bump means new and old nodes cannot talk. Options:

1. **Blue-green cutover.** Stand up a second cluster on the new MAJOR. Backfill from the old cluster via an admin-grade scan (per-partition `DM.SCAN`). Switch clients. Tear down the old cluster.
2. **Stop-the-world upgrade.** Drain all clients, shut down every pod, restart on the new MAJOR. Acceptable for caches that can tolerate a brief gap and a cold start.

There is no in-place rolling MAJOR upgrade. Documentation lists each MAJOR's migration path with the release.

## Deprecation Policy

A deprecated command, field, or config option:

1. Stays functional for the rest of its current MAJOR.
2. Logs a warning at startup (config) or on use (command), throttled to one per minute per node.
3. Increments `kamino_deprecated_use_total{name}` so dashboards can surface it.
4. Is removed in the next MAJOR.

The deprecation log line names the replacement. If there is no replacement, the deprecation is upgraded to a hard removal in the next MAJOR — incremental deprecation of features with no migration path is a worse experience than a clean break.

## Client Library Compatibility

The wire compatibility rules above apply equally to client libraries:

- A client built for Kamino MAJOR `N` works with any server in MAJOR `N`, regardless of MINOR/PATCH.
- A client built for MAJOR `N-1` works against MAJOR `N` **only** for commands present in both. New commands are gated on the `HELLO` `features` array.
- A client built for MAJOR `N` does not work against a MAJOR `N-1` server if it uses commands introduced in `N`.

Embedded mode (compile-time linkage of the library) is out of scope for wire compatibility; the API contract is the standard Rust SemVer one.

## What This Document Is Not

- It is not a stability guarantee for unreleased versions. Pre-1.0 releases may break wire compatibility on MINOR bumps; the rules above kick in at 1.0.
- It is not a substitute for release notes. Every release MUST enumerate added fields, added commands, deprecated items, and any quirks that require operator attention.
