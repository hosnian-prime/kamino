# tests/jepsen/

Workspace-level Jepsen-style fault injection harness (per `ROADMAP.md` §5
"Test layers" and §9 "Risks").

Scope (lands in Phase 5 — replication):

- **LWW convergence**: assert that after `partition → write on both
  sides → heal`, every node converges to the *deterministic* winner
  (highest timestamp; ties broken by `(node_id, partition_id)`).
- **Lost-update with `INCR`**: document the loss; the test asserts the
  expected loss profile, not "no loss" — the LWW model cannot guarantee
  PN-Counter semantics without CRDTs (`docs/04-replication.md`).
- **Quorum behaviour**: `member_count_quorum` rejects writes on the
  minority side; signature-clock + quorum together keep the majority
  side's routing table authoritative on heal
  (`docs/03-cluster-management.md` "Election").

Implementation will be Docker-compose + `iptables` partition injection
plus a small Rust harness that drives `RemoteClient` checkers against the
running cluster. No code yet — placeholder kept here so Phase 5 has a
landing spot.
