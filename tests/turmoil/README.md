# tests/turmoil/

Workspace-level deterministic SWIM simulations driven by
[`turmoil`](https://crates.io/crates/turmoil) (per `ROADMAP.md` §5 "Test
layers", §8 "Best leverage points", and §9 "Risks").

Scenarios that **will** live here once they have an owning phase:

- **Phase 3** (SWIM membership): cluster sizes 3, 5, 10, 50; injected
  packet loss at 0% / 1% / 5%; node death + suspicion convergence;
  coordinator agreement under simultaneous birthdates with deterministic
  seeds.
- **Phase 4** (consistent hashing + routing): transient dual-coordinator
  during SWIM convergence; routing-table `signature`-clock property test
  under random message ordering.
- **Phase 5** (replication): replica drift under partition + heal.
- **Phase 9** (Kubernetes discovery): fresh-bootstrap with
  `member_count_quorum=2` doesn't deadlock.

The first concrete scenarios were landed in
`crates/kamino-cluster/tests/swim_turmoil.rs` using real localhost
`UdpTransport` because `turmoil::net::UdpSocket` is not
`tokio::net::UdpSocket` and bridging the two requires a parallel
`Transport` impl. A `TurmoilTransport` is on the to-do list once Phase 4
needs the virtual TCP stack for routing-table push tests.

This directory deliberately lives at the workspace root so cross-crate
integration scenarios (server + cluster + storage + client end-to-end) have
a clean home.
