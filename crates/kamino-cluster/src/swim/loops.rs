//! Probe + receive loops.
//!
//! Contract (frozen for the parallel agents):
//!
//! * [`run_probe_loop`]: drives one probe per `probe_interval`. Picks a
//!   random `Alive` peer, sends `Ping`, waits up to `probe_timeout`; on
//!   timeout fans out `PingReq` to `indirect_probes` random peers; on
//!   *complete* failure, sets the target to `Suspect`.
//! * [`run_receive_loop`]: consumes envelopes from the transport, validates
//!   `cluster_secret`, dispatches into the membership view, and writes
//!   ack/indirect-ack replies back through the transport.

use std::net::SocketAddr;
use std::sync::Arc;

use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use rand::seq::SliceRandom;
use tokio::time::{Duration, timeout};

use super::SwimDriver;
use crate::join::JOIN_TARGET_SENTINEL;
use crate::membership::{ApplyOutcome, MemberState};
use crate::message::{Envelope, GossipEvent, SwimMessage, alive_for};

/// Maximum gossip events piggybacked on a single outgoing envelope. Keeps the
/// datagram well under the `MAX_DATAGRAM` budget defined in `message.rs`.
const PIGGYBACK_BATCH: usize = 8;

/// Outcome reported by a single probe round (used by unit tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// Direct ack received within `probe_timeout`.
    DirectAck,
    /// Indirect ack received via a proxy.
    IndirectAck,
    /// Both phases failed; target was set to `Suspect`.
    Suspected,
    /// No live peers to probe.
    Idle,
}

/// Run the probe loop until `cancel` fires.
pub async fn run_probe_loop(driver: Arc<SwimDriver>) {
    let mut ticker = tokio::time::interval(driver.config.probe_interval);
    // Avoid an immediate spurious probe before the receive loop is ready.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Burn the first immediate tick.
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            () = driver.cancel.cancelled() => {
                tracing::debug!("swim probe loop: cancelled");
                return;
            }
            _ = ticker.tick() => {
                let _ = probe_once_impl(&driver).await;
            }
        }
    }
}

/// Execute exactly one probe round. Public so integration tests under the
/// `test-support` feature can drive the state machine deterministically
/// without spinning the interval timer.
#[cfg(any(test, feature = "mock-transport"))]
pub async fn probe_once(driver: &Arc<SwimDriver>) -> ProbeOutcome {
    probe_once_impl(driver).await
}

async fn probe_once_impl(driver: &Arc<SwimDriver>) -> ProbeOutcome {
    let Some(target) = pick_target(driver).await else {
        return ProbeOutcome::Idle;
    };
    let Some(local_id) = driver.view.local_id() else {
        return ProbeOutcome::Idle;
    };

    // Direct probe.
    let seq = driver.next_seq();
    let rx = driver.register_inflight(seq);
    let gossip = drain_gossip(driver);
    let env = Envelope {
        cluster_secret: driver.cluster_secret.clone(),
        from: local_id,
        msg: SwimMessage::Ping {
            seq,
            target: target.id,
        },
        gossip,
    };
    if let Err(err) = driver.transport.send_to(&env, target.discovery_addr).await {
        tracing::debug!(target = %target.id, %err, "swim probe: direct send failed");
    }

    match timeout(driver.config.probe_timeout, rx).await {
        Ok(Ok(())) => return ProbeOutcome::DirectAck,
        Ok(Err(_)) | Err(_) => {
            driver.forget_inflight(seq);
        }
    }

    // Indirect probes.
    let proxies = pick_proxies(driver, target.id, driver.config.indirect_probes as usize).await;
    if proxies.is_empty() {
        suspect_target(driver, &target);
        return ProbeOutcome::Suspected;
    }

    let indirect_seq = driver.next_seq();
    let rx = driver.register_inflight(indirect_seq);
    for proxy in &proxies {
        let gossip = drain_gossip(driver);
        let env = Envelope {
            cluster_secret: driver.cluster_secret.clone(),
            from: local_id,
            msg: SwimMessage::PingReq {
                seq: indirect_seq,
                target: target.id,
                target_addr: target.discovery_addr,
            },
            gossip,
        };
        if let Err(err) = driver.transport.send_to(&env, proxy.discovery_addr).await {
            tracing::debug!(proxy = %proxy.id, %err, "swim probe: indirect send failed");
        }
    }

    if timeout(driver.config.probe_timeout, rx).await == Ok(Ok(())) {
        ProbeOutcome::IndirectAck
    } else {
        driver.forget_inflight(indirect_seq);
        suspect_target(driver, &target);
        ProbeOutcome::Suspected
    }
}

/// Promote `target` locally to `Suspect` and enqueue the gossip event.
fn suspect_target(driver: &Arc<SwimDriver>, target: &Member) {
    let Some(local_id) = driver.view.local_id() else {
        return;
    };
    let Some(entry) = driver.view.get(target.id) else {
        return;
    };
    let now_ns = driver.clock.now_micros().saturating_mul(1_000);
    let suspicion_timeout_ns = duration_to_nanos(driver.suspicion_timeout());
    let event = GossipEvent::Suspect {
        id: target.id,
        incarnation: entry.incarnation,
        from: local_id,
    };
    let outcome = driver.view.apply(&event, now_ns, suspicion_timeout_ns);
    if matches!(outcome, ApplyOutcome::Updated | ApplyOutcome::NewMember) {
        driver.queue.push(event, driver.view.live_count());
    }
}

/// Pick a random `Alive` non-local peer; returns `None` if no such peer
/// exists.
async fn pick_target(driver: &Arc<SwimDriver>) -> Option<Member> {
    let local_id = driver.view.local_id()?;
    let candidates: Vec<Member> = driver
        .view
        .snapshot_all()
        .into_iter()
        .filter_map(|(m, state, _)| {
            if m.id == local_id {
                return None;
            }
            if state == MemberState::Alive {
                Some(m)
            } else {
                None
            }
        })
        .collect();
    if candidates.is_empty() {
        return None;
    }
    let mut rng = driver.rng.lock().await;
    candidates.choose(&mut *rng).cloned()
}

/// Pick up to `k` random `Alive` non-local non-target peers as indirect-probe
/// proxies.
async fn pick_proxies(driver: &Arc<SwimDriver>, target_id: MemberId, k: usize) -> Vec<Member> {
    let Some(local_id) = driver.view.local_id() else {
        return Vec::new();
    };
    let mut candidates: Vec<Member> = driver
        .view
        .snapshot_all()
        .into_iter()
        .filter_map(|(m, state, _)| {
            if m.id == local_id || m.id == target_id {
                return None;
            }
            if state == MemberState::Alive {
                Some(m)
            } else {
                None
            }
        })
        .collect();
    if candidates.is_empty() || k == 0 {
        return Vec::new();
    }
    {
        let mut rng = driver.rng.lock().await;
        candidates.shuffle(&mut *rng);
    }
    candidates.truncate(k);
    candidates
}

/// Run the receive loop until `cancel` fires.
pub async fn run_receive_loop(driver: Arc<SwimDriver>) {
    loop {
        tokio::select! {
            biased;
            () = driver.cancel.cancelled() => {
                tracing::debug!("swim receive loop: cancelled");
                return;
            }
            recv = driver.transport.recv() => {
                match recv {
                    Ok((env, src)) => {
                        if let Err(err) = handle_envelope(&driver, env, src).await {
                            tracing::debug!(%err, "swim receive: handler error");
                        }
                    }
                    Err(err) => {
                        tracing::debug!(%err, "swim receive: transport error");
                    }
                }
            }
        }
    }
}

async fn handle_envelope(
    driver: &Arc<SwimDriver>,
    env: Envelope,
    src: SocketAddr,
) -> crate::error::ClusterResult<()> {
    env.check_secret(&driver.cluster_secret)?;
    apply_piggyback(driver, env.gossip);
    let Some(local_id) = driver.view.local_id() else {
        return Ok(());
    };

    match env.msg {
        SwimMessage::Ping { seq, target } => {
            // `target == JOIN_TARGET_SENTINEL` is the join-request marker (see
            // `join.rs` wire contract). A node that just woke up cannot know
            // our id yet, so it sends `target = MemberId(0)` and piggybacks an
            // `Alive` for itself. We answer with `Ack` + piggybacked `Alive`
            // for ourselves so the joiner learns who we are.
            let is_join_probe = target == JOIN_TARGET_SENTINEL;
            if !is_join_probe && target != local_id {
                tracing::trace!(target = %target, local = %local_id, "swim recv: ping for wrong target, dropping");
                return Ok(());
            }
            if is_join_probe {
                // Make sure our own Alive ends up in the ack gossip even if
                // the queue is empty at this moment.
                if let Some(entry) = driver.view.get(local_id) {
                    driver.queue.push(
                        alive_for(&entry.member, entry.incarnation),
                        driver.view.live_count(),
                    );
                }
            }
            let gossip = drain_gossip(driver);
            let ack = Envelope {
                cluster_secret: driver.cluster_secret.clone(),
                from: local_id,
                msg: SwimMessage::Ack {
                    seq,
                    from: local_id,
                },
                gossip,
            };
            driver.transport.send_to(&ack, src).await?;
        }
        SwimMessage::Ack { seq, .. } | SwimMessage::IndirectAck { seq, .. } => {
            driver.complete_inflight(seq);
        }
        SwimMessage::PingReq {
            seq,
            target,
            target_addr,
        } => {
            // Forward a Ping to the target on behalf of `env.from`.
            let inner_seq = driver.next_seq();
            let rx = driver.register_inflight(inner_seq);
            let gossip = drain_gossip(driver);
            let fwd = Envelope {
                cluster_secret: driver.cluster_secret.clone(),
                from: local_id,
                msg: SwimMessage::Ping {
                    seq: inner_seq,
                    target,
                },
                gossip,
            };
            if let Err(err) = driver.transport.send_to(&fwd, target_addr).await {
                tracing::debug!(target = %target, %err, "swim recv: pingreq forward failed");
                driver.forget_inflight(inner_seq);
                return Ok(());
            }
            let driver_for_task = Arc::clone(driver);
            let original_src = src;
            tokio::spawn(async move {
                match timeout(driver_for_task.config.probe_timeout, rx).await {
                    Ok(Ok(())) => {
                        let gossip = drain_gossip(&driver_for_task);
                        let Some(local_id) = driver_for_task.view.local_id() else {
                            return;
                        };
                        let ack = Envelope {
                            cluster_secret: driver_for_task.cluster_secret.clone(),
                            from: local_id,
                            msg: SwimMessage::IndirectAck {
                                seq,
                                target,
                                from: local_id,
                            },
                            gossip,
                        };
                        if let Err(err) =
                            driver_for_task.transport.send_to(&ack, original_src).await
                        {
                            tracing::debug!(%err, "swim recv: indirect ack send failed");
                        }
                    }
                    _ => {
                        driver_for_task.forget_inflight(inner_seq);
                    }
                }
            });
        }
    }
    Ok(())
}

/// Apply piggybacked gossip events. Re-queues `Suspect`/`Dead` we just
/// learned about so they continue to disseminate; bumps local incarnation if
/// a remote node claims we are suspect/dead.
fn apply_piggyback(driver: &Arc<SwimDriver>, events: Vec<GossipEvent>) {
    let now_ns = driver.clock.now_micros().saturating_mul(1_000);
    let suspicion_timeout_ns = duration_to_nanos(driver.suspicion_timeout());
    for event in events {
        let outcome = driver.view.apply(&event, now_ns, suspicion_timeout_ns);
        match outcome {
            ApplyOutcome::NewMember | ApplyOutcome::Updated => {
                driver.queue.push(event, driver.view.live_count());
            }
            ApplyOutcome::SelfRefutationNeeded => {
                let new_inc = driver.view.bump_local_incarnation();
                if let Some(local) = driver
                    .view
                    .local_id()
                    .and_then(|id| driver.view.get(id))
                    .map(|e| e.member)
                {
                    let refute = alive_for(&local, new_inc);
                    driver.queue.push(refute, driver.view.live_count());
                }
            }
            ApplyOutcome::Ignored => {}
        }
    }
}

/// Drain up to `PIGGYBACK_BATCH` events for an outgoing envelope.
fn drain_gossip(driver: &Arc<SwimDriver>) -> Vec<GossipEvent> {
    driver.queue.drain_for_send(PIGGYBACK_BATCH)
}

/// Convert a `Duration` to nanoseconds, saturating at `u64::MAX`.
fn duration_to_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}
