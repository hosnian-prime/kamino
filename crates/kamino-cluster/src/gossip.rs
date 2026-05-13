//! Gossip piggyback queue.
//!
//! Each `GossipEvent` is broadcast O(log N) times — once per protocol period
//! per peer in expectation — so the queue tracks a per-event "transmit
//! budget" (Das/Gupta/Motivala 2002 §3.3 "infection-style dissemination"):
//!
//! ```text
//! budget = ceil(λ * log2(N + 1))     for some constant λ (default 3)
//! ```
//!
//! Events with budget remaining are eligible for piggybacking; the queue
//! drains them in oldest-first order, decrementing the budget on each pick.

use std::collections::VecDeque;

use parking_lot::Mutex;

use crate::message::GossipEvent;

/// Multiplier on `ceil(log2(N + 1))` for transmit budget. Higher values
/// trade bytes for faster convergence.
pub const GOSSIP_FANOUT_MULT: u32 = 3;

/// A fixed-size FIFO with per-event transmit budgets.
#[derive(Debug, Default)]
pub struct GossipQueue {
    inner: Mutex<VecDeque<Pending>>,
    capacity: usize,
}

#[derive(Debug, Clone)]
struct Pending {
    event: GossipEvent,
    /// Remaining transmissions before the event is dropped.
    budget: u32,
}

impl GossipQueue {
    /// Build a queue holding at most `capacity` pending events. Overflow
    /// evicts the oldest entry — typically a stale incarnation that newer
    /// gossip has already superseded.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Enqueue a new event. Bumps the budget for any existing entry whose
    /// equivalent is already pending (deduplication by Eq).
    pub fn push(&self, event: GossipEvent, live_member_count: usize) {
        let mut q = self.inner.lock();
        if let Some(pos) = q.iter().position(|p| p.event == event) {
            q[pos].budget = q[pos].budget.max(budget_for(live_member_count));
            return;
        }
        if q.len() == self.capacity && self.capacity > 0 {
            q.pop_front();
        }
        q.push_back(Pending {
            event,
            budget: budget_for(live_member_count),
        });
    }

    /// Drain up to `max` events for piggybacking on a single message.
    /// Decrements each chosen event's budget; events whose budget hits zero
    /// are dropped from the queue.
    pub fn drain_for_send(&self, max: usize) -> Vec<GossipEvent> {
        if max == 0 {
            return Vec::new();
        }
        let mut q = self.inner.lock();
        let mut out = Vec::with_capacity(max.min(q.len()));
        let mut idx = 0;
        while out.len() < max && idx < q.len() {
            let pending = &mut q[idx];
            out.push(pending.event.clone());
            pending.budget = pending.budget.saturating_sub(1);
            if pending.budget == 0 {
                q.remove(idx);
            } else {
                idx += 1;
            }
        }
        out
    }

    /// Current number of pending events.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// True iff no events are pending.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

fn budget_for(live_member_count: usize) -> u32 {
    // Saturate the cluster size to u32::MAX — far above any realistic count;
    // the log2 step compresses the result back to a small integer.
    let n = u32::try_from(live_member_count.max(1)).unwrap_or(u32::MAX);
    let log2 = 32 - n.leading_zeros(); // ceil(log2(n + 1))
    GOSSIP_FANOUT_MULT.saturating_mul(log2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kamino_core::ids::MemberId;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn alive(id: u64, incarnation: u64) -> GossipEvent {
        GossipEvent::Alive {
            id: MemberId::from_raw(id),
            name: format!("n{id}"),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
            discovery_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3322),
            birthdate: 100,
            incarnation,
        }
    }

    #[test]
    fn dedupes_identical_events() {
        let q = GossipQueue::with_capacity(8);
        q.push(alive(1, 1), 5);
        q.push(alive(1, 1), 5);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn budget_decrements_per_drain() {
        let q = GossipQueue::with_capacity(8);
        q.push(alive(1, 1), 1);
        let b = budget_for(1);
        for _ in 0..b {
            assert_eq!(q.drain_for_send(4).len(), 1);
        }
        assert!(q.is_empty());
    }

    #[test]
    fn capacity_evicts_oldest() {
        let q = GossipQueue::with_capacity(2);
        q.push(alive(1, 1), 5);
        q.push(alive(2, 1), 5);
        q.push(alive(3, 1), 5);
        let drained = q.drain_for_send(8);
        assert_eq!(drained.len(), 2);
        // The first event (id=1) was evicted.
        let ids: Vec<u64> = drained
            .iter()
            .filter_map(|e| match e {
                GossipEvent::Alive { id, .. } => Some(id.as_u64()),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn budget_grows_with_cluster_size() {
        assert!(budget_for(1) < budget_for(100));
    }
}
