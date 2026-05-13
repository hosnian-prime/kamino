//! Phase 7 pub/sub service.
//!
//! Per `docs/11-pubsub.md`:
//!
//! - `BTreeMap<(is_pattern, channel_or_pattern, conn_id), Subscriber>` is the
//!   canonical registry shape. The triple keys it gives us range-scan
//!   semantics for `PUBSUB CHANNELS [pattern]` and per-channel lookups
//!   without scanning the whole map.
//! - Each subscribed connection owns a single bounded `mpsc::Sender`. The
//!   service writes one [`DeliveredMessage`] per match. Slow consumers fall
//!   off as `try_send` drops on `Full` — that matches the documented
//!   at-most-once delivery contract.
//! - Cluster-wide `PUBLISH` is fan-out by the server: it publishes locally
//!   first, then iterates live peers and sends `INTERNAL.NODE.PUBLISH`
//!   one-hop. Peer-arrival `INTERNAL.NODE.PUBLISH` only triggers
//!   [`PubSubService::publish_local`] — no re-broadcast. This avoids the
//!   O(N!) loop a naive forward would create.
//!
//! ## Glob matching
//!
//! [`pattern_matches`] is a direct iterative matcher (no regex). Supports
//! `*`, `?`, `[abc]`, `[^abc]`. Hot path for every PUBLISH/INTERNAL.NODE.PUBLISH
//! arrival; allocating a `Regex` per match would dominate.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::trace;

/// One delivered pub/sub message handed off to a subscriber's queue.
#[derive(Debug, Clone)]
pub struct DeliveredMessage {
    /// Concrete channel name the publisher used.
    pub channel: String,
    /// If the subscription matched via a glob pattern, the pattern source.
    /// `None` for exact-channel subscriptions.
    pub pattern: Option<String>,
    /// Message payload (binary-safe).
    pub payload: Bytes,
}

/// SUBSCRIBE/UNSUBSCRIBE ack record. Matches the Redis reply shape:
/// `[kind, channel, total_subscriptions_on_this_connection]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// Channel or pattern this ack refers to.
    pub channel: Bytes,
    /// `true` for `psubscribe`/`punsubscribe`, `false` for the exact variants.
    pub is_pattern: bool,
    /// Running total of subscriptions (exact + pattern) held by this
    /// connection after the operation. Matches Redis' third element.
    pub total_subscriptions: usize,
}

#[derive(Debug, Clone)]
struct Subscriber {
    sender: mpsc::Sender<DeliveredMessage>,
}

#[derive(Debug, Default)]
struct State {
    /// Registry keyed by `(is_pattern, name, conn_id)`. The tuple ordering
    /// makes "all exact channels" and "all patterns" contiguous ranges.
    entries: BTreeMap<(bool, String, u64), Subscriber>,
    /// Per-connection sender: the same sender is reused for every
    /// subscription this connection holds so subscribe/unsubscribe stays
    /// O(log n) without an inner allocation. Tracked separately from
    /// `entries` so a connection without any active subscriptions still has
    /// a registered sink (helpful for tests / programmatic clients).
    senders: BTreeMap<u64, mpsc::Sender<DeliveredMessage>>,
}

/// Cluster-wide pub/sub registry.
#[derive(Debug)]
pub struct PubSubService {
    state: Mutex<State>,
    next_conn_id: AtomicU64,
}

impl Default for PubSubService {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubService {
    /// Fresh registry with no subscribers.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            next_conn_id: AtomicU64::new(1),
        }
    }

    /// Allocate a fresh connection id. Caller stores it on the conn
    /// state and uses it for every subsequent subscribe/unsubscribe.
    #[must_use]
    pub fn next_conn_id(&self) -> u64 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Wrap an `mpsc::Sender` so the registry can deliver future messages
    /// to this connection. Re-registering with a different sender (e.g.
    /// after a connection reset on the same `conn_id`, which shouldn't
    /// happen in normal flow) replaces the prior sink — already-queued
    /// messages on the old sender stay queued.
    pub fn register_conn(&self, conn_id: u64, sender: mpsc::Sender<DeliveredMessage>) {
        self.state.lock().senders.insert(conn_id, sender);
    }

    /// Drop every subscription and the sender registered for `conn_id`.
    /// Called on connection close.
    pub fn cleanup_conn(&self, conn_id: u64) {
        let mut s = self.state.lock();
        s.entries.retain(|(_, _, id), _| *id != conn_id);
        s.senders.remove(&conn_id);
    }

    fn require_sender(state: &State, conn_id: u64) -> Option<mpsc::Sender<DeliveredMessage>> {
        state.senders.get(&conn_id).cloned()
    }

    fn subscribe_inner(&self, conn_id: u64, items: &[Bytes], is_pattern: bool) -> Vec<SubAck> {
        let mut out = Vec::with_capacity(items.len());
        let mut state = self.state.lock();
        let Some(sender) = Self::require_sender(&state, conn_id) else {
            // Caller must register_conn before subscribing. Returning an
            // empty ack list keeps the dispatcher's reply shape sane and
            // avoids a panic on misuse.
            return out;
        };
        for raw in items {
            let name = String::from_utf8_lossy(raw).into_owned();
            state.entries.insert(
                (is_pattern, name.clone(), conn_id),
                Subscriber {
                    sender: sender.clone(),
                },
            );
            let total = state
                .entries
                .iter()
                .filter(|((_, _, id), _)| *id == conn_id)
                .count();
            out.push(SubAck {
                channel: raw.clone(),
                is_pattern,
                total_subscriptions: total,
            });
        }
        out
    }

    /// Add exact-channel subscriptions for `conn_id`. Returns one ack per
    /// input — the running per-connection total grows monotonically.
    pub fn subscribe(&self, conn_id: u64, channels: &[Bytes]) -> Vec<SubAck> {
        self.subscribe_inner(conn_id, channels, false)
    }

    /// Add pattern subscriptions for `conn_id`.
    pub fn psubscribe(&self, conn_id: u64, patterns: &[Bytes]) -> Vec<SubAck> {
        self.subscribe_inner(conn_id, patterns, true)
    }

    fn unsubscribe_inner(
        &self,
        conn_id: u64,
        items: Option<&[Bytes]>,
        is_pattern: bool,
    ) -> Vec<SubAck> {
        let mut state = self.state.lock();
        let targets: Vec<String> = match items {
            Some(list) if !list.is_empty() => list
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect(),
            _ => state
                .entries
                .iter()
                .filter(|((p, _, id), _)| *p == is_pattern && *id == conn_id)
                .map(|((_, name, _), _)| name.clone())
                .collect(),
        };
        // Redis quirk: UNSUBSCRIBE with no subs still emits one ack with
        // `null` channel and total=0. We model that explicitly so the
        // server-side encoder reproduces the wire shape.
        if targets.is_empty() {
            let total = State::total_for_conn(&state, conn_id);
            return vec![SubAck {
                channel: Bytes::new(),
                is_pattern,
                total_subscriptions: total,
            }];
        }
        let mut out = Vec::with_capacity(targets.len());
        for name in targets {
            state.entries.remove(&(is_pattern, name.clone(), conn_id));
            let total = state
                .entries
                .iter()
                .filter(|((_, _, id), _)| *id == conn_id)
                .count();
            out.push(SubAck {
                channel: Bytes::copy_from_slice(name.as_bytes()),
                is_pattern,
                total_subscriptions: total,
            });
        }
        out
    }

    /// Remove exact-channel subscriptions. `None` (or an empty slice)
    /// removes every exact-channel sub held by `conn_id`.
    pub fn unsubscribe(&self, conn_id: u64, channels: Option<&[Bytes]>) -> Vec<SubAck> {
        self.unsubscribe_inner(conn_id, channels, false)
    }

    /// Remove pattern subscriptions. `None` removes every pattern sub.
    pub fn punsubscribe(&self, conn_id: u64, patterns: Option<&[Bytes]>) -> Vec<SubAck> {
        self.unsubscribe_inner(conn_id, patterns, true)
    }

    /// Fan a single `(channel, payload)` to every local subscriber whose
    /// exact channel or pattern matches. Returns the number of
    /// subscribers that **accepted** the delivery (slow consumers whose
    /// queue is full are silently skipped per the at-most-once contract).
    pub fn publish_local(&self, channel: &str, payload: &Bytes) -> usize {
        let state = self.state.lock();
        let mut delivered = 0usize;
        // Exact-channel subscribers.
        for ((_, name, _), sub) in state
            .entries
            .range((false, channel.to_string(), 0_u64)..(false, channel.to_string(), u64::MAX))
        {
            if name == channel
                && sub
                    .sender
                    .try_send(DeliveredMessage {
                        channel: channel.to_string(),
                        pattern: None,
                        payload: payload.clone(),
                    })
                    .is_ok()
            {
                delivered += 1;
            }
        }
        // Pattern subscribers: scan the pattern range. We can't bucket
        // patterns by their fixed prefix without losing the [..] forms, so
        // a full pattern scan is the simplest correct option. For typical
        // deployments the pattern count is small (single-digit to dozens).
        for ((_, pattern, _), sub) in state
            .entries
            .range((true, String::new(), 0_u64)..(true, String::from("\u{FFFD}"), u64::MAX))
        {
            if pattern_matches(pattern, channel)
                && sub
                    .sender
                    .try_send(DeliveredMessage {
                        channel: channel.to_string(),
                        pattern: Some(pattern.clone()),
                        payload: payload.clone(),
                    })
                    .is_ok()
            {
                delivered += 1;
            }
        }
        trace!(channel, delivered, "publish_local fan-out done");
        delivered
    }

    /// `PUBSUB CHANNELS [pattern]` — distinct exact channels with at least
    /// one local subscriber, optionally filtered by glob.
    #[must_use]
    pub fn pubsub_channels(&self, pattern: Option<&str>) -> Vec<String> {
        let state = self.state.lock();
        let mut out: Vec<String> = state
            .entries
            .keys()
            .filter(|(p, _, _)| !*p)
            .map(|(_, n, _)| n.clone())
            .collect();
        out.sort();
        out.dedup();
        if let Some(pat) = pattern {
            out.retain(|c| pattern_matches(pat, c));
        }
        out
    }

    /// `PUBSUB NUMSUB ch...` — per-channel subscriber count (exact only).
    #[must_use]
    pub fn pubsub_numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        let state = self.state.lock();
        channels
            .iter()
            .map(|c| {
                let name = String::from_utf8_lossy(c).into_owned();
                let count = state
                    .entries
                    .range((false, name.clone(), 0)..(false, name.clone(), u64::MAX))
                    .filter(|((_, n, _), _)| n == &name)
                    .count();
                (c.clone(), count)
            })
            .collect()
    }

    /// `PUBSUB NUMPAT` — distinct patterns with at least one subscriber.
    #[must_use]
    pub fn pubsub_numpat(&self) -> usize {
        let state = self.state.lock();
        let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for (p, name, _) in state.entries.keys() {
            if *p {
                seen.insert(name.as_str());
            }
        }
        seen.len()
    }
}

impl State {
    fn total_for_conn(state: &Self, conn_id: u64) -> usize {
        state
            .entries
            .iter()
            .filter(|((_, _, id), _)| *id == conn_id)
            .count()
    }
}

/// Object-safe handle the server dispatcher and the cluster runtime call
/// into. Behind the scenes every implementor wraps a [`PubSubService`].
///
/// No `Debug` bound: the cluster runtime self-implements this trait and
/// holds non-`Debug` `dyn` members; requiring `Debug` would force every
/// caller to add a manual impl that prints almost nothing useful.
pub trait PubSubProvider: Send + Sync {
    /// Allocate a new connection id.
    fn allocate_conn_id(&self) -> u64;
    /// Register the mpsc sender for this conn id.
    fn register_conn(&self, conn_id: u64, sender: mpsc::Sender<DeliveredMessage>);
    /// Drop subscriptions and sender for this conn id.
    fn cleanup_conn(&self, conn_id: u64);

    fn subscribe(&self, conn_id: u64, channels: &[Bytes]) -> Vec<SubAck>;
    fn psubscribe(&self, conn_id: u64, patterns: &[Bytes]) -> Vec<SubAck>;
    fn unsubscribe(&self, conn_id: u64, channels: Option<&[Bytes]>) -> Vec<SubAck>;
    fn punsubscribe(&self, conn_id: u64, patterns: Option<&[Bytes]>) -> Vec<SubAck>;

    /// Cluster-wide PUBLISH. Returns the total delivery count summed
    /// across this node and every reachable peer.
    fn publish<'a>(
        &'a self,
        channel: Bytes,
        message: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = usize> + Send + 'a>>;

    /// Single-hop arrival from `INTERNAL.NODE.PUBLISH`. Delivers locally
    /// only; never re-fans-out.
    fn publish_local(&self, channel: &str, message: &Bytes) -> usize;

    fn pubsub_channels(&self, pattern: Option<&str>) -> Vec<String>;
    fn pubsub_numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, usize)>;
    fn pubsub_numpat(&self) -> usize;
}

/// Standalone-mode [`PubSubProvider`]: no cluster fan-out, every
/// `publish` only touches local subscribers. Used by single-node servers
/// and by Phase 1 embedded clients.
#[derive(Debug)]
pub struct LocalPubSubProvider {
    service: Arc<PubSubService>,
}

impl LocalPubSubProvider {
    /// Wrap an existing service.
    #[must_use]
    pub const fn new(service: Arc<PubSubService>) -> Self {
        Self { service }
    }

    /// Underlying registry (for tests / shared embedding).
    #[must_use]
    pub fn service(&self) -> Arc<PubSubService> {
        Arc::clone(&self.service)
    }
}

impl PubSubProvider for LocalPubSubProvider {
    fn allocate_conn_id(&self) -> u64 {
        self.service.next_conn_id()
    }
    fn register_conn(&self, conn_id: u64, sender: mpsc::Sender<DeliveredMessage>) {
        self.service.register_conn(conn_id, sender);
    }
    fn cleanup_conn(&self, conn_id: u64) {
        self.service.cleanup_conn(conn_id);
    }
    fn subscribe(&self, conn_id: u64, channels: &[Bytes]) -> Vec<SubAck> {
        self.service.subscribe(conn_id, channels)
    }
    fn psubscribe(&self, conn_id: u64, patterns: &[Bytes]) -> Vec<SubAck> {
        self.service.psubscribe(conn_id, patterns)
    }
    fn unsubscribe(&self, conn_id: u64, channels: Option<&[Bytes]>) -> Vec<SubAck> {
        self.service.unsubscribe(conn_id, channels)
    }
    fn punsubscribe(&self, conn_id: u64, patterns: Option<&[Bytes]>) -> Vec<SubAck> {
        self.service.punsubscribe(conn_id, patterns)
    }
    fn publish<'a>(
        &'a self,
        channel: Bytes,
        message: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = usize> + Send + 'a>> {
        let svc = Arc::clone(&self.service);
        Box::pin(async move {
            let name = String::from_utf8_lossy(&channel).into_owned();
            svc.publish_local(&name, &message)
        })
    }
    fn publish_local(&self, channel: &str, message: &Bytes) -> usize {
        self.service.publish_local(channel, message)
    }
    fn pubsub_channels(&self, pattern: Option<&str>) -> Vec<String> {
        self.service.pubsub_channels(pattern)
    }
    fn pubsub_numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        self.service.pubsub_numsub(channels)
    }
    fn pubsub_numpat(&self) -> usize {
        self.service.pubsub_numpat()
    }
}

/// Glob matcher. Iterative two-pointer with backtracking on `*`. Linear
/// time on inputs without `*`; worst case O(n*m) when many `*` patterns
/// appear together — but pub/sub patterns are typically short prefixes.
#[must_use]
pub fn pattern_matches(pattern: &str, candidate: &str) -> bool {
    let pat = pattern.as_bytes();
    let cand = candidate.as_bytes();
    let (mut pi, mut ci) = (0_usize, 0_usize);
    let mut star: Option<usize> = None;
    let mut match_after_star: usize = 0;
    while ci < cand.len() {
        if pi < pat.len() {
            match pat[pi] {
                b'*' => {
                    star = Some(pi);
                    match_after_star = ci;
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    ci += 1;
                    continue;
                }
                b'[' => {
                    // Find the closing `]`. If missing, treat the `[` as a
                    // literal character — matches Redis behaviour.
                    let close = pat[pi + 1..].iter().position(|&b| b == b']');
                    let Some(close_rel) = close else {
                        if pat[pi] == cand[ci] {
                            pi += 1;
                            ci += 1;
                            continue;
                        }
                        return mismatch_backtrack(&mut pi, &mut ci, star, &mut match_after_star);
                    };
                    let set = &pat[pi + 1..pi + 1 + close_rel];
                    let (negate, set) = if set.first() == Some(&b'^') {
                        (true, &set[1..])
                    } else {
                        (false, set)
                    };
                    let in_set = set.contains(&cand[ci]);
                    let ok = if negate { !in_set } else { in_set };
                    if ok {
                        pi += 1 + close_rel + 1;
                        ci += 1;
                        continue;
                    }
                    if !mismatch_backtrack(&mut pi, &mut ci, star, &mut match_after_star) {
                        return false;
                    }
                    continue;
                }
                lit => {
                    if lit == cand[ci] {
                        pi += 1;
                        ci += 1;
                        continue;
                    }
                    if !mismatch_backtrack(&mut pi, &mut ci, star, &mut match_after_star) {
                        return false;
                    }
                    continue;
                }
            }
        }
        // Pattern exhausted but candidate not — only OK if we can return
        // via a star.
        if !mismatch_backtrack(&mut pi, &mut ci, star, &mut match_after_star) {
            return false;
        }
    }
    // Skip trailing stars.
    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }
    pi == pat.len()
}

fn mismatch_backtrack(
    pi: &mut usize,
    ci: &mut usize,
    star: Option<usize>,
    match_after_star: &mut usize,
) -> bool {
    star.is_some_and(|s| {
        *pi = s + 1;
        *match_after_star += 1;
        *ci = *match_after_star;
        true
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn make_conn(svc: &PubSubService) -> (u64, mpsc::Receiver<DeliveredMessage>) {
        let id = svc.next_conn_id();
        let (tx, rx) = mpsc::channel(16);
        svc.register_conn(id, tx);
        (id, rx)
    }

    #[test]
    fn glob_exact() {
        assert!(pattern_matches("foo", "foo"));
        assert!(!pattern_matches("foo", "bar"));
    }

    #[test]
    fn glob_star() {
        assert!(pattern_matches("foo*", "foobar"));
        assert!(pattern_matches("*bar", "foobar"));
        assert!(pattern_matches("*", "anything"));
        assert!(pattern_matches("a*b", "azzzb"));
        assert!(pattern_matches("a*b*c", "axxxbyyyc"));
        assert!(!pattern_matches("a*c", "ab"));
    }

    #[test]
    fn glob_question() {
        assert!(pattern_matches("a?c", "abc"));
        assert!(!pattern_matches("a?c", "ac"));
        assert!(!pattern_matches("a?c", "abbc"));
    }

    #[test]
    fn glob_charclass() {
        assert!(pattern_matches("a[bd]e", "abe"));
        assert!(pattern_matches("a[bd]e", "ade"));
        assert!(!pattern_matches("a[bd]e", "ace"));
    }

    #[test]
    fn glob_negated_charclass() {
        assert!(pattern_matches("a[^bd]e", "ace"));
        assert!(!pattern_matches("a[^bd]e", "abe"));
    }

    #[test]
    fn glob_unclosed_bracket_is_literal() {
        assert!(pattern_matches("a[bc", "a[bc"));
        assert!(!pattern_matches("a[bc", "abc"));
    }

    #[test]
    fn glob_pubsub_examples_from_docs() {
        assert!(pattern_matches("events.*", "events.created"));
        assert!(pattern_matches("events.*", "events.order.created"));
        assert!(pattern_matches("user.login.*", "user.login.alice"));
        assert!(pattern_matches("*.critical", "system.critical"));
        assert!(!pattern_matches("events.*", "alerts.created"));
    }

    #[tokio::test]
    async fn subscribe_then_publish_delivers() {
        let svc = PubSubService::new();
        let (conn, mut rx) = make_conn(&svc);
        let acks = svc.subscribe(conn, &[b("events")]);
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].total_subscriptions, 1);

        let delivered = svc.publish_local("events", &b("hello"));
        assert_eq!(delivered, 1);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "events");
        assert!(msg.pattern.is_none());
        assert_eq!(msg.payload, b("hello"));
    }

    #[tokio::test]
    async fn pattern_subscribe_delivers_with_source() {
        let svc = PubSubService::new();
        let (conn, mut rx) = make_conn(&svc);
        svc.psubscribe(conn, &[b("events.*")]);
        let delivered = svc.publish_local("events.created", &b("payload"));
        assert_eq!(delivered, 1);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "events.created");
        assert_eq!(msg.pattern.as_deref(), Some("events.*"));
    }

    #[tokio::test]
    async fn exact_and_pattern_subs_both_fire() {
        let svc = PubSubService::new();
        let (conn, mut rx) = make_conn(&svc);
        svc.subscribe(conn, &[b("ch")]);
        svc.psubscribe(conn, &[b("c?")]);
        let delivered = svc.publish_local("ch", &b("x"));
        // One subscription delivers as `message`, the other as `pmessage`.
        assert_eq!(delivered, 2);
        let m1 = rx.recv().await.unwrap();
        let m2 = rx.recv().await.unwrap();
        let patterns = [m1.pattern.clone(), m2.pattern];
        assert!(patterns.contains(&None));
        assert!(patterns.contains(&Some("c?".into())));
    }

    #[test]
    fn unsubscribe_specific() {
        let svc = PubSubService::new();
        let (conn, _rx) = make_conn(&svc);
        svc.subscribe(conn, &[b("a"), b("b"), b("c")]);
        let acks = svc.unsubscribe(conn, Some(&[b("b")]));
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].channel, b("b"));
        assert_eq!(acks[0].total_subscriptions, 2);
    }

    #[test]
    fn unsubscribe_all_emits_per_channel_acks() {
        let svc = PubSubService::new();
        let (conn, _rx) = make_conn(&svc);
        svc.subscribe(conn, &[b("a"), b("b")]);
        let acks = svc.unsubscribe(conn, None);
        assert_eq!(acks.len(), 2);
        assert_eq!(acks.last().unwrap().total_subscriptions, 0);
    }

    #[test]
    fn unsubscribe_when_nothing_subscribed_emits_null_ack() {
        let svc = PubSubService::new();
        let (conn, _rx) = make_conn(&svc);
        let acks = svc.unsubscribe(conn, None);
        assert_eq!(acks.len(), 1);
        assert!(acks[0].channel.is_empty());
        assert_eq!(acks[0].total_subscriptions, 0);
    }

    #[test]
    fn pubsub_channels_lists_only_exact() {
        let svc = PubSubService::new();
        let (conn, _rx) = make_conn(&svc);
        svc.subscribe(conn, &[b("events"), b("alerts")]);
        svc.psubscribe(conn, &[b("events.*")]);
        let chans = svc.pubsub_channels(None);
        assert_eq!(chans, vec!["alerts".to_string(), "events".to_string()]);
    }

    #[test]
    fn pubsub_channels_filters_by_glob() {
        let svc = PubSubService::new();
        let (conn, _rx) = make_conn(&svc);
        svc.subscribe(conn, &[b("events"), b("alerts"), b("audit")]);
        let chans = svc.pubsub_channels(Some("a*"));
        assert_eq!(chans, vec!["alerts".to_string(), "audit".to_string()]);
    }

    #[test]
    fn pubsub_numsub_returns_counts() {
        let svc = PubSubService::new();
        let (c1, _r1) = make_conn(&svc);
        let (c2, _r2) = make_conn(&svc);
        svc.subscribe(c1, &[b("events")]);
        svc.subscribe(c2, &[b("events")]);
        let r = svc.pubsub_numsub(&[b("events"), b("missing")]);
        assert_eq!(r[0].1, 2);
        assert_eq!(r[1].1, 0);
    }

    #[test]
    fn pubsub_numpat_counts_distinct_patterns() {
        let svc = PubSubService::new();
        let (c1, _r1) = make_conn(&svc);
        let (c2, _r2) = make_conn(&svc);
        svc.psubscribe(c1, &[b("a.*"), b("b.*")]);
        svc.psubscribe(c2, &[b("a.*")]);
        assert_eq!(svc.pubsub_numpat(), 2);
    }

    #[test]
    fn cleanup_conn_drops_everything() {
        let svc = PubSubService::new();
        let (conn, _rx) = make_conn(&svc);
        svc.subscribe(conn, &[b("a"), b("b")]);
        svc.psubscribe(conn, &[b("c.*")]);
        svc.cleanup_conn(conn);
        assert_eq!(svc.pubsub_numsub(&[b("a")])[0].1, 0);
        assert_eq!(svc.pubsub_numpat(), 0);
    }

    #[tokio::test]
    async fn slow_consumer_is_silently_dropped() {
        let svc = PubSubService::new();
        let id = svc.next_conn_id();
        let (tx, mut rx) = mpsc::channel(1);
        svc.register_conn(id, tx);
        svc.subscribe(id, &[b("ch")]);
        // Fill the queue.
        let n1 = svc.publish_local("ch", &b("a"));
        assert_eq!(n1, 1);
        // Second publish drops because the queue is full (the receiver
        // never drained).
        let n2 = svc.publish_local("ch", &b("b"));
        assert_eq!(n2, 0, "slow consumer must be skipped per at-most-once");
        // Drain to verify only the first message landed.
        let m1 = rx.recv().await.unwrap();
        assert_eq!(m1.payload, b("a"));
    }

    #[test]
    fn local_provider_wraps_service() {
        let svc = Arc::new(PubSubService::new());
        let p = LocalPubSubProvider::new(Arc::clone(&svc));
        let id = p.allocate_conn_id();
        let (tx, _rx) = mpsc::channel(4);
        p.register_conn(id, tx);
        let acks = p.subscribe(id, &[b("x")]);
        assert_eq!(acks[0].total_subscriptions, 1);
        assert_eq!(p.pubsub_numsub(&[b("x")])[0].1, 1);
    }
}
