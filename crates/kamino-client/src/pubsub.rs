//! Client-side pub/sub trait + types (Phase 7).
//!
//! Per `docs/08-api-design.md`:
//!
//! ```ignore
//! pub trait PubSub: Send + Sync {
//!     async fn subscribe(&self, channels: &[&str]) -> Result<Subscription>;
//!     async fn psubscribe(&self, patterns: &[&str]) -> Result<Subscription>;
//!     async fn publish(&self, channel: &str, message: &[u8]) -> Result<usize>;
//! }
//! ```
//!
//! `Subscription` exposes a `recv()` future and is `Send` — the contract
//! match the doc literally.
//!
//! Phase 7 ships the trait + the `EmbeddedPubSub` impl that talks to a
//! local `kamino_cluster::PubSubService`. The remote (TCP-backed) impl
//! lands alongside the broader `RemoteClient` Phase-7 work.

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::Result;

/// One delivered message handed to the subscriber.
#[derive(Debug, Clone)]
pub struct Message {
    /// The exact channel the publisher used.
    pub channel: String,
    /// The pattern that matched, if subscribed via `psubscribe`.
    pub pattern: Option<String>,
    /// Payload (binary-safe).
    pub payload: Vec<u8>,
}

/// Subscriber handle.
///
/// Drop to release the subscription — the underlying registry's
/// `cleanup_conn` fires when the `Sender` half is dropped on the
/// service side, but the `EmbeddedPubSub` also calls `cleanup_conn`
/// explicitly on `unsubscribe`.
pub struct Subscription {
    receiver: mpsc::Receiver<Message>,
    /// Optional cleanup callback fired on `Drop`. Keeps the service
    /// registry tidy when a subscriber walks away without explicitly
    /// unsubscribing.
    on_drop: Option<Box<dyn FnOnce() + Send>>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("has_cleanup", &self.on_drop.is_some())
            .finish_non_exhaustive()
    }
}

impl Subscription {
    /// Build a fresh subscription wrapper. Plumbing for impls.
    #[must_use]
    pub fn new(receiver: mpsc::Receiver<Message>) -> Self {
        Self {
            receiver,
            on_drop: None,
        }
    }

    /// Attach a one-shot cleanup hook fired when the subscription drops.
    #[must_use]
    pub fn with_cleanup(mut self, cleanup: impl FnOnce() + Send + 'static) -> Self {
        self.on_drop = Some(Box::new(cleanup));
        self
    }

    /// Wait for the next delivered message. Returns `None` once the
    /// underlying channel closes (the service was dropped or the
    /// connection was cleaned up server-side).
    pub async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(cb) = self.on_drop.take() {
            cb();
        }
    }
}

/// Top-level pub/sub trait shared between embedded and remote clients.
/// Mirrors `docs/08-api-design.md`.
#[async_trait]
pub trait PubSub: Send + Sync {
    /// Subscribe to one or more exact channel names. Returns a single
    /// [`Subscription`] that multiplexes every channel in the request.
    async fn subscribe(&self, channels: &[&str]) -> Result<Subscription>;

    /// Subscribe to one or more glob patterns.
    async fn psubscribe(&self, patterns: &[&str]) -> Result<Subscription>;

    /// Publish a single message. Returns the cluster-wide subscriber
    /// count that received the message.
    async fn publish(&self, channel: &str, message: &[u8]) -> Result<usize>;
}

/// In-process pub/sub backed by a shared `kamino_cluster::PubSubService`.
/// Used by `Kamino::embedded()` deployments and by tests.
#[derive(Debug)]
pub struct EmbeddedPubSub {
    service: std::sync::Arc<kamino_cluster::PubSubService>,
}

impl EmbeddedPubSub {
    /// Wrap a shared service.
    #[must_use]
    pub const fn new(service: std::sync::Arc<kamino_cluster::PubSubService>) -> Self {
        Self { service }
    }

    fn open_subscription(&self, items: &[&str], is_pattern: bool) -> Subscription {
        let id = self.service.next_conn_id();
        let (raw_tx, mut raw_rx) = mpsc::channel::<kamino_cluster::DeliveredMessage>(64);
        let (msg_tx, msg_rx) = mpsc::channel::<Message>(64);
        self.service.register_conn(id, raw_tx);
        let owned: Vec<Bytes> = items
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect();
        if is_pattern {
            self.service.psubscribe(id, &owned);
        } else {
            self.service.subscribe(id, &owned);
        }
        // Bridge task: re-map `DeliveredMessage` -> `Message`. The
        // re-allocation copies the payload once; for embedded use that's
        // acceptable because there's no wire serialisation in this path.
        tokio::spawn(async move {
            while let Some(m) = raw_rx.recv().await {
                let mapped = Message {
                    channel: m.channel,
                    pattern: m.pattern,
                    payload: m.payload.to_vec(),
                };
                if msg_tx.send(mapped).await.is_err() {
                    break;
                }
            }
        });
        let svc = std::sync::Arc::clone(&self.service);
        Subscription::new(msg_rx).with_cleanup(move || {
            svc.cleanup_conn(id);
        })
    }
}

#[async_trait]
impl PubSub for EmbeddedPubSub {
    async fn subscribe(&self, channels: &[&str]) -> Result<Subscription> {
        Ok(self.open_subscription(channels, false))
    }

    async fn psubscribe(&self, patterns: &[&str]) -> Result<Subscription> {
        Ok(self.open_subscription(patterns, true))
    }

    async fn publish(&self, channel: &str, message: &[u8]) -> Result<usize> {
        let payload = Bytes::copy_from_slice(message);
        Ok(self.service.publish_local(channel, &payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn embedded_subscribe_and_publish() {
        let svc = Arc::new(kamino_cluster::PubSubService::new());
        let ps = EmbeddedPubSub::new(Arc::clone(&svc));
        let mut sub = ps.subscribe(&["events"]).await.unwrap();
        let count = ps.publish("events", b"hello").await.unwrap();
        assert_eq!(count, 1);
        let msg = sub.recv().await.unwrap();
        assert_eq!(msg.channel, "events");
        assert_eq!(msg.payload, b"hello");
    }

    #[tokio::test]
    async fn embedded_psubscribe_pattern_match() {
        let svc = Arc::new(kamino_cluster::PubSubService::new());
        let ps = EmbeddedPubSub::new(Arc::clone(&svc));
        let mut sub = ps.psubscribe(&["events.*"]).await.unwrap();
        let count = ps.publish("events.created", b"payload").await.unwrap();
        assert_eq!(count, 1);
        let msg = sub.recv().await.unwrap();
        assert_eq!(msg.channel, "events.created");
        assert_eq!(msg.pattern.as_deref(), Some("events.*"));
    }

    #[tokio::test]
    async fn drop_subscription_cleans_up_registry() {
        let svc = Arc::new(kamino_cluster::PubSubService::new());
        let ps = EmbeddedPubSub::new(Arc::clone(&svc));
        {
            let _sub = ps.subscribe(&["c"]).await.unwrap();
            assert_eq!(svc.pubsub_numsub(&[Bytes::from_static(b"c")])[0].1, 1);
        }
        // After the Subscription drops, give the bridge task a chance to
        // notice the channel is closed.
        tokio::task::yield_now().await;
        // Subscription count should drop back to zero.
        assert_eq!(svc.pubsub_numsub(&[Bytes::from_static(b"c")])[0].1, 0);
    }
}
