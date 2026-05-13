//! Per-connection state.
//!
//! Kept tiny: auth status, negotiated RESP version, and the pub/sub mode
//! state added in Phase 7.

use kamino_protocol::ProtocolVersion;

/// Per-connection state machine.
#[derive(Debug)]
pub(crate) struct ConnState {
    pub(crate) auth: AuthState,
    pub(crate) version: ProtocolVersion,
    /// `true` once the peer has authenticated with the configured
    /// `cluster_secret` rather than (or in addition to) the client
    /// password. Gates `INTERNAL.NODE.*` commands per
    /// `docs/06-network-protocol.md`.
    pub(crate) internode: bool,
    /// Phase 7 pub/sub mode. `None` means the connection is in regular
    /// request/response mode; `Some(id)` is the pub/sub registry's
    /// per-connection id (allocated lazily on first SUBSCRIBE/PSUBSCRIBE).
    /// Once set, the connection stays in pub/sub mode for the rest of
    /// its lifetime — the only way out is `QUIT` or disconnect.
    pub(crate) pub_sub_id: Option<u64>,
    /// Running count of (exact + pattern) subscriptions on this connection.
    /// Mirrors what the registry knows so the dispatcher can answer the
    /// "is this a pub/sub connection?" question without locking the
    /// service mutex on every command.
    pub(crate) pub_sub_count: usize,
    #[allow(dead_code)]
    pub(crate) client_name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthState {
    /// No password configured server-side — every freshly accepted conn is
    /// pre-authenticated.
    NoAuthRequired,
    /// Server has a password; the client has not yet authenticated.
    Unauthenticated,
    /// Authenticated either via `AUTH ...` or an inline-AUTH `HELLO`.
    Authenticated,
}

impl ConnState {
    pub(crate) const fn new(auth_required: bool) -> Self {
        let auth = if auth_required {
            AuthState::Unauthenticated
        } else {
            AuthState::NoAuthRequired
        };
        Self {
            auth,
            version: ProtocolVersion::Resp2,
            internode: false,
            pub_sub_id: None,
            pub_sub_count: 0,
            client_name: None,
        }
    }

    pub(crate) const fn is_authed(&self) -> bool {
        matches!(
            self.auth,
            AuthState::NoAuthRequired | AuthState::Authenticated
        )
    }

    /// `true` once the connection has executed at least one
    /// `SUBSCRIBE`/`PSUBSCRIBE`. Subsequent commands outside the
    /// pub/sub allow-list are rejected.
    pub(crate) const fn in_pubsub_mode(&self) -> bool {
        self.pub_sub_id.is_some()
    }
}
