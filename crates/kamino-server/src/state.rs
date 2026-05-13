//! Per-connection state.
//!
//! Kept tiny: auth status, negotiated RESP version, and a (reserved) pub/sub
//! flag so Phase 7 doesn't have to restructure dispatch.

use kamino_protocol::ProtocolVersion;

/// Per-connection state machine.
#[derive(Debug)]
pub(crate) struct ConnState {
    pub(crate) auth: AuthState,
    pub(crate) version: ProtocolVersion,
    /// Reserved for Phase 7 pub/sub. Always `false` in Phase 2; the field
    /// exists so the connection-loop layout doesn't need to grow when
    /// `SUBSCRIBE` arrives.
    #[allow(dead_code)]
    pub(crate) pub_sub_mode: bool,
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
            pub_sub_mode: false,
            client_name: None,
        }
    }

    pub(crate) const fn is_authed(&self) -> bool {
        matches!(
            self.auth,
            AuthState::NoAuthRequired | AuthState::Authenticated
        )
    }
}
