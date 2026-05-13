//! `HELLO` payload parsing.
//!
//! Syntax: `HELLO [protover] [AUTH username password] [SETNAME clientname]`.

use bytes::Bytes;

/// Parsed `HELLO` arguments.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HelloArgs {
    /// `protover` from the command (2 or 3), or `None` if the client only
    /// sent `HELLO` to query server capabilities.
    pub protocol_version: Option<u32>,

    /// `(username, password)` from `AUTH <username> <password>`. The
    /// username is `None` when `AUTH <password>` is used in RESP2 fallback
    /// form (legacy single-arg AUTH).
    pub auth: Option<(Option<Bytes>, Bytes)>,

    /// `SETNAME <client-name>` — optional connection name.
    pub client_name: Option<Bytes>,
}

impl HelloArgs {
    /// Construct a minimal `HELLO` (no auth, no client name).
    #[must_use]
    pub const fn versioned(version: u32) -> Self {
        Self {
            protocol_version: Some(version),
            auth: None,
            client_name: None,
        }
    }
}
