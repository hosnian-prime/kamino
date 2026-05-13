//! RESP2/3 wire protocol codec and command AST for Kamino.
//!
//! Phase 2 surface (see `docs/06-network-protocol.md` and `ROADMAP.md` §6):
//!
//! - [`Frame`] — full RESP2 + RESP3 frame enumeration.
//! - [`RespCodec`] — `tokio_util::codec::{Encoder, Decoder}` impl. Stateful:
//!   tracks the protocol version so RESP3-only frame types are only emitted
//!   after `HELLO 3` succeeds.
//! - [`Command`] — strongly-typed AST for every Phase 2 command (DM.*,
//!   utility commands). Pub/sub, cluster and INTERNAL.* commands land in
//!   later phases.
//! - [`HelloArgs`] — parsed `HELLO` payload.
//! - [`Error`] / [`CommandError`] — typed wire and parser errors.
//!
//! Phase 2 contract: the public API surface in this crate is **frozen**.
//! Implementations may change internals freely, but signatures of the items
//! re-exported from `lib.rs` are the seam between the protocol crate and
//! the server + remote client.

pub mod codec;
pub mod command;
pub mod error;
pub mod frame;
pub mod hello;

pub use codec::{ProtocolVersion, RespCodec};
pub use command::{Command, PutCommandOptions, ScanCommandOptions};
pub use error::{CommandError, ProtocolError};
pub use frame::{BulkString, Frame};
pub use hello::HelloArgs;
