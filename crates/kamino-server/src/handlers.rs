//! Command handlers. Each function takes the parsed command arguments and
//! returns the [`Frame`] to send back. Side effects (`HELLO` flipping the
//! codec, `QUIT` closing the connection) are signalled via [`HandlerOutcome`].

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kamino_client::{Client, DMap, DMapOptions, Error as ClientError, PutOptions, ScanOptions};
use kamino_protocol::{BulkString, Frame, HelloArgs};

use crate::state::{AuthState, ConnState};

/// Side effects a handler may request from the connection loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerOutcome {
    Continue,
    UpgradeToResp3,
    Close,
}

#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) frame: Frame,
    pub(crate) outcome: HandlerOutcome,
}

impl Response {
    pub(crate) const fn ok(frame: Frame) -> Self {
        Self {
            frame,
            outcome: HandlerOutcome::Continue,
        }
    }
}

const NOAUTH: &str = "NOAUTH Authentication required";
const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled";
const NO_PASSWORD_SET: &str = "ERR Client sent AUTH, but no password is set";

pub(crate) fn ping(message: Option<&Bytes>) -> Response {
    let frame = match message {
        None => Frame::SimpleString("PONG".into()),
        Some(m) => Frame::Bulk(BulkString::from_bytes(m.clone())),
    };
    Response::ok(frame)
}

pub(crate) fn quit() -> Response {
    Response {
        frame: Frame::ok(),
        outcome: HandlerOutcome::Close,
    }
}

pub(crate) fn auth(
    state: &mut ConnState,
    server_password: &str,
    username: Option<&Bytes>,
    password: &Bytes,
) -> Response {
    if server_password.is_empty() {
        return Response::ok(Frame::Error(NO_PASSWORD_SET.into()));
    }
    if let Some(u) = username {
        if u.as_ref() != b"default" {
            return Response::ok(Frame::Error(WRONGPASS.into()));
        }
    }
    if password.as_ref() == server_password.as_bytes() {
        state.auth = AuthState::Authenticated;
        Response::ok(Frame::ok())
    } else {
        Response::ok(Frame::Error(WRONGPASS.into()))
    }
}

pub(crate) fn hello(
    state: &mut ConnState,
    server_password: &str,
    args: &HelloArgs,
    server_version: &str,
    server_id: u64,
) -> Response {
    if let Some((username, password)) = &args.auth {
        let resp = auth(state, server_password, username.as_ref(), password);
        if matches!(resp.frame, Frame::Error(_)) {
            return resp;
        }
    } else if matches!(state.auth, AuthState::Unauthenticated) {
        return Response::ok(Frame::Error(NOAUTH.into()));
    }

    let upgrade = match args.protocol_version {
        None | Some(2) => false,
        Some(3) => true,
        Some(other) => {
            return Response::ok(Frame::Error(format!(
                "NOPROTO unsupported protocol version {other}"
            )));
        }
    };

    if let Some(name) = &args.client_name {
        state.client_name = Some(String::from_utf8_lossy(name).into_owned());
    }

    let frame = build_hello_response(server_version, upgrade, server_id);
    Response {
        frame,
        outcome: if upgrade {
            HandlerOutcome::UpgradeToResp3
        } else {
            HandlerOutcome::Continue
        },
    }
}

fn build_hello_response(version: &str, resp3: bool, server_id: u64) -> Frame {
    let proto = if resp3 { 3_i64 } else { 2_i64 };
    let pairs: Vec<(Frame, Frame)> = vec![
        (
            Frame::Bulk(BulkString::from("server")),
            Frame::Bulk(BulkString::from("kamino")),
        ),
        (
            Frame::Bulk(BulkString::from("version")),
            Frame::Bulk(BulkString::from(version)),
        ),
        (
            Frame::Bulk(BulkString::from("proto")),
            Frame::Integer(proto),
        ),
        (
            Frame::Bulk(BulkString::from("id")),
            #[allow(clippy::cast_possible_wrap)]
            Frame::Integer(server_id as i64),
        ),
        (
            Frame::Bulk(BulkString::from("mode")),
            Frame::Bulk(BulkString::from("standalone")),
        ),
        (
            Frame::Bulk(BulkString::from("role")),
            Frame::Bulk(BulkString::from("master")),
        ),
        (
            Frame::Bulk(BulkString::from("modules")),
            Frame::Array(Some(Vec::new())),
        ),
    ];
    if resp3 {
        Frame::Map(pairs)
    } else {
        let mut flat = Vec::with_capacity(pairs.len() * 2);
        for (k, v) in pairs {
            flat.push(k);
            flat.push(v);
        }
        Frame::Array(Some(flat))
    }
}

pub(crate) fn stats(snap: crate::metrics::MetricsSnapshot, server_version: &str) -> Response {
    let pairs: [(&str, String); 5] = [
        ("uptime_secs", snap.uptime_secs.to_string()),
        ("connections", snap.current_connections.to_string()),
        ("connections_total", snap.total_connections.to_string()),
        ("commands_total", snap.commands_total.to_string()),
        ("version", server_version.to_string()),
    ];
    let mut frames = Vec::with_capacity(pairs.len() * 2 + 2);
    for (k, v) in &pairs {
        frames.push(Frame::Bulk(BulkString::from(*k)));
        frames.push(Frame::Bulk(BulkString::from(v.as_str())));
    }
    frames.push(Frame::Bulk(BulkString::from("mode")));
    frames.push(Frame::Bulk(BulkString::from("standalone")));
    Response::ok(Frame::Array(Some(frames)))
}

fn map_client_error(err: ClientError, _command: &str) -> Frame {
    match err {
        ClientError::KeyNotFound => Frame::Bulk(BulkString::null()),
        ClientError::KeyAlreadyExists | ClientError::KeyNotExists => {
            Frame::Bulk(BulkString::null())
        }
        ClientError::LockNotAcquired => Frame::Error("ERR lock not acquired".into()),
        ClientError::NoSuchLock => Frame::Error("ERR no such lock".into()),
        ClientError::InvalidCursor => {
            Frame::Error("CURSOR partition migrated; restart scan".into())
        }
        ClientError::Timeout => Frame::Error("ERR operation timed out".into()),
        ClientError::Storage(s) => match s.as_ref() {
            kamino_storage::Error::KeyTooLarge { .. } => Frame::Error("ERR key too large".into()),
            kamino_storage::Error::ValueTooLarge { .. } => {
                Frame::Error("ERR value too large".into())
            }
            kamino_storage::Error::EngineFull => Frame::Error("ERR engine is full".into()),
            other => Frame::Error(format!("ERR storage error: {other}")),
        },
        ClientError::NotANumber { expected, got } => {
            Frame::Error(format!("ERR value is not an {expected}: {got}"))
        }
        ClientError::InvalidArgument(msg) => Frame::Error(format!("ERR {msg}")),
        ClientError::Serialization(msg) => Frame::Error(format!("ERR serialization: {msg}")),
        ClientError::Unsupported(op) => Frame::Error(format!("ERR {op} not supported")),
        ClientError::DMapNotFound(name) => Frame::Error(format!("ERR dmap not found: {name}")),
        ClientError::Config(c) => Frame::Error(format!("ERR config: {c}")),
        // `kamino_client::Error` is `#[non_exhaustive]`; new variants surface
        // through `Display` so the server never silently drops a remote-side
        // error. The embedded client (the only data path in Phase 2) only
        // emits the variants matched above.
        other => Frame::Error(format!("ERR {other}")),
    }
}

async fn dmap_handle(client: &Arc<dyn Client>, name: &Bytes) -> Result<Arc<dyn DMap>, Frame> {
    let name_str = match std::str::from_utf8(name) {
        Ok(s) => s,
        Err(e) => return Err(Frame::Error(format!("ERR invalid dmap name: {e}"))),
    };
    client
        .new_dmap(name_str, DMapOptions::default())
        .await
        .map_err(|e| map_client_error(e, "DM.*"))
}

fn key_str<'a>(key: &'a Bytes) -> Result<&'a str, Frame> {
    std::str::from_utf8(key).map_err(|e| Frame::Error(format!("ERR invalid key utf-8: {e}")))
}

pub(crate) async fn dm_put(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    value: &Bytes,
    options: kamino_protocol::PutCommandOptions,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    let put_opts = PutOptions {
        ex: options.ex,
        px: options.px,
        exat: options.exat,
        pxat: options.pxat,
        nx: options.nx,
        xx: options.xx,
        timestamp: options.timestamp,
    };
    match d.put(k, value, put_opts).await {
        Ok(()) => Response::ok(Frame::ok()),
        Err(ClientError::KeyAlreadyExists | ClientError::KeyNotExists) => {
            Response::ok(Frame::Bulk(BulkString::null()))
        }
        Err(e) => Response::ok(map_client_error(e, "DM.PUT")),
    }
}

pub(crate) async fn dm_get(client: &Arc<dyn Client>, dmap: &Bytes, key: &Bytes) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.get(k).await {
        Ok(resp) => Response::ok(Frame::Bulk(BulkString::from(resp.value))),
        Err(ClientError::KeyNotFound) => Response::ok(Frame::Bulk(BulkString::null())),
        Err(e) => Response::ok(map_client_error(e, "DM.GET")),
    }
}

pub(crate) async fn dm_del(client: &Arc<dyn Client>, dmap: &Bytes, keys: &[Bytes]) -> Response {
    if keys.is_empty() {
        return Response::ok(Frame::Integer(0));
    }
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let mut deleted = 0_i64;
    for key in keys {
        let k = match key_str(key) {
            Ok(s) => s,
            Err(f) => return Response::ok(f),
        };
        match d.delete(k).await {
            Ok(true) => deleted += 1,
            Ok(false) => {}
            Err(e) => return Response::ok(map_client_error(e, "DM.DEL")),
        }
    }
    Response::ok(Frame::Integer(deleted))
}

pub(crate) async fn dm_expire(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    duration: Duration,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.expire(k, duration).await {
        Ok(()) => Response::ok(Frame::Integer(1)),
        Err(ClientError::KeyNotFound) => Response::ok(Frame::Integer(0)),
        Err(e) => Response::ok(map_client_error(e, "DM.EXPIRE")),
    }
}

pub(crate) async fn dm_incr(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    delta: i64,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.incr(k, delta).await {
        Ok(v) => Response::ok(Frame::Integer(v)),
        Err(e) => Response::ok(map_client_error(e, "DM.INCR")),
    }
}

pub(crate) async fn dm_decr(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    delta: i64,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.decr(k, delta).await {
        Ok(v) => Response::ok(Frame::Integer(v)),
        Err(e) => Response::ok(map_client_error(e, "DM.DECR")),
    }
}

pub(crate) async fn dm_get_put(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    value: &Bytes,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.get_put(k, value).await {
        Ok(Some(prev)) => Response::ok(Frame::Bulk(BulkString::from(prev.value))),
        Ok(None) => Response::ok(Frame::Bulk(BulkString::null())),
        Err(e) => Response::ok(map_client_error(e, "DM.GETPUT")),
    }
}

pub(crate) async fn dm_incr_by_float(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    key: &Bytes,
    delta: f64,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let k = match key_str(key) {
        Ok(s) => s,
        Err(f) => return Response::ok(f),
    };
    match d.incr_by_float(k, delta).await {
        Ok(v) => Response::ok(Frame::Bulk(BulkString::from(format!("{v}")))),
        Err(e) => Response::ok(map_client_error(e, "DM.INCRBYFLOAT")),
    }
}

pub(crate) async fn dm_destroy(client: &Arc<dyn Client>, dmap: &Bytes) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    match d.destroy().await {
        Ok(()) => Response::ok(Frame::ok()),
        Err(e) => Response::ok(map_client_error(e, "DM.DESTROY")),
    }
}

pub(crate) async fn dm_scan(
    client: &Arc<dyn Client>,
    dmap: &Bytes,
    partition_id: u32,
    _cursor: u64,
    options: kamino_protocol::ScanCommandOptions,
) -> Response {
    let d = match dmap_handle(client, dmap).await {
        Ok(d) => d,
        Err(f) => return Response::ok(f),
    };
    let match_pattern = match options.match_pattern {
        Some(p) => match String::from_utf8(p.to_vec()) {
            Ok(s) => Some(s),
            Err(e) => {
                return Response::ok(Frame::Error(format!("ERR invalid MATCH pattern: {e}")));
            }
        },
        None => None,
    };
    let scan_opts = ScanOptions {
        count: options.count.map(|c| c as usize),
        match_pattern,
    };
    let mut cursor = match d.scan(partition_id, scan_opts).await {
        Ok(c) => c,
        Err(e) => return Response::ok(map_client_error(e, "DM.SCAN")),
    };
    let mut items: Vec<Frame> = Vec::new();
    loop {
        match cursor.next().await {
            Ok(Some((k, v))) => {
                items.push(Frame::Bulk(BulkString::from(k)));
                items.push(Frame::Bulk(BulkString::from(v)));
            }
            Ok(None) => break,
            Err(e) => return Response::ok(map_client_error(e, "DM.SCAN")),
        }
    }
    let _ = cursor.close().await;
    // Phase 2: embedded cursor is materialised eagerly — return cursor "0"
    // (Redis convention for "iteration complete") plus the array.
    let pair = vec![
        Frame::Bulk(BulkString::from("0")),
        Frame::Array(Some(items)),
    ];
    Response::ok(Frame::Array(Some(pair)))
}
