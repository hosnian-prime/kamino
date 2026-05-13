//! Parsed command AST for every Phase 2 command (see
//! `docs/06-network-protocol.md` Command Set).
//!
//! Pub/sub, cluster (`CLUSTER.*`), `DM.LOCK*`, and `INTERNAL.NODE.*` land in
//! later phases; their variants are intentionally absent here so the dispatch
//! exhaustiveness check tells us when they show up.
//!
//! Phase 2 contract: variants and field names below are **frozen**. Adding
//! variants is backwards-compatible; renaming or removing is not.

use bytes::Bytes;

use crate::error::{ArityHint, CommandError};
use crate::frame::{BulkString, Frame};
use crate::hello::HelloArgs;

/// All commands the Phase 2 server understands.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    // --- Utility ---
    /// `PING [message]`.
    Ping(Option<Bytes>),
    /// `AUTH password` (legacy) or `AUTH username password` (Redis 6+).
    Auth {
        username: Option<Bytes>,
        password: Bytes,
    },
    /// `HELLO [protover] [AUTH ...] [SETNAME ...]`.
    Hello(HelloArgs),
    /// `QUIT`.
    Quit,
    /// `STATS`.
    Stats,

    // --- DMap data ops ---
    /// `DM.PUT dmap key value [options]`.
    DmPut {
        dmap: Bytes,
        key: Bytes,
        value: Bytes,
        options: PutCommandOptions,
    },
    /// `DM.GET dmap key`.
    DmGet { dmap: Bytes, key: Bytes },
    /// `DM.DEL dmap key [key ...]` — Phase 2 only accepts a single key per
    /// request; the multi-key cross-partition fan-out lands in Phase 4 per
    /// `docs/06-network-protocol.md` §"Multi-Key Operations".
    DmDel { dmap: Bytes, keys: Vec<Bytes> },
    /// `DM.EXPIRE dmap key seconds`.
    DmExpire {
        dmap: Bytes,
        key: Bytes,
        seconds: u64,
    },
    /// `DM.PEXPIRE dmap key milliseconds`.
    DmPexpire {
        dmap: Bytes,
        key: Bytes,
        milliseconds: u64,
    },
    /// `DM.INCR dmap key delta`.
    DmIncr { dmap: Bytes, key: Bytes, delta: i64 },
    /// `DM.DECR dmap key delta`.
    DmDecr { dmap: Bytes, key: Bytes, delta: i64 },
    /// `DM.GETPUT dmap key value`.
    DmGetPut {
        dmap: Bytes,
        key: Bytes,
        value: Bytes,
    },
    /// `DM.INCRBYFLOAT dmap key delta`.
    DmIncrByFloat { dmap: Bytes, key: Bytes, delta: f64 },
    /// `DM.DESTROY dmap`.
    DmDestroy { dmap: Bytes },
    /// `DM.SCAN partID dmap cursor [MATCH pat] [COUNT n]`.
    DmScan {
        partition_id: u32,
        dmap: Bytes,
        cursor: u64,
        options: ScanCommandOptions,
    },

    // --- Cluster (Phase 3+) ---
    /// `CLUSTER.MEMBERS` — returns the local view of cluster members.
    ClusterMembers,
    /// `CLUSTER.ROUTINGTABLE` — returns the local routing table
    /// (`MessagePack` payload wrapped in a single bulk string), or `+NORT`
    /// if no table has been built yet.
    ClusterRoutingTable,
    /// `CLUSTER.READY` — returns `+OK` iff the node has joined SWIM, has a
    /// routing table with signature > 0, and member count meets the quorum.
    /// Suitable for Kubernetes readiness probes.
    ClusterReady,
    /// `INTERNAL.NODE.UPDATEROUTING <msgpack-bytes>` — peer-to-peer routing
    /// table push. The receiver applies the table via the signature-clock
    /// gate; stale (`signature <= local`) tables are accepted-as-rejected
    /// with a `+STALE` reply, schema mismatches with a `+SCHEMA` reply,
    /// and applied tables with `+OK`.
    InternalNodeUpdateRouting { table: Bytes },
    /// `INTERNAL.NODE.LENGTHOFPART <partition_id>` — partition-size query
    /// used by the Phase 6 balancer. The Phase 4A server replies with `0`
    /// since the storage engine isn't partition-aware yet; the wire shape
    /// is frozen now so future versions add semantics, not arguments.
    InternalNodeLengthOfPart { partition_id: u32 },
    /// `INTERNAL.NODE.GETWITHTS <dmap> <key>` — Phase 5 read-quorum and
    /// read-repair fan-out. Returns either a 2-element array
    /// `[value, ts_unix_nanos]` (RESP integer) or a null bulk if the key
    /// is missing. Restricted to peers authenticated with
    /// `cluster_secret`; external clients never see this command shape.
    InternalNodeGetWithTs { dmap: Bytes, key: Bytes },
}

/// Optional flags for `DM.PUT`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutCommandOptions {
    /// `EX seconds`.
    pub ex: Option<u64>,
    /// `PX milliseconds`.
    pub px: Option<u64>,
    /// `EXAT unix-seconds`.
    pub exat: Option<u64>,
    /// `PXAT unix-milliseconds`.
    pub pxat: Option<u64>,
    /// `NX` — only set if missing.
    pub nx: bool,
    /// `XX` — only set if present.
    pub xx: bool,
    /// `TS unix-nanos` — client-supplied LWW timestamp override.
    pub timestamp: Option<i64>,
}

/// Optional flags for `DM.SCAN`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanCommandOptions {
    /// `MATCH pat` — glob pattern.
    pub match_pattern: Option<Bytes>,
    /// `COUNT n` — server-side hint.
    pub count: Option<u32>,
}

impl Command {
    /// Parse a top-level [`Frame::Array`] into a typed command.
    ///
    /// Errors:
    /// - [`CommandError::NotAnArray`] if the frame isn't an array of bulk
    ///   strings.
    /// - [`CommandError::UnknownCommand`] for unrecognised verbs.
    /// - [`CommandError::WrongArity`] / [`CommandError::InvalidArgument`]
    ///   for shape mismatches.
    pub fn parse(frame: Frame) -> Result<Self, CommandError> {
        let Frame::Array(Some(items)) = frame else {
            return Err(CommandError::NotAnArray);
        };
        if items.is_empty() {
            return Err(CommandError::NotAnArray);
        }
        let mut args = Vec::with_capacity(items.len());
        for item in items {
            match item {
                Frame::Bulk(BulkString(Some(b))) => args.push(b),
                Frame::Bulk(BulkString(None)) => {
                    return Err(CommandError::InvalidArgument {
                        command: "<unknown>",
                        reason: "null bulk in command array".into(),
                    });
                }
                _ => return Err(CommandError::NotAnArray),
            }
        }

        // Verb is args[0]. ASCII-uppercase a copy for case-insensitive
        // matching; bound the prefix we consider so an absurdly long bogus
        // verb doesn't force a large allocation.
        let verb_lower = &args[0];
        let mut verb_upper = [0_u8; 32];
        let n = verb_lower.len().min(verb_upper.len());
        verb_upper[..n].copy_from_slice(&verb_lower[..n]);
        for b in &mut verb_upper[..n] {
            b.make_ascii_uppercase();
        }
        let verb = &verb_upper[..n];

        // We pass `args` by value into each parser; `args[0]` is the verb
        // and `args[1..]` are the arguments.
        match verb {
            b"PING" => parse_ping(args),
            b"AUTH" => parse_auth(args),
            b"HELLO" => parse_hello(args),
            b"QUIT" => parse_quit(&args),
            b"STATS" => parse_stats(&args),
            b"DM.PUT" => parse_dm_put(args),
            b"DM.GET" => parse_dm_get(args),
            b"DM.DEL" => parse_dm_del(args),
            b"DM.EXPIRE" => parse_dm_expire(args),
            b"DM.PEXPIRE" => parse_dm_pexpire(args),
            b"DM.INCR" => parse_dm_incr(args),
            b"DM.DECR" => parse_dm_decr(args),
            b"DM.GETPUT" => parse_dm_getput(args),
            b"DM.INCRBYFLOAT" => parse_dm_incrbyfloat(args),
            b"DM.DESTROY" => parse_dm_destroy(args),
            b"DM.SCAN" => parse_dm_scan(args),
            b"CLUSTER.MEMBERS" => parse_cluster_members(&args),
            b"CLUSTER.ROUTINGTABLE" => parse_cluster_routing_table(&args),
            b"CLUSTER.READY" => parse_cluster_ready(&args),
            b"INTERNAL.NODE.UPDATEROUTING" => parse_internal_update_routing(args),
            b"INTERNAL.NODE.LENGTHOFPART" => parse_internal_length_of_part(args),
            b"INTERNAL.NODE.GETWITHTS" => parse_internal_get_with_ts(args),
            _ => Err(CommandError::UnknownCommand(
                String::from_utf8_lossy(verb_lower).into_owned(),
            )),
        }
    }

    /// Encode this command into a `Frame::Array` suitable for sending over
    /// the wire (client side). Inverse of [`Self::parse`].
    pub fn to_frame(&self) -> Frame {
        let parts: Vec<Frame> = match self {
            Self::Ping(None) => vec![bulk("PING")],
            Self::Ping(Some(msg)) => vec![
                bulk("PING"),
                Frame::Bulk(BulkString::from_bytes(msg.clone())),
            ],
            Self::Auth { username, password } => {
                let mut v = vec![bulk("AUTH")];
                if let Some(u) = username {
                    v.push(Frame::Bulk(BulkString::from_bytes(u.clone())));
                }
                v.push(Frame::Bulk(BulkString::from_bytes(password.clone())));
                v
            }
            Self::Hello(args) => hello_to_frame_parts(args),
            Self::Quit => vec![bulk("QUIT")],
            Self::Stats => vec![bulk("STATS")],

            Self::DmPut {
                dmap,
                key,
                value,
                options,
            } => {
                let mut v = vec![
                    bulk("DM.PUT"),
                    bulk_bytes(dmap),
                    bulk_bytes(key),
                    bulk_bytes(value),
                ];
                put_options_to_frames(options, &mut v);
                v
            }
            Self::DmGet { dmap, key } => vec![bulk("DM.GET"), bulk_bytes(dmap), bulk_bytes(key)],
            Self::DmDel { dmap, keys } => {
                let mut v = Vec::with_capacity(2 + keys.len());
                v.push(bulk("DM.DEL"));
                v.push(bulk_bytes(dmap));
                for k in keys {
                    v.push(bulk_bytes(k));
                }
                v
            }
            Self::DmExpire { dmap, key, seconds } => vec![
                bulk("DM.EXPIRE"),
                bulk_bytes(dmap),
                bulk_bytes(key),
                bulk(&seconds.to_string()),
            ],
            Self::DmPexpire {
                dmap,
                key,
                milliseconds,
            } => vec![
                bulk("DM.PEXPIRE"),
                bulk_bytes(dmap),
                bulk_bytes(key),
                bulk(&milliseconds.to_string()),
            ],
            Self::DmIncr { dmap, key, delta } => vec![
                bulk("DM.INCR"),
                bulk_bytes(dmap),
                bulk_bytes(key),
                bulk(&delta.to_string()),
            ],
            Self::DmDecr { dmap, key, delta } => vec![
                bulk("DM.DECR"),
                bulk_bytes(dmap),
                bulk_bytes(key),
                bulk(&delta.to_string()),
            ],
            Self::DmGetPut { dmap, key, value } => vec![
                bulk("DM.GETPUT"),
                bulk_bytes(dmap),
                bulk_bytes(key),
                bulk_bytes(value),
            ],
            Self::DmIncrByFloat { dmap, key, delta } => vec![
                bulk("DM.INCRBYFLOAT"),
                bulk_bytes(dmap),
                bulk_bytes(key),
                bulk(&format_float(*delta)),
            ],
            Self::DmDestroy { dmap } => vec![bulk("DM.DESTROY"), bulk_bytes(dmap)],
            Self::DmScan {
                partition_id,
                dmap,
                cursor,
                options,
            } => {
                let mut v = vec![
                    bulk("DM.SCAN"),
                    bulk(&partition_id.to_string()),
                    bulk_bytes(dmap),
                    bulk(&cursor.to_string()),
                ];
                if let Some(m) = &options.match_pattern {
                    v.push(bulk("MATCH"));
                    v.push(Frame::Bulk(BulkString::from_bytes(m.clone())));
                }
                if let Some(c) = options.count {
                    v.push(bulk("COUNT"));
                    v.push(bulk(&c.to_string()));
                }
                v
            }
            Self::ClusterMembers => vec![bulk("CLUSTER.MEMBERS")],
            Self::ClusterRoutingTable => vec![bulk("CLUSTER.ROUTINGTABLE")],
            Self::ClusterReady => vec![bulk("CLUSTER.READY")],
            Self::InternalNodeUpdateRouting { table } => vec![
                bulk("INTERNAL.NODE.UPDATEROUTING"),
                Frame::Bulk(BulkString::from_bytes(table.clone())),
            ],
            Self::InternalNodeLengthOfPart { partition_id } => vec![
                bulk("INTERNAL.NODE.LENGTHOFPART"),
                bulk(&partition_id.to_string()),
            ],
            Self::InternalNodeGetWithTs { dmap, key } => vec![
                bulk("INTERNAL.NODE.GETWITHTS"),
                bulk_bytes(dmap),
                bulk_bytes(key),
            ],
        };
        Frame::Array(Some(parts))
    }
}

// --- helpers ----------------------------------------------------------------

fn bulk(s: &str) -> Frame {
    Frame::Bulk(BulkString::from(s))
}

fn bulk_bytes(b: &Bytes) -> Frame {
    Frame::Bulk(BulkString::from_bytes(b.clone()))
}

fn hello_to_frame_parts(args: &HelloArgs) -> Vec<Frame> {
    let mut v = vec![bulk("HELLO")];
    if let Some(pv) = args.protocol_version {
        v.push(bulk(&pv.to_string()));
    }
    if let Some((user, pass)) = &args.auth {
        v.push(bulk("AUTH"));
        v.push(Frame::Bulk(BulkString::from_bytes(
            user.clone()
                .unwrap_or_else(|| Bytes::from_static(b"default")),
        )));
        v.push(Frame::Bulk(BulkString::from_bytes(pass.clone())));
    }
    if let Some(name) = &args.client_name {
        v.push(bulk("SETNAME"));
        v.push(Frame::Bulk(BulkString::from_bytes(name.clone())));
    }
    v
}

fn put_options_to_frames(opts: &PutCommandOptions, out: &mut Vec<Frame>) {
    if let Some(s) = opts.ex {
        out.push(bulk("EX"));
        out.push(bulk(&s.to_string()));
    }
    if let Some(ms) = opts.px {
        out.push(bulk("PX"));
        out.push(bulk(&ms.to_string()));
    }
    if let Some(s) = opts.exat {
        out.push(bulk("EXAT"));
        out.push(bulk(&s.to_string()));
    }
    if let Some(ms) = opts.pxat {
        out.push(bulk("PXAT"));
        out.push(bulk(&ms.to_string()));
    }
    if opts.nx {
        out.push(bulk("NX"));
    }
    if opts.xx {
        out.push(bulk("XX"));
    }
    if let Some(ts) = opts.timestamp {
        out.push(bulk("TS"));
        out.push(bulk(&ts.to_string()));
    }
}

/// Format a float for `DM.INCRBYFLOAT`. Use the default Display form which
/// preserves enough precision to round-trip via `f64::from_str`.
fn format_float(f: f64) -> String {
    if f.is_nan() {
        "nan".into()
    } else if f.is_infinite() {
        if f > 0.0 { "inf".into() } else { "-inf".into() }
    } else {
        format!("{f}")
    }
}

const fn arg_count(args: &[Bytes]) -> usize {
    args.len() - 1 // exclude verb
}

const fn require_exact(
    args: &[Bytes],
    command: &'static str,
    n: usize,
) -> Result<(), CommandError> {
    if arg_count(args) != n {
        return Err(CommandError::WrongArity {
            command,
            expected: ArityHint::Exactly(n),
            got: arg_count(args),
        });
    }
    Ok(())
}

const fn require_range(
    args: &[Bytes],
    command: &'static str,
    min: usize,
    max: usize,
) -> Result<(), CommandError> {
    let got = arg_count(args);
    if got < min || got > max {
        return Err(CommandError::WrongArity {
            command,
            expected: ArityHint::Range { min, max },
            got,
        });
    }
    Ok(())
}

const fn require_at_least(
    args: &[Bytes],
    command: &'static str,
    n: usize,
) -> Result<(), CommandError> {
    if arg_count(args) < n {
        return Err(CommandError::WrongArity {
            command,
            expected: ArityHint::AtLeast(n),
            got: arg_count(args),
        });
    }
    Ok(())
}

fn parse_int<T: std::str::FromStr>(
    bytes: &[u8],
    command: &'static str,
    field: &str,
) -> Result<T, CommandError> {
    let s = std::str::from_utf8(bytes).map_err(|_| CommandError::InvalidArgument {
        command,
        reason: format!("{field} not utf-8"),
    })?;
    s.parse::<T>().map_err(|_| CommandError::InvalidArgument {
        command,
        reason: format!("{field} not a valid integer ({s:?})"),
    })
}

fn parse_float(bytes: &[u8], command: &'static str, field: &str) -> Result<f64, CommandError> {
    let s = std::str::from_utf8(bytes).map_err(|_| CommandError::InvalidArgument {
        command,
        reason: format!("{field} not utf-8"),
    })?;
    let v: f64 = s.parse().map_err(|_| CommandError::InvalidArgument {
        command,
        reason: format!("{field} not a valid float ({s:?})"),
    })?;
    Ok(v)
}

/// Case-insensitive equality of a flag literal against an arg.
fn eq_ascii_ci(arg: &[u8], lit: &[u8]) -> bool {
    arg.eq_ignore_ascii_case(lit)
}

// --- per-command parsers ----------------------------------------------------

fn parse_ping(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_range(&args, "PING", 0, 1)?;
    if args.len() == 2 {
        Ok(Command::Ping(Some(args.into_iter().nth(1).unwrap())))
    } else {
        Ok(Command::Ping(None))
    }
}

fn parse_auth(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_range(&args, "AUTH", 1, 2)?;
    let mut it = args.into_iter();
    let _verb = it.next();
    if let Some(first) = it.next() {
        if let Some(second) = it.next() {
            return Ok(Command::Auth {
                username: Some(first),
                password: second,
            });
        }
        return Ok(Command::Auth {
            username: None,
            password: first,
        });
    }
    unreachable!("arity check guarantees ≥ 1 arg")
}

fn parse_hello(args: Vec<Bytes>) -> Result<Command, CommandError> {
    // HELLO has variable arity. Minimum: 0 args (just `HELLO`).
    // Walk the remaining tokens left-to-right.
    let mut hello = HelloArgs::default();
    let mut iter = args.into_iter();
    let _verb = iter.next();

    // First positional optional: protover.
    let mut peek: Option<Bytes> = iter.next();
    if let Some(b) = &peek {
        // It's the protover IFF it's a non-empty number (and not "AUTH"/"SETNAME").
        if !is_keyword(b, b"AUTH") && !is_keyword(b, b"SETNAME") {
            let pv: u32 = parse_int(b, "HELLO", "protover")?;
            hello.protocol_version = Some(pv);
            peek = iter.next();
        }
    }

    // Then any combination of AUTH and SETNAME clauses, in any order.
    while let Some(tok) = peek {
        if is_keyword(&tok, b"AUTH") {
            let user = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "HELLO",
                reason: "AUTH requires username and password".into(),
            })?;
            let pass = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "HELLO",
                reason: "AUTH requires username and password".into(),
            })?;
            hello.auth = Some((Some(user), pass));
        } else if is_keyword(&tok, b"SETNAME") {
            let name = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "HELLO",
                reason: "SETNAME requires a value".into(),
            })?;
            hello.client_name = Some(name);
        } else {
            return Err(CommandError::InvalidArgument {
                command: "HELLO",
                reason: format!("unexpected token {:?}", String::from_utf8_lossy(&tok)),
            });
        }
        peek = iter.next();
    }

    Ok(Command::Hello(hello))
}

fn is_keyword(arg: &[u8], lit: &[u8]) -> bool {
    eq_ascii_ci(arg, lit)
}

fn parse_quit(args: &[Bytes]) -> Result<Command, CommandError> {
    require_exact(args, "QUIT", 0)?;
    Ok(Command::Quit)
}

fn parse_stats(args: &[Bytes]) -> Result<Command, CommandError> {
    require_exact(args, "STATS", 0)?;
    Ok(Command::Stats)
}

fn parse_cluster_members(args: &[Bytes]) -> Result<Command, CommandError> {
    require_exact(args, "CLUSTER.MEMBERS", 0)?;
    Ok(Command::ClusterMembers)
}

fn parse_cluster_routing_table(args: &[Bytes]) -> Result<Command, CommandError> {
    require_exact(args, "CLUSTER.ROUTINGTABLE", 0)?;
    Ok(Command::ClusterRoutingTable)
}

fn parse_cluster_ready(args: &[Bytes]) -> Result<Command, CommandError> {
    require_exact(args, "CLUSTER.READY", 0)?;
    Ok(Command::ClusterReady)
}

fn parse_internal_update_routing(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "INTERNAL.NODE.UPDATEROUTING", 1)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let table = iter.next().unwrap();
    Ok(Command::InternalNodeUpdateRouting { table })
}

fn parse_internal_length_of_part(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "INTERNAL.NODE.LENGTHOFPART", 1)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let partition_raw = iter.next().unwrap();
    let partition_id = parse_int(&partition_raw, "INTERNAL.NODE.LENGTHOFPART", "partition_id")?;
    Ok(Command::InternalNodeLengthOfPart { partition_id })
}

fn parse_internal_get_with_ts(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "INTERNAL.NODE.GETWITHTS", 2)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    Ok(Command::InternalNodeGetWithTs {
        dmap: iter.next().unwrap(),
        key: iter.next().unwrap(),
    })
}

fn parse_dm_put(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_at_least(&args, "DM.PUT", 3)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let dmap = iter.next().unwrap();
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();

    let mut opts = PutCommandOptions::default();
    while let Some(tok) = iter.next() {
        if eq_ascii_ci(&tok, b"EX") {
            let v = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "DM.PUT",
                reason: "EX requires a value".into(),
            })?;
            if opts.ex.is_some() {
                return Err(CommandError::ConflictingOptions {
                    command: "DM.PUT",
                    detail: "EX specified more than once",
                });
            }
            opts.ex = Some(parse_int(&v, "DM.PUT", "EX")?);
        } else if eq_ascii_ci(&tok, b"PX") {
            let v = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "DM.PUT",
                reason: "PX requires a value".into(),
            })?;
            if opts.px.is_some() {
                return Err(CommandError::ConflictingOptions {
                    command: "DM.PUT",
                    detail: "PX specified more than once",
                });
            }
            opts.px = Some(parse_int(&v, "DM.PUT", "PX")?);
        } else if eq_ascii_ci(&tok, b"EXAT") {
            let v = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "DM.PUT",
                reason: "EXAT requires a value".into(),
            })?;
            if opts.exat.is_some() {
                return Err(CommandError::ConflictingOptions {
                    command: "DM.PUT",
                    detail: "EXAT specified more than once",
                });
            }
            opts.exat = Some(parse_int(&v, "DM.PUT", "EXAT")?);
        } else if eq_ascii_ci(&tok, b"PXAT") {
            let v = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "DM.PUT",
                reason: "PXAT requires a value".into(),
            })?;
            if opts.pxat.is_some() {
                return Err(CommandError::ConflictingOptions {
                    command: "DM.PUT",
                    detail: "PXAT specified more than once",
                });
            }
            opts.pxat = Some(parse_int(&v, "DM.PUT", "PXAT")?);
        } else if eq_ascii_ci(&tok, b"NX") {
            opts.nx = true;
        } else if eq_ascii_ci(&tok, b"XX") {
            opts.xx = true;
        } else if eq_ascii_ci(&tok, b"TS") {
            let v = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "DM.PUT",
                reason: "TS requires a value".into(),
            })?;
            if opts.timestamp.is_some() {
                return Err(CommandError::ConflictingOptions {
                    command: "DM.PUT",
                    detail: "TS specified more than once",
                });
            }
            opts.timestamp = Some(parse_int(&v, "DM.PUT", "TS")?);
        } else {
            return Err(CommandError::InvalidArgument {
                command: "DM.PUT",
                reason: format!("unknown option {:?}", String::from_utf8_lossy(&tok)),
            });
        }
    }

    if opts.nx && opts.xx {
        return Err(CommandError::ConflictingOptions {
            command: "DM.PUT",
            detail: "NX and XX are mutually exclusive",
        });
    }
    let expiry_count = [
        opts.ex.is_some(),
        opts.px.is_some(),
        opts.exat.is_some(),
        opts.pxat.is_some(),
    ]
    .iter()
    .filter(|b| **b)
    .count();
    if expiry_count > 1 {
        return Err(CommandError::ConflictingOptions {
            command: "DM.PUT",
            detail: "EX, PX, EXAT and PXAT are mutually exclusive",
        });
    }

    Ok(Command::DmPut {
        dmap,
        key,
        value,
        options: opts,
    })
}

fn parse_dm_get(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "DM.GET", 2)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    Ok(Command::DmGet {
        dmap: iter.next().unwrap(),
        key: iter.next().unwrap(),
    })
}

fn parse_dm_del(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_at_least(&args, "DM.DEL", 2)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let dmap = iter.next().unwrap();
    let keys: Vec<Bytes> = iter.collect();
    Ok(Command::DmDel { dmap, keys })
}

fn parse_dm_expire(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "DM.EXPIRE", 3)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let dmap = iter.next().unwrap();
    let key = iter.next().unwrap();
    let seconds_raw = iter.next().unwrap();
    let seconds = parse_int(&seconds_raw, "DM.EXPIRE", "seconds")?;
    Ok(Command::DmExpire { dmap, key, seconds })
}

fn parse_dm_pexpire(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "DM.PEXPIRE", 3)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let dmap = iter.next().unwrap();
    let key = iter.next().unwrap();
    let ms_raw = iter.next().unwrap();
    let milliseconds = parse_int(&ms_raw, "DM.PEXPIRE", "milliseconds")?;
    Ok(Command::DmPexpire {
        dmap,
        key,
        milliseconds,
    })
}

fn parse_dm_incr(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "DM.INCR", 3)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let dmap = iter.next().unwrap();
    let key = iter.next().unwrap();
    let delta_raw = iter.next().unwrap();
    let delta = parse_int(&delta_raw, "DM.INCR", "delta")?;
    Ok(Command::DmIncr { dmap, key, delta })
}

fn parse_dm_decr(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "DM.DECR", 3)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let dmap = iter.next().unwrap();
    let key = iter.next().unwrap();
    let delta_raw = iter.next().unwrap();
    let delta = parse_int(&delta_raw, "DM.DECR", "delta")?;
    Ok(Command::DmDecr { dmap, key, delta })
}

fn parse_dm_getput(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "DM.GETPUT", 3)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    Ok(Command::DmGetPut {
        dmap: iter.next().unwrap(),
        key: iter.next().unwrap(),
        value: iter.next().unwrap(),
    })
}

fn parse_dm_incrbyfloat(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "DM.INCRBYFLOAT", 3)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let dmap = iter.next().unwrap();
    let key = iter.next().unwrap();
    let delta_raw = iter.next().unwrap();
    let delta = parse_float(&delta_raw, "DM.INCRBYFLOAT", "delta")?;
    Ok(Command::DmIncrByFloat { dmap, key, delta })
}

fn parse_dm_destroy(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_exact(&args, "DM.DESTROY", 1)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    Ok(Command::DmDestroy {
        dmap: iter.next().unwrap(),
    })
}

fn parse_dm_scan(args: Vec<Bytes>) -> Result<Command, CommandError> {
    require_at_least(&args, "DM.SCAN", 3)?;
    let mut iter = args.into_iter();
    let _verb = iter.next();
    let part_raw = iter.next().unwrap();
    let dmap = iter.next().unwrap();
    let cursor_raw = iter.next().unwrap();
    let partition_id = parse_int(&part_raw, "DM.SCAN", "partID")?;
    let cursor = parse_int(&cursor_raw, "DM.SCAN", "cursor")?;

    let mut opts = ScanCommandOptions::default();
    while let Some(tok) = iter.next() {
        if eq_ascii_ci(&tok, b"MATCH") {
            let pat = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "DM.SCAN",
                reason: "MATCH requires a pattern".into(),
            })?;
            if opts.match_pattern.is_some() {
                return Err(CommandError::ConflictingOptions {
                    command: "DM.SCAN",
                    detail: "MATCH specified more than once",
                });
            }
            opts.match_pattern = Some(pat);
        } else if eq_ascii_ci(&tok, b"COUNT") {
            let c = iter.next().ok_or_else(|| CommandError::InvalidArgument {
                command: "DM.SCAN",
                reason: "COUNT requires a value".into(),
            })?;
            if opts.count.is_some() {
                return Err(CommandError::ConflictingOptions {
                    command: "DM.SCAN",
                    detail: "COUNT specified more than once",
                });
            }
            opts.count = Some(parse_int(&c, "DM.SCAN", "COUNT")?);
        } else {
            return Err(CommandError::InvalidArgument {
                command: "DM.SCAN",
                reason: format!("unknown option {:?}", String::from_utf8_lossy(&tok)),
            });
        }
    }

    Ok(Command::DmScan {
        partition_id,
        dmap,
        cursor,
        options: opts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use proptest::prelude::*;

    fn arr(items: &[&[u8]]) -> Frame {
        Frame::Array(Some(
            items
                .iter()
                .map(|b| Frame::Bulk(BulkString::from(*b)))
                .collect(),
        ))
    }

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn parse_ping_no_arg() {
        let frame = arr(&[b"PING"]);
        assert_eq!(Command::parse(frame).unwrap(), Command::Ping(None));
    }

    #[test]
    fn parse_ping_with_arg() {
        let frame = arr(&[b"PING", b"hello"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::Ping(Some(b("hello")))
        );
    }

    #[test]
    fn parse_ping_too_many() {
        let frame = arr(&[b"PING", b"a", b"b"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::WrongArity {
                command: "PING",
                ..
            })
        );
    }

    #[test]
    fn case_insensitive_verb() {
        let frame = arr(&[b"pInG"]);
        assert_eq!(Command::parse(frame).unwrap(), Command::Ping(None));
    }

    #[test]
    fn parse_auth_legacy() {
        let frame = arr(&[b"AUTH", b"sekret"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::Auth {
                username: None,
                password: b("sekret"),
            }
        );
    }

    #[test]
    fn parse_auth_user_pass() {
        let frame = arr(&[b"AUTH", b"alice", b"sekret"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::Auth {
                username: Some(b("alice")),
                password: b("sekret"),
            }
        );
    }

    #[test]
    fn parse_auth_no_args_rejected() {
        let frame = arr(&[b"AUTH"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::WrongArity {
                command: "AUTH",
                ..
            })
        );
    }

    #[test]
    fn parse_hello_bare() {
        let frame = arr(&[b"HELLO"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::Hello(HelloArgs::default())
        );
    }

    #[test]
    fn parse_hello_protover() {
        let frame = arr(&[b"HELLO", b"3"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::Hello(HelloArgs::versioned(3))
        );
    }

    #[test]
    fn parse_hello_full() {
        let frame = arr(&[
            b"HELLO", b"3", b"AUTH", b"alice", b"sekret", b"SETNAME", b"app",
        ]);
        let expected = Command::Hello(HelloArgs {
            protocol_version: Some(3),
            auth: Some((Some(b("alice")), b("sekret"))),
            client_name: Some(b("app")),
        });
        assert_eq!(Command::parse(frame).unwrap(), expected);
    }

    #[test]
    fn parse_hello_setname_only() {
        let frame = arr(&[b"HELLO", b"SETNAME", b"app"]);
        let expected = Command::Hello(HelloArgs {
            protocol_version: None,
            auth: None,
            client_name: Some(b("app")),
        });
        assert_eq!(Command::parse(frame).unwrap(), expected);
    }

    #[test]
    fn parse_hello_setname_missing_value() {
        let frame = arr(&[b"HELLO", b"SETNAME"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::InvalidArgument {
                command: "HELLO",
                ..
            })
        );
    }

    #[test]
    fn parse_quit_stats() {
        assert_eq!(Command::parse(arr(&[b"QUIT"])).unwrap(), Command::Quit);
        assert_eq!(Command::parse(arr(&[b"STATS"])).unwrap(), Command::Stats);
    }

    #[test]
    fn parse_dm_put_minimal() {
        let frame = arr(&[b"DM.PUT", b"dm", b"k", b"v"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmPut {
                dmap: b("dm"),
                key: b("k"),
                value: b("v"),
                options: PutCommandOptions::default(),
            }
        );
    }

    #[test]
    fn parse_dm_put_all_options() {
        let frame = arr(&[
            b"DM.PUT", b"dm", b"k", b"v", b"EX", b"30", b"NX", b"TS", b"123",
        ]);
        let Command::DmPut { options, .. } = Command::parse(frame).unwrap() else {
            panic!();
        };
        assert_eq!(options.ex, Some(30));
        assert!(options.nx);
        assert_eq!(options.timestamp, Some(123));
    }

    #[test]
    fn parse_dm_put_nx_xx_conflict() {
        let frame = arr(&[b"DM.PUT", b"dm", b"k", b"v", b"NX", b"XX"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::ConflictingOptions {
                command: "DM.PUT",
                detail: "NX and XX are mutually exclusive"
            })
        );
    }

    #[test]
    fn parse_dm_put_expiry_conflict() {
        let frame = arr(&[b"DM.PUT", b"dm", b"k", b"v", b"EX", b"1", b"PX", b"1000"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::ConflictingOptions {
                command: "DM.PUT",
                detail: "EX, PX, EXAT and PXAT are mutually exclusive"
            })
        );
    }

    #[test]
    fn parse_dm_put_expiry_conflict_exat_pxat() {
        let frame = arr(&[
            b"DM.PUT", b"dm", b"k", b"v", b"EXAT", b"1", b"PXAT", b"1000",
        ]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::ConflictingOptions {
                command: "DM.PUT",
                ..
            })
        );
    }

    #[test]
    fn parse_dm_put_unknown_option() {
        let frame = arr(&[b"DM.PUT", b"dm", b"k", b"v", b"WAT"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::InvalidArgument {
                command: "DM.PUT",
                ..
            })
        );
    }

    #[test]
    fn parse_dm_put_ex_missing_value() {
        let frame = arr(&[b"DM.PUT", b"dm", b"k", b"v", b"EX"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::InvalidArgument {
                command: "DM.PUT",
                ..
            })
        );
    }

    #[test]
    fn parse_dm_put_duplicate_option() {
        let frame = arr(&[b"DM.PUT", b"dm", b"k", b"v", b"EX", b"1", b"EX", b"2"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::ConflictingOptions {
                command: "DM.PUT",
                ..
            })
        );
    }

    #[test]
    fn parse_dm_put_ex_not_int() {
        let frame = arr(&[b"DM.PUT", b"dm", b"k", b"v", b"EX", b"oops"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::InvalidArgument {
                command: "DM.PUT",
                ..
            })
        );
    }

    #[test]
    fn parse_dm_get() {
        let frame = arr(&[b"DM.GET", b"dm", b"k"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmGet {
                dmap: b("dm"),
                key: b("k"),
            }
        );
    }

    #[test]
    fn parse_dm_get_wrong_arity() {
        let frame = arr(&[b"DM.GET", b"dm"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::WrongArity {
                command: "DM.GET",
                ..
            })
        );
    }

    #[test]
    fn parse_dm_del_single() {
        let frame = arr(&[b"DM.DEL", b"dm", b"k1"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmDel {
                dmap: b("dm"),
                keys: vec![b("k1")],
            }
        );
    }

    #[test]
    fn parse_dm_del_multi() {
        let frame = arr(&[b"DM.DEL", b"dm", b"k1", b"k2", b"k3"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmDel {
                dmap: b("dm"),
                keys: vec![b("k1"), b("k2"), b("k3")],
            }
        );
    }

    #[test]
    fn parse_dm_del_no_key() {
        let frame = arr(&[b"DM.DEL", b"dm"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::WrongArity {
                command: "DM.DEL",
                ..
            })
        );
    }

    #[test]
    fn parse_dm_expire_pexpire() {
        let frame = arr(&[b"DM.EXPIRE", b"dm", b"k", b"30"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmExpire {
                dmap: b("dm"),
                key: b("k"),
                seconds: 30
            }
        );
        let frame = arr(&[b"DM.PEXPIRE", b"dm", b"k", b"30000"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmPexpire {
                dmap: b("dm"),
                key: b("k"),
                milliseconds: 30_000
            }
        );
    }

    #[test]
    fn parse_dm_incr_decr() {
        let frame = arr(&[b"DM.INCR", b"dm", b"k", b"5"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmIncr {
                dmap: b("dm"),
                key: b("k"),
                delta: 5,
            }
        );
        let frame = arr(&[b"DM.DECR", b"dm", b"k", b"-2"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmDecr {
                dmap: b("dm"),
                key: b("k"),
                delta: -2,
            }
        );
    }

    #[test]
    fn parse_dm_getput() {
        let frame = arr(&[b"DM.GETPUT", b"dm", b"k", b"new"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmGetPut {
                dmap: b("dm"),
                key: b("k"),
                value: b("new"),
            }
        );
    }

    #[test]
    fn parse_dm_incrbyfloat() {
        let frame = arr(&[b"DM.INCRBYFLOAT", b"dm", b"k", b"2.5"]);
        let Command::DmIncrByFloat { delta, .. } = Command::parse(frame).unwrap() else {
            panic!();
        };
        assert!((delta - 2.5).abs() < 1e-9);
    }

    #[test]
    fn parse_dm_destroy() {
        let frame = arr(&[b"DM.DESTROY", b"dm"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmDestroy { dmap: b("dm") }
        );
    }

    #[test]
    fn parse_dm_scan_minimal() {
        let frame = arr(&[b"DM.SCAN", b"0", b"dm", b"0"]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmScan {
                partition_id: 0,
                dmap: b("dm"),
                cursor: 0,
                options: ScanCommandOptions::default(),
            }
        );
    }

    #[test]
    fn parse_dm_scan_with_match_count() {
        let frame = arr(&[
            b"DM.SCAN", b"5", b"dm", b"123", b"MATCH", b"foo*", b"COUNT", b"100",
        ]);
        assert_eq!(
            Command::parse(frame).unwrap(),
            Command::DmScan {
                partition_id: 5,
                dmap: b("dm"),
                cursor: 123,
                options: ScanCommandOptions {
                    match_pattern: Some(b("foo*")),
                    count: Some(100),
                },
            }
        );
    }

    #[test]
    fn parse_dm_scan_duplicate_match() {
        let frame = arr(&[
            b"DM.SCAN", b"0", b"dm", b"0", b"MATCH", b"a", b"MATCH", b"b",
        ]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::ConflictingOptions {
                command: "DM.SCAN",
                ..
            })
        );
    }

    #[test]
    fn parse_dm_scan_bad_int() {
        let frame = arr(&[b"DM.SCAN", b"notanumber", b"dm", b"0"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::InvalidArgument {
                command: "DM.SCAN",
                ..
            })
        );
    }

    #[test]
    fn parse_cluster_routing_table() {
        let frame = arr(&[b"CLUSTER.ROUTINGTABLE"]);
        assert_eq!(Command::parse(frame).unwrap(), Command::ClusterRoutingTable);
    }

    #[test]
    fn parse_cluster_ready() {
        let frame = arr(&[b"CLUSTER.READY"]);
        assert_eq!(Command::parse(frame).unwrap(), Command::ClusterReady);
    }

    #[test]
    fn parse_internal_update_routing_roundtrip() {
        let cmd = Command::InternalNodeUpdateRouting {
            table: Bytes::from_static(b"\x82\xa4ping\x01"),
        };
        assert_eq!(Command::parse(cmd.to_frame()).unwrap(), cmd);
    }

    #[test]
    fn parse_internal_update_routing_wrong_arity() {
        let frame = arr(&[b"INTERNAL.NODE.UPDATEROUTING"]);
        assert_matches!(Command::parse(frame), Err(CommandError::WrongArity { .. }));
    }

    #[test]
    fn parse_internal_length_of_part_roundtrip() {
        let cmd = Command::InternalNodeLengthOfPart { partition_id: 42 };
        assert_eq!(Command::parse(cmd.to_frame()).unwrap(), cmd);
    }

    #[test]
    fn parse_internal_length_of_part_bad_int() {
        let frame = arr(&[b"INTERNAL.NODE.LENGTHOFPART", b"abc"]);
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::InvalidArgument { .. })
        );
    }

    #[test]
    fn parse_internal_get_with_ts_roundtrip() {
        let cmd = Command::InternalNodeGetWithTs {
            dmap: b("sessions"),
            key: b("u1"),
        };
        assert_eq!(Command::parse(cmd.to_frame()).unwrap(), cmd);
    }

    #[test]
    fn parse_internal_get_with_ts_wrong_arity() {
        let frame = arr(&[b"INTERNAL.NODE.GETWITHTS", b"only-dmap"]);
        assert_matches!(Command::parse(frame), Err(CommandError::WrongArity { .. }));
    }

    #[test]
    fn parse_unknown_verb() {
        let frame = arr(&[b"NOPE"]);
        assert_matches!(Command::parse(frame), Err(CommandError::UnknownCommand(s)) if s == "NOPE");
    }

    #[test]
    fn parse_not_an_array() {
        assert_matches!(
            Command::parse(Frame::SimpleString("hi".into())),
            Err(CommandError::NotAnArray)
        );
        assert_matches!(
            Command::parse(Frame::Array(None)),
            Err(CommandError::NotAnArray)
        );
        assert_matches!(
            Command::parse(Frame::Array(Some(vec![]))),
            Err(CommandError::NotAnArray)
        );
    }

    #[test]
    fn parse_array_with_non_bulk() {
        let frame = Frame::Array(Some(vec![Frame::Integer(1)]));
        assert_matches!(Command::parse(frame), Err(CommandError::NotAnArray));
    }

    #[test]
    fn parse_array_with_null_bulk() {
        let frame = Frame::Array(Some(vec![
            Frame::Bulk(BulkString::from("PING")),
            Frame::Bulk(BulkString::null()),
        ]));
        assert_matches!(
            Command::parse(frame),
            Err(CommandError::InvalidArgument { .. })
        );
    }

    #[test]
    fn roundtrip_ping() {
        let cmd = Command::Ping(Some(b("hi")));
        assert_eq!(Command::parse(cmd.to_frame()).unwrap(), cmd);
    }

    #[test]
    fn roundtrip_hello_full() {
        let cmd = Command::Hello(HelloArgs {
            protocol_version: Some(3),
            auth: Some((Some(b("alice")), b("pw"))),
            client_name: Some(b("app")),
        });
        assert_eq!(Command::parse(cmd.to_frame()).unwrap(), cmd);
    }

    #[test]
    fn roundtrip_dm_put_full_options() {
        let cmd = Command::DmPut {
            dmap: b("dm"),
            key: b("k"),
            value: b("v"),
            options: PutCommandOptions {
                ex: Some(60),
                px: None,
                exat: None,
                pxat: None,
                nx: true,
                xx: false,
                timestamp: Some(-42),
            },
        };
        assert_eq!(Command::parse(cmd.to_frame()).unwrap(), cmd);
    }

    #[test]
    fn roundtrip_dm_scan_with_opts() {
        let cmd = Command::DmScan {
            partition_id: 7,
            dmap: b("dm"),
            cursor: 99,
            options: ScanCommandOptions {
                match_pattern: Some(b("p*")),
                count: Some(20),
            },
        };
        assert_eq!(Command::parse(cmd.to_frame()).unwrap(), cmd);
    }

    // -------- proptest: roundtrip across every variant ----------------------

    // Generators are bounded so test runs stay fast.
    fn bytes_strategy() -> impl Strategy<Value = Bytes> {
        // Avoid CRLF inside arguments — they're binary-safe at the wire layer
        // but argument-level keywords are matched case-insensitively, and a
        // pathological argument like "EX" would shadow option parsing. We
        // accept any bytes ≤ 64 here; the option keywords are looked up only
        // among the tail tokens so binary keys/values are fine.
        prop::collection::vec(any::<u8>(), 0..=64).prop_map(Bytes::from)
    }

    fn put_opts_strategy() -> impl Strategy<Value = PutCommandOptions> {
        (
            prop::option::of(any::<u64>()),
            prop::option::of(any::<u64>()),
            prop::option::of(any::<u64>()),
            prop::option::of(any::<u64>()),
            any::<bool>(),
            any::<bool>(),
            prop::option::of(any::<i64>()),
        )
            .prop_map(|(ex, px, exat, pxat, nx, xx, ts)| {
                // Resolve conflicts by keeping at most one expiry and dropping
                // XX if NX is set (the parser is the source of truth).
                let mut opts = PutCommandOptions {
                    ex,
                    px: None,
                    exat: None,
                    pxat: None,
                    nx,
                    xx: if nx { false } else { xx },
                    timestamp: ts,
                };
                if opts.ex.is_some() {
                    // keep
                } else if px.is_some() {
                    opts.px = px;
                } else if exat.is_some() {
                    opts.exat = exat;
                } else if pxat.is_some() {
                    opts.pxat = pxat;
                }
                opts
            })
    }

    fn scan_opts_strategy() -> impl Strategy<Value = ScanCommandOptions> {
        (
            prop::option::of(bytes_strategy()),
            prop::option::of(any::<u32>()),
        )
            .prop_map(|(mp, count)| ScanCommandOptions {
                match_pattern: mp,
                count,
            })
    }

    fn command_strategy() -> impl Strategy<Value = Command> {
        prop_oneof![
            prop::option::of(bytes_strategy()).prop_map(Command::Ping),
            (prop::option::of(bytes_strategy()), bytes_strategy()).prop_map(|(u, p)| {
                Command::Auth {
                    username: u,
                    password: p,
                }
            }),
            Just(Command::Quit),
            Just(Command::Stats),
            // HELLO — protover only optionally; AUTH/SETNAME independently.
            (
                prop::option::of(any::<u32>()),
                prop::option::of((bytes_strategy(), bytes_strategy())),
                prop::option::of(bytes_strategy()),
            )
                .prop_map(|(pv, auth, name)| {
                    Command::Hello(HelloArgs {
                        protocol_version: pv,
                        auth: auth.map(|(u, p)| (Some(u), p)),
                        client_name: name,
                    })
                }),
            (
                bytes_strategy(),
                bytes_strategy(),
                bytes_strategy(),
                put_opts_strategy()
            )
                .prop_map(|(dmap, key, value, options)| Command::DmPut {
                    dmap,
                    key,
                    value,
                    options
                }),
            (bytes_strategy(), bytes_strategy())
                .prop_map(|(dmap, key)| Command::DmGet { dmap, key }),
            (
                bytes_strategy(),
                prop::collection::vec(bytes_strategy(), 1..=4)
            )
                .prop_map(|(dmap, keys)| Command::DmDel { dmap, keys }),
            (bytes_strategy(), bytes_strategy(), any::<u64>())
                .prop_map(|(dmap, key, seconds)| Command::DmExpire { dmap, key, seconds }),
            (bytes_strategy(), bytes_strategy(), any::<u64>()).prop_map(|(dmap, key, ms)| {
                Command::DmPexpire {
                    dmap,
                    key,
                    milliseconds: ms,
                }
            }),
            (bytes_strategy(), bytes_strategy(), any::<i64>())
                .prop_map(|(dmap, key, delta)| Command::DmIncr { dmap, key, delta }),
            (bytes_strategy(), bytes_strategy(), any::<i64>())
                .prop_map(|(dmap, key, delta)| Command::DmDecr { dmap, key, delta }),
            (bytes_strategy(), bytes_strategy(), bytes_strategy())
                .prop_map(|(dmap, key, value)| Command::DmGetPut { dmap, key, value }),
            // Floats: avoid NaN since NaN != NaN breaks the equality assertion.
            (
                bytes_strategy(),
                bytes_strategy(),
                proptest::num::f64::NORMAL | proptest::num::f64::ZERO,
            )
                .prop_map(|(dmap, key, delta)| Command::DmIncrByFloat {
                    dmap,
                    key,
                    delta
                }),
            bytes_strategy().prop_map(|dmap| Command::DmDestroy { dmap }),
            (
                any::<u32>(),
                bytes_strategy(),
                any::<u64>(),
                scan_opts_strategy(),
            )
                .prop_map(|(partition_id, dmap, cursor, options)| Command::DmScan {
                    partition_id,
                    dmap,
                    cursor,
                    options,
                }),
            Just(Command::ClusterMembers),
            Just(Command::ClusterRoutingTable),
            Just(Command::ClusterReady),
            bytes_strategy().prop_map(|table| Command::InternalNodeUpdateRouting { table }),
            any::<u32>()
                .prop_map(|partition_id| Command::InternalNodeLengthOfPart { partition_id }),
            (bytes_strategy(), bytes_strategy())
                .prop_map(|(dmap, key)| Command::InternalNodeGetWithTs { dmap, key }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        #[test]
        fn command_roundtrip(cmd in command_strategy()) {
            let frame = cmd.to_frame();
            let back = Command::parse(frame).expect("parse must succeed");
            prop_assert_eq!(back, cmd);
        }
    }
}
