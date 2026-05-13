//! `tokio_util::codec::{Encoder, Decoder}` implementation for RESP2/3.
//!
//! The codec is **stateful** in exactly one dimension: the negotiated
//! protocol version. RESP3-only frame types are rejected on encode while
//! the version is `Resp2`. Decode accepts only RESP2 framing until upgraded;
//! after `HELLO 3` the decoder also recognises RESP3-specific prefixes.

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::ProtocolError;
use crate::frame::{BulkString, Frame};

/// Negotiated wire-protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// RESP2 baseline. Default for a freshly-accepted connection.
    #[default]
    Resp2,
    /// Upgraded after a successful `HELLO 3`.
    Resp3,
}

/// Maximum bulk-string size accepted by `decode`. Defends against a
/// malicious client claiming `$9223372036854775807\r\n`.
pub const DEFAULT_MAX_BULK_SIZE: u64 = 512 * 1024 * 1024; // 512 MiB

/// Maximum array length (number of elements). Same rationale as above.
pub const DEFAULT_MAX_ARRAY_LEN: u64 = 1_048_576; // 2^20

/// Maximum nesting depth. RESP frames can nest (arrays inside arrays, maps
/// of arrays, etc.). Bound recursion to keep the stack predictable.
const MAX_DEPTH: usize = 32;

/// RESP codec implementing `tokio_util::codec::{Encoder, Decoder}`.
#[derive(Debug)]
pub struct RespCodec {
    /// Current negotiated version. Server flips this after a successful
    /// `HELLO 3`; client-side codecs typically set it themselves.
    pub(crate) version: ProtocolVersion,
    /// Maximum acceptable bulk size on decode.
    pub(crate) max_bulk_size: u64,
    /// Maximum acceptable array length on decode.
    pub(crate) max_array_len: u64,
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl RespCodec {
    /// Construct a fresh codec in `Resp2` mode with default limits.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            version: ProtocolVersion::Resp2,
            max_bulk_size: DEFAULT_MAX_BULK_SIZE,
            max_array_len: DEFAULT_MAX_ARRAY_LEN,
        }
    }

    /// Currently negotiated protocol version.
    #[must_use]
    pub const fn version(&self) -> ProtocolVersion {
        self.version
    }

    /// Mark the codec as RESP3 (call after a successful HELLO 3).
    pub const fn upgrade_to_resp3(&mut self) {
        self.version = ProtocolVersion::Resp3;
    }

    /// Set the maximum bulk size accepted on decode.
    pub const fn set_max_bulk_size(&mut self, bytes: u64) {
        self.max_bulk_size = bytes;
    }

    /// Set the maximum array length accepted on decode.
    pub const fn set_max_array_len(&mut self, len: u64) {
        self.max_array_len = len;
    }
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        if src.is_empty() {
            return Ok(None);
        }

        // Inline-form PING fast path: redis-cli connects with `PING\r\n`.
        if let Some(consumed) = match_inline_ping(src) {
            src.advance_by(consumed);
            return Ok(Some(Frame::Array(Some(vec![Frame::Bulk(
                BulkString::from("PING"),
            )]))));
        }

        // Reject any other inline command up front. Inline commands have no
        // `*`/`$`/`+`/`-`/`:`/RESP3 prefix.
        if !is_known_prefix(src[0], self.version) {
            // If the first byte is a printable ASCII letter we treat it as
            // an attempted inline command rather than garbage framing — that
            // gives a clearer error to clients that accidentally connect in
            // inline mode.
            if src[0].is_ascii_alphabetic() {
                return Err(ProtocolError::InlineCommandRejected(
                    "only PING is accepted inline",
                ));
            }
            return Err(ProtocolError::InvalidEncoding(format!(
                "unexpected leading byte 0x{:02x}",
                src[0]
            )));
        }

        match parse_frame(
            src.as_ref(),
            self.version,
            self.max_bulk_size,
            self.max_array_len,
            0,
        ) {
            Ok((frame, consumed)) => {
                src.advance_by(consumed);
                Ok(Some(frame))
            }
            Err(ProtocolError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = ProtocolError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        encode_frame(&frame, self.version, dst)
    }
}

// Allow encoding by reference so the server can re-use frames without moving.
impl Encoder<&Frame> for RespCodec {
    type Error = ProtocolError;

    fn encode(&mut self, frame: &Frame, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        encode_frame(frame, self.version, dst)
    }
}

// --- Tiny helper trait so we can advance a BytesMut without pulling Buf into scope at the call site. ---
trait AdvanceBy {
    fn advance_by(&mut self, n: usize);
}
impl AdvanceBy for BytesMut {
    fn advance_by(&mut self, n: usize) {
        use bytes::Buf;
        Buf::advance(self, n);
    }
}

// --------------- Decoder internals ---------------

fn is_known_prefix(b: u8, version: ProtocolVersion) -> bool {
    match b {
        b'+' | b'-' | b':' | b'$' | b'*' => true,
        b'%' | b'~' | b'>' | b'#' | b',' | b'_' | b'(' => version == ProtocolVersion::Resp3,
        _ => false,
    }
}

/// Match the inline-PING shortcut. Accept `PING\r\n` and `PING\n` (the
/// latter is what some interactive clients emit). Returns the number of
/// bytes consumed.
fn match_inline_ping(buf: &[u8]) -> Option<usize> {
    if buf.len() < 5 {
        return None;
    }
    // Compare case-insensitively against "PING".
    let head = &buf[..4];
    if !head.eq_ignore_ascii_case(b"PING") {
        return None;
    }
    if buf[4] == b'\n' {
        return Some(5);
    }
    if buf.len() >= 6 && buf[4] == b'\r' && buf[5] == b'\n' {
        return Some(6);
    }
    None
}

/// Parse a single frame from `buf`. Returns `(frame, consumed_bytes)` or
/// [`ProtocolError::Incomplete`] when more bytes are needed.
fn parse_frame(
    buf: &[u8],
    version: ProtocolVersion,
    max_bulk: u64,
    max_array: u64,
    depth: usize,
) -> Result<(Frame, usize), ProtocolError> {
    if depth > MAX_DEPTH {
        return Err(ProtocolError::InvalidEncoding(format!(
            "nesting deeper than {MAX_DEPTH}"
        )));
    }
    if buf.is_empty() {
        return Err(ProtocolError::Incomplete);
    }

    let prefix = buf[0];
    let rest = &buf[1..];
    match prefix {
        b'+' => {
            let (line, n) = read_line(rest)?;
            let s = std::str::from_utf8(line)
                .map_err(|_| ProtocolError::InvalidEncoding("simple string not utf-8".into()))?
                .to_owned();
            Ok((Frame::SimpleString(s), 1 + n))
        }
        b'-' => {
            let (line, n) = read_line(rest)?;
            let s = std::str::from_utf8(line)
                .map_err(|_| ProtocolError::InvalidEncoding("error not utf-8".into()))?
                .to_owned();
            Ok((Frame::Error(s), 1 + n))
        }
        b':' => {
            let (line, n) = read_line(rest)?;
            let v = parse_i64(line)?;
            Ok((Frame::Integer(v), 1 + n))
        }
        b'$' => parse_bulk(rest, max_bulk).map(|(f, n)| (f, 1 + n)),
        b'*' => parse_array(rest, version, max_bulk, max_array, depth).map(|(f, n)| (f, 1 + n)),
        // RESP3-only prefixes — gated on version.
        b'%' if version == ProtocolVersion::Resp3 => {
            parse_map(rest, version, max_bulk, max_array, depth).map(|(f, n)| (f, 1 + n))
        }
        b'~' if version == ProtocolVersion::Resp3 => {
            parse_set(rest, version, max_bulk, max_array, depth).map(|(f, n)| (f, 1 + n))
        }
        b'>' if version == ProtocolVersion::Resp3 => {
            parse_push(rest, version, max_bulk, max_array, depth).map(|(f, n)| (f, 1 + n))
        }
        b'#' if version == ProtocolVersion::Resp3 => {
            let (line, n) = read_line(rest)?;
            let b = match line {
                b"t" => true,
                b"f" => false,
                _ => return Err(ProtocolError::InvalidEncoding("invalid boolean".into())),
            };
            Ok((Frame::Boolean(b), 1 + n))
        }
        b',' if version == ProtocolVersion::Resp3 => {
            let (line, n) = read_line(rest)?;
            let s = std::str::from_utf8(line)
                .map_err(|_| ProtocolError::InvalidEncoding("double not utf-8".into()))?;
            // RESP3 allows "inf", "-inf", "nan" as textual specials.
            let v: f64 = match s {
                "inf" | "+inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                "nan" => f64::NAN,
                _ => s
                    .parse()
                    .map_err(|_| ProtocolError::InvalidEncoding(format!("bad double: {s}")))?,
            };
            Ok((Frame::Double(v), 1 + n))
        }
        b'_' if version == ProtocolVersion::Resp3 => {
            // `_\r\n`
            let (line, n) = read_line(rest)?;
            if !line.is_empty() {
                return Err(ProtocolError::InvalidEncoding(
                    "null frame has no payload".into(),
                ));
            }
            Ok((Frame::Null, 1 + n))
        }
        b'(' if version == ProtocolVersion::Resp3 => {
            let (line, n) = read_line(rest)?;
            let s = std::str::from_utf8(line)
                .map_err(|_| ProtocolError::InvalidEncoding("big number not utf-8".into()))?
                .to_owned();
            // Light validation — must be non-empty and contain only an optional sign + digits.
            if s.is_empty()
                || !s
                    .as_bytes()
                    .iter()
                    .enumerate()
                    .all(|(i, c)| c.is_ascii_digit() || (i == 0 && (*c == b'+' || *c == b'-')))
            {
                return Err(ProtocolError::InvalidEncoding(format!(
                    "bad big number: {s}"
                )));
            }
            Ok((Frame::BigNumber(s), 1 + n))
        }
        // RESP3 prefixes seen but we're stuck in RESP2 — reject.
        b'%' | b'~' | b'>' | b'#' | b',' | b'_' | b'(' => Err(ProtocolError::InvalidEncoding(
            format!("RESP3-only prefix 0x{prefix:02x} on RESP2 connection"),
        )),
        other => Err(ProtocolError::InvalidEncoding(format!(
            "unknown prefix 0x{other:02x}"
        ))),
    }
}

/// Read up to (and including) the next CRLF; return `(line_without_crlf, bytes_consumed)`.
fn read_line(buf: &[u8]) -> Result<(&[u8], usize), ProtocolError> {
    // Search for `\r\n`. We require the LF to immediately follow the CR.
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Ok((&buf[..i], i + 2));
        }
        i += 1;
    }
    Err(ProtocolError::Incomplete)
}

/// Parse an ASCII signed decimal integer.
fn parse_i64(buf: &[u8]) -> Result<i64, ProtocolError> {
    let s = std::str::from_utf8(buf)
        .map_err(|_| ProtocolError::InvalidEncoding("integer not utf-8".into()))?;
    s.parse::<i64>()
        .map_err(|_| ProtocolError::InvalidEncoding(format!("bad integer: {s}")))
}

fn parse_bulk(buf: &[u8], max_bulk: u64) -> Result<(Frame, usize), ProtocolError> {
    let (line, header_n) = read_line(buf)?;
    let len = parse_i64(line)?;
    if len == -1 {
        return Ok((Frame::Bulk(BulkString::null()), header_n));
    }
    if len < 0 {
        return Err(ProtocolError::InvalidEncoding(format!(
            "negative bulk length {len}"
        )));
    }
    // Cast to u64 is safe — len >= 0.
    #[allow(clippy::cast_sign_loss)]
    let ulen = len as u64;
    if ulen > max_bulk {
        return Err(ProtocolError::BulkTooLarge {
            size: ulen,
            max: max_bulk,
        });
    }
    let ulen = usize::try_from(ulen)
        .map_err(|_| ProtocolError::InvalidEncoding("bulk length exceeds usize".into()))?;
    // We need `ulen` payload bytes followed by CRLF.
    let need = ulen
        .checked_add(2)
        .ok_or_else(|| ProtocolError::InvalidEncoding("bulk length overflow".into()))?;
    let rest = &buf[header_n..];
    if rest.len() < need {
        return Err(ProtocolError::Incomplete);
    }
    let payload = &rest[..ulen];
    if rest[ulen] != b'\r' || rest[ulen + 1] != b'\n' {
        return Err(ProtocolError::InvalidEncoding(
            "bulk missing trailing CRLF".into(),
        ));
    }
    let bytes = Bytes::copy_from_slice(payload);
    Ok((Frame::Bulk(BulkString::from_bytes(bytes)), header_n + need))
}

fn parse_array(
    buf: &[u8],
    version: ProtocolVersion,
    max_bulk: u64,
    max_array: u64,
    depth: usize,
) -> Result<(Frame, usize), ProtocolError> {
    let (line, header_n) = read_line(buf)?;
    let n = parse_i64(line)?;
    if n == -1 {
        return Ok((Frame::Array(None), header_n));
    }
    if n < 0 {
        return Err(ProtocolError::InvalidEncoding(format!(
            "negative array length {n}"
        )));
    }
    #[allow(clippy::cast_sign_loss)]
    let un = n as u64;
    if un > max_array {
        return Err(ProtocolError::ArrayTooLarge {
            len: un,
            max: max_array,
        });
    }
    let count = usize::try_from(un)
        .map_err(|_| ProtocolError::InvalidEncoding("array length exceeds usize".into()))?;

    let mut consumed = header_n;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let (item, used) = parse_frame(&buf[consumed..], version, max_bulk, max_array, depth + 1)?;
        items.push(item);
        consumed += used;
    }
    Ok((Frame::Array(Some(items)), consumed))
}

fn parse_set(
    buf: &[u8],
    version: ProtocolVersion,
    max_bulk: u64,
    max_array: u64,
    depth: usize,
) -> Result<(Frame, usize), ProtocolError> {
    let (line, header_n) = read_line(buf)?;
    let n = parse_i64(line)?;
    if n < 0 {
        return Err(ProtocolError::InvalidEncoding(format!(
            "negative set length {n}"
        )));
    }
    #[allow(clippy::cast_sign_loss)]
    let un = n as u64;
    if un > max_array {
        return Err(ProtocolError::ArrayTooLarge {
            len: un,
            max: max_array,
        });
    }
    let count = usize::try_from(un)
        .map_err(|_| ProtocolError::InvalidEncoding("set length exceeds usize".into()))?;
    let mut consumed = header_n;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let (item, used) = parse_frame(&buf[consumed..], version, max_bulk, max_array, depth + 1)?;
        items.push(item);
        consumed += used;
    }
    Ok((Frame::Set(items), consumed))
}

fn parse_push(
    buf: &[u8],
    version: ProtocolVersion,
    max_bulk: u64,
    max_array: u64,
    depth: usize,
) -> Result<(Frame, usize), ProtocolError> {
    let (line, header_n) = read_line(buf)?;
    let n = parse_i64(line)?;
    if n < 0 {
        return Err(ProtocolError::InvalidEncoding(format!(
            "negative push length {n}"
        )));
    }
    #[allow(clippy::cast_sign_loss)]
    let un = n as u64;
    if un > max_array {
        return Err(ProtocolError::ArrayTooLarge {
            len: un,
            max: max_array,
        });
    }
    let count = usize::try_from(un)
        .map_err(|_| ProtocolError::InvalidEncoding("push length exceeds usize".into()))?;
    let mut consumed = header_n;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let (item, used) = parse_frame(&buf[consumed..], version, max_bulk, max_array, depth + 1)?;
        items.push(item);
        consumed += used;
    }
    Ok((Frame::Push(items), consumed))
}

fn parse_map(
    buf: &[u8],
    version: ProtocolVersion,
    max_bulk: u64,
    max_array: u64,
    depth: usize,
) -> Result<(Frame, usize), ProtocolError> {
    let (line, header_n) = read_line(buf)?;
    let n = parse_i64(line)?;
    if n < 0 {
        return Err(ProtocolError::InvalidEncoding(format!(
            "negative map length {n}"
        )));
    }
    #[allow(clippy::cast_sign_loss)]
    let un = n as u64;
    if un > max_array {
        return Err(ProtocolError::ArrayTooLarge {
            len: un,
            max: max_array,
        });
    }
    let count = usize::try_from(un)
        .map_err(|_| ProtocolError::InvalidEncoding("map length exceeds usize".into()))?;
    let mut consumed = header_n;
    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        let (k, used_k) = parse_frame(&buf[consumed..], version, max_bulk, max_array, depth + 1)?;
        consumed += used_k;
        let (v, used_v) = parse_frame(&buf[consumed..], version, max_bulk, max_array, depth + 1)?;
        consumed += used_v;
        pairs.push((k, v));
    }
    Ok((Frame::Map(pairs), consumed))
}

// --------------- Encoder internals ---------------

fn encode_frame(
    frame: &Frame,
    version: ProtocolVersion,
    dst: &mut BytesMut,
) -> Result<(), ProtocolError> {
    use std::fmt::Write as _;

    if version == ProtocolVersion::Resp2 && frame.requires_resp3() {
        return Err(ProtocolError::Resp3Required(frame_kind(frame)));
    }
    match frame {
        Frame::SimpleString(s) => {
            if s.as_bytes().iter().any(|&b| b == b'\r' || b == b'\n') {
                return Err(ProtocolError::InvalidEncoding(
                    "simple string contains CR/LF".into(),
                ));
            }
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        Frame::Error(s) => {
            if s.as_bytes().iter().any(|&b| b == b'\r' || b == b'\n') {
                return Err(ProtocolError::InvalidEncoding(
                    "error string contains CR/LF".into(),
                ));
            }
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        Frame::Integer(i) => {
            let mut tmp = itoa_buf();
            write!(&mut tmp, "{i}").unwrap();
            dst.extend_from_slice(b":");
            dst.extend_from_slice(tmp.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        Frame::Bulk(BulkString(None)) => {
            dst.extend_from_slice(b"$-1\r\n");
        }
        Frame::Bulk(BulkString(Some(b))) => {
            let mut tmp = itoa_buf();
            write!(&mut tmp, "{}", b.len()).unwrap();
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(tmp.as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(b);
            dst.extend_from_slice(b"\r\n");
        }
        Frame::Array(None) => {
            dst.extend_from_slice(b"*-1\r\n");
        }
        Frame::Array(Some(items)) => {
            let mut tmp = itoa_buf();
            write!(&mut tmp, "{}", items.len()).unwrap();
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(tmp.as_bytes());
            dst.extend_from_slice(b"\r\n");
            for item in items {
                encode_frame(item, version, dst)?;
            }
        }
        Frame::Map(pairs) => {
            let mut tmp = itoa_buf();
            write!(&mut tmp, "{}", pairs.len()).unwrap();
            dst.extend_from_slice(b"%");
            dst.extend_from_slice(tmp.as_bytes());
            dst.extend_from_slice(b"\r\n");
            for (k, v) in pairs {
                encode_frame(k, version, dst)?;
                encode_frame(v, version, dst)?;
            }
        }
        Frame::Set(items) => {
            let mut tmp = itoa_buf();
            write!(&mut tmp, "{}", items.len()).unwrap();
            dst.extend_from_slice(b"~");
            dst.extend_from_slice(tmp.as_bytes());
            dst.extend_from_slice(b"\r\n");
            for item in items {
                encode_frame(item, version, dst)?;
            }
        }
        Frame::Push(items) => {
            let mut tmp = itoa_buf();
            write!(&mut tmp, "{}", items.len()).unwrap();
            dst.extend_from_slice(b">");
            dst.extend_from_slice(tmp.as_bytes());
            dst.extend_from_slice(b"\r\n");
            for item in items {
                encode_frame(item, version, dst)?;
            }
        }
        Frame::Boolean(b) => {
            dst.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
        }
        Frame::Double(d) => {
            let mut tmp = String::with_capacity(24);
            if d.is_nan() {
                tmp.push_str("nan");
            } else if d.is_infinite() {
                tmp.push_str(if *d > 0.0 { "inf" } else { "-inf" });
            } else {
                // RESP3 doubles use textual decimal form. Avoid scientific
                // notation by going through the default Display impl for
                // f64 which already prints decimals reasonably.
                write!(&mut tmp, "{d}").unwrap();
            }
            dst.extend_from_slice(b",");
            dst.extend_from_slice(tmp.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        Frame::Null => {
            dst.extend_from_slice(b"_\r\n");
        }
        Frame::BigNumber(s) => {
            dst.extend_from_slice(b"(");
            dst.extend_from_slice(s.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
    }
    Ok(())
}

const fn frame_kind(f: &Frame) -> &'static str {
    match f {
        Frame::SimpleString(_) => "SimpleString",
        Frame::Error(_) => "Error",
        Frame::Integer(_) => "Integer",
        Frame::Bulk(_) => "Bulk",
        Frame::Array(_) => "Array",
        Frame::Map(_) => "Map",
        Frame::Set(_) => "Set",
        Frame::Push(_) => "Push",
        Frame::Boolean(_) => "Boolean",
        Frame::Double(_) => "Double",
        Frame::Null => "Null",
        Frame::BigNumber(_) => "BigNumber",
    }
}

/// Small stack string for itoa-style integer formatting.
fn itoa_buf() -> String {
    String::with_capacity(20)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use bytes::BytesMut;

    fn decode_one(bytes: &[u8]) -> Frame {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(bytes);
        let frame = codec
            .decode(&mut buf)
            .expect("decode ok")
            .expect("complete");
        assert!(buf.is_empty(), "trailing bytes: {buf:?}");
        frame
    }

    fn encode_one(codec: &mut RespCodec, frame: Frame) -> BytesMut {
        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).expect("encode ok");
        buf
    }

    #[test]
    fn decode_simple_string() {
        assert_eq!(decode_one(b"+OK\r\n"), Frame::SimpleString("OK".into()));
    }

    #[test]
    fn decode_error() {
        assert_eq!(
            decode_one(b"-ERR oops\r\n"),
            Frame::Error("ERR oops".into())
        );
    }

    #[test]
    fn decode_integer() {
        assert_eq!(decode_one(b":42\r\n"), Frame::Integer(42));
        assert_eq!(decode_one(b":-7\r\n"), Frame::Integer(-7));
    }

    #[test]
    fn decode_bulk() {
        assert_eq!(
            decode_one(b"$5\r\nhello\r\n"),
            Frame::Bulk(BulkString::from("hello"))
        );
    }

    #[test]
    fn decode_null_bulk() {
        assert_eq!(decode_one(b"$-1\r\n"), Frame::Bulk(BulkString::null()));
    }

    #[test]
    fn decode_empty_bulk() {
        assert_eq!(decode_one(b"$0\r\n\r\n"), Frame::Bulk(BulkString::from("")));
    }

    #[test]
    fn decode_array_of_bulks() {
        let frame = decode_one(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(
            frame,
            Frame::Array(Some(vec![
                Frame::Bulk(BulkString::from("foo")),
                Frame::Bulk(BulkString::from("bar")),
            ]))
        );
    }

    #[test]
    fn decode_empty_array() {
        assert_eq!(decode_one(b"*0\r\n"), Frame::Array(Some(vec![])));
    }

    #[test]
    fn decode_null_array() {
        assert_eq!(decode_one(b"*-1\r\n"), Frame::Array(None));
    }

    #[test]
    fn decode_nested_array() {
        let frame = decode_one(b"*2\r\n*1\r\n:1\r\n:2\r\n");
        assert_eq!(
            frame,
            Frame::Array(Some(vec![
                Frame::Array(Some(vec![Frame::Integer(1)])),
                Frame::Integer(2),
            ]))
        );
    }

    #[test]
    fn truncated_returns_none() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"$5\r\nhel"[..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        // Adding the rest gives us a frame; buffer must hold partial state.
        buf.extend_from_slice(b"lo\r\n");
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Bulk(BulkString::from("hello")));
    }

    #[test]
    fn truncated_one_byte_short_each_position() {
        // Every prefix of a valid frame must be Incomplete (Ok(None)).
        let full: &[u8] = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        for i in 1..full.len() {
            let mut codec = RespCodec::new();
            let mut buf = BytesMut::from(&full[..i]);
            assert!(
                codec.decode(&mut buf).unwrap().is_none(),
                "prefix len {i} should be incomplete"
            );
        }
    }

    #[test]
    fn empty_buffer_is_none() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::new();
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn bulk_over_max_rejected() {
        let mut codec = RespCodec::new();
        codec.set_max_bulk_size(8);
        let mut buf = BytesMut::from(&b"$100\r\n"[..]);
        assert_matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::BulkTooLarge { size: 100, max: 8 })
        );
    }

    #[test]
    fn array_over_max_rejected() {
        let mut codec = RespCodec::new();
        codec.set_max_array_len(2);
        let mut buf = BytesMut::from(&b"*10\r\n"[..]);
        assert_matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::ArrayTooLarge { len: 10, max: 2 })
        );
    }

    #[test]
    fn inline_ping_decodes_to_array() {
        let frame = decode_one(b"PING\r\n");
        assert_eq!(
            frame,
            Frame::Array(Some(vec![Frame::Bulk(BulkString::from("PING"))]))
        );
        // case-insensitive
        let frame = decode_one(b"ping\r\n");
        assert_eq!(
            frame,
            Frame::Array(Some(vec![Frame::Bulk(BulkString::from("PING"))]))
        );
    }

    #[test]
    fn inline_other_command_rejected() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"GET foo\r\n"[..]);
        assert_matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InlineCommandRejected(_))
        );
    }

    #[test]
    fn garbage_prefix_rejected() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&[0x00_u8, 0x01, 0x02][..]);
        assert_matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidEncoding(_))
        );
    }

    #[test]
    fn resp3_prefix_rejected_in_resp2() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"#t\r\n"[..]);
        // First byte `#` looks like an "ascii alphabetic"? No — `#` is not.
        // It hits the InvalidEncoding path.
        assert_matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidEncoding(_))
        );
    }

    #[test]
    fn resp3_frames_decode_after_upgrade() {
        let mut codec = RespCodec::new();
        codec.upgrade_to_resp3();
        let mut buf = BytesMut::from(&b"#t\r\n"[..]);
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::Boolean(true)
        );

        let mut buf = BytesMut::from(&b"_\r\n"[..]);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), Frame::Null);

        let mut buf = BytesMut::from(&b",2.5\r\n"[..]);
        let Frame::Double(d) = codec.decode(&mut buf).unwrap().unwrap() else {
            panic!("expected Double");
        };
        assert!((d - 2.5).abs() < 1e-9);

        let mut buf = BytesMut::from(&b"(123456789012345678901234567890\r\n"[..]);
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::BigNumber("123456789012345678901234567890".into())
        );

        let mut buf = BytesMut::from(&b"%2\r\n+a\r\n:1\r\n+b\r\n:2\r\n"[..]);
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::Map(vec![
                (Frame::SimpleString("a".into()), Frame::Integer(1)),
                (Frame::SimpleString("b".into()), Frame::Integer(2)),
            ])
        );

        let mut buf = BytesMut::from(&b"~2\r\n+x\r\n+y\r\n"[..]);
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::Set(vec![
                Frame::SimpleString("x".into()),
                Frame::SimpleString("y".into()),
            ])
        );

        let mut buf = BytesMut::from(&b">2\r\n+pub\r\n+msg\r\n"[..]);
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::Push(vec![
                Frame::SimpleString("pub".into()),
                Frame::SimpleString("msg".into()),
            ])
        );
    }

    #[test]
    fn encode_resp3_frame_on_resp2_fails() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::new();
        assert_matches!(
            codec.encode(Frame::Map(vec![]), &mut buf),
            Err(ProtocolError::Resp3Required("Map"))
        );
        assert_matches!(
            codec.encode(Frame::Null, &mut buf),
            Err(ProtocolError::Resp3Required("Null"))
        );
    }

    #[test]
    fn encode_simple_frames_resp2() {
        let mut codec = RespCodec::new();
        assert_eq!(
            &encode_one(&mut codec, Frame::SimpleString("OK".into()))[..],
            b"+OK\r\n"
        );
        assert_eq!(
            &encode_one(&mut codec, Frame::Error("ERR oops".into()))[..],
            b"-ERR oops\r\n"
        );
        assert_eq!(&encode_one(&mut codec, Frame::Integer(-7))[..], b":-7\r\n");
        assert_eq!(
            &encode_one(&mut codec, Frame::Bulk(BulkString::from("hi")))[..],
            b"$2\r\nhi\r\n"
        );
        assert_eq!(
            &encode_one(&mut codec, Frame::Bulk(BulkString::null()))[..],
            b"$-1\r\n"
        );
        assert_eq!(&encode_one(&mut codec, Frame::Array(None))[..], b"*-1\r\n");
        let frame = Frame::Array(Some(vec![
            Frame::Bulk(BulkString::from("DM.GET")),
            Frame::Bulk(BulkString::from("dm")),
            Frame::Bulk(BulkString::from("k")),
        ]));
        assert_eq!(
            &encode_one(&mut codec, frame)[..],
            b"*3\r\n$6\r\nDM.GET\r\n$2\r\ndm\r\n$1\r\nk\r\n"
        );
    }

    #[test]
    fn encode_by_reference() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::new();
        let f = Frame::SimpleString("OK".into());
        Encoder::<&Frame>::encode(&mut codec, &f, &mut buf).unwrap();
        assert_eq!(&buf[..], b"+OK\r\n");
        // f still usable
        assert_eq!(f, Frame::SimpleString("OK".into()));
    }

    #[test]
    fn encode_resp3_after_upgrade() {
        let mut codec = RespCodec::new();
        codec.upgrade_to_resp3();
        assert_eq!(&encode_one(&mut codec, Frame::Null)[..], b"_\r\n");
        assert_eq!(&encode_one(&mut codec, Frame::Boolean(true))[..], b"#t\r\n");
        assert_eq!(
            &encode_one(&mut codec, Frame::Boolean(false))[..],
            b"#f\r\n"
        );
        let m = Frame::Map(vec![(Frame::SimpleString("k".into()), Frame::Integer(1))]);
        assert_eq!(&encode_one(&mut codec, m)[..], b"%1\r\n+k\r\n:1\r\n");
    }

    #[test]
    fn round_trip_all_resp2_frames() {
        let frames = [
            Frame::SimpleString("OK".into()),
            Frame::Error("ERR bad".into()),
            Frame::Integer(0),
            Frame::Integer(i64::MIN),
            Frame::Integer(i64::MAX),
            Frame::Bulk(BulkString::from("hello")),
            Frame::Bulk(BulkString::null()),
            Frame::Bulk(BulkString::from("")),
            Frame::Array(None),
            Frame::Array(Some(vec![])),
            Frame::Array(Some(vec![
                Frame::Bulk(BulkString::from("a")),
                Frame::Integer(7),
            ])),
        ];
        let mut codec = RespCodec::new();
        for f in frames {
            let mut buf = BytesMut::new();
            codec.encode(f.clone(), &mut buf).unwrap();
            let mut codec2 = RespCodec::new();
            let back = codec2.decode(&mut buf).unwrap().unwrap();
            assert_eq!(back, f);
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn encode_simple_string_rejects_crlf() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::new();
        assert_matches!(
            codec.encode(Frame::SimpleString("a\r\nb".into()), &mut buf),
            Err(ProtocolError::InvalidEncoding(_))
        );
    }

    #[test]
    fn bulk_with_binary_payload_round_trips() {
        let bin: Vec<u8> = (0..=255_u8).collect();
        let frame = Frame::Bulk(BulkString::from(bin));
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        let mut codec2 = RespCodec::new();
        let back = codec2.decode(&mut buf).unwrap().unwrap();
        assert_eq!(back, frame);
    }

    #[test]
    fn deeply_nested_rejected() {
        // Build N nested *1\r\n... eventually exceeding MAX_DEPTH.
        let mut payload = Vec::new();
        for _ in 0..(MAX_DEPTH + 2) {
            payload.extend_from_slice(b"*1\r\n");
        }
        payload.extend_from_slice(b":1\r\n");
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(payload.as_slice());
        let res = codec.decode(&mut buf);
        assert_matches!(res, Err(ProtocolError::InvalidEncoding(_)));
    }

    #[test]
    fn multiple_frames_back_to_back() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"+A\r\n+B\r\n:1\r\n"[..]);
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::SimpleString("A".into())
        );
        assert_eq!(
            codec.decode(&mut buf).unwrap().unwrap(),
            Frame::SimpleString("B".into())
        );
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), Frame::Integer(1));
        assert!(buf.is_empty());
    }

    #[test]
    fn malformed_integer_rejected() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b":notanumber\r\n"[..]);
        assert_matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidEncoding(_))
        );
    }

    #[test]
    fn bulk_missing_trailing_crlf_rejected() {
        // Declared length 3 but the trailing chars aren't CRLF.
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"$3\r\nfooXX"[..]);
        assert_matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::InvalidEncoding(_))
        );
    }
}
