//! `ScanCursor` trait and the in-memory implementation backed by a fragment scan.

use async_trait::async_trait;

use crate::error::Result;

/// Options applied to a single `scan` call.
#[derive(Debug, Default, Clone)]
pub struct ScanOptions {
    /// Approximate number of keys per round-trip. Ignored by the in-memory
    /// cursor (it materialises a single snapshot up front).
    pub count: Option<usize>,
    /// Glob pattern matched against keys. Translated to a regex by the impl.
    pub match_pattern: Option<String>,
}

/// Cursor returned by [`crate::DMap::scan`].
#[async_trait]
pub trait ScanCursor: Send {
    /// Return the next `(key, value)` pair or `None` once the partition is
    /// exhausted.
    async fn next(&mut self) -> Result<Option<(String, Vec<u8>)>>;

    /// Release any server-side cursor state. The default impl is a no-op.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// In-memory cursor: snapshots a fragment's matching keys and iterates them.
#[derive(Debug)]
pub struct BufferedCursor {
    buffer: std::vec::IntoIter<(String, Vec<u8>)>,
}

impl BufferedCursor {
    /// Build a cursor from a pre-collected vector. The caller is responsible
    /// for applying `count` if a paginated wire protocol ever needs it.
    #[must_use]
    pub fn new(items: Vec<(String, Vec<u8>)>) -> Self {
        Self {
            buffer: items.into_iter(),
        }
    }
}

#[async_trait]
impl ScanCursor for BufferedCursor {
    async fn next(&mut self) -> Result<Option<(String, Vec<u8>)>> {
        Ok(self.buffer.next())
    }
}

/// Translate a Redis-style glob (`*`, `?`, `[abc]`) into a regex anchored at
/// both ends.
#[must_use]
pub fn glob_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 2);
    out.push('^');
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            '[' => {
                out.push('[');
                while let Some(&inner) = chars.peek() {
                    chars.next();
                    if inner == ']' {
                        out.push(']');
                        break;
                    }
                    out.push(inner);
                }
            }
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '{' | '}' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            other => out.push(other),
        }
    }
    out.push('$');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_translates_stars() {
        assert_eq!(glob_to_regex("user:*"), "^user:.*$");
        assert_eq!(glob_to_regex("a?c"), "^a.c$");
        assert_eq!(glob_to_regex("[ab]c"), "^[ab]c$");
    }

    #[test]
    fn glob_escapes_regex_metas() {
        let r = glob_to_regex("a.b");
        assert_eq!(r, "^a\\.b$");
    }

    #[tokio::test]
    async fn buffered_cursor_drains() {
        let mut c = BufferedCursor::new(vec![
            ("a".into(), b"1".to_vec()),
            ("b".into(), b"2".to_vec()),
        ]);
        assert_eq!(c.next().await.unwrap().unwrap().0, "a");
        assert_eq!(c.next().await.unwrap().unwrap().0, "b");
        assert!(c.next().await.unwrap().is_none());
    }
}
