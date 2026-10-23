//! SQL text parsing: sqlcommenter-style comment scanning and W3C `traceparent`
//! extraction from statement strings.
//!
//! All parsing operates on the raw SQL text; it never touches Postgres
//! internals, so it is pure and freely unit-testable.

/// A parsed W3C traceparent: `00-<32hex traceid>-<16hex spanid>-<2hex flags>`
#[derive(Debug, Clone, Copy)]
pub struct TraceCtx {
    pub trace_id: [u8; 16],
    pub parent_span_id: [u8; 8],
    pub flags: u8,
}

fn hex_decode(hex: &str, out: &mut [u8]) -> Option<()> {
    let bytes = hex.as_bytes();
    if bytes.len() != out.len() * 2 {
        return None;
    }
    for (i, chunk) in bytes.chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16)?;
        let lo = (chunk[1] as char).to_digit(16)?;
        #[expect(clippy::cast_possible_truncation)]
        let byte = ((hi << 4) | lo) as u8;
        out[i] = byte;
    }
    Some(())
}

fn parse_traceparent(s: &str) -> Option<TraceCtx> {
    let s = s.trim();
    let mut parts = s.splitn(4, '-');
    let version = parts.next()?;
    let trace = parts.next()?;
    let span = parts.next()?;
    let flags = parts.next()?;
    if version.len() != 2 || trace.len() != 32 || span.len() != 16 || flags.len() != 2 {
        return None;
    }
    let mut trace_id = [0u8; 16];
    let mut span_id = [0u8; 8];
    hex_decode(trace, &mut trace_id)?;
    hex_decode(span, &mut span_id)?;
    if trace_id == [0u8; 16] || span_id == [0u8; 8] {
        return None;
    }
    let flags = u8::from_str_radix(flags, 16).ok()?;
    Some(TraceCtx {
        trace_id,
        parent_span_id: span_id,
        flags,
    })
}

/// Byte index of the first occurrence of `pat` in `bytes` at or after `from`.
fn find_bytes(bytes: &[u8], from: usize, pat: &[u8]) -> Option<usize> {
    if pat.len() > bytes.len().saturating_sub(from) {
        return None;
    }
    bytes[from..]
        .windows(pat.len())
        .position(|window| window == pat)
        .map(|pos| pos + from)
}

/// Skip a single-quoted SQL string or double-quoted identifier starting at
/// `bytes[i]` (which must be the opening quote). `escapes` enables backslash
/// escapes (`E'...'` strings; plain `'...'` strings and identifiers only use
/// quote doubling). Returns the index after the closing quote, or
/// `bytes.len()` when unterminated.
fn skip_quoted(bytes: &[u8], i: usize, escapes: bool) -> usize {
    let quote = bytes[i];
    let mut k = i + 1;
    while k < bytes.len() {
        match bytes[k] {
            b'\\' if escapes => k += 2,
            b if b == quote && bytes.get(k + 1) == Some(&quote) => k += 2,
            b if b == quote => return k + 1,
            _ => k += 1,
        }
    }
    bytes.len()
}

/// Skip a dollar-quoted string starting at `bytes[i] == b'$'`. The tag
/// between the two `$`s must be a valid unquoted identifier (or empty), so
/// parameter references like `$1` and bare `$` are left alone. Returns the
/// index after the closing tag, `bytes.len()` when unterminated, or `i + 1`
/// when this is not a dollar quote at all.
fn skip_dollar_quoted(bytes: &[u8], i: usize) -> usize {
    let mut j = i + 1;
    if j < bytes.len() && (bytes[j].is_ascii_alphabetic() || bytes[j] == b'_') {
        j += 1;
        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
    }
    if j >= bytes.len() || bytes[j] != b'$' {
        return i + 1;
    }
    let tag = &bytes[i..=j];
    find_bytes(bytes, j + 1, tag).map_or(bytes.len(), |end| end + tag.len())
}

/// Contents of every SQL comment in `query`, in order.
///
/// Both `/* ... */` block comments and `-- ...` line comments (sqlcommenter
/// allows either). String literals — `'...'` with `''` doubling, `E'...'`
/// with backslash escapes, dollar-quoted `$tag$...$tag$` — and double-quoted
/// identifiers are skipped first, so comment syntax inside a string value is
/// never mistaken for a comment.
fn comments(query: &str) -> impl Iterator<Item = &str> {
    struct Comments<'a> {
        query: &'a str,
        pos: usize,
    }

    impl<'a> Iterator for Comments<'a> {
        type Item = &'a str;

        fn next(&mut self) -> Option<Self::Item> {
            let bytes = self.query.as_bytes();
            while self.pos < bytes.len() {
                match bytes[self.pos] {
                    b'\'' | b'"' => self.pos = skip_quoted(bytes, self.pos, false),
                    b'e' | b'E' if bytes.get(self.pos + 1) == Some(&b'\'') => {
                        self.pos = skip_quoted(bytes, self.pos + 1, true);
                    }
                    b'$' => self.pos = skip_dollar_quoted(bytes, self.pos),
                    b'-' if bytes.get(self.pos + 1) == Some(&b'-') => {
                        let content = self.pos + 2;
                        let end = find_bytes(bytes, content, b"\n").unwrap_or(bytes.len());
                        self.pos = (end + 1).min(bytes.len());
                        return Some(&self.query[content..end]);
                    }
                    b'/' if bytes.get(self.pos + 1) == Some(&b'*') => {
                        let content = self.pos + 2;
                        let end = find_bytes(bytes, content, b"*/").unwrap_or(bytes.len());
                        self.pos = (end + 2).min(bytes.len());
                        return Some(&self.query[content..end]);
                    }
                    _ => self.pos += 1,
                }
            }
            None
        }
    }

    Comments { query, pos: 0 }
}

/// Locate a quoted value: given `bytes[i] == b'\''`, return the byte range
/// between the opening quote and its matching close (`''` doubles a quote),
/// plus the index after the close. An unterminated quote yields the rest of
/// the input.
fn quoted_value(bytes: &[u8], i: usize) -> ((usize, usize), usize) {
    let start = i + 1;
    let mut k = start;
    while k < bytes.len() {
        if bytes[k] == b'\'' {
            if bytes.get(k + 1) == Some(&b'\'') {
                k += 2;
            } else {
                return ((start, k), k + 1);
            }
        } else {
            k += 1;
        }
    }
    ((start, bytes.len()), bytes.len())
}

/// Parse sqlcommenter-style `key=value` pairs out of a comment body.
///
/// Values are single-quoted strings — internal quotes doubled per the
/// sqlcommenter spec — or bare tokens ending at the next `,`. A quoted value
/// is consumed as a whole, so text inside it (including `key=` patterns) is
/// never mistaken for another pair.
fn kv_pairs(comment: &str) -> impl Iterator<Item = (&str, &str)> {
    struct KvPairs<'a> {
        comment: &'a str,
        pos: usize,
    }

    impl<'a> Iterator for KvPairs<'a> {
        type Item = (&'a str, &'a str);

        fn next(&mut self) -> Option<Self::Item> {
            let bytes = self.comment.as_bytes();
            let mut i = self.pos;
            while i < bytes.len() {
                match bytes[i] {
                    b',' | b' ' | b'\t' | b'\r' | b'\n' => i += 1,
                    b'\'' => i = quoted_value(bytes, i).1, // stray quote: skip its content
                    _ => {
                        let key_start = i;
                        while i < bytes.len()
                            && (bytes[i].is_ascii_alphanumeric()
                                || matches!(bytes[i], b'_' | b'-' | b'.'))
                        {
                            i += 1;
                        }
                        if i == key_start {
                            i += 1; // not a key character; skip
                            continue;
                        }
                        let key = &self.comment[key_start..i];
                        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t') {
                            i += 1;
                        }
                        if i >= bytes.len() || bytes[i] != b'=' {
                            continue; // bare token, not a key=value pair
                        }
                        i += 1;
                        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t') {
                            i += 1;
                        }
                        if i >= bytes.len() {
                            break;
                        }
                        let (value, next) = if bytes[i] == b'\'' {
                            let (range, next) = quoted_value(bytes, i);
                            (&self.comment[range.0..range.1], next)
                        } else {
                            let value_start = i;
                            while i < bytes.len() && bytes[i] != b',' {
                                i += 1;
                            }
                            (self.comment[value_start..i].trim_end(), i)
                        };
                        self.pos = next;
                        return Some((key, value));
                    }
                }
            }
            self.pos = i;
            None
        }
    }

    KvPairs { comment, pos: 0 }
}

/// Extract sqlcommenter-style `traceparent='00-...'` from a statement.
///
/// Only actual SQL comments are considered (`/* ... */`, plus `-- ...` per
/// the sqlcommenter spec), with string literals skipped so that
/// `traceparent=...` inside a string value is never mistaken for propagated
/// context. Within a comment, `key=value` pairs are parsed properly: quoted
/// values are consumed as a whole, so a `traceparent` appearing as part of
/// another key's string value is ignored.
pub fn traceparent_from_statement(query: &str) -> Option<TraceCtx> {
    for comment in comments(query) {
        for (key, value) in kv_pairs(comment) {
            if key.eq_ignore_ascii_case("traceparent")
                && let Some(ctx) = parse_traceparent(value)
            {
                return Some(ctx);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_traceparent() {
        let ctx = parse_traceparent("00-0af7651916cd43dd8448eb8c8defd531-00f067aa0ba902b7-01")
            .expect("valid traceparent");
        assert_eq!(
            ctx.trace_id,
            [
                0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x8c, 0x8d, 0xef,
                0xd5, 0x31
            ]
        );
        assert_eq!(
            ctx.parent_span_id,
            [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]
        );
        assert_eq!(ctx.flags, 1);
        assert!(parse_traceparent("garbage").is_none());
        assert!(parse_traceparent("00-00-00-01").is_none());
    }

    #[test]
    fn test_comment_extraction() {
        const TP: &str = "00-0af7651916cd43dd8448eb8c8defd531-00f067aa0ba902b7-01";
        assert!(
            traceparent_from_statement(&format!("SELECT 1 /* traceparent='{TP}' */")).is_some()
        );
        // among other keys, quoted value before it
        assert!(
            traceparent_from_statement(&format!(
                "SELECT 1 /* application='my app',traceparent='{TP}' */"
            ))
            .is_some()
        );
        // a doubled quote in an earlier value must not derail the parser
        assert!(
            traceparent_from_statement(&format!(
                "SELECT 1 /* note='it''s fine',traceparent='{TP}' */"
            ))
            .is_some()
        );
        // line comments are valid sqlcommenter carriers too
        assert!(traceparent_from_statement(&format!("SELECT 1 -- traceparent='{TP}'")).is_some());
        assert!(traceparent_from_statement("SELECT 1").is_none());
    }

    #[test]
    fn test_string_values_not_mistaken_for_context() {
        const TP: &str = "00-0af7651916cd43dd8448eb8c8defd531-00f067aa0ba902b7-01";
        // A SQL string literal containing `traceparent=...` is a value, not
        // propagated context.
        assert!(traceparent_from_statement(&format!("SELECT 'traceparent={TP}'")).is_none());
        // Same, with SQL-style doubled quotes inside the literal.
        assert!(traceparent_from_statement(&format!("SELECT 'traceparent=''{TP}'''")).is_none());
        // A block comment inside a string literal is not a comment either.
        assert!(
            traceparent_from_statement(&format!("SELECT '/* traceparent=''{TP}'' */'")).is_none()
        );
        // `traceparent='...'` appearing as the *value* of another sqlcommenter
        // key must not be extracted.
        assert!(
            traceparent_from_statement(&format!(
                "SELECT 1 /* application='traceparent=''{TP}''' */"
            ))
            .is_none()
        );
        // A dollar-quoted string is a value too.
        assert!(traceparent_from_statement(&format!("SELECT $$traceparent={TP}$$")).is_none());
        // A double-quoted identifier can carry the same text.
        assert!(
            traceparent_from_statement(&format!("SELECT \"traceparent='{TP}'\" FROM t")).is_none()
        );
        // `traceparent` as an identifier (alias) is not context either.
        assert!(
            traceparent_from_statement(&format!("SELECT 1 AS traceparent FROM t WHERE x = '{TP}'"))
                .is_none()
        );
        // A parameter reference is not a dollar-quote opening tag.
        assert!(
            traceparent_from_statement(&format!("SELECT $1 + $2 /* traceparent='{TP}' */"))
                .is_some()
        );
    }
}
