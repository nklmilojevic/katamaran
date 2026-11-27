//! Validation and normalization of the host's `~/.katamaran/k3s-config.yaml`.
//!
//! k3s reads its config file with `gopkg.in/yaml.v2` (v2.4.0 in every k3s
//! release katamaran accepts, v1.31 through v1.37). Several go-yaml failure
//! modes quote raw scalars in the error k3s writes to its log (for example
//! ``cannot decode !!str `<value>` as a !!int``), and k3s's S3 client logs a
//! malformed `etcd-s3-proxy` URL, credentials included. Instead of emulating
//! go-yaml, katamaran accepts a small, well-defined YAML subset, validates it,
//! and hands k3s a canonical JSON rewrite (JSON is valid YAML) that contains
//! only string, null and list-of-string values. k3s never parses the user's
//! raw YAML, so the value katamaran validates is the value k3s uses.
//!
//! The supported subset (anything else is rejected with an error that never
//! includes the offending value):
//!
//! - A top-level block mapping: one `key: value` per line at column 0, with an
//!   optional leading `---` and `#` comments. Keys are plain
//!   `[A-Za-z0-9][A-Za-z0-9._-]*` with an optional `+` (k3s's append suffix).
//! - Values are a single-line scalar, a single-line flow sequence of scalars
//!   (`[a, "b"]`), or a block sequence of single-line scalars (`- a`).
//! - Scalars are plain, single-quoted, or double-quoted without backslash
//!   escapes. Plain scalars that go-yaml would type as something other than a
//!   string (`yes`, `on`, `0644`, `1e3`, `0x1F`, `2024-01-01`, ...) must be
//!   quoted; `true`, `false`, canonical decimal integers and null (`~`,
//!   `null`, empty) are accepted because k3s stringifies them identically.
//! - Not supported: anchors, aliases, tags, merge keys, directives, multiple
//!   documents, block scalars (`|`, `>`), multi-line scalars, nested mappings,
//!   flow mappings, complex keys, tabs and non-printable characters.
//!
//! Duplicate keys and `key+` appends are resolved exactly as k3s
//! `configfilearg.readConfigFile` does, so the emitted file yields the same
//! flags k3s would have generated from the original.

use std::collections::HashMap;

pub(super) const INVALID_ETCD_S3_PROXY_ERR: &str = "invalid etcd-s3-proxy in k3s config YAML: must be \"none\" or a valid URL with scheme and host (for example http://proxy.example.com:8080)";

const MAX_KEY_LEN: usize = 128;

#[derive(Clone, Debug, PartialEq)]
enum Value {
    Null,
    Str(String),
    /// List items; `None` is a YAML null item.
    List(Vec<Option<String>>),
}

fn unsupported(line: usize, what: &str) -> String {
    format!(
        "k3s-config.yaml line {line}: {what}; katamaran accepts a YAML subset (see the Kubernetes docs)"
    )
}

/// Parse `yaml` as the supported subset, validate `etcd-s3-proxy`, and return
/// the canonical JSON document to hand to k3s.
pub(super) fn normalize_k3s_config_yaml(yaml: &str) -> Result<String, String> {
    let entries = parse_subset(yaml)?;
    let (order, values) = merge_like_k3s(entries);
    if let Some(v) = values.get("etcd-s3-proxy") {
        let ok = match v {
            Value::Null => true,
            Value::Str(s) => is_valid_etcd_s3_proxy(s),
            Value::List(items) => items
                .iter()
                .all(|i| is_valid_etcd_s3_proxy(i.as_deref().unwrap_or(""))),
        };
        if !ok {
            return Err(INVALID_ETCD_S3_PROXY_ERR.to_string());
        }
    }
    Ok(emit_json(&order, &values))
}

/// Apply k3s `readConfigFile` semantics to one file's entries: a plain key
/// replaces the earlier value, `key+` appends via `toSlice` (a string becomes
/// a one-item list, null contributes nothing) when the key already exists,
/// and keys keep their first-seen order.
fn merge_like_k3s(entries: Vec<(String, Value)>) -> (Vec<String>, HashMap<String, Value>) {
    fn to_slice(v: Value) -> Vec<Option<String>> {
        match v {
            Value::Null => Vec::new(),
            Value::Str(s) => vec![Some(s)],
            Value::List(l) => l,
        }
    }
    let mut order = Vec::new();
    let mut values: HashMap<String, Value> = HashMap::new();
    for (raw_key, v) in entries {
        let (key, append) = match raw_key.strip_suffix('+') {
            Some(k) => (k.to_string(), true),
            None => (raw_key, false),
        };
        match values.remove(&key) {
            Some(old) if append => {
                let mut l = to_slice(old);
                l.extend(to_slice(v));
                values.insert(key, Value::List(l));
            }
            Some(_) => {
                values.insert(key, v);
            }
            None => {
                order.push(key.clone());
                values.insert(key, v);
            }
        }
    }
    (order, values)
}

fn emit_json(order: &[String], values: &HashMap<String, Value>) -> String {
    fn s(v: &str) -> String {
        serde_json::to_string(v).expect("string serialization cannot fail")
    }
    let mut out = String::from("{");
    for (i, key) in order.iter().enumerate() {
        out.push_str(if i == 0 { "\n  " } else { ",\n  " });
        out.push_str(&s(key));
        out.push_str(": ");
        match &values[key] {
            Value::Null => out.push_str("null"),
            Value::Str(v) => out.push_str(&s(v)),
            Value::List(items) => {
                out.push('[');
                for (j, item) in items.iter().enumerate() {
                    if j > 0 {
                        out.push_str(", ");
                    }
                    match item {
                        Some(v) => out.push_str(&s(v)),
                        None => out.push_str("null"),
                    }
                }
                out.push(']');
            }
        }
    }
    out.push_str(if order.is_empty() { "}\n" } else { "\n}\n" });
    out
}

fn check_characters(text: &str) -> Result<(), String> {
    let mut line = 1;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\n' => line += 1,
            '\r' if chars.peek() == Some(&'\n') => {}
            '\t' => return Err(unsupported(line, "tab characters are not supported")),
            c if (c as u32) < 0x20
                || ('\u{7f}'..='\u{9f}').contains(&c)
                || matches!(
                    c,
                    '\u{2028}' | '\u{2029}' | '\u{feff}' | '\u{fffe}' | '\u{ffff}'
                ) =>
            {
                return Err(unsupported(
                    line,
                    "control and non-printable characters are not supported",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn is_doc_marker(t: &str, marker: &str) -> bool {
    t.strip_prefix(marker)
        .is_some_and(|r| r.is_empty() || r.starts_with(' '))
}

/// True when `rest` (text after a complete value) is empty or a comment.
fn is_trailing_comment_or_empty(rest: &str) -> bool {
    rest.is_empty()
        || (rest.starts_with(' ') && {
            let t = rest.trim_start_matches(' ');
            t.is_empty() || t.starts_with('#')
        })
}

fn parse_key(t: &str) -> Option<(&str, &str)> {
    let colon = t.find(':')?;
    let key = &t[..colon];
    let rest = &t[colon + 1..];
    let base = key.strip_suffix('+').unwrap_or(key);
    let mut bytes = base.bytes();
    let first = bytes.next()?;
    if key.len() > MAX_KEY_LEN
        || !first.is_ascii_alphanumeric()
        || !bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        || !(rest.is_empty() || rest.starts_with(' '))
    {
        return None;
    }
    // go-yaml types some plain keys (`y`, `on`, `null`, `0x1`, ...); k3s
    // would then stringify them differently from how they are written.
    if !matches!(classify_plain(base), Ok(Some(_))) {
        return None;
    }
    Some((key, rest))
}

fn parse_subset(yaml: &str) -> Result<Vec<(String, Value)>, String> {
    let text = yaml.strip_prefix('\u{feff}').unwrap_or(yaml);
    check_characters(text)?;
    let lines: Vec<&str> = text
        .split('\n')
        .map(|l| l.strip_suffix('\r').unwrap_or(l))
        .collect();
    let indent_of = |l: &str| l.len() - l.trim_start_matches(' ').len();
    let is_skippable = |l: &str| {
        let t = l.trim_start_matches(' ');
        t.is_empty() || t.starts_with('#')
    };

    let mut entries = Vec::new();
    let mut seen_content = false;
    let mut i = 0;
    while i < lines.len() {
        let raw = lines[i];
        let no = i + 1;
        if is_skippable(raw) {
            i += 1;
            continue;
        }
        if indent_of(raw) > 0 {
            return Err(unsupported(
                no,
                "indented line outside a block sequence (nested mappings and multi-line values are not supported)",
            ));
        }
        if raw.starts_with('%') {
            return Err(unsupported(no, "YAML directives are not supported"));
        }
        if is_doc_marker(raw, "---") {
            if seen_content || !is_trailing_comment_or_empty(&raw[3..]) {
                return Err(unsupported(
                    no,
                    "only a single leading `---` with nothing after it is supported",
                ));
            }
            seen_content = true;
            i += 1;
            continue;
        }
        if is_doc_marker(raw, "...") {
            return Err(unsupported(no, "document end markers are not supported"));
        }
        seen_content = true;
        let (key, rest) = parse_key(raw).ok_or_else(|| {
            unsupported(
                no,
                "expected `key: value` with a plain key such as `node-label` or `disable+`",
            )
        })?;
        i += 1;
        let value = if is_trailing_comment_or_empty(rest) {
            // Value on following lines: a block sequence, or null.
            let next = (i..lines.len()).find(|&j| !is_skippable(lines[j]));
            match next {
                Some(j) if is_seq_item(lines[j].trim_start_matches(' ')) => {
                    let item_indent = indent_of(lines[j]);
                    let mut items = Vec::new();
                    while i < lines.len() {
                        let l = lines[i];
                        if is_skippable(l) {
                            i += 1;
                            continue;
                        }
                        let ind = indent_of(l);
                        let t = &l[ind..];
                        if ind == item_indent && is_seq_item(t) {
                            items.push(parse_seq_item(t, i + 1)?);
                            i += 1;
                        } else if ind == 0 && item_indent > 0 || ind == 0 && !is_seq_item(t) {
                            break;
                        } else {
                            return Err(unsupported(
                                i + 1,
                                "sequence items must be single-line scalars at one indentation (nested or multi-line items are not supported)",
                            ));
                        }
                    }
                    Value::List(items)
                }
                Some(j) if indent_of(lines[j]) > 0 => {
                    return Err(unsupported(
                        j + 1,
                        "nested mappings and multi-line values are not supported",
                    ));
                }
                _ => Value::Null,
            }
        } else {
            let v = rest[1..].trim_start_matches(' ');
            if v.starts_with('[') {
                Value::List(parse_flow_sequence(v, no)?)
            } else {
                match parse_scalar(v, no, false)? {
                    (Some(s), after) if is_trailing_comment_or_empty(after) => Value::Str(s),
                    (None, after) if is_trailing_comment_or_empty(after) => Value::Null,
                    _ => {
                        return Err(unsupported(no, "unexpected text after the value"));
                    }
                }
            }
        };
        entries.push((key.to_string(), value));
    }
    Ok(entries)
}

fn is_seq_item(t: &str) -> bool {
    t == "-" || t.starts_with("- ")
}

fn parse_seq_item(t: &str, no: usize) -> Result<Option<String>, String> {
    let rest = &t[1..];
    if is_trailing_comment_or_empty(rest) {
        return Ok(None);
    }
    let v = rest.trim_start_matches(' ');
    if v.starts_with('[') || is_seq_item(v) {
        return Err(unsupported(no, "nested sequences are not supported"));
    }
    match parse_scalar(v, no, false)? {
        (item, after) if is_trailing_comment_or_empty(after) => Ok(item),
        _ => Err(unsupported(no, "unexpected text after the sequence item")),
    }
}

fn parse_flow_sequence(v: &str, no: usize) -> Result<Vec<Option<String>>, String> {
    let mut rest = &v[1..];
    let mut items = Vec::new();
    loop {
        rest = rest.trim_start_matches(' ');
        if let Some(after) = rest.strip_prefix(']') {
            if !items.is_empty() {
                // `[a, ]`: a trailing comma before `]`.
                return Err(unsupported(
                    no,
                    "trailing commas in flow sequences are not supported",
                ));
            }
            if !is_trailing_comment_or_empty(after) {
                return Err(unsupported(no, "unexpected text after the flow sequence"));
            }
            return Ok(items);
        }
        let (item, after) = parse_scalar(rest, no, true)?;
        items.push(item);
        let after = after.trim_start_matches(' ');
        if let Some(r) = after.strip_prefix(',') {
            rest = r;
            if rest.trim_start_matches(' ').starts_with(']') {
                return Err(unsupported(
                    no,
                    "trailing commas in flow sequences are not supported",
                ));
            }
        } else if let Some(r) = after.strip_prefix(']') {
            if !is_trailing_comment_or_empty(r) {
                return Err(unsupported(no, "unexpected text after the flow sequence"));
            }
            return Ok(items);
        } else {
            return Err(unsupported(
                no,
                "flow sequences must hold single-line scalars and close on the same line",
            ));
        }
    }
}

/// Parse one scalar at the start of `s` (no leading spaces). Returns the value
/// (`None` for YAML null) and the unconsumed text after it.
fn parse_scalar(s: &str, no: usize, in_flow: bool) -> Result<(Option<String>, &str), String> {
    if let Some(body) = s.strip_prefix('"') {
        let end = body
            .find('"')
            .ok_or_else(|| unsupported(no, "quoted values must close on the same line"))?;
        let content = &body[..end];
        if content.contains('\\') {
            return Err(unsupported(
                no,
                "backslash escapes in double-quoted values are not supported; use single quotes",
            ));
        }
        return Ok((Some(content.to_string()), &body[end + 1..]));
    }
    if let Some(body) = s.strip_prefix('\'') {
        let mut out = String::new();
        let mut it = body.char_indices();
        while let Some((idx, c)) = it.next() {
            if c == '\'' {
                if body[idx + 1..].starts_with('\'') {
                    out.push('\'');
                    it.next();
                } else {
                    return Ok((Some(out), &body[idx + 1..]));
                }
            } else {
                out.push(c);
            }
        }
        return Err(unsupported(no, "quoted values must close on the same line"));
    }

    let first = s.chars().next().unwrap_or(' ');
    let second_is_blank =
        s[first.len_utf8()..].is_empty() || s[first.len_utf8()..].starts_with(' ');
    if "[]{},#&*!|>%@`".contains(first) || ("-?:".contains(first) && second_is_blank) {
        return Err(unsupported(
            no,
            "values starting with a YAML indicator (flow mapping, anchor, alias, tag, block scalar, ...) are not supported; quote the value",
        ));
    }
    let end = if in_flow {
        s.find([',', ']']).unwrap_or(s.len())
    } else {
        s.find(" #").unwrap_or(s.len())
    };
    let plain = s[..end].trim_end_matches(' ');
    let after = &s[end..];
    if plain.contains(": ")
        || plain.ends_with(':')
        || plain.contains(" #")
        || (in_flow && (plain.contains(['[', '{', '}']) || plain.contains(":,")))
    {
        return Err(unsupported(
            no,
            "plain values containing `: `, ` #` or flow indicators are not supported; quote the value",
        ));
    }
    let v = classify_plain(plain).map_err(|()| {
        unsupported(
            no,
            "YAML would read this unquoted value as a boolean, number or timestamp; quote it",
        )
    })?;
    Ok((v, after))
}

/// Classify a plain scalar the way go-yaml v2.4.0 `resolve` would, returning
/// `Ok(None)` for null, `Ok(Some(text))` when k3s ends up with exactly `text`,
/// and `Err(())` when go-yaml would type it (bool, int, float, timestamp,
/// merge) and k3s's `fmt.Sprint` could differ from what was written. The
/// numeric checks are deliberately broader than go-yaml's parsers.
fn classify_plain(plain: &str) -> Result<Option<String>, ()> {
    match plain {
        "" | "~" | "null" | "Null" | "NULL" => return Ok(None),
        "true" | "false" => return Ok(Some(plain.to_string())),
        "y" | "Y" | "yes" | "Yes" | "YES" | "True" | "TRUE" | "on" | "On" | "ON" | "n" | "N"
        | "no" | "No" | "NO" | "False" | "FALSE" | "off" | "Off" | "OFF" | "<<" => return Err(()),
        _ => {}
    }
    let bytes = plain.as_bytes();
    match bytes[0] {
        b'.' => {
            // `.5`, `.inf`, `.NaN`, ...: go-yaml tries strconv.ParseFloat.
            if bytes.get(1).is_some_and(|b| b.is_ascii_digit())
                || matches!(&plain[1..], "inf" | "Inf" | "INF" | "nan" | "NaN" | "NAN")
            {
                return Err(());
            }
        }
        b'+' | b'-' | b'0'..=b'9' => {
            if is_canonical_decimal(plain) {
                return Ok(Some(plain.to_string()));
            }
            let p: String = plain.chars().filter(|&c| c != '_').collect();
            if looks_like_go_int(&p)
                || is_yaml_style_float(&p)
                || matches!(
                    &p[..],
                    "+.inf" | "+.Inf" | "+.INF" | "-.inf" | "-.Inf" | "-.INF"
                )
                || is_timestamp_prefix(plain)
            {
                return Err(());
            }
        }
        _ => {}
    }
    Ok(Some(plain.to_string()))
}

/// `-?(0|[1-9][0-9]*)`, short enough to fit an int64: go-yaml yields an int
/// whose `fmt.Sprint` is the same text.
fn is_canonical_decimal(s: &str) -> bool {
    let d = s.strip_prefix('-').unwrap_or(s);
    !d.is_empty()
        && d.len() <= 18
        && d.bytes().all(|b| b.is_ascii_digit())
        && (d == "0" || !d.starts_with('0'))
        && s != "-0"
}

/// Superset of what Go `strconv.ParseInt/ParseUint(s, 0, 64)` accept.
fn looks_like_go_int(p: &str) -> bool {
    let p = p.strip_prefix(['+', '-']).unwrap_or(p);
    let digits = ["0x", "0X", "0o", "0O", "0b", "0B"]
        .iter()
        .find_map(|pre| p.strip_prefix(pre))
        .unwrap_or(p);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_hexdigit())
}

/// go-yaml's `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`.
fn is_yaml_style_float(p: &str) -> bool {
    fn digits(s: &str) -> usize {
        s.bytes().take_while(u8::is_ascii_digit).count()
    }
    let mut s = p.strip_prefix(['+', '-']).unwrap_or(p);
    if let Some(r) = s.strip_prefix('.') {
        let n = digits(r);
        if n == 0 {
            return false;
        }
        s = &r[n..];
    } else {
        let n = digits(s);
        if n == 0 {
            return false;
        }
        s = &s[n..];
        if let Some(r) = s.strip_prefix('.') {
            s = &r[digits(r)..];
        }
    }
    if let Some(r) = s.strip_prefix(['e', 'E']) {
        let r = r.strip_prefix(['+', '-']).unwrap_or(r);
        let n = digits(r);
        return n > 0 && n == r.len();
    }
    s.is_empty()
}

/// go-yaml's `parseTimestamp` requires four leading digits and a `-`.
fn is_timestamp_prefix(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() > 4 && b[..4].iter().all(u8::is_ascii_digit) && b[4] == b'-'
}

// ---- etcd-s3-proxy URL validation (mirrors Go net/url as used by k3s) ----

pub(super) fn is_valid_etcd_s3_proxy(val: &str) -> bool {
    // Match k3s `configfilearg.ToString`, which applies Go `strings.TrimSpace`
    // (identical to Rust `str::trim`) before passing the flag to `s3.GetClient`.
    let val = val.trim();
    if val.is_empty() || val == "none" {
        return true;
    }
    is_valid_go_proxy_url(val)
}

fn decode_hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn has_valid_percent_escapes(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            if i + 2 >= b.len()
                || decode_hex_nibble(b[i + 1]).is_none()
                || decode_hex_nibble(b[i + 2]).is_none()
            {
                return false;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    true
}

/// Mirrors Go's `net/url.shouldEscape(c, encodeHost)`.
fn go_should_escape_host(b: u8) -> bool {
    !(b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'_'
                | b'.'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'['
                | b']'
                | b'<'
                | b'>'
                | b'"'
        ))
}

fn is_valid_go_userinfo(s: &str) -> bool {
    if !has_valid_percent_escapes(s) {
        return false;
    }
    s.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'.'
                    | b'_'
                    | b':'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b'%'
                    // Go's validUserinfo accepts a raw '@' (go.dev/issue/22655);
                    // parseAuthority splits userinfo from host at the last '@'.
                    | b'@'
            )
    })
}

/// Mirrors Go's `net/url.unescape(host, encodeHost)` on a registered hostname:
/// percent escapes may only encode non-ASCII bytes (`>= 0x80`) or `%25`.
fn is_valid_go_reg_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            let Some(hi) = bytes.get(i + 1).copied().and_then(decode_hex_nibble) else {
                return false;
            };
            let Some(lo) = bytes.get(i + 2).copied().and_then(decode_hex_nibble) else {
                return false;
            };
            let decoded = (hi << 4) | lo;
            if decoded < 0x80 && decoded != b'%' {
                return false;
            }
            i += 3;
            continue;
        }
        if b < 0x80
            && !(b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'-' | b'.'
                        | b'_'
                        | b'~'
                        | b'!'
                        | b'$'
                        | b'&'
                        | b'\''
                        | b'('
                        | b')'
                        | b'*'
                        | b'+'
                        | b','
                        | b';'
                        | b'='
                ))
        {
            return false;
        }
        i += 1;
    }
    true
}

/// Mirrors Go's `net/url.unescape(zone, encodeZone)` for IPv6 zone identifiers.
fn is_valid_go_zone(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            let Some(hi) = bytes.get(i + 1).copied().and_then(decode_hex_nibble) else {
                return false;
            };
            let Some(lo) = bytes.get(i + 2).copied().and_then(decode_hex_nibble) else {
                return false;
            };
            let decoded = (hi << 4) | lo;
            if decoded != b'%' && decoded != b' ' && go_should_escape_host(decoded) {
                return false;
            }
            i += 3;
            continue;
        }
        if b < 0x80 && (matches!(b, b'[' | b']') || go_should_escape_host(b)) {
            return false;
        }
        i += 1;
    }
    true
}

fn is_valid_port(port: &str) -> bool {
    !port.is_empty()
        && port.bytes().all(|b| b.is_ascii_digit())
        && port.parse::<u16>().is_ok_and(|p| p > 0)
}

/// Validate a proxy URL against Go's `net/url.Parse` and k3s's requirement
/// that both scheme and host are non-empty.
fn is_valid_go_proxy_url(s: &str) -> bool {
    if s.bytes()
        .any(|b| b < 0x20 || b == 0x7f || b.is_ascii_whitespace())
    {
        return false;
    }
    let Some((scheme, rest)) = s.split_once("://") else {
        return false;
    };
    let mut scheme_bytes = scheme.bytes();
    let Some(first) = scheme_bytes.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic()
        || !scheme_bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
    {
        return false;
    }

    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let tail = &rest[authority_end..];
    if authority.is_empty() {
        return false;
    }

    let host = match authority.rsplit_once('@') {
        Some((userinfo, host)) => {
            if !is_valid_go_userinfo(userinfo) {
                return false;
            }
            host
        }
        None => authority,
    };
    if host.is_empty() {
        return false;
    }

    if let Some(after_open) = host.strip_prefix('[') {
        let Some((inside, after_close)) = after_open.split_once(']') else {
            return false;
        };
        if inside.is_empty() {
            return false;
        }
        let (addr_part, zone_opt) = match inside.split_once("%25") {
            Some((a, z)) => (a, Some(z)),
            None => (inside, None),
        };
        if addr_part.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        if let Some(zone) = zone_opt
            && !is_valid_go_zone(zone)
        {
            return false;
        }
        if !after_close.is_empty() {
            let Some(port) = after_close.strip_prefix(':') else {
                return false;
            };
            if !is_valid_port(port) {
                return false;
            }
        }
    } else {
        if host.contains(['[', ']']) {
            return false;
        }
        if let Some((name, port)) = host.split_once(':') {
            if !is_valid_go_reg_name(name) || !is_valid_port(port) {
                return false;
            }
        } else if !is_valid_go_reg_name(host) {
            return false;
        }
    }

    // Check path, query, and fragment for malformed percent escapes.
    let (before_frag, frag) = match tail.split_once('#') {
        Some((b, f)) => (b, Some(f)),
        None => (tail, None),
    };
    let (path, query) = match before_frag.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (before_frag, None),
    };
    if !has_valid_percent_escapes(path) {
        return false;
    }
    if let Some(q) = query
        && !has_valid_percent_escapes(q)
    {
        return false;
    }
    if let Some(f) = frag
        && !has_valid_percent_escapes(f)
    {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(yaml: &str) -> String {
        normalize_k3s_config_yaml(yaml).unwrap_or_else(|e| panic!("{yaml:?}: {e}"))
    }

    #[test]
    fn normalizes_supported_subset_to_canonical_json() {
        let yaml = "\u{feff}--- # k3s settings\n\
            # comment\n\
            node-label:\n  - role=dev   # trailing comment\n  - 'quote''d'\n\
            disable: [traefik, \"servicelb\", 'metrics-server']\n\
            write-kubeconfig-mode: \"0644\"\r\n\
            cluster-cidr: 10.42.0.0/16\n\
            secrets-encryption: true\n\
            max-pods: 200\n\
            token:\n\
            kubelet-arg:\n- a=b\n-\n\
            etcd-s3-proxy: http://proxy.example.com:8080\n\
            empty: []\n\
            v: ~\n";
        assert_eq!(
            norm(yaml),
            "{\n  \"node-label\": [\"role=dev\", \"quote'd\"],\n  \"disable\": [\"traefik\", \"servicelb\", \"metrics-server\"],\n  \"write-kubeconfig-mode\": \"0644\",\n  \"cluster-cidr\": \"10.42.0.0/16\",\n  \"secrets-encryption\": \"true\",\n  \"max-pods\": \"200\",\n  \"token\": null,\n  \"kubelet-arg\": [\"a=b\", null],\n  \"etcd-s3-proxy\": \"http://proxy.example.com:8080\",\n  \"empty\": [],\n  \"v\": null\n}\n"
        );
        assert_eq!(norm(""), "{}\n");
        assert_eq!(norm("# only comments\n\n"), "{}\n");
        // Values keep characters JSON must escape; single quotes may hold `"` and `\`.
        assert_eq!(norm("k: 'a\"b\\c'\n"), "{\n  \"k\": \"a\\\"b\\\\c\"\n}\n");
    }

    /// Duplicate keys and `key+` appends follow k3s `readConfigFile`.
    #[test]
    fn merges_duplicate_and_append_keys_like_k3s() {
        assert_eq!(
            norm("a: 1\nb: x\na: 2\n"),
            "{\n  \"a\": \"2\",\n  \"b\": \"x\"\n}\n"
        );
        assert_eq!(
            norm("a: x\na+: [p, q]\n"),
            "{\n  \"a\": [\"x\", \"p\", \"q\"]\n}\n"
        );
        assert_eq!(norm("a: [x]\na+: p\n"), "{\n  \"a\": [\"x\", \"p\"]\n}\n");
        // A null append contributes nothing; a null previous value is dropped.
        assert_eq!(norm("a: x\na+:\n"), "{\n  \"a\": [\"x\"]\n}\n");
        assert_eq!(norm("a:\na+: [p]\n"), "{\n  \"a\": [\"p\"]\n}\n");
        // `key+` without an earlier value just sets it.
        assert_eq!(norm("a+: x\n"), "{\n  \"a\": \"x\"\n}\n");
    }

    #[test]
    fn validates_effective_etcd_s3_proxy() {
        for ok in [
            "etcd-s3-proxy: none\n",
            "etcd-s3-proxy:\n",
            "etcd-s3-proxy: ''\n",
            "etcd-s3-proxy: \"  http://proxy.example.com:8080  \"\n",
            "etcd-s3-proxy: [http://proxy.example.com:8080]\n",
            "etcd-s3-proxy:\n  - http://proxy.example.com:8080\n",
            "etcd-s3-proxy: http://proxy.invalid.example:badport\netcd-s3-proxy: none\n",
        ] {
            norm(ok);
        }
        let marker = "placeholder-marker-5521";
        for bad in [
            format!("etcd-s3-proxy: http://proxy.invalid.example:badport/{marker}\n"),
            format!("etcd-s3-proxy: 'http://proxy.invalid.example:badport/{marker}'\n"),
            format!("etcd-s3-proxy: [none, \"http://proxy.invalid.example:badport/{marker}\"]\n"),
            format!(
                "etcd-s3-proxy: none\netcd-s3-proxy+: [http://proxy.invalid.example:badport/{marker}]\n"
            ),
            // Every list item is checked, not only the one k3s ends up using.
            format!(
                "etcd-s3-proxy:\n  - http://proxy.invalid.example:badport/{marker}\n  - none\n"
            ),
            format!("etcd-s3-proxy+: proxy.invalid.example:8080/{marker}\n"),
        ] {
            assert_eq!(
                normalize_k3s_config_yaml(&bad),
                Err(INVALID_ETCD_S3_PROXY_ERR.to_string()),
                "{bad:?}"
            );
        }
    }

    /// Everything outside the subset is rejected with an error that names the
    /// line but never echoes the value.
    #[test]
    fn rejects_yaml_outside_the_subset_without_echoing_values() {
        let m = "placeholder-marker-9174";
        let cases = [
            // anchors, aliases, tags, merges, directives, documents
            format!("a: &x {m}\n"),
            format!("a: *x{m}\n"),
            format!("a: !!int {m}\n"),
            format!("a: !!str {m}\n"),
            format!("a: [!!null {m}]\n"),
            format!("<<: {{k: {m}}}\n"),
            format!("%TAG !e! tag:yaml.org,2002:\n---\na: {m}\n"),
            format!("a: x\n---\nb: {m}\n"),
            format!("--- {{a: {m}}}\n"),
            format!("a: {m}\n...\n"),
            // structure
            format!("{{a: {m}}}\n"),
            format!("a:\n  b: {m}\n"),
            format!("a: {{b: {m}}}\n"),
            format!("a: |\n  {m}\n"),
            format!("a: >-\n  {m}\n"),
            format!("a: x\n  {m}\n"),
            format!("a:\n  {m}\n"),
            format!("a:\n  - x\n    {m}\n"),
            format!("a:\n  - x\n - {m}\n"),
            format!("a:\n  - - {m}\n"),
            format!("a: [x, [{m}]]\n"),
            format!("a: [{m}: x]\n"),
            format!("a: [{m}, ]\n"),
            format!("a: [{m}\n  ]\n"),
            format!("? a\n: {m}\n"),
            format!("\"a\": {m}\n"),
            format!("a b: {m}\n"),
            format!("a:{m}\n"),
            format!("  a: {m}\n"),
            format!("a: \"{m}\n  x\"\n"),
            format!("a: '{m}\n"),
            format!("a: \"{m}\\n\"\n"),
            format!("a: {m}: x\n"),
            format!("a: \"{m}\" x\n"),
            format!("a: x # ok\n{m}\n"),
            // characters
            format!("a:\t{m}\n"),
            format!("a: {m}\u{2028}x\n"),
            format!("a: {m}\rb: x\n"),
            format!("a: {m}\u{7}\n"),
            // values go-yaml would type differently from how they are written
            format!("a: yes\nb: {m}x\nc: on\n"),
            "a: 0644\n".to_string(),
            "a: 1_000\n".to_string(),
            "a: +5\n".to_string(),
            "a: 1e3\n".to_string(),
            "a: 0x1F\n".to_string(),
            "a: .5\n".to_string(),
            "a: -.inf\n".to_string(),
            "a: 2024-01-01\n".to_string(),
            "a: [TRUE]\n".to_string(),
            "y: x\n".to_string(),
            "null: x\n".to_string(),
        ];
        for yaml in &cases {
            let err =
                normalize_k3s_config_yaml(yaml).expect_err(&format!("{yaml:?} should be rejected"));
            assert!(err.starts_with("k3s-config.yaml line "), "{yaml:?}: {err}");
            assert!(!err.contains(m), "{yaml:?} echoed its value: {err}");
        }
    }

    /// Each expectation was checked against gopkg.in/yaml.v2 v2.4.0: accepted
    /// plain scalars decode to a string (or to a bool/int whose `fmt.Sprint`
    /// is the same text); rejected ones decode to a value that k3s would
    /// stringify differently, except `2024-01-01`, `1.5` and `1abc`, which are
    /// rejected conservatively.
    #[test]
    fn classifies_plain_scalars_like_go_yaml() {
        for same in [
            "node-1",
            "10.42.0.0/16",
            "192.168.1.10",
            "1:20",
            "-foo",
            "a#b",
            "http://h:8080",
            "deadbeef",
            "true",
            "false",
            "10",
            "-5",
            "0",
            ".kube",
            "v1.31",
        ] {
            assert_eq!(classify_plain(same), Ok(Some(same.to_string())), "{same}");
        }
        for null in ["~", "null", "Null", "NULL", ""] {
            assert_eq!(classify_plain(null), Ok(None), "{null}");
        }
        for typed in [
            "yes",
            "on",
            "Y",
            "TRUE",
            "0644",
            "1_0",
            "+5",
            "1e3",
            "0x1F",
            ".5",
            "-0",
            "1.5",
            ".inf",
            "-.inf",
            "0b1",
            "1234567890123456789",
            "2024-01-01",
            "<<",
            // go-yaml reads this as a string; rejected by the deliberately
            // broad numeric check.
            "1abc",
        ] {
            assert_eq!(classify_plain(typed), Err(()), "{typed}");
        }
    }

    /// Expectations match Go `net/url.Parse` plus k3s's need for a scheme and
    /// host, except that out-of-range ports are also rejected.
    #[test]
    fn validates_proxy_urls_like_go() {
        for ok in [
            "none",
            "",
            "  http://proxy.example.com:8080  ",
            "http://proxy.example.com:8080",
            "https://ops%40corp@proxy.example.com:8443",
            "http://ops@proxy%c3%a9.example.com:8080",
            "socks5://ops@[::1]:1080",
            "socks5://ops@[fe80::1%25eth0]:1080",
            "socks5://ops@[fe80::1%25en%200]:1080",
            // Go splits userinfo at the last '@' and accepts a raw '@' in it.
            "http://ops@corp@proxy.example.com:8080",
        ] {
            assert!(is_valid_etcd_s3_proxy(ok), "{ok}");
        }
        for bad in [
            "http://proxy.invalid.example:badport",
            "http://ops@proxy.invalid.example%61:8080",
            "socks5://ops@[fe80::1%25en%0a0]:1080",
            "http://ops%zz@proxy.invalid.example:8080",
            "proxy.invalid.example:8080",
            "http://proxy.invalid.example:99999",
            "http://ops@corp@proxy.invalid.example:badport",
            "not-a-url",
        ] {
            assert!(!is_valid_etcd_s3_proxy(bad), "{bad}");
        }
    }
}
