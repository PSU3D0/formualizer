//! Namespace-aware, bounded XML events with source offsets for surgical edits.
//!
//! Events borrow the input: element and attribute names are slices of the
//! part, attribute values and text are decoded once and borrowed unless an
//! entity or end-of-line normalization changed them, and namespace names are
//! interned per walk. The validation policy is the one the owned-string
//! walker (kept as a test oracle in `reference`) enforced, error for error.
#![cfg_attr(not(feature = "xlsx-recalc"), allow(dead_code))]
use crate::IoError;
use crate::xlsx_cache_options::{CacheOptions, checkpoint, unsupported};
use quick_xml::{
    Decoder, Reader,
    events::{
        BytesStart, Event,
        attributes::{AttrError, Attribute as RawAttribute, Attributes},
    },
    name::{NamespaceResolver, QName, ResolveResult},
};
use rustc_hash::FxHashMap;
use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::{Deref, Range};
use std::rc::Rc;

/// A relative uppercase A1 coordinate within the Excel grid.
pub(crate) fn plain_coord(value: &str) -> Option<(u32, u32)> {
    let bytes = value.as_bytes();
    let letters = bytes.iter().take_while(|b| b.is_ascii_uppercase()).count();
    let digits = &bytes[letters..];
    if !(1..=3).contains(&letters)
        || !(1..=7).contains(&digits.len())
        || digits[0] == b'0'
        || !digits.iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let col = bytes[..letters]
        .iter()
        .fold(0u32, |n, b| n * 26 + u32::from(b - b'A') + 1);
    let row = digits
        .iter()
        .fold(0u32, |n, b| n * 10 + u32::from(b - b'0'));
    (row <= 1_048_576 && col <= 16_384).then_some((row, col))
}

pub(crate) const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
pub(crate) const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
pub(crate) const OFFICE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

/// A resolved namespace name (empty when unbound). Common names are static;
/// any other is interned once per walk and shared by reference count.
#[derive(Debug, Clone)]
pub(crate) enum Ns {
    /// [`MAIN`].
    Main,
    Static(&'static str),
    Shared(Rc<str>),
}
impl Deref for Ns {
    type Target = str;
    fn deref(&self) -> &str {
        match self {
            Ns::Main => MAIN,
            Ns::Static(s) => s,
            Ns::Shared(s) => s,
        }
    }
}
impl Ns {
    /// Whether this is the SpreadsheetML main namespace.
    pub fn is_main(&self) -> bool {
        matches!(self, Ns::Main)
    }
}
impl PartialEq<str> for Ns {
    fn eq(&self, other: &str) -> bool {
        match self {
            Ns::Main => std::ptr::eq(MAIN, other) || MAIN == other,
            _ => **self == *other,
        }
    }
}
impl PartialEq<&str> for Ns {
    fn eq(&self, other: &&str) -> bool {
        **self == **other
    }
}
impl PartialEq for Ns {
    fn eq(&self, other: &Ns) -> bool {
        **self == **other
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Element<'a> {
    pub ns: Ns,
    pub local: &'a str,
    pub qualified: &'a str,
}
#[derive(Debug)]
pub(crate) struct Attribute<'a> {
    pub ns: Ns,
    pub local: &'a str,
    pub qualified: &'a str,
    pub value: Cow<'a, str>,
    /// Key through closing quote, excluding preceding whitespace.
    pub span: Range<usize>,
}
#[derive(Debug)]
pub(crate) enum Kind<'n, 'a> {
    Open {
        empty: bool,
        attributes: &'n [Attribute<'a>],
    },
    Close,
    Text(Cow<'a, str>),
}
#[derive(Debug)]
pub(crate) struct Node<'n, 'a> {
    pub kind: Kind<'n, 'a>,
    pub span: Range<usize>,
}
impl<'n, 'a> Node<'n, 'a> {
    pub fn attribute(&self, ns: &str, local: &str) -> Option<&'n Attribute<'a>> {
        match self.kind {
            Kind::Open { attributes, .. } => {
                attributes.iter().find(|a| a.local == local && *a.ns == *ns)
            }
            _ => None,
        }
    }
    pub fn value(&self, name: &str) -> Option<&'n str> {
        self.attribute("", name).map(|a| &*a.value)
    }
    pub fn required(&self, name: &str) -> Result<&'n str, IoError> {
        self.required_attribute(name).map(|a| &*a.value)
    }
    /// The unprefixed attribute `name`, which must be present.
    pub fn required_attribute(&self, name: &str) -> Result<&'n Attribute<'a>, IoError> {
        self.attribute("", name)
            .ok_or_else(|| unsupported(format!("missing {name} attribute"), "XLSX XML"))
    }
}
pub(crate) fn path_is(path: &[Element<'_>], ns: &str, names: &[&str]) -> bool {
    path.len() == names.len()
        && path
            .iter()
            .zip(names)
            .all(|(e, n)| e.local == *n && *e.ns == *ns)
}
fn valid_char(c: char) -> bool {
    matches!(c, '\t'|'\n'|'\r'|' '..='\u{d7ff}'|'\u{e000}'..='\u{fffd}'|'\u{10000}'..='\u{10ffff}')
}
/// XML 1.0 `Char` production over the whole string, with a branch-free scan
/// of printable-ASCII blocks.
fn valid_text(text: &str) -> Result<(), IoError> {
    const BLOCK: usize = 16;
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(block) = bytes.get(i..i + BLOCK)
            && block
                .iter()
                .fold(true, |ok, &b| ok & (0x20..0x80).contains(&b))
        {
            i += BLOCK;
            continue;
        }
        // `i` is always a character boundary: blocks are ASCII.
        let c = text[i..].chars().next().expect("in bounds");
        if !valid_char(c) {
            return Err(unsupported("XML-invalid character", "XLSX XML"));
        }
        i += c.len_utf8();
    }
    Ok(())
}
fn qualified_name(bytes: &[u8]) -> Result<(), IoError> {
    // OOXML names are ASCII; reject other naming grammars rather than relying
    // on quick-xml's intentionally permissive lexical name handling.
    let mut pieces = 0;
    for p in bytes.split(|b| *b == b':') {
        pieces += 1;
        if pieces > 2
            || p.is_empty()
            || !(p[0].is_ascii_alphabetic() || p[0] == b'_')
            || p[1..]
                .iter()
                .any(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-')))
        {
            return Err(unsupported(
                "unsupported/malformed XML qualified name",
                "XLSX XML",
            ));
        }
    }
    Ok(())
}
/// Per-walk namespace interner, hashed: a part may bind any number of
/// distinct namespace names.
struct Namespaces(FxHashMap<Box<[u8]>, Ns>);
impl Namespaces {
    fn new() -> Self {
        Self(
            [Ns::Main, Ns::Static(RELS), Ns::Static(OFFICE)]
                .into_iter()
                .map(|ns| (ns.as_bytes().into(), ns))
                .collect(),
        )
    }
    fn get(&mut self, value: ResolveResult<'_>) -> Result<Ns, IoError> {
        match value {
            ResolveResult::Bound(ns) => {
                let ns = ns.as_ref();
                if let Some(known) = self.0.get(ns) {
                    return Ok(known.clone());
                }
                let text =
                    std::str::from_utf8(ns).map_err(|e| IoError::from_backend("xlsx-xml", e))?;
                let interned = Ns::Shared(Rc::from(text));
                self.0.insert(ns.into(), interned.clone());
                Ok(interned)
            }
            ResolveResult::Unbound => Ok(Ns::Static("")),
            ResolveResult::Unknown(_) => {
                Err(unsupported("unbound XML namespace prefix", "XLSX XML"))
            }
        }
    }
}
/// quick-xml's namespace scopes plus a cache of resolved element prefixes,
/// valid while no binding is added or removed.
struct Scopes<'a> {
    resolver: NamespaceResolver,
    namespaces: Namespaces,
    /// Per open scope: whether it declared a binding.
    declared: Vec<bool>,
    /// Element prefix (`None` for the default namespace) to namespace.
    cache: FxHashMap<Option<&'a [u8]>, Ns>,
}
impl<'a> Scopes<'a> {
    fn new() -> Self {
        Self {
            resolver: NamespaceResolver::default(),
            namespaces: Namespaces::new(),
            declared: Vec::new(),
            cache: FxHashMap::default(),
        }
    }
    fn pop(&mut self) {
        self.resolver.pop();
        if self.declared.pop() == Some(true) {
            self.cache.clear();
        }
    }
    fn element(&mut self, qualified: &'a str) -> Result<Ns, IoError> {
        let prefix = qualified
            .as_bytes()
            .iter()
            .position(|b| *b == b':')
            .map(|at| &qualified.as_bytes()[..at]);
        if let Some(ns) = self.cache.get(&prefix) {
            return Ok(ns.clone());
        }
        let (ns, _) = self.resolver.resolve_element(QName(qualified.as_bytes()));
        let ns = self.namespaces.get(ns)?;
        self.cache.insert(prefix, ns.clone());
        Ok(ns)
    }
    fn attribute(&mut self, key: &[u8]) -> Result<Ns, IoError> {
        if !key.contains(&b':') {
            return Ok(Ns::Static(""));
        }
        let (ns, _) = self.resolver.resolve_attribute(QName(key));
        self.namespaces.get(ns)
    }
}
// Attribute slices returned by quick-xml borrow the start-event buffer. Bounds
// checks make the offset derivation fail closed if that implementation changes.
// This is safe pointer arithmetic only: no pointer is dereferenced or retained.
fn offset(whole: &[u8], part: &[u8]) -> Result<usize, IoError> {
    let n = (part.as_ptr() as usize)
        .checked_sub(whole.as_ptr() as usize)
        .filter(|n| {
            n.checked_add(part.len())
                .is_some_and(|end| end <= whole.len())
        });
    n.ok_or_else(|| unsupported("unavailable XML attribute source span", "XLSX XML"))
}
fn backend(e: impl std::error::Error) -> IoError {
    IoError::from_backend("xlsx-xml", e)
}
/// Name of a local part without its prefix (quick-xml's `local_name`).
fn local_part(qualified: &str) -> &str {
    qualified
        .split_once(':')
        .map_or(qualified, |(_, local)| local)
}
/// One attribute as parsed (without duplicate checks) while binding the
/// element's namespace declarations.
struct Parsed<'a> {
    key: &'a [u8],
    value: &'a [u8],
}
/// Reusable per-element state.
#[derive(Default)]
struct Scratch<'a> {
    parsed: Vec<Parsed<'a>>,
    attributes: Vec<Attribute<'a>>,
}
/// Start-tag content of a borrowed event as a slice of the input.
fn content<'a>(text: &'a str, e: &BytesStart<'_>) -> Result<(usize, &'a str), IoError> {
    let raw = e.as_ref();
    let at = offset(text.as_bytes(), raw)?;
    text.get(at..at + raw.len())
        .map(|s| (at, s))
        .ok_or_else(|| unsupported("unavailable XML attribute source span", "XLSX XML"))
}
/// Open a namespace scope for `e`, exactly as quick-xml's namespace reader
/// does (bindings from every attribute up to the first malformed one), and
/// keep the unchecked parse. Returns whether the parse stopped at an error.
fn bind<'a>(
    scopes: &mut Scopes<'_>,
    content: &'a str,
    name_len: usize,
    parsed: &mut Vec<Parsed<'a>>,
) -> Result<bool, IoError> {
    // Open a scope (what `NamespaceResolver::push` does before binding).
    let level = scopes.resolver.level();
    scopes.resolver.set_level(level + 1);
    scopes.declared.push(false);
    parsed.clear();
    if plain_attributes(content, name_len, parsed) {
        for p in parsed.iter() {
            if let Some(prefix) = QName(p.key).as_namespace_binding() {
                *scopes.declared.last_mut().expect("open scope") = true;
                scopes.cache.clear();
                scopes
                    .resolver
                    .add(prefix, quick_xml::name::Namespace(p.value))
                    .map_err(|e| backend(quick_xml::Error::from(e)))?;
            }
        }
        return Ok(false);
    }
    parsed.clear();
    let mut attributes = Attributes::new(content, name_len);
    attributes.with_checks(false);
    for a in attributes {
        let Ok(a) = a else {
            return Ok(true);
        };
        if let Some(prefix) = a.key.as_namespace_binding() {
            *scopes.declared.last_mut().expect("open scope") = true;
            scopes.cache.clear();
            scopes
                .resolver
                .add(prefix, quick_xml::name::Namespace(&a.value))
                .map_err(|e| backend(quick_xml::Error::from(e)))?;
        }
        let (Cow::Borrowed(value), QName(key)) = (a.value, a.key) else {
            return Err(unsupported(
                "unavailable XML attribute source span",
                "XLSX XML",
            ));
        };
        parsed.push(Parsed { key, value });
    }
    Ok(false)
}
/// Parse attributes written strictly as `key="value"` or `key='value'`
/// (separated by optional XML whitespace), which quick-xml's attribute
/// iterator reads as the same key and value slices. Returns `false`, leaving
/// `parsed` partial, on any other form; the caller then uses quick-xml.
fn plain_attributes<'a>(content: &'a str, name_len: usize, parsed: &mut Vec<Parsed<'a>>) -> bool {
    let bytes = content.as_bytes();
    let mut at = name_len;
    loop {
        while at < bytes.len() && matches!(bytes[at], b' ' | b'\r' | b'\n' | b'\t') {
            at += 1;
        }
        if at >= bytes.len() {
            return true;
        }
        let key_start = at;
        while at < bytes.len() && !matches!(bytes[at], b'=' | b' ' | b'\r' | b'\n' | b'\t') {
            at += 1;
        }
        if at == key_start || bytes.get(at) != Some(&b'=') {
            return false;
        }
        let key = &bytes[key_start..at];
        let Some(&quote @ (b'"' | b'\'')) = bytes.get(at + 1) else {
            return false;
        };
        let value_start = at + 2;
        let Some(len) = bytes
            .get(value_start..)
            .and_then(|rest| rest.iter().position(|b| *b == quote))
        else {
            return false;
        };
        parsed.push(Parsed {
            key,
            value: &bytes[value_start..value_start + len],
        });
        at = value_start + len + 1;
    }
}
/// Validate and resolve one attribute; `None` for namespace declarations.
#[allow(clippy::too_many_arguments)]
fn attribute<'a>(
    a: RawAttribute<'a>,
    content: &'a str,
    start: usize,
    decoder: Decoder,
    scopes: &mut Scopes<'_>,
    seen: &mut Option<HashSet<(Ns, &'a str)>>,
    attributes: &mut Vec<Attribute<'a>>,
) -> Result<(), IoError> {
    let raw = content.as_bytes();
    let QName(key_bytes) = a.key;
    qualified_name(key_bytes)?;
    let (mut whitespace, mut markup, mut reference) = (false, false, false);
    for b in a.value.iter() {
        whitespace |= matches!(b, b'\t' | b'\r' | b'\n');
        markup |= *b == b'<';
        reference |= *b == b'&';
    }
    if whitespace {
        return Err(unsupported(
            "unnormalized XML attribute whitespace",
            "XLSX XML",
        ));
    }
    if markup {
        return Err(unsupported("unescaped attribute markup", "XLSX XML"));
    }
    // Without a reference the decoded value is the raw slice of the already
    // validated input; otherwise decode and validate the replacement text.
    let decoded = if reference {
        let decoded = a.decode_and_unescape_value(decoder).map_err(backend)?;
        if let Cow::Owned(decoded) = &decoded {
            valid_text(decoded)?;
        }
        decoded
    } else {
        Cow::Borrowed("")
    };
    if key_bytes == b"xmlns" || key_bytes.starts_with(b"xmlns:") {
        return Ok(());
    }
    let ns = scopes.attribute(key_bytes)?;
    let key_offset = offset(raw, key_bytes)?;
    let qualified = &content[key_offset..key_offset + key_bytes.len()];
    let local = local_part(qualified);
    let duplicate = match seen {
        Some(seen) => !seen.insert((ns.clone(), local)),
        None => {
            let duplicate = attributes.iter().any(|b| b.local == local && *b.ns == *ns);
            if !duplicate && attributes.len() >= 16 {
                let mut set: HashSet<(Ns, &'a str)> =
                    attributes.iter().map(|b| (b.ns.clone(), b.local)).collect();
                set.insert((ns.clone(), local));
                *seen = Some(set);
            }
            duplicate
        }
    };
    if duplicate {
        return Err(unsupported("duplicate expanded XML attribute", "XLSX XML"));
    }
    let value_offset = offset(raw, &a.value)?;
    let value_end = value_offset + a.value.len();
    if !matches!(raw.get(value_end), Some(b'\'' | b'"')) {
        return Err(unsupported("unquoted XML attribute", "XLSX XML"));
    }
    let value = match decoded {
        Cow::Borrowed(_) => Cow::Borrowed(&content[value_offset..value_end]),
        Cow::Owned(v) => Cow::Owned(v),
    };
    attributes.push(Attribute {
        ns,
        local,
        qualified,
        value,
        span: start + 1 + key_offset..start + 1 + value_end + 1,
    });
    Ok(())
}
impl PartialEq<Ns> for &str {
    fn eq(&self, other: &Ns) -> bool {
        **self == **other
    }
}
impl Eq for Ns {}
impl std::hash::Hash for Ns {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}
pub(crate) fn walk<'a>(
    bytes: &'a [u8],
    options: &CacheOptions,
    mut visit: impl FnMut(&[Element<'a>], Node<'_, 'a>) -> Result<(), IoError>,
) -> Result<(), IoError> {
    let text = std::str::from_utf8(bytes).map_err(|_| unsupported("non-UTF-8 XML", "XLSX XML"))?;
    valid_text(text)?;
    let mut reader = Reader::from_str(text);
    reader.config_mut().check_end_names = true;
    reader.config_mut().check_comments = true;
    let decoder = reader.decoder();
    let mut scopes = Scopes::new();
    let mut scratch = Scratch::default();
    let mut path: Vec<Element<'a>> = Vec::new();
    let mut roots = 0;
    let mut events = 0u64;
    let mut declaration = false;
    // A closed element's namespace scope ends before the next event, as in
    // quick-xml's namespace-aware reader.
    let mut pending_pop = false;
    loop {
        if events & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        events += 1;
        if pending_pop {
            scopes.pop();
            pending_pop = false;
        }
        let start = reader.buffer_position() as usize;
        let event = reader.read_event().map_err(backend)?;
        let end = reader.buffer_position() as usize;
        match event {
            Event::Start(ref e) | Event::Empty(ref e) => {
                let empty = matches!(event, Event::Empty(_));
                let (_, content) = content(text, e)?;
                let name_len = e.name().as_ref().len();
                let malformed = bind(&mut scopes, content, name_len, &mut scratch.parsed)?;
                pending_pop = empty;
                if path.len() >= options.limits.max_xml_depth {
                    return Err(unsupported("XML depth limit", "XLSX XML"));
                }
                if path.is_empty() {
                    roots += 1;
                    if roots != 1 {
                        return Err(unsupported("multiple XML roots", "XLSX XML"));
                    }
                }
                let qualified = &content[..name_len];
                qualified_name(qualified.as_bytes())?;
                let element = Element {
                    ns: scopes.element(qualified)?,
                    local: local_part(qualified),
                    qualified,
                };
                let attributes = &mut scratch.attributes;
                attributes.clear();
                let mut seen = None;
                if malformed {
                    // Replay with quick-xml's own duplicate checks for its
                    // exact error.
                    for a in Attributes::new(content, name_len) {
                        let a = a.map_err(backend)?;
                        attribute(
                            a,
                            content,
                            start,
                            decoder,
                            &mut scopes,
                            &mut seen,
                            attributes,
                        )?;
                    }
                } else {
                    for (i, p) in scratch.parsed.iter().enumerate() {
                        // quick-xml reports a repeated raw key before its value.
                        if let Some(prev) = scratch.parsed[..i].iter().find(|q| q.key == p.key) {
                            let raw = content.as_bytes();
                            return Err(backend(AttrError::Duplicated(
                                offset(raw, p.key)?,
                                offset(raw, prev.key)?,
                            )));
                        }
                        attribute(
                            RawAttribute {
                                key: QName(p.key),
                                value: Cow::Borrowed(p.value),
                            },
                            content,
                            start,
                            decoder,
                            &mut scopes,
                            &mut seen,
                            attributes,
                        )?;
                    }
                }
                path.push(element);
                visit(
                    &path,
                    Node {
                        kind: Kind::Open {
                            empty,
                            attributes: &scratch.attributes,
                        },
                        span: start..end,
                    },
                )?;
                if empty {
                    path.pop();
                }
            }
            Event::End(_) => {
                pending_pop = true;
                if path.is_empty() {
                    return Err(unsupported("unbalanced XML", "XLSX XML"));
                }
                visit(
                    &path,
                    Node {
                        kind: Kind::Close,
                        span: start..end,
                    },
                )?;
                path.pop();
            }
            Event::Text(t) => {
                let value = match plain_text(text, &t) {
                    Some(value) => Cow::Borrowed(value),
                    None => {
                        if cdata_end(&t) {
                            return Err(unsupported("CDATA terminator in text", "XLSX XML"));
                        }
                        let value = t.xml_content().map_err(backend)?;
                        // A borrowed value is a slice of the already validated input.
                        if let Cow::Owned(value) = &value {
                            valid_text(value)?;
                        }
                        value
                    }
                };
                if path.is_empty() && !value.trim().is_empty() {
                    return Err(unsupported("text outside XML root", "XLSX XML"));
                }
                visit(
                    &path,
                    Node {
                        kind: Kind::Text(value),
                        span: start..end,
                    },
                )?;
            }
            Event::CData(_) => {
                // Calamine's formula/string readers ignore these events rather
                // than concatenating their text. Never accept a divergent view.
                return Err(unsupported(
                    "CDATA is not supported by the ingestion view",
                    "XLSX XML",
                ));
            }
            Event::GeneralRef(reference) => {
                let name = reference.decode().map_err(backend)?;
                let value: Cow<'a, str> = match name.as_ref() {
                    "amp" => "&".into(),
                    "lt" => "<".into(),
                    "gt" => ">".into(),
                    "quot" => "\"".into(),
                    "apos" => "'".into(),
                    _ => reference
                        .resolve_char_ref()
                        .map_err(backend)?
                        .ok_or_else(|| unsupported("unknown XML entity", "XLSX XML"))?
                        .to_string()
                        .into(),
                };
                if path.is_empty() {
                    return Err(unsupported("entity outside XML root", "XLSX XML"));
                }
                valid_text(&value)?;
                visit(
                    &path,
                    Node {
                        kind: Kind::Text(value),
                        span: start..end,
                    },
                )?;
            }
            Event::DocType(_) => return Err(unsupported("DTD/entity declarations", "XLSX XML")),
            Event::Decl(d) => {
                if roots != 0 || declaration {
                    return Err(unsupported("misplaced XML declaration", "XLSX XML"));
                }
                declaration = true;
                if d.version().map_err(backend)?.as_ref() != b"1.0" {
                    return Err(unsupported("unsupported XML version", "XLSX XML"));
                }
                if let Some(encoding) = d.encoding() {
                    let encoding = encoding.map_err(backend)?;
                    if !(encoding.eq_ignore_ascii_case(b"utf-8")
                        || encoding.eq_ignore_ascii_case(b"us-ascii") && bytes.is_ascii())
                    {
                        return Err(unsupported("unsupported XML encoding", "XLSX XML"));
                    }
                }
            }
            Event::Eof => {
                if !path.is_empty() || roots != 1 {
                    return Err(unsupported("incomplete XML document", "XLSX XML"));
                }
                return Ok(());
            }
            Event::Comment(_) | Event::PI(_) => {}
        }
    }
}
/// The text event as a slice of the validated input when it needs no
/// end-of-line normalization (no `\r`, U+0085 or U+2028 lead byte) and
/// cannot contain `]]>`; otherwise `None`.
fn plain_text<'a>(input: &'a str, raw: &[u8]) -> Option<&'a str> {
    if raw.iter().any(|b| matches!(b, b'\r' | 0xC2 | 0xE2 | b']')) {
        return None;
    }
    let at = offset(input.as_bytes(), raw).ok()?;
    input.get(at..at + raw.len())
}
/// Whether text contains the CDATA section terminator `]]>`.
fn cdata_end(text: &[u8]) -> bool {
    let mut from = 0;
    while let Some(at) = text[from..].iter().position(|b| *b == b'>') {
        let at = from + at;
        if at >= 2 && text[at - 2] == b']' && text[at - 1] == b']' {
            return true;
        }
        from = at + 1;
    }
    false
}
