//! Namespace-aware, bounded XML events with source offsets for surgical edits.
use super::{IoError, XlsxRecalculateOptions, checkpoint, unsupported};
use quick_xml::{NsReader, events::Event, name::ResolveResult};
use std::collections::HashSet;
use std::ops::Range;

pub(super) const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
pub(super) const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
pub(super) const OFFICE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

#[derive(Debug)]
pub(super) struct Element {
    pub ns: String,
    pub local: String,
    pub qualified: String,
}
#[derive(Debug)]
pub(super) struct Attribute {
    pub ns: String,
    pub local: String,
    pub qualified: String,
    pub value: String,
    /// Key through closing quote, excluding preceding whitespace.
    pub span: Range<usize>,
}
#[derive(Debug)]
pub(super) enum Kind {
    Open {
        empty: bool,
        attributes: Vec<Attribute>,
    },
    Close,
    Text(String),
}
#[derive(Debug)]
pub(super) struct Node {
    pub kind: Kind,
    pub span: Range<usize>,
}
impl Node {
    pub fn attribute(&self, ns: &str, local: &str) -> Option<&Attribute> {
        match &self.kind {
            Kind::Open { attributes, .. } => {
                attributes.iter().find(|a| a.ns == ns && a.local == local)
            }
            _ => None,
        }
    }
    pub fn value(&self, name: &str) -> Option<&str> {
        self.attribute("", name).map(|a| a.value.as_str())
    }
    pub fn required(&self, name: &str) -> Result<&str, IoError> {
        self.value(name)
            .ok_or_else(|| unsupported(format!("missing {name} attribute"), "XLSX XML"))
    }
}
pub(super) fn path_is(path: &[Element], ns: &str, names: &[&str]) -> bool {
    path.len() == names.len()
        && path
            .iter()
            .zip(names)
            .all(|(e, n)| e.ns == ns && e.local == *n)
}
fn valid_text(text: &str) -> Result<(), IoError> {
    if text.chars().all(|c| matches!(c, '\t'|'\n'|'\r'|' '..='\u{d7ff}'|'\u{e000}'..='\u{fffd}'|'\u{10000}'..='\u{10ffff}')) { Ok(()) }
    else { Err(unsupported("XML-invalid character", "XLSX XML")) }
}
fn qualified_name(bytes: &[u8]) -> Result<(), IoError> {
    // OOXML names are ASCII; reject other naming grammars rather than relying
    // on quick-xml's intentionally permissive lexical name handling.
    let pieces: Vec<_> = bytes.split(|b| *b == b':').collect();
    if pieces.len() > 2
        || pieces.iter().any(|p| {
            p.is_empty()
                || !(p[0].is_ascii_alphabetic() || p[0] == b'_')
                || p[1..]
                    .iter()
                    .any(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-')))
        })
    {
        return Err(unsupported(
            "unsupported/malformed XML qualified name",
            "XLSX XML",
        ));
    }
    Ok(())
}
fn namespace(value: ResolveResult<'_>) -> Result<String, IoError> {
    match value {
        ResolveResult::Bound(ns) => String::from_utf8(ns.as_ref().to_vec())
            .map_err(|e| IoError::from_backend("xlsx-xml", e)),
        ResolveResult::Unbound => Ok(String::new()),
        ResolveResult::Unknown(_) => Err(unsupported("unbound XML namespace prefix", "XLSX XML")),
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
pub(super) fn walk(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mut visit: impl FnMut(&[Element], Node) -> Result<(), IoError>,
) -> Result<(), IoError> {
    let text = std::str::from_utf8(bytes).map_err(|_| unsupported("non-UTF-8 XML", "XLSX XML"))?;
    valid_text(text)?;
    let mut reader = NsReader::from_str(text);
    reader.config_mut().check_end_names = true;
    reader.config_mut().check_comments = true;
    let mut path = Vec::new();
    let mut roots = 0;
    let mut events = 0u64;
    let mut declaration = false;
    loop {
        if events & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        events += 1;
        let start = reader.buffer_position() as usize;
        let event = reader
            .read_event()
            .map_err(|e| IoError::from_backend("xlsx-xml", e))?;
        let end = reader.buffer_position() as usize;
        match event {
            Event::Start(ref e) | Event::Empty(ref e) => {
                if path.len() >= options.limits.max_xml_depth {
                    return Err(unsupported("XML depth limit", "XLSX XML"));
                }
                if path.is_empty() {
                    roots += 1;
                    if roots != 1 {
                        return Err(unsupported("multiple XML roots", "XLSX XML"));
                    }
                }
                qualified_name(e.name().as_ref())?;
                let (ns, local) = reader.resolver().resolve_element(e.name());
                let element = Element {
                    ns: namespace(ns)?,
                    local: String::from_utf8(local.as_ref().to_vec())
                        .map_err(|e| IoError::from_backend("xlsx-xml", e))?,
                    qualified: String::from_utf8(e.name().as_ref().to_vec())
                        .map_err(|e| IoError::from_backend("xlsx-xml", e))?,
                };
                let mut attributes = Vec::new();
                let mut seen = HashSet::new();
                for a in e.attributes() {
                    let a = a.map_err(|e| IoError::from_backend("xlsx-xml", e))?;
                    qualified_name(a.key.as_ref())?;
                    if a.value.iter().any(|b| matches!(b, b'\t' | b'\r' | b'\n')) {
                        return Err(unsupported(
                            "unnormalized XML attribute whitespace",
                            "XLSX XML",
                        ));
                    }
                    if a.value.contains(&b'<') {
                        return Err(unsupported("unescaped attribute markup", "XLSX XML"));
                    }
                    let decoded = a
                        .decode_and_unescape_value(reader.decoder())
                        .map_err(|e| IoError::from_backend("xlsx-xml", e))?
                        .into_owned();
                    valid_text(&decoded)?;
                    if a.key.as_ref() == b"xmlns" || a.key.as_ref().starts_with(b"xmlns:") {
                        continue;
                    }
                    let (ns, local) = reader.resolver().resolve_attribute(a.key);
                    let ns = namespace(ns)?;
                    let local = String::from_utf8(local.as_ref().to_vec())
                        .map_err(|e| IoError::from_backend("xlsx-xml", e))?;
                    if !seen.insert((ns.clone(), local.clone())) {
                        return Err(unsupported("duplicate expanded XML attribute", "XLSX XML"));
                    }
                    let key_offset = offset(e.as_ref(), a.key.as_ref())?;
                    let value_end = offset(e.as_ref(), a.value.as_ref())? + a.value.len();
                    if !matches!(e.get(value_end), Some(b'\'' | b'"')) {
                        return Err(unsupported("unquoted XML attribute", "XLSX XML"));
                    }
                    attributes.push(Attribute {
                        ns,
                        local,
                        qualified: String::from_utf8(a.key.as_ref().to_vec())
                            .map_err(|e| IoError::from_backend("xlsx-xml", e))?,
                        value: decoded,
                        span: start + 1 + key_offset..start + 1 + value_end + 1,
                    });
                }
                let empty = matches!(event, Event::Empty(_));
                path.push(element);
                visit(
                    &path,
                    Node {
                        kind: Kind::Open { empty, attributes },
                        span: start..end,
                    },
                )?;
                if empty {
                    path.pop();
                }
            }
            Event::End(_) => {
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
                if t.windows(3).any(|w| w == b"]]>") {
                    return Err(unsupported("CDATA terminator in text", "XLSX XML"));
                }
                let value = t
                    .xml_content()
                    .map_err(|e| IoError::from_backend("xlsx-xml", e))?;
                valid_text(&value)?;
                if path.is_empty() && !value.trim().is_empty() {
                    return Err(unsupported("text outside XML root", "XLSX XML"));
                }
                visit(
                    &path,
                    Node {
                        kind: Kind::Text(value.into_owned()),
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
                let name = reference
                    .decode()
                    .map_err(|e| IoError::from_backend("xlsx-xml", e))?;
                let value = match name.as_ref() {
                    "amp" => "&".into(),
                    "lt" => "<".into(),
                    "gt" => ">".into(),
                    "quot" => "\"".into(),
                    "apos" => "'".into(),
                    _ => reference
                        .resolve_char_ref()
                        .map_err(|e| IoError::from_backend("xlsx-xml", e))?
                        .ok_or_else(|| unsupported("unknown XML entity", "XLSX XML"))?
                        .to_string(),
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
                if d.version()
                    .map_err(|e| IoError::from_backend("xlsx-xml", e))?
                    .as_ref()
                    != b"1.0"
                {
                    return Err(unsupported("unsupported XML version", "XLSX XML"));
                }
                if let Some(encoding) = d.encoding() {
                    let encoding = encoding.map_err(|e| IoError::from_backend("xlsx-xml", e))?;
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
