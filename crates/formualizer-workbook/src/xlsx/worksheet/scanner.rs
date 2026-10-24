//! Flat worksheet scan emitting borrowed literals and retained formula marks.
//! Effects follow source event order without editing authoritative bytes.
pub(in crate::xlsx) use super::events as emission;
use super::*;
mod generated;
mod rules;
use rules::{RuleContext, RuleEvent};
mod state;
use state::SheetState;
#[cfg(test)]
mod path_tests;
mod scalar;
use super::shared;
use scalar::{FormulaState, ScalarCell};

use super::cursor::{Cursor, Position};

#[derive(Clone, Copy)]
enum Boundary {
    Open { empty: bool },
    Close,
}

/// Preserve the source spelling for the native sink without changing decoded
/// admission/value semantics. Every input here has passed attribute validation.
fn raw_attribute_span(bytes: &[u8], span: &Range<usize>) -> Result<Range<usize>, IoError> {
    let raw = bytes
        .get(span.clone())
        .ok_or_else(|| unsupported("unavailable XML attribute source span", "XLSX XML"))?;
    let quote = raw
        .iter()
        .position(|b| matches!(b, b'\'' | b'"'))
        .ok_or_else(|| unsupported("unavailable XML attribute source span", "XLSX XML"))?;
    let end = span
        .end
        .checked_sub(1)
        .filter(|end| span.start + quote < *end)
        .ok_or_else(|| unsupported("unavailable XML attribute source span", "XLSX XML"))?;
    Ok(span.start + quote + 1..end)
}

struct AttributeValue<'a> {
    value: Cow<'a, str>,
    span: Range<usize>,
}

/// Typed hot locals or the exact generic cold validation result. No generic
/// attribute record is manufactured for an ignored hot attribute.
enum TagAttributes<'n, 'a> {
    Hot(&'n crate::xlsx::xml::worksheet::attributes::HotAttributes<'a>),
    Cold(&'n [xml::Attribute<'a>]),
}
impl<'n, 'a> TagAttributes<'n, 'a> {
    fn generic(&self) -> &'n [xml::Attribute<'a>] {
        match self {
            Self::Cold(a) => a,
            Self::Hot(_) => &[],
        }
    }
    fn attribute(&self, ns: &str, local: &str) -> Option<AttributeValue<'a>> {
        match self {
            Self::Hot(a) if ns.is_empty() => a.get(local).map(|a| AttributeValue {
                value: Cow::Borrowed(a.value),
                span: a.span.clone(),
            }),
            Self::Hot(_) => None,
            Self::Cold(a) => a
                .iter()
                .find(|a| a.local == local && *a.ns == *ns)
                .map(|a| AttributeValue {
                    value: a.value.clone(),
                    span: a.span.clone(),
                }),
        }
    }
    fn value(&self, name: &str) -> Option<&'n str> {
        match self {
            Self::Hot(a) => a.get(name).map(|a| a.value),
            Self::Cold(a) => a
                .iter()
                .find(|a| a.local == name && a.ns.is_empty())
                .map(|a| a.value.as_ref()),
        }
    }
    fn required(&self, name: &str) -> Result<&'n str, IoError> {
        self.value(name)
            .ok_or_else(|| unsupported(format!("missing {name} attribute"), "XLSX XML"))
    }
    fn required_attribute(&self, name: &str) -> Result<AttributeValue<'a>, IoError> {
        self.attribute("", name)
            .ok_or_else(|| unsupported(format!("missing {name} attribute"), "XLSX XML"))
    }
}

/// Canonical coordinates in one pass. Uncommon accepted spellings still
/// go through the unchanged `coord` fallback and retain their source spelling.
pub(super) fn plain_coord(value: &str) -> Option<(u32, u32)> {
    let (mut row, mut col) = (0u32, 0u32);
    let (mut letters, mut digits) = (0u8, 0u8);
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' if digits == 0 && letters < 3 => {
                letters += 1;
                col = col * 26 + u32::from(b - b'A') + 1;
            }
            b'0'..=b'9' if letters != 0 && digits < 7 => {
                if digits == 0 && b == b'0' {
                    return None;
                }
                digits += 1;
                row = row * 10 + u32::from(b - b'0');
            }
            _ => return None,
        }
    }
    (digits != 0 && row <= 1_048_576 && col <= 16_384).then_some((row, col))
}

#[cfg(test)]
pub(in crate::xlsx) fn scan(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanRetention,
    observed: &mut usize,
    logical_cells: &mut u64,
) -> Result<Scanned, IoError> {
    scan_emitting(bytes, options, mode, observed, logical_cells, |_| Ok(()))
}

pub(in crate::xlsx) fn scan_emitting(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanRetention,
    observed: &mut usize,
    logical_cells: &mut u64,
    mut emit: impl FnMut(emission::Literal<'_>) -> Result<(), IoError>,
) -> Result<Scanned, IoError> {
    let mut needs_index = false;
    match scan_inner::<false>(
        bytes,
        options,
        mode,
        (observed, logical_cells),
        &mut needs_index,
        &mut emit,
        &mut (0, 0),
    ) {
        Err(_) if needs_index => Ok(Scanned::NeedsIndex),
        result => result.map(Scanned::Done),
    }
}

fn scan_inner<const COUNT_PATHS: bool>(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanRetention,
    counts: (&mut usize, &mut u64),
    needs_index: &mut bool,
    emit: &mut impl FnMut(emission::Literal<'_>) -> Result<(), IoError>,
    path_counts: &mut (usize, usize),
) -> Result<Scan, IoError> {
    let (observed, logical_cells) = counts;
    let mut sheet = SheetState::new(mode);
    let mut cursor = Cursor::new();
    use crate::xlsx::options::checkpoint;
    use crate::xlsx::xml::{
        Element, Kind, Node, Ns, Scopes, attribute, backend, bind, cdata_end, content, local_part,
        offset, plain_text, qualified_name, valid_text,
    };
    use quick_xml::{
        Reader,
        events::{
            Event,
            attributes::{AttrError, Attribute as RawAttribute, Attributes},
        },
        name::QName,
    };
    let text = std::str::from_utf8(bytes).map_err(|_| unsupported("non-UTF-8 XML", "XLSX XML"))?;
    valid_text(text)?;
    let mut reader = Reader::from_str(text);
    reader.config_mut().check_end_names = true;
    reader.config_mut().check_comments = true;
    let mut scopes = Scopes::new();
    let mut parsed = Vec::new();
    let mut attributes = Vec::new();
    let mut hot = crate::xlsx::xml::worksheet::attributes::HotAttributes::default();
    let mut path = Vec::new();
    let mut depth = 0usize;
    let mut roots = 0;
    let mut events = 0u64;
    let mut declaration = false;
    let mut pending_pop = false;
    let mut pending_path_pop = false;
    let mut scoped_depths = Vec::new();
    let mut sheet_data_generation = None;
    let mut sheet_data_depth = None;
    // Text goes straight from the borrowed quick-xml event to cell state.
    // Only the uncommon rich-inline adapter needs a generic XML node.
    macro_rules! accept_text {
        ($value:expr, $start:expr, $end:expr) => {{
            let value = $value;
            if sheet.open_cell.is_inline() {
                cursor.materialize(&mut path, depth)?;
            }
            sheet
                .open_cell
                .text(value, $start..$end, text, &path, cursor.position)?;
        }};
    }
    loop {
        if events & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        events += 1;
        if pending_path_pop {
            if path.len() == depth {
                path.pop();
            }
            depth -= 1;
            pending_path_pop = false;
        }
        if pending_pop {
            scopes.pop();
            pending_pop = false;
        }
        let decoder = reader.decoder();
        let start = reader.buffer_position() as usize;
        let event = reader.read_event().map_err(backend)?;
        let end = reader.buffer_position() as usize;
        let span = start..end;
        let (boundary, attrs, element) = match event {
            Event::Start(ref e) | Event::Empty(ref e) => {
                let empty = matches!(event, Event::Empty(_));
                let (_, content) = content(text, e)?;
                let name_len = e.name().as_ref().len();
                let qualified = &content[..name_len];
                let fast = sheet_data_depth.is_some()
                    && matches!(
                        cursor.position,
                        Position::SheetData
                            | Position::Row
                            | Position::Cell
                            | Position::CellFormula
                            | Position::CellValue
                            | Position::CellInline
                    )
                    && sheet_data_generation == Some(scopes.generation)
                    && matches!(qualified, "row" | "c" | "f" | "v" | "is" | "t")
                    && hot.parse(content, qualified, start);
                if COUNT_PATHS {
                    if fast {
                        path_counts.0 += 1;
                    } else {
                        path_counts.1 += 1;
                    }
                }
                let malformed = if fast {
                    false
                } else {
                    bind(&mut scopes, content, name_len, &mut parsed)?
                };
                pending_pop = empty && !fast;
                if depth >= options.limits.max_xml_depth {
                    return Err(unsupported("XML depth limit", "XLSX XML"));
                }
                if depth == 0 {
                    roots += 1;
                    if roots != 1 {
                        return Err(unsupported("multiple XML roots", "XLSX XML"));
                    }
                }
                if !fast {
                    qualified_name(qualified.as_bytes())?;
                }
                let element = Element {
                    ns: if fast {
                        Ns::Main
                    } else {
                        scopes.element(qualified)?
                    },
                    local: local_part(qualified),
                    qualified,
                };
                attributes.clear();
                let mut seen = None;
                if !fast {
                    if malformed {
                        for a in Attributes::new(content, name_len) {
                            attribute(
                                a.map_err(backend)?,
                                content,
                                start,
                                decoder,
                                &mut scopes,
                                &mut seen,
                                &mut attributes,
                            )?;
                        }
                    } else {
                        for (i, p) in parsed.iter().enumerate() {
                            if let Some(prev) = parsed[..i].iter().find(|q| q.key == p.key) {
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
                                &mut attributes,
                            )?;
                        }
                    }
                }
                // Re-establish a default-main witness only after full cold
                // binding, name resolution and attribute validation of this tag.
                if !fast && !qualified.contains(':') && element.ns.is_main() {
                    sheet_data_generation = Some(scopes.generation);
                }
                if !fast {
                    cursor.materialize(&mut path, depth)?;
                    path.push(element.clone());
                }
                depth += 1;
                if !empty && !fast {
                    scoped_depths.push(depth);
                }
                if depth == 2
                    && path[0].ns.is_main()
                    && path[0].local == "worksheet"
                    && path[1].ns.is_main()
                    && path[1].local == "sheetData"
                    && !empty
                {
                    sheet_data_depth = Some(depth);
                }
                pending_path_pop = empty;
                (
                    Boundary::Open { empty },
                    if fast {
                        TagAttributes::Hot(&hot)
                    } else {
                        TagAttributes::Cold(&attributes)
                    },
                    element,
                )
            }
            Event::End(e) => {
                if sheet_data_depth == Some(depth) {
                    sheet_data_depth = None;
                }
                pending_pop = scoped_depths.last().copied() == Some(depth);
                if pending_pop {
                    scoped_depths.pop();
                }
                if depth == 0 {
                    return Err(unsupported("unbalanced XML", "XLSX XML"));
                }
                let element = if path.len() == depth {
                    path.last()
                        .ok_or_else(|| unsupported("unbalanced XML", "XLSX XML"))?
                        .clone()
                } else {
                    let name = e.name();
                    let raw = name.as_ref();
                    let at = offset(bytes, raw)?;
                    let qualified = text
                        .get(at..at + raw.len())
                        .ok_or_else(|| unsupported("unavailable XML name span", "XLSX XML"))?;
                    Element {
                        ns: Ns::Main,
                        local: qualified,
                        qualified,
                    }
                };
                pending_path_pop = true;
                (Boundary::Close, TagAttributes::Cold(&[]), element)
            }
            Event::Text(t) => {
                let value = match plain_text(text, &t) {
                    Some(value) => Cow::Borrowed(value),
                    None => {
                        if cdata_end(&t) {
                            return Err(unsupported("CDATA terminator in text", "XLSX XML"));
                        }
                        let value = t.xml_content().map_err(backend)?;
                        if let Cow::Owned(value) = &value {
                            valid_text(value)?;
                        }
                        value
                    }
                };
                if path.is_empty() && !value.trim().is_empty() {
                    return Err(unsupported("text outside XML root", "XLSX XML"));
                }
                accept_text!(value, start, end);
                continue;
            }
            Event::CData(_) => {
                return Err(unsupported(
                    "CDATA is not supported by the ingestion view",
                    "XLSX XML",
                ));
            }
            Event::GeneralRef(reference) => {
                let name = reference.decode().map_err(backend)?;
                let value: Cow<'_, str> = match name.as_ref() {
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
                accept_text!(value, start, end);
                continue;
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
                continue;
            }
            Event::Eof => {
                if !path.is_empty() || roots != 1 {
                    return Err(unsupported("incomplete XML document", "XLSX XML"));
                }
                break;
            }
            Event::Comment(_) | Event::PI(_) => continue,
        };
        if sheet.open_cell.is_inline() {
            if path.len() < depth {
                cursor.materialize(&mut path, depth - 1)?;
                path.push(element.clone());
            }
            let kind = match boundary {
                Boundary::Open { empty } => Kind::Open {
                    empty,
                    attributes: attrs.generic(),
                },
                Boundary::Close => Kind::Close,
            };
            sheet
                .open_cell
                .inline_text
                .event(&path, &Node::new(kind, span.clone()), bytes)?;
        }
        let tag = Tag::of(&element);
        if matches!(boundary, Boundary::Open { .. }) {
            cursor.open(tag, element.local, depth);
        }
        let closes = matches!(boundary, Boundary::Open { empty: true } | Boundary::Close);
        let rule_event = RuleEvent {
            attrs,
            element,
            path: &path,
            span,
            tag,
            position: cursor.position,
            restore: cursor.restore,
            depth,
            direct: cursor.direct(depth),
            empty: matches!(boundary, Boundary::Open { empty: true }),
        };
        let mut context = RuleContext {
            bytes,
            options,
            mode,
            observed,
            needs_index,
        };
        match boundary {
            Boundary::Open { .. } => sheet.open(&rule_event, &mut context, emit)?,
            Boundary::Close => sheet.close(&rule_event, &mut context, emit)?,
        }

        if closes {
            cursor.close(depth);
        }
    }
    sheet.finish(options, logical_cells)
}
