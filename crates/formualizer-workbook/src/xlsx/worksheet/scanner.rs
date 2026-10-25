//! Flat worksheet scan emitting borrowed literals and retained formula marks.
//! Effects follow source event order without editing authoritative bytes.
pub(in crate::xlsx) use super::events as emission;
use super::*;
use crate::xlsx::options::{ReadOptions, ReadPolicy};
mod generated;
mod rules;
use rules::{RuleContext, RuleEvent};
mod state;
mod tokenizer;
use state::SheetState;
#[cfg(test)]
mod path_tests;
#[cfg(test)]
mod policy_tests;
#[cfg(test)]
mod retention_tests;
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
    mode: ScanStrategy,
    observed: &mut usize,
    logical_cells: &mut u64,
) -> Result<Scanned, IoError> {
    scan_emitting(bytes, options, mode, observed, logical_cells, |_| Ok(()))
}

pub(in crate::xlsx) fn scan_emitting(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanStrategy,
    observed: &mut usize,
    logical_cells: &mut u64,
    emit: impl FnMut(emission::Literal<'_>) -> Result<(), IoError>,
) -> Result<Scanned, IoError> {
    scan_with(
        bytes,
        &ReadOptions::from(options),
        ScanRequest::recalculate(mode),
        observed,
        logical_cells,
        emission::Literals(emit),
    )
}

pub(in crate::xlsx) fn scan_with(
    bytes: &[u8],
    options: &ReadOptions,
    request: ScanRequest,
    observed: &mut usize,
    logical_cells: &mut u64,
    mut emit: impl emission::Sink,
) -> Result<Scanned, IoError> {
    let mut needs_index = false;
    // Select the rule set once. Const instantiation removes strict-only work;
    // scalar decoding never reads a runtime admission flag.
    let result = match options.policy {
        ReadPolicy::Strict => scan_inner::<false, true>(
            bytes,
            options,
            request,
            (observed, logical_cells),
            &mut needs_index,
            &mut emit,
            &mut (0, 0),
        ),
        ReadPolicy::Compatible => scan_inner::<false, false>(
            bytes,
            options,
            request,
            (observed, logical_cells),
            &mut needs_index,
            &mut emit,
            &mut (0, 0),
        ),
    };
    match result {
        Err(_) if needs_index => Ok(Scanned::NeedsIndex),
        result => result.map(Scanned::Done),
    }
}

fn scan_inner<const COUNT_PATHS: bool, const STRICT: bool>(
    bytes: &[u8],
    options: &ReadOptions,
    request: ScanRequest,
    counts: (&mut usize, &mut u64),
    needs_index: &mut bool,
    emit: &mut impl emission::Sink,
    path_counts: &mut (usize, usize),
) -> Result<Scan, IoError> {
    let (observed, logical_cells) = counts;
    let mut sheet = SheetState::<STRICT>::new(request);
    let mut cursor = Cursor::new();
    use crate::xlsx::xml::{Kind, Node, backend};
    use quick_xml::events::Event;
    let mut lexical = tokenizer::XmlState::new(bytes)?;
    // Text goes straight from the borrowed quick-xml event to cell state.
    // Only the uncommon rich-inline adapter needs a generic XML node.
    macro_rules! accept_text {
        ($value:expr, $start:expr, $end:expr) => {{
            let value = $value;
            if sheet.open_cell.is_inline() {
                cursor.materialize(&mut lexical.path, lexical.depth)?;
            }
            sheet.open_cell.text(
                value,
                $start..$end,
                lexical.text,
                &lexical.path,
                cursor.position,
            )?;
        }};
    }
    loop {
        lexical.advance(options)?;
        let start = lexical.reader.buffer_position() as usize;
        let event = lexical.reader.read_event().map_err(backend)?;
        let end = lexical.reader.buffer_position() as usize;
        let span = start..end;
        let (boundary, attrs, element) = match event {
            Event::Start(ref e) | Event::Empty(ref e) => {
                let empty = matches!(event, Event::Empty(_));
                let (fast, element) = lexical.open::<COUNT_PATHS>(
                    e,
                    empty,
                    start,
                    options,
                    &mut cursor,
                    path_counts,
                )?;
                (
                    Boundary::Open { empty },
                    if fast {
                        TagAttributes::Hot(&lexical.hot)
                    } else {
                        TagAttributes::Cold(&lexical.attributes)
                    },
                    element,
                )
            }
            Event::End(e) => (Boundary::Close, TagAttributes::Cold(&[]), lexical.close(e)?),
            Event::Text(t) => {
                let value = lexical.text(t)?;
                accept_text!(value, start, end);
                continue;
            }
            Event::CData(data) => {
                if STRICT {
                    return Err(unsupported(
                        "CDATA is not supported by the ingestion view",
                        "XLSX XML",
                    ));
                }
                if lexical.path.is_empty() {
                    return Err(unsupported("CDATA outside XML root", "XLSX XML"));
                }
                let value = data.xml_content().map_err(backend)?;
                accept_text!(value, start + 9, end - 3);
                continue;
            }
            Event::GeneralRef(reference) => {
                let value = lexical.reference(reference)?;
                accept_text!(value, start, end);
                continue;
            }
            Event::DocType(_) => return Err(unsupported("DTD/entity declarations", "XLSX XML")),
            Event::Decl(d) => {
                lexical.declaration(d)?;
                continue;
            }
            Event::Eof => {
                lexical.finish()?;
                break;
            }
            Event::Comment(_) | Event::PI(_) => continue,
        };
        if sheet.open_cell.is_inline() {
            if lexical.path.len() < lexical.depth {
                cursor.materialize(&mut lexical.path, lexical.depth - 1)?;
                lexical.path.push(element.clone());
            }
            let kind = match boundary {
                Boundary::Open { empty } => Kind::Open {
                    empty,
                    attributes: attrs.generic(),
                },
                Boundary::Close => Kind::Close,
            };
            sheet.open_cell.inline_text.event(
                &lexical.path,
                &Node::new(kind, span.clone()),
                bytes,
            )?;
        }
        let mut tag = Tag::of(&element);
        if !STRICT {
            // Ordinary loading recognizes structural local names. Namespace
            // binding/well-formedness was still checked by the tokenizer.
            tag.main = true;
        }
        if matches!(boundary, Boundary::Open { .. }) {
            cursor.open(tag, element.local, lexical.depth);
        }
        let closes = matches!(boundary, Boundary::Open { empty: true } | Boundary::Close);
        let rule_event = RuleEvent {
            attrs,
            element,
            path: &lexical.path,
            span,
            tag,
            position: cursor.position,
            restore: cursor.restore,
            depth: lexical.depth,
            direct: cursor.direct(lexical.depth),
            empty: matches!(boundary, Boundary::Open { empty: true }),
        };
        let mut context = RuleContext {
            bytes,
            options,
            request,
            observed,
            needs_index,
        };
        match boundary {
            Boundary::Open { .. } => sheet.open(&rule_event, &mut context, emit)?,
            Boundary::Close => sheet.close(&rule_event, &mut context, emit)?,
        }

        if closes {
            cursor.close(lexical.depth);
        }
    }
    sheet.finish(options, logical_cells)
}
