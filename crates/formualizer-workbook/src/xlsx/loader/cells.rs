// SPDX-License-Identifier: MIT
// Copyright 2016-2026, Johann Tuffe.
// Portions adapted from Calamine 0.36.0, src/xlsx/{cells_reader.rs,mod.rs}
// and src/{attrs.rs,utils.rs}. See LICENSE.calamine in this directory.
//
// XLSX-only direct reader: Calamine's local-name cell loop feeds our Arrow
// sinks and source-formula records without publication state or XML nodes.
use super::*;
use calamine::XlsxError;
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use quick_xml_cells as quick_xml;
use std::{borrow::Cow, ops::Range};
use worksheet::events::Value;

type Result<T> = std::result::Result<T, IoError>;
fn xlsx(error: XlsxError) -> IoError {
    IoError::Calamine(calamine::Error::Xlsx(error))
}
fn eof(node: &'static str) -> IoError {
    xlsx(XlsxError::XmlEof(node))
}
fn xml_error(error: impl Into<quick_xml::Error>) -> IoError {
    xlsx(XlsxError::Xml(error.into()))
}

// Bound checks ride on the existing event stream; no validation rescan.
struct CellXml<'a, 'o> {
    reader: Reader<&'a [u8]>,
    options: &'o ReadOptions,
    depth: usize,
}
impl<'a, 'o> CellXml<'a, 'o> {
    fn new(source: &'a [u8], options: &'o ReadOptions) -> Self {
        let mut reader = Reader::from_reader(source);
        reader.config_mut().expand_empty_elements = true;
        Self {
            reader,
            options,
            depth: 0,
        }
    }
    #[inline]
    fn read_event(&mut self) -> Result<Event<'a>> {
        super::super::options::checkpoint(&self.options.cancel)?;
        let event = self.reader.read_event().map_err(xml_error)?;
        match &event {
            Event::Start(_) => {
                self.depth += 1;
                if self.depth > self.options.limits.max_xml_depth {
                    return Err(super::super::options::unsupported(
                        "XML depth limit",
                        "worksheet XML",
                    ));
                }
            }
            Event::End(_) => self.depth = self.depth.saturating_sub(1),
            Event::DocType(_) => {
                return Err(super::super::options::unsupported("DTD", "worksheet XML"));
            }
            Event::Eof if self.depth != 0 => return Err(eof("worksheet")),
            _ => (),
        }
        Ok(event)
    }
    fn buffer_position(&self) -> u64 {
        self.reader.buffer_position()
    }
    fn read_to_end(&mut self, closing: quick_xml::name::QName<'_>) -> Result<()> {
        let depth = self.depth;
        loop {
            match self.read_event()? {
                Event::End(e) if self.depth < depth && e.name() == closing => return Ok(()),
                Event::Eof => return Err(eof("v")),
                _ => (),
            }
        }
    }
}

use quick_xml::events::attributes::AttrError;
struct RawAttrIter<'a> {
    raw: &'a [u8],
    pos: usize,
}

impl<'a> RawAttrIter<'a> {
    #[inline]
    fn new(raw: &'a [u8]) -> Self {
        Self { raw, pos: 0 }
    }
}

impl<'a> Iterator for RawAttrIter<'a> {
    type Item = std::result::Result<(&'a [u8], &'a [u8]), AttrError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let raw = self.raw;
        let len = raw.len();

        // note: make "pos" local to ensure the compiler keeps it in
        // a register across the scan loops (struct field otherwise)
        let mut pos = self.pos;

        // skip whitespace before the key
        while pos < len && raw[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len {
            self.pos = pos;
            return None;
        }

        // key: scan to '=' with a single comparison
        // per byte (handling whitespace around '=')
        let key_start = pos;
        while pos < len && raw[pos] != b'=' {
            pos += 1;
        }
        if pos >= len {
            // a key with no '=' is malformed; park the cursor at the end so the
            // next call returns None rather than re-reporting the same error
            self.pos = len;
            return Some(Err(AttrError::ExpectedEq(key_start)));
        }
        let key = raw[key_start..pos].trim_ascii_end();
        pos += 1; // skip '='

        // skip whitespace after '=' (normally none: loop exits immediately)
        while pos < len && raw[pos].is_ascii_whitespace() {
            pos += 1;
        }

        // quoted value (XML mandates quotes; anything else, including a missing
        // value at end-of-input, is malformed)
        let quote = raw.get(pos).copied();
        if quote != Some(b'"') && quote != Some(b'\'') {
            self.pos = len;
            return Some(Err(AttrError::UnquotedValue(pos)));
        }
        let quote = quote.unwrap();
        pos += 1; // skip opening quote
        let val_start = pos;
        while pos < len && raw[pos] != quote {
            pos += 1;
        }
        let val = &raw[val_start..pos];
        if pos < len {
            pos += 1; // skip closing quote
        }
        self.pos = pos;
        Some(Ok((key, val)))
    }
}

trait RawAttributes {
    fn iter_raw_attrs(&self) -> RawAttrIter<'_>;
}
impl RawAttributes for BytesStart<'_> {
    #[inline]
    fn iter_raw_attrs(&self) -> RawAttrIter<'_> {
        RawAttrIter::new(self.attributes_raw())
    }
}
macro_rules! get_attrs {
    ($e:expr, $($key:expr => $var:ident),+ $(,)?) => {{
        $(let mut $var = None;)+
        let mut found = 0u8;
        let total = get_attrs!(@count $($key),+);
        let mut result = Ok(());
        for item in $e.iter_raw_attrs() {
            match item {
                Ok((k, v)) => {
                    match k {
                        $($key => { $var = Some(v); found += 1; })+
                        _ => {}
                    }
                    if found == total {
                        break;
                    }
                }
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        result.map(|()| ($($var),+))
    }};
    (@count $first:expr $(, $rest:expr)*) => {
        1u8 $(+ get_attrs!(@count_one $rest))*
    };
    (@count_one $e:expr) => { 1u8 };
}

#[inline]
fn source_span(source: &[u8], value: Option<&[u8]>) -> Option<Range<usize>> {
    value.map(|value| {
        let start = value.as_ptr() as usize - source.as_ptr() as usize;
        start..start + value.len()
    })
}

fn bytes<'a>(source: &'a [u8], span: &Option<Range<usize>>) -> Option<&'a [u8]> {
    span.as_ref().map(|s| &source[s.clone()])
}

pub(super) fn scan(source: &[u8], options: &ReadOptions, sink: &mut LoadSink<'_>) -> Result<()> {
    let mut xml = CellXml::new(source, options);
    // Keep balance checking enabled: malformed XML is not a loader tolerance.
    let mut row_index = 0;
    let mut col_index = 0;
    let mut first_kind = None;
    let mut cell_count = 0usize;
    loop {
        match xml.read_event()? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"dimension" => {
                    let span = e
                        .iter_raw_attrs()
                        .find_map(|a| match a {
                            Ok((b"ref", v)) => Some(Ok(v)),
                            Err(e) => Some(Err(e)),
                            _ => None,
                        })
                        .transpose()
                        .map_err(|e| xml_error(quick_xml::Error::InvalidAttr(e)))?;
                    let span = source_span(source, span);
                    dimension(
                        bytes(source, &span)
                            .ok_or_else(|| xlsx(XlsxError::UnexpectedNode("dimension")))?,
                    )?;
                }
                b"sheetData" => break,
                kind => {
                    if first_kind.is_none() {
                        first_kind = Some(String::from_utf8_lossy(kind).into_owned());
                    }
                }
            },
            Event::DocType(_) => {
                return Err(IoError::Unsupported {
                    feature: "XML DTD".into(),
                    context: "XLSX worksheet".into(),
                });
            }
            Event::Eof => {
                return Err(first_kind
                    .map(|k| xlsx(XlsxError::NotAWorksheet(k)))
                    .unwrap_or_else(|| eof("worksheet")));
            }
            _ => (),
        }
    }
    loop {
        match xml.read_event()? {
            Event::Start(e) if e.local_name().as_ref() == b"row" => {
                let r = e
                    .iter_raw_attrs()
                    .find_map(|a| match a {
                        Ok((b"r", v)) => Some(Ok(v)),
                        Err(e) => Some(Err(e)),
                        _ => None,
                    })
                    .transpose()
                    .map_err(|e| xml_error(quick_xml::Error::InvalidAttr(e)))?;
                let r = source_span(source, r);
                if let Some(r) = bytes(source, &r) {
                    row_index = row_column_optional(r).map_err(xlsx)?.0;
                }
            }
            Event::End(e) if e.local_name().as_ref() == b"row" => {
                row_index += 1;
                col_index = 0;
            }
            Event::Start(e) if e.local_name().as_ref() == b"c" => {
                let (r, style, kind) = get_attrs!(e, b"r" => r, b"s" => style, b"t" => kind)
                    .map_err(|e| xml_error(quick_xml::Error::InvalidAttr(e)))?;
                let (r, style, kind) = (
                    source_span(source, r),
                    source_span(source, style),
                    source_span(source, kind),
                );
                let pos = if let Some(r) = bytes(source, &r) {
                    let p = coordinate(r)?;
                    col_index = p.1;
                    p
                } else {
                    (row_index, col_index)
                };
                cell_count += 1;
                if cell_count > options.limits.max_cells {
                    return Err(super::super::options::unsupported(
                        "cell count limit",
                        "worksheet XML",
                    ));
                }
                if pos.0 >= 1_048_576 || pos.1 >= options.limits.max_columns {
                    return Err(super::super::options::unsupported(
                        "cell coordinate limit",
                        "worksheet XML",
                    ));
                }
                let mut literal = Literal {
                    row: pos.0 + 1,
                    col: pos.1 + 1,
                    style: None,
                    generated_child_hint: false,
                    raw_coordinate_span: r,
                    raw_style_span: style,
                    raw_type_span: kind,
                    raw_value_span: None,
                    value: Value::Empty,
                };
                let mut formula = None;
                loop {
                    match xml.read_event()? {
                        Event::Start(e) if e.local_name().as_ref() == b"f" => {
                            let text = text(&mut xml, e.name(), false)?;
                            let (t, si, range) =
                                get_attrs!(e, b"t" => t, b"si" => si, b"ref" => range)
                                    .map_err(|e| xml_error(quick_xml::Error::InvalidAttr(e)))?;
                            let (t, si, range) = (
                                source_span(source, t),
                                source_span(source, si),
                                source_span(source, range),
                            );
                            let sequence = sink.records.len() as u64;
                            let coord0 = SourceCoord {
                                row: pos.0,
                                col: pos.1,
                            };
                            let parsed = (|| -> Result<_> {
                                let record = if bytes(source, &t) == Some(b"shared") {
                                    let index = bytes(source, &si)
                                        .and_then(|v| {
                                            atoi_simd::parse::<usize, true, false>(v).ok()
                                        })
                                        .ok_or_else(|| {
                                            xlsx(XlsxError::Unexpected(
                                                "si attribute must be a number",
                                            ))
                                        })?;
                                    if let Some(range) = bytes(source, &range) {
                                        OwnedSpoolFormulaRecord::SharedAnchor {
                                            sequence,
                                            coord0,
                                            shared_index: index,
                                            declared_range: Some(dimension(range)?),
                                            text,
                                        }
                                    } else {
                                        OwnedSpoolFormulaRecord::SharedDescendant {
                                            sequence,
                                            coord0,
                                            shared_index: index,
                                        }
                                    }
                                } else {
                                    OwnedSpoolFormulaRecord::Ordinary {
                                        sequence,
                                        coord0,
                                        text,
                                    }
                                };
                                Ok(record)
                            })();
                            match parsed {
                                Ok(record) => formula = Some(record),
                                Err(error) => {
                                    let error = DeferredReadError::capture(error)?;
                                    sink.stream_error.get_or_insert_with(|| error.clone());
                                    sink.formula_error.get_or_insert(error);
                                }
                            }
                        }
                        Event::Start(e) if e.local_name().as_ref() == b"is" => {
                            literal.value =
                                Value::Text(Cow::Owned(read_string(&mut xml, e.name())?));
                            literal.raw_value_span = None;
                        }
                        Event::Start(e) if e.local_name().as_ref() == b"v" => {
                            let start = xml.buffer_position() as usize;
                            let t = bytes(source, &literal.raw_type_span);
                            let (value, end) = read_value(&mut xml, e.name(), t)?;
                            if !matches!(t, Some(b"inlineStr" | b"is")) {
                                literal.value = value;
                                literal.raw_value_span = Some(start..end);
                            } else {
                                literal.value = Value::Empty;
                                literal.raw_value_span = None;
                            }
                        }
                        Event::Start(_) => {
                            return Err(xlsx(XlsxError::UnexpectedNode("v, f, or is")));
                        }
                        Event::End(e) if e.local_name().as_ref() == b"c" => break,
                        Event::Eof => return Err(eof("c")),
                        _ => (),
                    }
                }
                if let Some(record) = formula {
                    sink.values.observe_formula(literal.row, literal.col)?;
                    sink.last_formula_coord = Some((literal.row, literal.col));
                    sink.records.push(record);
                    sink.formula_cache(literal)?;
                } else {
                    sink.literal(literal)?;
                }
                col_index += 1;
            }
            Event::End(e) if e.local_name().as_ref() == b"sheetData" => {
                // Consume the remainder for balance, DTD/depth and cancellation
                // checks without interpreting a second sheetData as values.
                while !matches!(xml.read_event()?, Event::Eof) {}
                return Ok(());
            }
            Event::Eof => return Err(eof("sheetData")),
            Event::DocType(_) => {
                return Err(IoError::Unsupported {
                    feature: "XML DTD".into(),
                    context: "XLSX worksheet".into(),
                });
            }
            _ => (),
        }
    }
}

fn read_value<'a>(
    xml: &mut CellXml<'a, '_>,
    closing: quick_xml::name::QName<'_>,
    kind: Option<&[u8]>,
) -> Result<(Value<'a>, usize)> {
    let mut end = xml.buffer_position() as usize;
    let value = if matches!(kind, Some(b"n" | b"s" | b"b" | b"e") | None) {
        let value = match xml.read_event()? {
            Event::Text(t) => {
                end = xml.buffer_position() as usize;
                scalar(t.as_ref(), kind)?
            }
            Event::End(e) if e.name() == closing => return Ok((Value::Empty, end)),
            Event::Eof => return Err(eof("v")),
            _ => Value::Empty,
        };
        xml.read_to_end(closing)?;
        value
    } else {
        let start = xml.buffer_position() as usize;
        let content = text(xml, closing, false)?;
        // Payload spans are needed only for raw numeric/shared-index handling.
        end = start;
        match kind {
            Some(b"str" | b"d") => Value::Text(Cow::Owned(content)),
            Some(b"inlineStr" | b"is") => Value::Empty,
            Some(other) => {
                return Err(xlsx(XlsxError::CellTAttribute(
                    String::from_utf8_lossy(other).into_owned(),
                )));
            }
            None => unreachable!(),
        }
    };
    Ok((value, end))
}
fn scalar(v: &[u8], kind: Option<&[u8]>) -> Result<Value<'static>> {
    let s = std::str::from_utf8(v)
        .map_err(|_| xlsx(XlsxError::Unexpected("invalid UTF-8 in cell value")))?;
    Ok(match kind {
        Some(b"s") => {
            if v.is_empty() {
                Value::Empty
            } else {
                Value::Shared(0)
            }
        } // raw index resolved once by ValueSink
        Some(b"b") => Value::Boolean(v != b"0"),
        Some(b"e") => Value::Error(s.parse().map_err(xlsx)?),
        Some(b"n") | None => {
            if v.is_empty() {
                Value::Empty
            } else {
                match fast_float2::parse::<f64, _>(v) {
                    Ok(n) => Value::Number(n),
                    Err(_) if kind.is_none() => Value::Text(Cow::Owned(s.to_owned())),
                    Err(_) => {
                        return Err(xlsx(XlsxError::ParseFloat(s.parse::<f64>().unwrap_err())));
                    }
                }
            }
        }
        _ => unreachable!(),
    })
}

fn text(
    xml: &mut CellXml<'_, '_>,
    closing: quick_xml::name::QName<'_>,
    cdata: bool,
) -> Result<String> {
    let mut value = String::new();
    loop {
        match xml.read_event()? {
            Event::Text(t) => value.push_str(&t.xml10_content().map_err(xml_error)?),
            Event::CData(t) if cdata => value.push_str(&t.xml10_content().map_err(xml_error)?),
            Event::GeneralRef(e) => unescape_entity_to_buffer(&e, &mut value).map_err(xml_error)?,
            Event::End(e) if e.name() == closing => return Ok(value),
            Event::Eof => return Err(eof("text")),
            Event::DocType(_) => return Err(eof("text")),
            _ => (),
        }
    }
}
fn read_string(xml: &mut CellXml<'_, '_>, closing: quick_xml::name::QName<'_>) -> Result<String> {
    let mut value = String::new();
    let mut phonetic = false;
    loop {
        match xml.read_event()? {
            Event::Start(e) if e.local_name().as_ref() == b"rPh" => phonetic = true,
            Event::End(e) if e.local_name().as_ref() == b"rPh" => phonetic = false,
            Event::End(e) if e.name() == closing => return Ok(value),
            Event::Start(e) if e.local_name().as_ref() == b"t" && !phonetic => {
                let preserve = e
                    .iter_raw_attrs()
                    .find_map(|a| match a {
                        Ok((b"xml:space", v)) => Some(Ok(v == b"preserve")),
                        Err(e) => Some(Err(e)),
                        _ => None,
                    })
                    .transpose()
                    .map_err(|e| xml_error(quick_xml::Error::InvalidAttr(e)))?
                    .unwrap_or(false);
                let part = text(xml, e.name(), true)?;
                value.push_str(&unescape_xml(if preserve {
                    &part
                } else {
                    part.trim_matches([' ', '\t', '\r', '\n'])
                }));
            }
            Event::Eof => return Err(eof("")),
            _ => (),
        }
    }
}

use quick_xml::{escape::resolve_xml_entity, events::BytesRef};
const UNICODE_ESCAPE_LENGTH: usize = 7;
fn unescape_entity_to_buffer(
    entity: &BytesRef,
    buffer: &mut String,
) -> std::result::Result<(), quick_xml::Error> {
    let decoded = entity.decode()?;

    // Handle standard XML entities directly.
    if let Some(unescaped_xml_entity) = resolve_xml_entity(&decoded) {
        buffer.push_str(unescaped_xml_entity);
        return Ok(());
    }

    // Handle character references like `#xA0`.
    if let Some(unescaped_char) = entity.resolve_char_ref()? {
        buffer.push(unescaped_char);
        return Ok(());
    }

    // Raise a quick_xml::Error if we failed to unescape the entity. The
    // location information isn't useful to the end user in this context so we
    // return `0..0`.
    Err(quick_xml::Error::Escape(
        quick_xml::escape::EscapeError::UnrecognizedEntity(0..0, format!("&{decoded};")),
    ))
}

// Function to unescape Excel XML escapes in a string. Excel encodes a character
// like "\r" as "_x000D_". In turn it escapes the literal string "_x000D_" as
// "_x005F_x000D_".
fn unescape_xml(original: &str) -> Cow<'_, str> {
    if !original.contains("_x00") {
        return Cow::Borrowed(original);
    }

    let bytes = original.as_bytes();
    let mut escaped_string = String::with_capacity(original.len());
    let mut i = 0;
    let mut has_changes = false;

    while i < bytes.len() {
        // Look for an escape sequence like: "_x00HH_"
        if i + UNICODE_ESCAPE_LENGTH <= bytes.len()
            && bytes[i] == b'_'
            && bytes.get(i..i + 4) == Some(b"_x00")
            && bytes[i + 6] == b'_'
        {
            // Extract and validate the hex digits.
            if let Ok(hex_str) = std::str::from_utf8(&bytes[i + 4..i + 6])
                && let Ok(hex_value) = u8::from_str_radix(hex_str, 16)
            {
                // Valid escape sequence, convert to a character.
                escaped_string.push(hex_value as char);

                // Skip the processed escape sequence.
                i += UNICODE_ESCAPE_LENGTH;
                has_changes = true;
                continue;
            }
        }

        // Not an escape sequence, so we copy the current UTF-8 character.
        let remaining = &original[i..];
        match remaining.chars().next() {
            Some(ch) => {
                escaped_string.push(ch);
                i += ch.len_utf8();
            }
            None => break,
        }
    }

    if has_changes {
        Cow::Owned(escaped_string)
    } else {
        Cow::Borrowed(original)
    }
}

fn row_column_optional(range: &[u8]) -> std::result::Result<(u32, Option<u32>), XlsxError> {
    let len = range.len();
    let mut i = 0;

    // Column: accumulate base-26 from letters
    // (eg: A=1, B=2, ..., Z=26, AA=27, ..., AZ=52, ..., etc)
    let mut col: u32 = 0;
    let mut row: u32 = 0;
    while i < len {
        match range[i] {
            c @ b'A'..=b'Z' => col = col.saturating_mul(26).saturating_add((c - b'A') as u32 + 1),
            c @ b'a'..=b'z' => col = col.saturating_mul(26).saturating_add((c - b'a') as u32 + 1),
            c @ b'0'..=b'9' => {
                // on first digit, capture it and transition to the row loop
                row = (c - b'0') as u32;
                i += 1;
                break;
            }
            c => return Err(XlsxError::Alphanumeric(c)),
        }
        i += 1;
    }

    // Row: accumulate base-10 from remaining digits (1-based in source)
    while i < len {
        match range[i] {
            c @ b'0'..=b'9' => row = row.saturating_mul(10).saturating_add((c - b'0') as u32),
            c => return Err(XlsxError::Alphanumeric(c)),
        }
        i += 1;
    }

    // Convert from 1-based to 0-based (col=0 means no column found)
    let row = row
        .checked_sub(1)
        .ok_or(XlsxError::RangeWithoutRowComponent)?;
    Ok((row, col.checked_sub(1)))
}

fn coordinate(raw: &[u8]) -> Result<(u32, u32)> {
    let (row, col) = row_column_optional(raw).map_err(xlsx)?;
    Ok((
        row,
        col.ok_or_else(|| xlsx(XlsxError::RangeWithoutColumnComponent))?,
    ))
}
fn dimension(raw: &[u8]) -> Result<SourceRect> {
    let parts = raw
        .split(|b| *b == b':')
        .map(coordinate)
        .collect::<Result<Vec<_>>>()?;
    let (start, end) = match parts.as_slice() {
        [a] => (*a, *a),
        [a, b] => (*a, *b),
        other => return Err(xlsx(XlsxError::DimensionCount(other.len()))),
    };
    Ok(SourceRect {
        start: SourceCoord {
            row: start.0,
            col: start.1,
        },
        end: SourceCoord {
            row: end.0,
            col: end.1,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direct_xml_keeps_depth_dtd_balance_and_cancellation_bounds() {
        let mut options = ReadOptions::default();
        options.limits.max_xml_depth = 2;
        for source in [
            b"<a><b><c/></b></a>".as_slice(),
            b"<!DOCTYPE a><a/>",
            b"<a><b></a>",
        ] {
            let mut xml = CellXml::new(source, &options);
            loop {
                match xml.read_event() {
                    Err(_) => break,
                    Ok(Event::Eof) => panic!("invalid source was accepted"),
                    _ => (),
                }
            }
        }
        let token = formualizer_eval::engine::CancelToken::new();
        options.cancel = Some(token.clone());
        token.cancel();
        assert!(CellXml::new(b"<a/>", &options).read_event().is_err());
    }
}
