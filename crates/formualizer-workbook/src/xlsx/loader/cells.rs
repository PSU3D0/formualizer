// SPDX-License-Identifier: MIT
// Copyright 2016-2026, Johann Tuffe.
// Portions adapted from Calamine 0.36.0, src/xlsx/{cells_reader.rs,mod.rs}
// and src/{attrs.rs,utils.rs}. See LICENSE.calamine in this directory.
//
// XLSX-only direct reader: Calamine's local-name cell loop feeds our Arrow
// sinks and source-formula records without publication state or XML nodes.
use super::*;
use crate::xlsx::types::XlsxError;
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use quick_xml_cells as quick_xml;
use std::borrow::Cow;
use worksheet::events::Value;

type Result<T> = std::result::Result<T, IoError>;
fn xlsx(error: XlsxError) -> IoError {
    IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(error))
}
fn eof(node: &'static str) -> IoError {
    xlsx(XlsxError::XmlEof(node))
}
fn xml_error(error: impl Into<quick_xml::Error>) -> IoError {
    xlsx(XlsxError::Xml(error.into()))
}

// Bound checks ride on the existing event stream; no validation rescan.
pub(in crate::xlsx) struct CellXml<'a, 'o> {
    reader: Reader<&'a [u8]>,
    options: &'o ReadOptions,
    depth: usize,
    events_until_poll: u8,
    last_poll_position: u64,
}
impl<'a, 'o> CellXml<'a, 'o> {
    pub(in crate::xlsx) fn new(source: &'a [u8], options: &'o ReadOptions) -> Self {
        let mut reader = Reader::from_reader(source);
        reader.config_mut().expand_empty_elements = true;
        Self {
            reader,
            options,
            depth: 0,
            events_until_poll: 0,
            last_poll_position: 0,
        }
    }
    #[inline]
    pub(in crate::xlsx) fn read_event(&mut self) -> Result<Event<'a>> {
        if self.events_until_poll == 0
            || (self.options.cancel.is_some()
                && self
                    .reader
                    .buffer_position()
                    .saturating_sub(self.last_poll_position)
                    >= 64 * 1024)
        {
            if self.reader.buffer_position() == 0
                && self.reader.get_ref().len() > self.options.limits.max_worksheet_bytes
            {
                return Err(super::super::options::resource(
                    self.options,
                    "XML part byte limit",
                    "worksheet XML",
                ));
            }
            super::super::options::checkpoint(&self.options.cancel)?;
            self.events_until_poll = 128;
            self.last_poll_position = self.reader.buffer_position();
        }
        self.events_until_poll -= 1;
        let event = self.reader.read_event().map_err(xml_error)?;
        match &event {
            Event::Start(e) => {
                if e.local_name().as_ref() == b"row" {
                    super::super::options::checkpoint(&self.options.cancel)?;
                    self.events_until_poll = 128;
                    self.last_poll_position = self.reader.buffer_position();
                }
                self.depth += 1;
                if self.depth > self.options.limits.max_xml_depth {
                    return Err(super::super::options::resource(
                        self.options,
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
            Event::Eof => super::super::options::checkpoint(&self.options.cancel)?,
            _ => (),
        }
        Ok(event)
    }
    fn read_to_end(&mut self, closing: quick_xml::name::QName<'_>) -> Result<()> {
        let depth = self.depth;
        loop {
            let event = match self.read_event() {
                Err(IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                    XlsxError::Xml(quick_xml::Error::IllFormed(
                        quick_xml::errors::IllFormedError::MismatchedEndTag { .. },
                    )),
                )))
                | Err(IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                    XlsxError::XmlEof(_),
                ))) => {
                    // The pinned reader's read_to_end reports the missing
                    // requested close, not the mismatched tag encountered
                    // inside a skipped scalar. Keep the balance refusal.
                    return Err(xml_error(quick_xml::Error::IllFormed(
                        quick_xml::errors::IllFormedError::MissingEndTag(
                            String::from_utf8_lossy(closing.as_ref()).into_owned(),
                        ),
                    )));
                }
                result => result?,
            };
            match event {
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

pub(super) fn header(xml: &mut CellXml<'_, '_>) -> Result<(usize, usize)> {
    let mut first_kind = None;
    let mut dimensions = (1, 1);
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
                    let rect = dimension(
                        span.ok_or_else(|| xlsx(XlsxError::UnexpectedNode("dimension")))?,
                    )?;
                    dimensions = (rect.end.row as usize + 1, rect.end.col as usize + 1);
                }
                b"sheetData" => return Ok(dimensions),
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
}

pub(super) fn scan(
    xml: &mut CellXml<'_, '_>,
    options: &ReadOptions,
    sink: &mut LoadSink<'_>,
) -> Result<()> {
    let mut row_index = 0;
    let mut col_index = 0;
    let mut cell_count = 0usize;
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
                if let Some(r) = r {
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
                let pos = if let Some(r) = r {
                    let p = coordinate(r)?;
                    col_index = p.1;
                    p
                } else {
                    (row_index, col_index)
                };
                cell_count += 1;
                if cell_count > options.limits.max_cells {
                    return Err(super::super::options::resource(
                        options,
                        "cell count limit",
                        "worksheet XML",
                    ));
                }
                if pos.0 >= 1_048_576 || pos.1 >= options.limits.max_columns {
                    return Err(super::super::options::resource(
                        options,
                        "cell coordinate limit",
                        "worksheet XML",
                    ));
                }
                let mut value = Value::Empty;
                let mut formula = None;
                loop {
                    match xml.read_event()? {
                        Event::Start(e) if e.local_name().as_ref() == b"f" => {
                            let text = text(xml, e.name(), false)?;
                            let (t, si, range) =
                                get_attrs!(e, b"t" => t, b"si" => si, b"ref" => range)
                                    .map_err(|e| xml_error(quick_xml::Error::InvalidAttr(e)))?;

                            let sequence = sink.record_count as u64;
                            let coord0 = SourceCoord {
                                row: pos.0,
                                col: pos.1,
                            };
                            let parsed = (|| -> Result<_> {
                                let record = if t == Some(b"shared") {
                                    let index = si
                                        .and_then(|v| {
                                            atoi_simd::parse::<usize, true, false>(v).ok()
                                        })
                                        .ok_or_else(|| {
                                            xlsx(XlsxError::Unexpected(
                                                "si attribute must be a number",
                                            ))
                                        })?;
                                    if let Some(range) = range {
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
                                    if sink.emit.is_some() {
                                        return Err(sink
                                            .stream_error
                                            .as_ref()
                                            .expect("captured error")
                                            .to_error());
                                    }
                                }
                            }
                        }
                        Event::Start(e) if e.local_name().as_ref() == b"is" => {
                            value = Value::Text(Cow::Owned(read_string(xml, e.name())?));
                        }
                        Event::Start(e) if e.local_name().as_ref() == b"v" => {
                            value = read_value(xml, e.name(), kind)?;
                            if matches!(kind, Some(b"inlineStr" | b"is")) {
                                value = Value::Empty;
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
                let format = if let Value::Number(number) = value {
                    sink.styles.format_index(style.map(index), number)
                } else {
                    None
                };
                sink.emit_cell(pos.0 + 1, pos.1 + 1, value, format, formula)?;
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
) -> Result<Value<'a>> {
    let value = if matches!(kind, Some(b"n" | b"s" | b"b" | b"e") | None) {
        let value = match xml.read_event()? {
            Event::Text(t) => scalar(t.as_ref(), kind)?,
            Event::End(e) if e.name() == closing => return Ok(Value::Empty),
            Event::Eof => return Err(eof("v")),
            _ => Value::Empty,
        };
        xml.read_to_end(closing)?;
        value
    } else {
        let content = text(xml, closing, false)?;
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
    Ok(value)
}
fn index(raw: &[u8]) -> usize {
    // The pinned parser already rejects signs, non-digits and overflow while
    // accepting arbitrarily long leading-zero runs. Do not rescan the token.
    atoi_simd::parse::<usize, true, false>(raw).unwrap_or(0)
}

fn scalar(v: &[u8], kind: Option<&[u8]>) -> Result<Value<'static>> {
    let s = std::str::from_utf8(v)
        .map_err(|_| xlsx(XlsxError::Unexpected("invalid UTF-8 in cell value")))?;
    Ok(match kind {
        Some(b"s") => {
            if v.is_empty() {
                Value::Empty
            } else {
                Value::Shared(index(v))
            }
        } // Resolve the typed index once in the sink.
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

pub(in crate::xlsx) fn shared_strings(source: &[u8], options: &ReadOptions) -> Result<Vec<String>> {
    let mut xml = CellXml::new(source, options);
    let mut values = Vec::new();
    let mut done = false;
    loop {
        match xml.read_event()? {
            Event::Start(e) if !done && e.local_name().as_ref() == b"si" => {
                values.push(read_string(&mut xml, e.name())?)
            }
            Event::End(e) if e.local_name().as_ref() == b"sst" => done = true,
            Event::Eof => return Ok(values),
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
    let mut parts = raw.split(|b| *b == b':');
    let start = coordinate(parts.next().expect("split has at least one element"))?;
    let mut count = 1;
    let end = if let Some(raw) = parts.next() {
        count += 1;
        coordinate(raw)?
    } else {
        start
    };
    // Validate every token before the count error, as the pinned reader does,
    // without allocating a vector proportional to a malicious ref attribute.
    for raw in parts {
        coordinate(raw)?;
        count += 1;
    }
    if count > 2 {
        return Err(xlsx(XlsxError::DimensionCount(count)));
    }
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
    fn bulk_cancellation_is_amortized_but_bounded_inside_one_row() {
        let token = formualizer_eval::engine::CancelToken::new();
        let options = ReadOptions {
            cancel: Some(token.clone()),
            ..Default::default()
        };
        let source = format!(
            "<worksheet><sheetData><row>{}</row></sheetData></worksheet>",
            "<c><v>1</v></c>".repeat(1024)
        );
        let mut xml = CellXml::new(source.as_bytes(), &options);
        // Entry check succeeds. A cancellation inside a huge row must be seen
        // within 128 events, without loading an atomic on every event.
        xml.read_event().unwrap();
        token.cancel();
        let mut successful = 0;
        loop {
            match xml.read_event() {
                Err(_) => break,
                Ok(Event::Eof) => panic!("cancelled row escaped its checkpoint"),
                _ => successful += 1,
            }
            assert!(successful <= 128);
        }
        assert!(successful > 0, "do not poll an atomic on every event");
    }

    #[test]
    fn bulk_header_and_raw_tokens_preserve_dimensions_and_fallbacks() {
        let options = ReadOptions::default();
        for (source, expected) in [
            ("<worksheet><sheetData/>", (1, 1)),
            ("<worksheet><dimension ref=\"B2:D5\"/><sheetData/>", (5, 4)),
        ] {
            let mut xml = CellXml::new(source.as_bytes(), &options);
            assert_eq!(header(&mut xml).unwrap(), expected);
        }
        for raw in [
            b"+1".as_slice(),
            b" 1",
            b"1 ",
            b"1x",
            b"18446744073709551616000",
            b"",
        ] {
            assert_eq!(index(raw), 0);
        }
        assert_eq!(index(b"000000000000000000000000000001"), 1);
        let many = std::iter::repeat_n("A1", 10_000)
            .collect::<Vec<_>>()
            .join(":");
        assert!(matches!(
            dimension(many.as_bytes()),
            Err(IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                XlsxError::DimensionCount(10_000)
            )))
        ));
        assert!(matches!(
            dimension(b"A1:B2:bad"),
            Err(IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                XlsxError::RangeWithoutRowComponent
            )))
        ));
        assert!(coordinate(b"$A$1").is_err());
        assert!(coordinate(b"A0000000000000000000000000000000000000000000000000000001").is_ok());
    }

    #[test]
    fn bulk_text_cdata_entities_and_unicode_keep_pinned_reader_semantics() {
        let options = ReadOptions::default();
        // Ordinary cached string values ignore CDATA; inline display strings
        // retain it. Do not turn on CDATA globally to remove the old repair pass.
        let source = "<v>a<![CDATA[b]]>c&amp;d\u{85}e\u{2028}f</v>";
        let mut xml = CellXml::new(source.as_bytes(), &options);
        let Event::Start(e) = xml.read_event().unwrap() else {
            panic!()
        };
        match read_value(&mut xml, e.name(), Some(b"str")).unwrap() {
            Value::Text(value) => assert_eq!(value, "ac&d\u{85}e\u{2028}f"),
            _ => panic!(),
        }
        let source = "<is><t>a<![CDATA[b]]>c&amp;d</t></is>";
        let mut xml = CellXml::new(source.as_bytes(), &options);
        let Event::Start(e) = xml.read_event().unwrap() else {
            panic!()
        };
        assert_eq!(read_string(&mut xml, e.name()).unwrap(), "abc&d");
    }

    #[test]
    fn bulk_large_text_forces_the_next_byte_checkpoint_and_part_limits() {
        let token = formualizer_eval::engine::CancelToken::new();
        let options = ReadOptions {
            cancel: Some(token.clone()),
            ..Default::default()
        };
        let source = format!("<v>{}</v>", "x".repeat(128 * 1024));
        let mut xml = CellXml::new(source.as_bytes(), &options);
        xml.read_event().unwrap();
        xml.read_event().unwrap();
        token.cancel();
        assert!(
            xml.read_event().is_err(),
            "large text must not wait for another 128 events"
        );
        let mut options = ReadOptions::default();
        options.limits.max_worksheet_bytes = 3;
        assert!(CellXml::new(b"<sst/>", &options).read_event().is_err());
    }

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
