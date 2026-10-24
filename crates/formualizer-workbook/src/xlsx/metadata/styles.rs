//! Calculation format annotations from immutable workbook style declarations.
//! Raw style tokens retain their pinned-reader interpretation.
use crate::xlsx::{options::ReadOptions, xml};
use crate::{IoError, cache_recalculate::XlsxRecalculateOptions};
use formualizer_eval::format::FormatId;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum NumberClass {
    #[default]
    Other,
    DateTime,
    Duration,
}
impl NumberClass {
    fn builtin(id: &str) -> Self {
        // The pinned reader compares raw attribute spellings, not integer IDs.
        match id {
            "14" | "15" | "16" | "17" | "18" | "19" | "20" | "21" | "22" | "45" | "47" => {
                Self::DateTime
            }
            "46" => Self::Duration,
            _ => Self::Other,
        }
    }
    fn annotation(self, number: f64) -> Option<FormatId> {
        match self {
            Self::Other => None,
            Self::Duration => Some(FormatId::DURATION),
            Self::DateTime if (0.0..1.0).contains(&number) => Some(FormatId::TIME),
            Self::DateTime if number.fract().abs() > f64::EPSILON => Some(FormatId::DATETIME),
            Self::DateTime => Some(FormatId::DATE),
        }
    }
    /// Compatibility with calamine 0.36's custom-format classifier (MIT,
    /// copyright Johann Tuffe): only the first section matters, quoted and
    /// escaped tokens are inert, elapsed-time brackets precede date tokens.
    fn custom(format: &str) -> Self {
        let (mut escaped, mut quoted, mut am_pm) = (false, false, false);
        let (mut depth, mut elapsed, mut previous) = (0usize, false, ' ');
        for ch in format.chars() {
            if escaped {
                escaped = false;
            } else if matches!(ch, '_' | '\\' | '*') {
                escaped = true;
            } else if quoted {
                if ch == '"' {
                    quoted = false;
                }
            } else if ch == '"' {
                quoted = true;
            } else if ch == ';' {
                return Self::Other;
            } else if ch == '[' {
                depth += 1;
            } else if ch == ']' {
                if depth == 1 && elapsed {
                    return Self::Duration;
                }
                depth = depth.saturating_sub(1);
            } else if depth == 0 && !am_pm && matches!(ch, 'a' | 'A') {
                am_pm = true;
            } else if depth == 0
                && ((am_pm && matches!(ch, 'p' | 'm' | '/' | 'P' | 'M'))
                    || (!am_pm
                        && matches!(
                            ch,
                            'd' | 'm' | 'h' | 'y' | 's' | 'D' | 'M' | 'H' | 'Y' | 'S'
                        )))
            {
                return Self::DateTime;
            } else if !(elapsed && ch.eq_ignore_ascii_case(&previous)) {
                elapsed = previous == '[' && matches!(ch, 'm' | 'h' | 's' | 'M' | 'H' | 'S');
            }
            previous = ch;
        }
        Self::Other
    }
}

#[derive(Debug, Default)]
pub(in crate::xlsx) struct Styles(Vec<NumberClass>);
impl Styles {
    pub(in crate::xlsx) fn parse(
        bytes: &[u8],
        options: &XlsxRecalculateOptions,
    ) -> Result<Self, IoError> {
        Self::parse_read(bytes, &ReadOptions::from(options))
    }

    pub(in crate::xlsx) fn parse_direct(
        bytes: &[u8],
        options: &ReadOptions,
    ) -> Result<Self, IoError> {
        use quick_xml_cells::events::Event;
        let mut xml = crate::xlsx::loader::cells::CellXml::new(bytes, options);
        let mut custom = HashMap::new();
        let mut values = Vec::new();
        let mut section: Option<Vec<u8>> = None;
        let mut done = false;
        loop {
            match xml.read_event()? {
                Event::Start(e) if !done => {
                    let local = e.local_name();
                    let kind = local.as_ref();
                    if section.is_none() {
                        if matches!(kind, b"numFmts" | b"cellXfs") {
                            section = Some(kind.to_vec());
                        }
                        continue;
                    }
                    if kind == b"numFmt" && section.as_deref() == Some(b"numFmts") {
                        let (mut id, mut code) = (None, None);
                        for attr in e.attributes().with_checks(false) {
                            let attr = attr.map_err(|e| IoError::from_backend("xlsx-style", e))?;
                            match attr.key.as_ref() {
                                b"numFmtId" => {
                                    id = Some(
                                        String::from_utf8(attr.value.into_owned())
                                            .map_err(|e| IoError::from_backend("xlsx-style", e))?,
                                    )
                                }
                                b"formatCode" => {
                                    let raw = String::from_utf8(attr.value.into_owned())
                                        .map_err(|e| IoError::from_backend("xlsx-style", e))?;
                                    code = Some(
                                        quick_xml_cells::escape::unescape(&raw)
                                            .map_err(|e| IoError::from_backend("xlsx-style", e))?
                                            .into_owned(),
                                    );
                                }
                                _ => (),
                            }
                        }
                        if let (Some(id), Some(code)) = (id, code) {
                            custom.insert(id, NumberClass::custom(&code));
                        }
                    } else if kind == b"xf" && section.as_deref() == Some(b"cellXfs") {
                        let mut class = NumberClass::Other;
                        for attr in e.attributes().with_checks(false) {
                            let attr = attr.map_err(|e| IoError::from_backend("xlsx-style", e))?;
                            if attr.key.as_ref() == b"numFmtId" {
                                let raw = std::str::from_utf8(&attr.value)
                                    .map_err(|e| IoError::from_backend("xlsx-style", e))?;
                                class = custom
                                    .get(raw)
                                    .copied()
                                    .unwrap_or_else(|| NumberClass::builtin(raw));
                                break;
                            }
                        }
                        values.push(class);
                    }
                }
                Event::End(e) => {
                    if section.as_deref() == Some(e.local_name().as_ref()) {
                        section = None;
                    } else if section.is_none() && e.local_name().as_ref() == b"styleSheet" {
                        done = true;
                    }
                }
                Event::Eof => return Ok(Self(values)),
                _ => (),
            }
        }
    }

    pub(in crate::xlsx) fn format_index(
        &self,
        index: Option<usize>,
        number: f64,
    ) -> Option<FormatId> {
        self.0
            .get(index?)
            .copied()
            .unwrap_or_default()
            .annotation(number)
    }

    /// Extraction uses shared read bounds, not evaluation/publication admission.
    pub(in crate::xlsx) fn parse_read(
        bytes: &[u8],
        options: &ReadOptions,
    ) -> Result<Self, IoError> {
        let mut custom = HashMap::new();
        let mut values = Vec::new();
        let mut section = None;
        let mut done = false;
        xml::walk(bytes, options, |path, node| {
            let Some(element) = path.last() else {
                return Ok(());
            };
            if done {
                return Ok(());
            }
            if matches!(node.kind, xml::Kind::Close) {
                if section == Some(element.local) {
                    section = None;
                } else if section.is_none() && element.local == "styleSheet" {
                    done = true;
                }
                return Ok(());
            }
            let xml::Kind::Open { empty, .. } = &node.kind else {
                return Ok(());
            };
            if options.policy == crate::xlsx::options::ReadPolicy::Compatible
                && *empty
                && section == Some(element.local)
            {
                // Empty elements own an immediate close in the ordinary reader.
                section = None;
                return Ok(());
            }
            if section.is_none() {
                if matches!(element.local, "numFmts" | "cellXfs") && !empty {
                    section = Some(element.local);
                } else if element.local == "styleSheet" && *empty {
                    done = true;
                }
                return Ok(());
            }
            if element.local == "numFmt" && section == Some("numFmts") {
                if let (Some(id), Some(format)) = (
                    raw_attribute(bytes, &node, "numFmtId"),
                    node.attribute("", "formatCode"),
                ) {
                    custom.insert(id, NumberClass::custom(&format.value));
                }
            } else if element.local == "xf" && section == Some("cellXfs") {
                values.push(raw_attribute(bytes, &node, "numFmtId").map_or(
                    NumberClass::Other,
                    |id| {
                        custom
                            .get(id)
                            .copied()
                            .unwrap_or_else(|| NumberClass::builtin(id))
                    },
                ));
            }
            Ok(())
        })?;
        Ok(Self(values))
    }
    pub(in crate::xlsx) fn len(&self) -> usize {
        self.0.len()
    }
    /// `style` is the raw source token, not XML-unescaped text. Missing means
    /// unformatted even if style zero is a date; malformed means style zero.
    pub(in crate::xlsx) fn format(&self, style: Option<&str>, number: f64) -> Option<FormatId> {
        let raw = style?;
        let index = if raw.bytes().all(|b| b.is_ascii_digit()) {
            raw.parse::<usize>().unwrap_or(0)
        } else {
            0
        };
        self.0
            .get(index)
            .copied()
            .unwrap_or_default()
            .annotation(number)
    }
}
pub(in crate::xlsx) fn raw_attribute<'a>(
    bytes: &'a [u8],
    node: &xml::Node<'_, '_>,
    name: &str,
) -> Option<&'a str> {
    let attribute = node.attribute("", name)?;
    let raw = std::str::from_utf8(bytes.get(attribute.span.clone())?).ok()?;
    let value = raw
        .get(raw.find('=')? + 1..)?
        .trim_start_matches([' ', '\t', '\r', '\n']);
    value.get(1..value.len().checked_sub(1)?)
}
