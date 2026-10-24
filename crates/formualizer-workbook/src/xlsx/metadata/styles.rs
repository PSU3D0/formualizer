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
