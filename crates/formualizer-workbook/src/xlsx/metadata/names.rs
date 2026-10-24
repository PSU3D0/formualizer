//! Compatibility parsing of defined-name literals and absolute ranges.
//! Formula text is assembled before trimming, preserving XML entity whitespace.
use crate::traits::{DefinedName, DefinedNameDefinition, DefinedNameScope};
use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_parse::parser::ReferenceType;
use quick_xml::{
    Reader as XmlReader,
    events::{BytesRef, BytesStart},
    name::QName,
};
use std::{collections::HashSet, io::BufRead};

pub(crate) struct NameParsing;
impl NameParsing {
    const EXCEL_MAX_ROWS: u32 = 1_048_576;
    const EXCEL_MAX_COLS: u32 = 16_384;

    fn normalize_open_ended_bounds(
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    ) -> Option<(u32, u32, u32, u32)> {
        let mut sr = start_row;
        let mut sc = start_col;
        let mut er = end_row;
        let mut ec = end_col;
        if sr.is_none() && er.is_none() {
            sr = Some(1);
            er = Some(Self::EXCEL_MAX_ROWS);
        }
        if sc.is_none() && ec.is_none() {
            sc = Some(1);
            ec = Some(Self::EXCEL_MAX_COLS);
        }
        if sr.is_some() && er.is_none() {
            er = Some(Self::EXCEL_MAX_ROWS);
        }
        if er.is_some() && sr.is_none() {
            sr = Some(1);
        }
        if sc.is_some() && ec.is_none() {
            ec = Some(Self::EXCEL_MAX_COLS);
        }
        if ec.is_some() && sc.is_none() {
            sc = Some(1);
        }
        let (sr, sc, er, ec) = (sr?, sc?, er?, ec?);
        if er < sr || ec < sc {
            return None;
        }
        Some((sr, sc, er, ec))
    }

    pub(crate) fn convert_defined_name(
        name: &str,
        raw_formula: &str,
        local_sheet_id: Option<usize>,
        sheet_names: &[String],
    ) -> Option<DefinedName> {
        let mut trimmed = raw_formula.trim();
        if let Some(rest) = trimmed.strip_prefix('=') {
            trimmed = rest.trim();
        }
        if trimmed.is_empty() {
            return None;
        }
        let scope_sheet = local_sheet_id.and_then(|idx| sheet_names.get(idx).cloned());
        if local_sheet_id.is_some() && scope_sheet.is_none() {
            return None;
        }
        let literal = trimmed
            .parse::<f64>()
            .ok()
            .filter(|n| n.is_finite())
            .map(LiteralValue::Number)
            .or_else(|| {
                let ast = formualizer_parse::parser::parse(format!("={trimmed}")).ok()?;
                match ast.node_type {
                    formualizer_parse::parser::ASTNodeType::Literal(value) => Some(value),
                    _ => None,
                }
            });
        if let Some(value) = literal {
            return Some(DefinedName {
                name: name.to_owned(),
                scope: if scope_sheet.is_some() {
                    DefinedNameScope::Sheet
                } else {
                    DefinedNameScope::Workbook
                },
                scope_sheet,
                definition: DefinedNameDefinition::Literal { value },
            });
        }
        if crate::backends::has_union_comma(trimmed) {
            return None;
        }
        let reference = ReferenceType::from_string(trimmed).ok()?;
        let scope_sheet = local_sheet_id.and_then(|idx| sheet_names.get(idx).cloned());
        let scope = if scope_sheet.is_some() {
            DefinedNameScope::Sheet
        } else {
            DefinedNameScope::Workbook
        };
        let base_sheet = scope_sheet.as_deref();
        let (sheet_name, start_row, start_col, end_row, end_col) = match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet = sheet.or_else(|| base_sheet.map(|s| s.to_string()))?;
                (sheet, row, col, row, col)
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                let (sr, sc, er, ec) =
                    Self::normalize_open_ended_bounds(start_row, start_col, end_row, end_col)?;
                let sheet = sheet.or_else(|| base_sheet.map(|s| s.to_string()))?;
                (sheet, sr, sc, er, ec)
            }
            _ => return None,
        };
        let address = RangeAddress::new(sheet_name, start_row, start_col, end_row, end_col).ok()?;
        Some(DefinedName {
            name: name.to_string(),
            scope,
            scope_sheet,
            definition: DefinedNameDefinition::Range { address },
        })
    }

    pub(crate) fn decode_attr<R: BufRead>(
        reader: &XmlReader<R>,
        start: &BytesStart<'_>,
        key: &[u8],
    ) -> Option<String> {
        start
            .attributes()
            .filter_map(Result::ok)
            .find(|attr| attr.key == QName(key))
            .and_then(|attr| {
                attr.decode_and_unescape_value(reader.decoder())
                    .ok()
                    .map(|v| v.into_owned())
            })
    }

    pub(crate) fn append_xml_entity(
        entity: &BytesRef<'_>,
        buffer: &mut String,
    ) -> Result<(), quick_xml::Error> {
        let decoded = entity.decode()?;
        match decoded.as_ref() {
            "lt" => buffer.push('<'),
            "gt" => buffer.push('>'),
            "amp" => buffer.push('&'),
            "apos" => buffer.push('\''),
            "quot" => buffer.push('"'),
            _ => {
                if let Some(ch) = entity.resolve_char_ref()? {
                    buffer.push(ch);
                } else {
                    return Err(quick_xml::Error::Escape(
                        quick_xml::escape::EscapeError::UnrecognizedEntity(
                            0..0,
                            format!("&{decoded};"),
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn fallback_defined_names(
        names: &[(String, String)],
        sheet_names: &[String],
    ) -> Vec<DefinedName> {
        let mut out = Vec::new();
        let mut seen: HashSet<(DefinedNameScope, Option<String>, String)> = HashSet::new();
        for (name, formula) in names {
            if let Some(converted) = Self::convert_defined_name(name, formula, None, sheet_names) {
                let key = (
                    converted.scope.clone(),
                    converted.scope_sheet.clone(),
                    converted.name.clone(),
                );
                if seen.insert(key) {
                    out.push(converted);
                }
            }
        }
        out
    }
}
