//! Ingest-only token lowering for immutable, validated source table geometry.
//! It never rewrites output formulas. Parser enums, not string splitting,
//! describe selectors; decoded formula bytes outside reference tokens survive.
use super::{
    IoError, Patch, SheetPlan, XlsxRecalculateOptions, checkpoint, package, sheet, tables::Table,
    unsupported,
};
use formualizer_parse::parser::{ReferenceType, SpecialItem, TableReference, TableSpecifier};
use formualizer_parse::tokenizer::{TokenStream, TokenSubType, TokenType};

fn err(t: &Table, spelling: &str) -> IoError {
    unsupported(
        "unsupported structured-reference spelling/context",
        format!("table {} reference {spelling}", t.name),
    )
}
struct Selection {
    rows: u8,
    cols: Option<(usize, usize)>,
}
fn select(
    t: &Table,
    spec: &TableSpecifier,
    out: &mut Selection,
    spelling: &str,
) -> Result<(), IoError> {
    let item = match spec {
        TableSpecifier::All => Some(SpecialItem::All),
        TableSpecifier::Data => Some(SpecialItem::Data),
        TableSpecifier::Headers => Some(SpecialItem::Headers),
        TableSpecifier::Totals => Some(SpecialItem::Totals),
        TableSpecifier::SpecialItem(s) => Some(s.clone()),
        TableSpecifier::Row(formualizer_parse::parser::TableRowSpecifier::Current) => {
            Some(SpecialItem::ThisRow)
        }
        TableSpecifier::Column(c) => {
            let i = t
                .columns
                .iter()
                .position(|h| h.name.to_lowercase() == c.to_lowercase())
                .ok_or_else(|| err(t, spelling))?;
            if out.cols.replace((i, i)).is_some() {
                return Err(err(t, spelling));
            }
            None
        }
        TableSpecifier::ColumnRange(a, b) => {
            let a = t
                .columns
                .iter()
                .position(|h| h.name.to_lowercase() == a.to_lowercase())
                .ok_or_else(|| err(t, spelling))?;
            let b = t
                .columns
                .iter()
                .position(|h| h.name.to_lowercase() == b.to_lowercase())
                .ok_or_else(|| err(t, spelling))?;
            if out.cols.replace((a.min(b), a.max(b))).is_some() {
                return Err(err(t, spelling));
            }
            None
        }
        TableSpecifier::Combination(items) => {
            for s in items {
                select(t, s, out, spelling)?;
            }
            None
        }
        _ => return Err(err(t, spelling)),
    };
    if let Some(item) = item {
        let bit = match item {
            SpecialItem::All => 16,
            SpecialItem::Data => 2,
            SpecialItem::Headers => 1,
            SpecialItem::Totals => 4,
            SpecialItem::ThisRow => 8,
        };
        if out.rows & bit != 0 {
            return Err(err(t, spelling));
        }
        out.rows |= bit;
    }
    Ok(())
}
fn column(mut col: u32) -> String {
    let mut s = Vec::new();
    while col != 0 {
        col -= 1;
        s.push((b'A' + (col % 26) as u8) as char);
        col /= 26;
    }
    s.into_iter().rev().collect()
}
fn lower(
    reference: &TableReference,
    spelling: &str,
    sheet_name: &str,
    cell: &sheet::Cell,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
) -> Result<String, IoError> {
    let mut found = None;
    for (sheet, plan) in sheets.iter().zip(plans) {
        for table in &plan.tables {
            if (reference.name.is_empty()
                && sheet.name == sheet_name
                && table.rect.contains(cell.row, cell.col))
                || (!reference.name.is_empty()
                    && table.name.to_lowercase() == reference.name.to_lowercase())
            {
                if found.is_some() {
                    return Err(unsupported("ambiguous structured reference", spelling));
                }
                found = Some((sheet, table));
            }
        }
    }
    let (sheet, table) =
        found.ok_or_else(|| unsupported("unknown table in structured reference", spelling))?;
    let mut selection = Selection {
        rows: 0,
        cols: None,
    };
    if let Some(spec) = &reference.specifier {
        select(table, spec, &mut selection, spelling)?;
    } else {
        return Err(err(table, spelling));
    }
    let r = table.rect;
    let rows = if selection.rows == 0 {
        2
    } else {
        selection.rows
    };
    let (first_row, last_row) = if rows == 16 {
        (r.first_row, r.last_row)
    } else if rows == 8 {
        if sheet.name != sheet_name
            || !r.contains(cell.row, cell.col)
            || (table.header && cell.row == r.first_row)
        {
            return Err(err(table, spelling));
        }
        if let Some(span) = cell.shared_rect()
            && (!r.contains(span.first_row, span.first_col)
                || !r.contains(span.last_row, span.last_col)
                || (table.header && span.first_row == r.first_row))
        {
            return Err(err(table, spelling));
        }
        (cell.row, cell.row)
    } else {
        if rows & !7 != 0 || (rows & 1 != 0 && !table.header) || (rows & 4 != 0 && !table.totals) {
            return Err(err(table, spelling));
        }
        let body_first = r.first_row + u32::from(table.header);
        let body_last = r.last_row - u32::from(table.totals);
        if rows == 5 && body_first <= body_last {
            return Err(unsupported(
                "nonrectangular structured-reference selection",
                format!("table {} reference {spelling}", table.name),
            ));
        }
        let first = if rows & 1 != 0 {
            r.first_row
        } else if rows & 2 != 0 {
            body_first
        } else {
            r.last_row
        };
        let last = if rows & 4 != 0 {
            r.last_row
        } else if rows & 2 != 0 {
            body_last
        } else {
            r.first_row
        };
        (first, last)
    };
    if first_row > last_row {
        return Err(unsupported(
            "empty structured-reference selection",
            format!("table {} reference {spelling}", table.name),
        ));
    }
    let (first_col, last_col) = selection
        .cols
        .map(|(a, b)| (r.first_col + a as u32, r.first_col + b as u32))
        .unwrap_or((r.first_col, r.last_col));
    let prefix = if sheet.name == sheet_name {
        String::new()
    } else {
        format!("'{}'!", sheet.name.replace('\'', "''"))
    };
    // Relative this-row columns are exact only when replay cannot displace
    // the formula horizontally. Whole-table bounds always stay absolute.
    let one_column_placement = if cell.formula_kind == "shared" {
        cell.shared_rect()
            .is_some_and(|span| span.first_col == span.last_col)
    } else {
        cell.formula_kind == "normal"
    };
    let col_absolute = if rows == 8 && one_column_placement {
        ""
    } else {
        "$"
    };
    let row_absolute = if rows == 8 { "" } else { "$" };
    let first = format!(
        "{col_absolute}{}{}{}",
        column(first_col),
        row_absolute,
        first_row
    );
    let last = format!(
        "{col_absolute}{}{}{}",
        column(last_col),
        row_absolute,
        last_row
    );
    Ok(if first == last {
        format!("{prefix}{first}")
    } else {
        format!("{prefix}{first}:{last}")
    })
}

pub(super) fn patches(
    sheet_name: &str,
    plan: &SheetPlan,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    options: &XlsxRecalculateOptions,
) -> Result<Vec<Patch>, IoError> {
    let mut patches = Vec::new();
    for cell in &plan.cells {
        checkpoint(&options.cancel)?;
        let formula = &cell.formula_text;
        if formula.is_empty()
            || (!formula.contains('[') && !formula.to_ascii_uppercase().contains("INDIRECT"))
        {
            continue;
        }
        let token_formula = if formula.starts_with('=') {
            formula.clone()
        } else {
            format!("={formula}")
        };
        let offset = usize::from(!formula.starts_with('='));
        let stream = TokenStream::new(&token_formula).map_err(|e| {
            unsupported(
                "cannot tokenize table-bearing formula",
                format!("{sheet_name} {}: {e}", cell.address),
            )
        })?;
        let indirect = stream.spans.iter().any(|s| {
            s.token_type == TokenType::Func
                && stream.source()[s.start..s.end]
                    .trim_end_matches('(')
                    .rsplit('.')
                    .next()
                    .is_some_and(|n| n.eq_ignore_ascii_case("INDIRECT"))
        });
        if indirect
            && stream.spans.iter().any(|s| {
                s.subtype == TokenSubType::Text
                    && (stream.source()[s.start..s.end].contains('[')
                        || plans.iter().flat_map(|p| &p.tables).any(|t| {
                            stream.source()[s.start..s.end]
                                .to_lowercase()
                                .contains(&t.name.to_lowercase())
                        }))
            })
        {
            return Err(unsupported(
                "text-built structured reference through INDIRECT",
                format!("{sheet_name} {}", cell.address),
            ));
        }
        let mut lowered = String::new();
        let mut at = 0;
        let mut changed = false;
        for span in &stream.spans {
            if span.token_type != TokenType::Operand || span.subtype != TokenSubType::Range {
                continue;
            }
            let spelling = &stream.source()[span.start..span.end];
            if !spelling.contains('[') {
                continue;
            }
            let reference = ReferenceType::from_string(spelling).map_err(|e| {
                unsupported(
                    "unsupported structured-reference encoding",
                    format!("{spelling}: {e}"),
                )
            })?;
            let ReferenceType::Table(reference) = reference else {
                return Err(unsupported("unsupported bracket reference", spelling));
            };
            let value = lower(&reference, spelling, sheet_name, cell, sheets, plans)?;
            lowered.push_str(&formula[at..span.start - offset]);
            lowered.push_str(&value);
            at = span.end - offset;
            changed = true;
        }
        if changed {
            lowered.push_str(&formula[at..]);
            let raw = &plan.data[cell.formula_open.end..cell.formula_end];
            let close = raw
                .iter()
                .rposition(|b| *b == b'<')
                .ok_or_else(|| unsupported("missing formula end tag", &cell.address))?;
            patches.push(Patch {
                span: cell.formula_open.end..cell.formula_open.end + close,
                replacement: quick_xml::escape::escape(&lowered)
                    .replace('\r', "&#13;")
                    .into_bytes(),
            });
        }
    }
    Ok(patches)
}
