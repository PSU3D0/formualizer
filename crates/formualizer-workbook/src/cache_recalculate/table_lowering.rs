//! Ingest-only token lowering for immutable, validated source table geometry.
//! It never rewrites output formulas. Parser enums, not string splitting,
//! describe selectors; decoded formula bytes outside reference tokens survive.
use super::{
    IoError, Patch, SheetPlan, XlsxRecalculateOptions, checkpoint, package, sheet, tables::Table,
    unsupported,
};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{
    ASTNode, ASTNodeType, ReferenceType, SpecialItem, TableReference, TableSpecifier,
};
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
        // #This Row is defined only in the data body: header and totals
        // placements are refused rather than guessed.
        if sheet.name != sheet_name
            || !r.contains(cell.row, cell.col)
            || (table.header && cell.row == r.first_row)
            || (table.totals && cell.row == r.last_row)
        {
            return Err(err(table, spelling));
        }
        if let Some(span) = cell.shared_rect()
            && (!r.contains(span.first_row, span.first_col)
                || !r.contains(span.last_row, span.last_col)
                || (table.header && span.first_row == r.first_row)
                || (table.totals && span.last_row == r.last_row))
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
    // Calamine replays shared followers from the lowered master text; refuse
    // an injected qualifier its expansion would rewrite.
    if !prefix.is_empty()
        && cell
            .shared_rect()
            .is_some_and(|span| span.cell_count() != Some(1))
        && super::shared_qualifiers::shifted_by_shared_expansion(&prefix)
    {
        return Err(unsupported(
            "cross-sheet table reference in a shared formula would be rewritten by shared-formula expansion",
            format!(
                "table {} on sheet {} reference {spelling}",
                table.name, sheet.name
            ),
        ));
    }
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

/// Literal text built only from string/number literals joined by `&`.
fn literal_text(ast: &ASTNode) -> Option<String> {
    match &ast.node_type {
        ASTNodeType::Literal(LiteralValue::Text(s)) => Some(s.clone()),
        ASTNodeType::Literal(LiteralValue::Number(n)) => Some(n.to_string()),
        ASTNodeType::Literal(LiteralValue::Int(n)) => Some(n.to_string()),
        ASTNodeType::BinaryOp { op, left, right } if op == "&" => {
            Some(literal_text(left)? + &literal_text(right)?)
        }
        _ => None,
    }
}
/// In a table-bearing workbook INDIRECT text must be proven free of table
/// syntax: only literal (or literal-concatenation) text without `[` or any
/// table name is admitted. Cell-sourced or computed text could name a table,
/// which would bypass this source-only lowering.
pub(super) fn indirect_text_is_table_free(ast: &ASTNode, names: &[String]) -> bool {
    match &ast.node_type {
        ASTNodeType::Function { name, args } => {
            let n = name.rsplit('.').next().unwrap_or(name);
            if n.eq_ignore_ascii_case("INDIRECT") {
                let Some(text) = args.first().and_then(literal_text) else {
                    return false;
                };
                let text = text.to_lowercase();
                if text.contains('[') || names.iter().any(|t| text.contains(t.as_str())) {
                    return false;
                }
            }
            args.iter().all(|a| indirect_text_is_table_free(a, names))
        }
        ASTNodeType::UnaryOp { expr, .. } => indirect_text_is_table_free(expr, names),
        ASTNodeType::BinaryOp { left, right, .. } => {
            indirect_text_is_table_free(left, names) && indirect_text_is_table_free(right, names)
        }
        ASTNodeType::Call { callee, args } => {
            indirect_text_is_table_free(callee, names)
                && args.iter().all(|a| indirect_text_is_table_free(a, names))
        }
        ASTNodeType::Array(rows) => rows
            .iter()
            .flatten()
            .all(|a| indirect_text_is_table_free(a, names)),
        _ => true,
    }
}
fn is_function(
    stream: &TokenStream,
    span: &formualizer_parse::tokenizer::TokenSpan,
    name: &str,
) -> bool {
    span.token_type == TokenType::Func
        && stream.source()[span.start..span.end]
            .trim_end_matches('(')
            .rsplit('.')
            .next()
            .is_some_and(|n| n.eq_ignore_ascii_case(name))
}

/// Refuse a defined-name formula that names a table without brackets
/// (`T = Table1`) or builds INDIRECT text that could name one: the
/// source-only lowering does not rewrite defined names.
pub(super) fn validate_defined_name(text: &str, names: &[String]) -> Result<(), IoError> {
    let folded = text.to_lowercase();
    let mentions = names.iter().any(|t| folded.contains(t.as_str()));
    if !mentions && !folded.contains("indirect") {
        return Ok(());
    }
    let formula = format!("={text}");
    let refuse = || unsupported("defined-name formula refers to a table name", text);
    let stream = TokenStream::new(&formula).map_err(|_| refuse())?;
    if mentions
        && stream.spans.iter().any(|s| {
            s.token_type == TokenType::Operand && s.subtype == TokenSubType::Range && {
                let token = stream.source()[s.start..s.end].to_lowercase();
                let bare = token.rsplit('!').next().unwrap_or(&token).to_owned();
                names.contains(&bare)
            }
        })
    {
        return Err(refuse());
    }
    if stream
        .spans
        .iter()
        .any(|s| is_function(&stream, s, "INDIRECT"))
    {
        let ast = formualizer_parse::parser::parse(&formula).map_err(|_| refuse())?;
        if !indirect_text_is_table_free(&ast, names) {
            return Err(unsupported(
                "INDIRECT text in a table-bearing workbook must be literal and free of table names/structured references",
                text,
            ));
        }
    }
    Ok(())
}

pub(super) fn patches(
    sheet_name: &str,
    plan: &SheetPlan,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    options: &XlsxRecalculateOptions,
) -> Result<Vec<Patch>, IoError> {
    let mut patches = Vec::new();
    let names: Vec<String> = plans
        .iter()
        .flat_map(|p| &p.tables)
        .map(|t| t.name.to_lowercase())
        .collect();
    for cell in &plan.cells {
        checkpoint(&options.cancel)?;
        let formula = &cell.formula_text;
        if formula.is_empty() {
            continue;
        }
        let folded = formula.to_lowercase();
        if !folded.contains('[')
            && !folded.contains("indirect")
            && !names.iter().any(|t| folded.contains(t.as_str()))
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
        if stream
            .spans
            .iter()
            .any(|s| is_function(&stream, s, "INDIRECT"))
        {
            let ast = formualizer_parse::parser::parse(&token_formula).map_err(|e| {
                unsupported(
                    "cannot parse INDIRECT formula in a table-bearing workbook",
                    format!("{sheet_name} {}: {e}", cell.address),
                )
            })?;
            if !indirect_text_is_table_free(&ast, &names) {
                return Err(unsupported(
                    "INDIRECT text in a table-bearing workbook must be literal and free of table names/structured references",
                    format!("{sheet_name} {}", cell.address),
                ));
            }
        }
        let binds_names = stream
            .spans
            .iter()
            .any(|s| is_function(&stream, s, "LET") || is_function(&stream, s, "LAMBDA"));
        let mut lowered = String::new();
        let mut at = 0;
        let mut changed = false;
        for span in &stream.spans {
            if span.token_type != TokenType::Operand || span.subtype != TokenSubType::Range {
                continue;
            }
            let spelling = &stream.source()[span.start..span.end];
            if !spelling.contains('[') {
                // A bare table name means its data body (`Table1[#Data]`).
                // Exact token equality only: `Table10`, `Table1x` and string
                // literals are other tokens. Defined names cannot share a
                // table name (refused at admission).
                let folded = spelling.to_lowercase();
                let (qualifier, bare) = match folded.rsplit_once('!') {
                    Some((q, name)) => (Some(q), name),
                    None => (None, folded.as_str()),
                };
                if !names.iter().any(|t| t == bare) {
                    continue;
                }
                if qualifier.is_some() {
                    return Err(unsupported(
                        "sheet-qualified table name",
                        format!("{sheet_name} {}: {spelling}", cell.address),
                    ));
                }
                if binds_names {
                    return Err(unsupported(
                        "bare table name in a LET/LAMBDA formula",
                        format!("{sheet_name} {}: {spelling}", cell.address),
                    ));
                }
                let reference = TableReference {
                    name: spelling.to_owned(),
                    specifier: Some(TableSpecifier::Data),
                };
                let value = lower(&reference, spelling, sheet_name, cell, sheets, plans)?;
                lowered.push_str(&formula[at..span.start - offset]);
                lowered.push_str(&value);
                at = span.end - offset;
                changed = true;
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
