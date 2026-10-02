//! Refuse reducers whose row-visibility semantics cannot be represented by
//! the source ingestion view. Only workbooks with stored hidden rows or active filters pay for
//! this formula inspection pass.
use super::{IoError, SheetPlan, XlsxRecalculateOptions, checkpoint, package, unsupported};
use crate::workbook::WBResolver;
use formualizer_eval::engine::{Engine, named_range::NamedDefinition};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
fn hidden(
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    sheet: &str,
    first: u32,
    last: u32,
) -> bool {
    sheets
        .iter()
        .zip(plans)
        .find(|(s, _)| s.name.to_lowercase() == sheet.to_lowercase())
        .is_some_and(|(_, p)| {
            let at = p.hidden_rows.partition_point(|r| *r < first);
            p.hidden_rows.get(at).is_some_and(|r| *r <= last)
                || p.active_filters
                    .iter()
                    .chain(p.tables.iter().filter_map(|t| t.active_filter.as_ref()))
                    .any(|r| r.first_row <= last && first <= r.last_row)
        })
}
struct Context<'a> {
    engine: &'a Engine<WBResolver>,
    sheets: &'a [package::Sheet],
    plans: &'a [SheetPlan],
    sheet: &'a str,
    location: &'a str,
    options: &'a XlsxRecalculateOptions,
}
fn range(reference: &ReferenceType, c: &Context<'_>, depth: usize) -> Result<(), IoError> {
    if depth > c.options.limits.max_xml_depth {
        return Err(unsupported(
            "hidden-row reference resolution depth limit",
            c.location,
        ));
    }
    let (sheet, first, last) = match reference {
        ReferenceType::Cell { sheet, row, .. } => (sheet.as_deref().unwrap_or(c.sheet), *row, *row),
        ReferenceType::Range {
            sheet,
            start_row,
            end_row,
            ..
        } => (
            sheet.as_deref().unwrap_or(c.sheet),
            start_row.unwrap_or(1),
            end_row.unwrap_or(1_048_576),
        ),
        ReferenceType::NamedRange(name) => {
            let id = c.engine.sheet_id(c.sheet).expect("admitted worksheet");
            let entry = c.engine.resolve_name_entry(name, id).ok_or_else(|| {
                unsupported(
                    "cannot prove hidden-row reducer name bounds",
                    format!("{} name {name}", c.location),
                )
            })?;
            match &entry.definition {
                NamedDefinition::Cell(cell) => {
                    let sheet = c.engine.sheet_name(cell.sheet_id);
                    if hidden(
                        c.sheets,
                        c.plans,
                        sheet,
                        cell.coord.row() + 1,
                        cell.coord.row() + 1,
                    ) {
                        return Err(unsupported(
                            "SUBTOTAL/AGGREGATE references a stored hidden row; source row visibility is not hydrated",
                            c.location,
                        ));
                    }
                    return Ok(());
                }
                NamedDefinition::Range(r) => {
                    let sheet = c.engine.sheet_name(r.start.sheet_id);
                    if hidden(
                        c.sheets,
                        c.plans,
                        sheet,
                        r.start.coord.row() + 1,
                        r.end.coord.row() + 1,
                    ) {
                        return Err(unsupported(
                            "SUBTOTAL/AGGREGATE references a stored hidden row; source row visibility is not hydrated",
                            c.location,
                        ));
                    }
                    return Ok(());
                }
                NamedDefinition::Literal(_) => return Ok(()),
                NamedDefinition::Formula { ast, .. } => return referenced(ast, c, depth + 1),
            }
        }
        _ => {
            return Err(unsupported(
                "cannot prove hidden-row reducer reference bounds",
                c.location,
            ));
        }
    };
    if hidden(c.sheets, c.plans, sheet, first, last) {
        return Err(unsupported(
            "SUBTOTAL/AGGREGATE references a stored hidden row; source row visibility is not hydrated",
            format!("{} range {sheet}!{first}:{last}", c.location),
        ));
    }
    Ok(())
}
fn referenced(ast: &ASTNode, c: &Context<'_>, depth: usize) -> Result<(), IoError> {
    checkpoint(&c.options.cancel)?;
    match &ast.node_type {
        ASTNodeType::Reference { reference, .. } => range(reference, c, depth + 1),
        ASTNodeType::UnaryOp { expr, .. } => referenced(expr, c, depth + 1),
        ASTNodeType::BinaryOp { left, right, .. } => {
            referenced(left, c, depth + 1)?;
            referenced(right, c, depth + 1)
        }
        ASTNodeType::Function { name, args } => {
            let n = name.rsplit('.').next().unwrap_or(name);
            if n.eq_ignore_ascii_case("OFFSET") || n.eq_ignore_ascii_case("INDIRECT") {
                return Err(unsupported(
                    "cannot prove dynamic reducer range against stored hidden rows",
                    c.location,
                ));
            }
            for a in args {
                referenced(a, c, depth + 1)?;
            }
            Ok(())
        }
        ASTNodeType::Call { .. } => Err(unsupported(
            "cannot prove callable reducer range against stored hidden rows",
            c.location,
        )),
        ASTNodeType::Array(rows) => {
            for a in rows.iter().flatten() {
                referenced(a, c, depth + 1)?;
            }
            Ok(())
        }
        ASTNodeType::Literal(_) | ASTNodeType::Omitted => Ok(()),
    }
}
fn inspect(ast: &ASTNode, c: &Context<'_>) -> Result<(), IoError> {
    checkpoint(&c.options.cancel)?;
    match &ast.node_type {
        ASTNodeType::Function { name, args } => {
            let n = name.rsplit('.').next().unwrap_or(name);
            let skip = if n.eq_ignore_ascii_case("SUBTOTAL") {
                Some(1)
            } else if n.eq_ignore_ascii_case("AGGREGATE") {
                Some(2)
            } else {
                None
            };
            if let Some(skip) = skip {
                for a in args.iter().skip(skip) {
                    referenced(a, c, 0)?;
                }
            }
            for a in args {
                inspect(a, c)?;
            }
            Ok(())
        }
        ASTNodeType::UnaryOp { expr, .. } => inspect(expr, c),
        ASTNodeType::BinaryOp { left, right, .. } => {
            inspect(left, c)?;
            inspect(right, c)
        }
        ASTNodeType::Call { callee, args } => {
            inspect(callee, c)?;
            for a in args {
                inspect(a, c)?;
            }
            Ok(())
        }
        ASTNodeType::Array(rows) => {
            for a in rows.iter().flatten() {
                inspect(a, c)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
pub(super) fn validate(
    engine: &Engine<WBResolver>,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if plans.iter().all(|p| {
        p.hidden_rows.is_empty()
            && p.active_filters.is_empty()
            && p.tables.iter().all(|t| t.active_filter.is_none())
    }) {
        return Ok(());
    }
    for (name, named) in engine.named_ranges_iter() {
        if let NamedDefinition::Formula { ast, .. } = &named.definition {
            let location = format!("defined name {name}");
            inspect(
                ast,
                &Context {
                    engine,
                    sheets,
                    plans,
                    sheet: engine.default_sheet_name(),
                    location: &location,
                    options,
                },
            )?;
        }
    }
    for ((id, name), named) in engine.sheet_named_ranges_iter() {
        if let NamedDefinition::Formula { ast, .. } = &named.definition {
            let location = format!("defined name {}:{name}", engine.sheet_name(*id));
            inspect(
                ast,
                &Context {
                    engine,
                    sheets,
                    plans,
                    sheet: engine.sheet_name(*id),
                    location: &location,
                    options,
                },
            )?;
        }
    }
    for (sheet, plan) in sheets.iter().zip(plans) {
        for cell in &plan.cells {
            checkpoint(&options.cancel)?;
            let address = formualizer_common::CellAddress::new(&sheet.name, cell.row, cell.col)
                .map_err(|e| IoError::from_backend("xlsx-coordinate", e))?;
            let formula = engine
                .inspect_cell(
                    &address,
                    &formualizer_eval::engine::inspect::SnapshotOptions::default(),
                )
                .map_err(|e| IoError::from_backend("xlsx-inspect", e))?
                .cell
                .formula;
            if let Some(formula) = formula {
                let upper = formula.to_ascii_uppercase();
                if !upper.contains("SUBTOTAL") && !upper.contains("AGGREGATE") {
                    continue;
                }
                let ast = formualizer_parse::parser::parse(&formula).map_err(|e| {
                    unsupported(
                        "cannot inspect hidden-row reducer formula",
                        format!("{}!{}: {e}", sheet.name, cell.address),
                    )
                })?;
                let location = format!("{}!{}", sheet.name, cell.address);
                inspect(
                    &ast,
                    &Context {
                        engine,
                        sheets,
                        plans,
                        sheet: &sheet.name,
                        location: &location,
                        options,
                    },
                )?;
            }
        }
    }
    Ok(())
}
