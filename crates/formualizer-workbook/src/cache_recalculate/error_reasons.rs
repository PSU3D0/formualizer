//! Reasons for `#NAME?` results in the receipt.
//!
//! Computed values are stored as error codes, so the engine's message (for
//! example `Unknown function: SPDVOL`) does not survive to the result read.
//! For a `#NAME?` cell the reason is recovered from the cell's own formula
//! with the evaluator's lookup rules: the first function call (in evaluation
//! order) that is neither a registered function, a `LET`/`LAMBDA` local nor
//! a defined name gives `Unknown function: NAME`, and the first name
//! reference that resolves to nothing gives `Undefined name: NAME`, the
//! messages the evaluator itself raises. A cell that only inherits `#NAME?`
//! from a precedent has no reason of its own.
use crate::workbook::WBResolver;
use formualizer_eval::engine::Engine;
use formualizer_eval::traits::FunctionProvider;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

/// The reason this formula cell evaluates to `#NAME?`, if it has one of its
/// own.
pub(super) fn name_error_reason(
    engine: &Engine<WBResolver>,
    sheet: &str,
    row: u32,
    col: u32,
) -> Option<String> {
    let sheet_id = engine.sheet_id(sheet)?;
    let ast = match engine.get_cell(sheet, row, col) {
        Some((Some(ast), _)) => ast,
        // Compressed formula families keep no per-cell AST; parse the text.
        _ => {
            let address = formualizer_common::CellAddress::new(sheet, row, col).ok()?;
            let snapshot = engine
                .inspect_cell(
                    &address,
                    &formualizer_eval::engine::inspect::SnapshotOptions::default()
                        .with_include_values(false),
                )
                .ok()?;
            let text = snapshot.cell.formula?;
            formualizer_parse::parse(&text).ok()?
        }
    };
    let mut locals = Vec::new();
    first_reason(engine, sheet_id, &ast, &mut locals)
}

fn first_reason(
    engine: &Engine<WBResolver>,
    sheet_id: formualizer_eval::SheetId,
    node: &ASTNode,
    locals: &mut Vec<String>,
) -> Option<String> {
    let is_local =
        |locals: &[String], name: &str| locals.iter().any(|l| l.eq_ignore_ascii_case(name));
    match &node.node_type {
        ASTNodeType::Function { name, args } => {
            let upper = name.to_ascii_uppercase();
            if <Engine<WBResolver> as FunctionProvider>::get_function(engine, "", name).is_none()
                && !is_local(locals, name)
                && engine.resolve_name_entry(name, sheet_id).is_none()
            {
                return Some(format!("Unknown function: {name}"));
            }
            let depth = locals.len();
            let reason = match upper.as_str() {
                // LET(name1, value1, ..., body): each name is bound for the
                // following arguments.
                "LET" | "_XLFN.LET" => {
                    let mut found = None;
                    for (i, arg) in args.iter().enumerate() {
                        if i % 2 == 0
                            && i + 1 < args.len()
                            && let Some(name) = local_name(arg)
                        {
                            locals.push(name);
                            continue;
                        }
                        if let Some(reason) = first_reason(engine, sheet_id, arg, locals) {
                            found = Some(reason);
                            break;
                        }
                    }
                    found
                }
                // LAMBDA(param, ..., body): parameters are bound in the body.
                "LAMBDA" | "_XLFN.LAMBDA" => {
                    if let Some((body, params)) = args.split_last() {
                        locals.extend(params.iter().filter_map(local_name));
                        first_reason(engine, sheet_id, body, locals)
                    } else {
                        None
                    }
                }
                _ => args
                    .iter()
                    .find_map(|arg| first_reason(engine, sheet_id, arg, locals)),
            };
            locals.truncate(depth);
            reason
        }
        ASTNodeType::Reference {
            reference: ReferenceType::NamedRange(name),
            ..
        } => (!is_local(locals, name)
            && engine.resolve_name_entry(name, sheet_id).is_none()
            && engine.table_metadata(name).is_none())
        .then(|| format!("Undefined name: {name}")),
        ASTNodeType::UnaryOp { expr, .. } => first_reason(engine, sheet_id, expr, locals),
        ASTNodeType::BinaryOp { left, right, .. } => first_reason(engine, sheet_id, left, locals)
            .or_else(|| first_reason(engine, sheet_id, right, locals)),
        ASTNodeType::Call { callee, args } => first_reason(engine, sheet_id, callee, locals)
            .or_else(|| {
                args.iter()
                    .find_map(|arg| first_reason(engine, sheet_id, arg, locals))
            }),
        ASTNodeType::Array(rows) => rows
            .iter()
            .flatten()
            .find_map(|item| first_reason(engine, sheet_id, item, locals)),
        ASTNodeType::Literal(_) | ASTNodeType::Omitted | ASTNodeType::Reference { .. } => None,
    }
}

fn local_name(node: &ASTNode) -> Option<String> {
    match &node.node_type {
        ASTNodeType::Reference {
            reference: ReferenceType::NamedRange(name),
            ..
        } => Some(name.clone()),
        _ => None,
    }
}
