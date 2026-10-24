//! Non-calculating sheet positions and references to them.
//! Sheet ordinals are retained without admitting calculation cells or references.
use crate::xlsx::recalculate::{
    IoError, SheetPlan, XlsxRecalculateOptions, package, unsupported, xml,
};
use formualizer_parse::parser::{ASTNode, RefView};

pub(in crate::xlsx) const EMPTY: &str = "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData/></worksheet>";

/// Preserve the old projection collision refusal without creating any view.
pub(in crate::xlsx) fn validate_projection_names(
    archive: &mut package::Archive<'_>,
    sheets: &[package::Sheet],
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    let mut ids = Vec::new();
    for (i, sheet) in sheets.iter().enumerate().filter(|(_, sheet)| sheet.inert) {
        if archive
            .file_names()
            .any(|name| name == format!("xl/worksheets/__inert_sheet_{i}.xml"))
        {
            return Err(unsupported("inert sheet projection collision", &sheet.name));
        }
        ids.push(format!("__inert_sheet_{i}"));
    }
    if ids.is_empty() {
        return Ok(());
    }
    let part = "xl/_rels/workbook.xml.rels";
    let data = package::read_part(archive, part, options.limits.max_worksheet_bytes)?;
    xml::walk(&data, options, |_, node| {
        if node
            .value("Id")
            .is_some_and(|id| ids.iter().any(|expected| expected == id))
        {
            return Err(unsupported("inert sheet projection collision", part));
        }
        Ok(())
    })
}
pub(in crate::xlsx) fn validate_native(
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    names: Vec<ASTNode>,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if !sheets.iter().any(|sheet| sheet.inert) {
        return Ok(());
    }
    for cell in plans.iter().flat_map(|plan| &plan.cells) {
        crate::xlsx::recalculate::checkpoint(&options.cancel)?;
        if !cell.formula_text.is_empty()
            && let Ok(ast) = formualizer_parse::parser::parse(format!("={}", cell.formula_text))
        {
            check(&ast, sheets)?;
        }
    }
    for ast in names {
        check(&ast, sheets)?;
    }
    Ok(())
}
fn check_indirect(ast: &ASTNode, sheets: &[package::Sheet]) -> Result<(), IoError> {
    use formualizer_parse::parser::ASTNodeType as N;
    match &ast.node_type {
        N::Function { name, args } => {
            if crate::xlsx::ingest::name_references::is_indirect(name) {
                let Some(ASTNode {
                    node_type: N::Literal(formualizer_common::LiteralValue::Text(text)),
                    ..
                }) = args.first()
                else {
                    return Err(unsupported(
                        "nonliteral INDIRECT in a workbook with non-worksheet sheets",
                        "calculation formula",
                    ));
                };
                if let Ok(target) = formualizer_parse::parser::parse(format!("={text}")) {
                    check(&target, sheets)?;
                    let mut inert_scope = false;
                    target.visit_refs(|r| {
                        if let RefView::NamedRange { name } = r
                            && let Some((sheet, _)) = name.rsplit_once('!')
                        {
                            let sheet = sheet
                                .strip_prefix('\'')
                                .and_then(|s| s.strip_suffix('\''))
                                .unwrap_or(sheet)
                                .replace("''", "'");
                            inert_scope |= sheets
                                .iter()
                                .any(|s| s.inert && s.name.to_lowercase() == sheet.to_lowercase());
                        }
                    });
                    if inert_scope {
                        return Err(unsupported(
                            "reference to a non-worksheet sheet",
                            "INDIRECT text",
                        ));
                    }
                }
            }
            for arg in args {
                check_indirect(arg, sheets)?;
            }
        }
        N::UnaryOp { expr, .. } => check_indirect(expr, sheets)?,
        N::BinaryOp { left, right, .. } => {
            check_indirect(left, sheets)?;
            check_indirect(right, sheets)?;
        }
        N::Call { callee, args } => {
            check_indirect(callee, sheets)?;
            for arg in args {
                check_indirect(arg, sheets)?;
            }
        }
        N::Array(rows) => {
            for arg in rows.iter().flatten() {
                check_indirect(arg, sheets)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn check(ast: &ASTNode, sheets: &[package::Sheet]) -> Result<(), IoError> {
    let position = |name: &str| {
        sheets
            .iter()
            .position(|s| s.name.to_lowercase() == name.to_lowercase())
    };
    let mut bad = false;
    ast.visit_refs(|r| {
        bad |= match r {
            RefView::Cell {
                sheet: Some(name), ..
            }
            | RefView::Range {
                sheet: Some(name), ..
            } => position(name).is_some_and(|i| sheets[i].inert),
            RefView::Cell3D {
                sheet_first,
                sheet_last,
                ..
            }
            | RefView::Range3D {
                sheet_first,
                sheet_last,
                ..
            } => position(sheet_first)
                .zip(position(sheet_last))
                .is_some_and(|(a, b)| sheets[a.min(b)..=a.max(b)].iter().any(|s| s.inert)),
            _ => false,
        };
    });
    if bad {
        return Err(unsupported(
            "reference to a non-worksheet sheet",
            "calculation formula",
        ));
    }
    check_indirect(ast, sheets)
}
