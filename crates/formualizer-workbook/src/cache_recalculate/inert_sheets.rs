//! Non-calculating sheet positions in the transient adapter view.
use super::{
    IoError, Patch, SheetPlan, XlsxRecalculateOptions, apply_patches, package, unsupported, xml,
};
use crate::backends::calamine::CalamineAdapter;
use formualizer_parse::parser::{ASTNode, RefView};

pub(super) const EMPTY: &str = "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData/></worksheet>";

pub(super) fn project(
    archive: &mut package::Archive<'_>,
    sheets: &[package::Sheet],
    edits: &mut package::Edits,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if !sheets.iter().any(|s| s.inert) {
        return Ok(());
    }
    let workbook = package::read_part(
        archive,
        "xl/workbook.xml",
        options.limits.max_worksheet_bytes,
    )?;
    let mut patches = Vec::new();
    let mut relationships = Vec::new();
    let mut parts = Vec::new();
    xml::walk(&workbook, options, |path, node| {
        if xml::path_is(path, xml::MAIN, &["workbook", "sheets", "sheet"])
            && let Some(name) = node.value("name")
            && let Some((i, _)) = sheets
                .iter()
                .enumerate()
                .find(|(_, s)| s.inert && s.name == name)
        {
            let id = format!("__inert_sheet_{i}");
            let part = format!("xl/worksheets/__inert_sheet_{i}.xml");
            if archive.file_names().any(|n| n == part) {
                return Err(unsupported("inert sheet projection collision", name));
            }
            patches.push(Patch {
                span: node
                    .attribute(xml::OFFICE, "id")
                    .expect("admitted r:id")
                    .span
                    .clone(),
                replacement: format!("r:id=\"{id}\"").into_bytes(),
            });
            relationships.push((id, part.clone()));
            parts.push(part);
        }
        Ok(())
    })?;
    edits.replace.insert(
        "xl/workbook.xml".into(),
        apply_patches(&workbook, patches, options.limits.max_worksheet_bytes)?,
    );
    let rel_part = "xl/_rels/workbook.xml.rels";
    let rels = package::read_part(archive, rel_part, options.limits.max_worksheet_bytes)?;
    xml::walk(&rels, options, |_, node| {
        if node
            .value("Id")
            .is_some_and(|id| relationships.iter().any(|(r, _)| r == id))
        {
            return Err(unsupported("inert sheet projection collision", rel_part));
        }
        Ok(())
    })?;
    edits.replace.insert(rel_part.into(), package::append_child(&rels, rel_part, |prefix| {
        relationships.iter().map(|(id, part)| format!("<{prefix}Relationship Id=\"{id}\" Type=\"{}/worksheet\" Target=\"/{}\"/>", xml::OFFICE, part)).collect()
    }, options)?);
    let types_part = "[Content_Types].xml";
    let types = package::read_part(archive, types_part, options.limits.max_worksheet_bytes)?;
    edits.replace.insert(types_part.into(), package::append_child(&types, types_part, |prefix| {
        parts.iter().map(|part| format!("<{prefix}Override PartName=\"/{part}\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>")).collect()
    }, options)?);
    for part in parts {
        edits.add.insert(part, EMPTY.as_bytes().to_vec());
    }
    Ok(())
}

fn check_indirect(ast: &ASTNode, sheets: &[package::Sheet]) -> Result<(), IoError> {
    use formualizer_parse::parser::ASTNodeType as N;
    match &ast.node_type {
        N::Function { name, args } => {
            if crate::backends::calamine::name_references::is_indirect(name) {
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

pub(super) fn validate(
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    adapter: &CalamineAdapter,
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    if !sheets.iter().any(|s| s.inert) {
        return Ok(());
    }
    for cell in plans.iter().flat_map(|p| &p.cells) {
        super::checkpoint(&options.cancel)?;
        if !cell.formula_text.is_empty()
            && let Ok(ast) = formualizer_parse::parser::parse(format!("={}", cell.formula_text))
        {
            check(&ast, sheets)?;
        }
    }
    for ast in adapter.calculation_name_asts()? {
        check(&ast, sheets)?;
    }
    Ok(())
}
