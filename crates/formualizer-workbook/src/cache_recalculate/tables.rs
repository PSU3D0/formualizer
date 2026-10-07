//! Strict, bounded ListObject metadata. The source table part is never edited.
use super::{IoError, XlsxRecalculateOptions, checkpoint, package, sheet, unsupported, xml};
use std::collections::HashSet;

#[derive(Debug)]
pub(super) struct Table {
    pub name: String,
    pub rect: sheet::SourceRect,
    pub header: bool,
    pub totals: bool,
    pub active_filter: Option<sheet::SourceRect>,
    pub columns: Vec<Column>,
}
#[derive(Debug)]
pub(super) struct Column {
    pub name: String,
    calculated: bool,
    total: bool,
}
#[derive(Default)]
pub(super) struct Budget {
    count: usize,
    columns: usize,
    area: u64,
}
fn number(node: &xml::Node, name: &str, default: Option<u32>) -> Result<u32, IoError> {
    node.value(name)
        .map(|v| {
            v.parse()
                .map_err(|_| unsupported(format!("invalid table {name}"), "table XML"))
        })
        .unwrap_or_else(|| {
            default.ok_or_else(|| unsupported(format!("missing table {name}"), "table XML"))
        })
}
const MC: &str = "http://schemas.openxmlformats.org/markup-compatibility/2006";
/// Excel revision namespaces whose `uid` attributes Excel 2016+ writes on
/// tables (`xr`) and table columns (`xr3`).
const XR: &str = "http://schemas.microsoft.com/office/spreadsheetml/2014/revision";
const XR3: &str = "http://schemas.microsoft.com/office/spreadsheetml/2016/revision3";
/// Extension attributes admitted on a table element. Calamine does not read
/// table parts during source recalculation (structured references are lowered
/// in the ingestion view), the parser here reads only the attributes it names,
/// and the part is never rewritten. `mc:Ignorable` only lists namespaces a
/// consumer may skip, and neither reader applies markup compatibility, so
/// every attribute in another namespace must still be listed here.
const ROOT_EXTENSIONS: &[(&str, &str)] = &[(MC, "Ignorable"), (XR, "uid")];
const COLUMN_EXTENSIONS: &[(&str, &str)] = &[(XR3, "uid")];
fn prefix_list(value: &str) -> bool {
    !value.is_empty()
        && value.split(' ').all(|p| {
            p.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                && p.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
        })
}
fn attrs(node: &xml::Node, allowed: &[&str]) -> Result<(), IoError> {
    extended_attrs(node, allowed, &[])
}
fn extended_attrs(
    node: &xml::Node,
    allowed: &[&str],
    extensions: &[(&str, &str)],
) -> Result<(), IoError> {
    if let xml::Kind::Open { attributes, .. } = &node.kind {
        for a in attributes {
            let admitted = if a.ns.is_empty() {
                allowed.contains(&a.local.as_str())
            } else {
                extensions.contains(&(a.ns.as_str(), a.local.as_str()))
                    && (a.ns != MC || prefix_list(&a.value))
            };
            if !admitted {
                return Err(unsupported(
                    format!("unsupported table attribute {}", a.qualified),
                    "table XML",
                ));
            }
        }
    }
    Ok(())
}
fn name(value: &str) -> bool {
    if value.chars().count() > 255 {
        return false;
    }
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|c| c.is_alphabetic() || matches!(c, '_' | '\\'))
        && chars.all(|c| c.is_alphanumeric() || matches!(c, '_' | '.'))
        && !matches!(value.to_lowercase().as_str(), "r" | "c")
        && sheet::SourceRect::parse(value).is_err()
}
pub(super) fn parse(
    archive: &mut package::Archive<'_>,
    part: &str,
    options: &XlsxRecalculateOptions,
    budget: &mut Budget,
) -> Result<Table, IoError> {
    budget.count = budget
        .count
        .checked_add(1)
        .ok_or_else(|| unsupported("table count overflow", part))?;
    if budget.count > options.limits.max_entries.min(options.limits.max_cells) {
        return Err(unsupported("table count limit", part));
    }
    let (parent, file) = part.rsplit_once('/').unwrap_or(("", part));
    let rel_part = format!("{parent}/_rels/{file}.rels");
    if archive.file_names().any(|n| n == rel_part)
        && !package::relationships(archive, part, options)?.is_empty()
    {
        return Err(unsupported(
            "table relationships (including query/connection semantics)",
            part,
        ));
    }
    let bytes = package::read_part(archive, part, options.limits.max_worksheet_bytes)?;
    let mut table: Option<Table> = None;
    let mut count = None;
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    let mut sections = HashSet::new();
    let mut formulas = HashSet::new();
    let mut filter_ref: Option<sheet::SourceRect> = None;
    let mut filter_active = false;
    xml::walk(&bytes, options, |path, node| {
        if !matches!(node.kind, xml::Kind::Open { .. }) {
            return Ok(());
        }
        let e = path.last().expect("open element");
        if path.len() == 1 {
            if !xml::path_is(path, xml::MAIN, &["table"]) || table.is_some() {
                return Err(unsupported("table XML root/namespace", part));
            }
            extended_attrs(
                &node,
                &[
                    "id",
                    "name",
                    "displayName",
                    "ref",
                    "headerRowCount",
                    "totalsRowCount",
                    "totalsRowShown",
                    "tableType",
                    "comment",
                    "headerRowDxfId",
                    "dataDxfId",
                    "totalsRowDxfId",
                    "headerRowBorderDxfId",
                    "tableBorderDxfId",
                    "totalsRowBorderDxfId",
                    "headerRowCellStyle",
                    "dataCellStyle",
                    "totalsRowCellStyle",
                ],
                ROOT_EXTENSIONS,
            )?;
            if number(&node, "id", None)? == 0 {
                return Err(unsupported("invalid table ID", part));
            }
            let n = node.required("name")?;
            if !name(n) || node.required("displayName")? != n {
                return Err(unsupported("invalid/ambiguous table name", part));
            }
            if !matches!(node.value("tableType"), None | Some("worksheet")) {
                return Err(unsupported("connection-backed table", part));
            }
            if !matches!(
                node.value("totalsRowShown"),
                None | Some("0" | "1" | "false" | "true")
            ) {
                return Err(unsupported("invalid totalsRowShown", part));
            }
            let rect = sheet::SourceRect::parse(node.required("ref")?)?;
            let header = number(&node, "headerRowCount", Some(1))?;
            let totals = number(&node, "totalsRowCount", Some(0))?;
            if header > 1 || totals > 1 || header + totals > rect.last_row - rect.first_row + 1 {
                return Err(unsupported("unsupported table header/totals layout", part));
            }
            let width = (rect.last_col - rect.first_col + 1) as usize;
            budget.columns = budget
                .columns
                .checked_add(width)
                .ok_or_else(|| unsupported("table column overflow", part))?;
            budget.area = budget
                .area
                .checked_add(
                    rect.cell_count()
                        .ok_or_else(|| unsupported("table area overflow", part))?,
                )
                .ok_or_else(|| unsupported("table area overflow", part))?;
            if rect.last_col > options.limits.max_columns
                || budget.columns > options.limits.max_columns as usize
                || budget.area > options.limits.max_cells as u64
            {
                return Err(unsupported("table area/column limit", part));
            }
            table = Some(Table {
                name: n.to_owned(),
                rect,
                header: header == 1,
                totals: totals == 1,
                active_filter: None,
                columns: Vec::new(),
            });
            return Ok(());
        }
        // Display/filter/sort and extension payloads have no formula authority.
        if path.get(1).is_some_and(|p| {
            p.ns == xml::MAIN
                && matches!(
                    p.local.as_str(),
                    "autoFilter" | "sortState" | "tableStyleInfo" | "extLst"
                )
        }) {
            if path.len() == 2 && !sections.insert(e.local.clone()) {
                return Err(unsupported("duplicate table metadata", part));
            }
            if xml::path_is(path, xml::MAIN, &["table", "autoFilter"]) {
                let r = sheet::SourceRect::parse(node.required("ref")?)?;
                filter_ref = Some(r);
            }
            if path.len() == 4 && path[1].local == "autoFilter" && path[2].local == "filterColumn" {
                filter_active = true;
            }
            // Unknown calculation-bearing payloads are not inert.
            if matches!(
                e.local.as_str(),
                "calculatedColumnFormula" | "totalsRowFormula" | "queryTable" | "connection"
            ) {
                return Err(unsupported("unsupported table extension semantics", part));
            }
            return Ok(());
        }
        if xml::path_is(path, xml::MAIN, &["table", "tableColumns"]) {
            attrs(&node, &["count"])?;
            if count.is_some() {
                return Err(unsupported("duplicate tableColumns", part));
            }
            count = Some(number(&node, "count", None)? as usize);
            let t = table
                .as_ref()
                .ok_or_else(|| unsupported("missing table root", part))?;
            if count != Some((t.rect.last_col - t.rect.first_col + 1) as usize) {
                return Err(unsupported("tableColumns width disagreement", part));
            }
        } else if xml::path_is(path, xml::MAIN, &["table", "tableColumns", "tableColumn"]) {
            extended_attrs(
                &node,
                &[
                    "id",
                    "name",
                    "totalsRowFunction",
                    "totalsRowLabel",
                    "headerRowDxfId",
                    "dataDxfId",
                    "totalsRowDxfId",
                    "headerRowCellStyle",
                    "dataCellStyle",
                    "totalsRowCellStyle",
                ],
                COLUMN_EXTENSIONS,
            )?;
            let t = table
                .as_mut()
                .ok_or_else(|| unsupported("missing table root", part))?;
            if t.columns.len() >= count.unwrap_or(0) {
                return Err(unsupported("table column count limit", part));
            }
            let id = number(&node, "id", None)?;
            let n = node.required("name")?;
            if id == 0 || n.is_empty() || !ids.insert(id) || !names.insert(n.to_lowercase()) {
                return Err(unsupported("duplicate/invalid table column", part));
            }
            let total = match node.value("totalsRowFunction") {
                None | Some("none" | "custom") => false,
                Some(
                    "sum" | "min" | "max" | "average" | "count" | "countNums" | "stdDev" | "var",
                ) => true,
                _ => return Err(unsupported("unsupported totalsRowFunction", part)),
            };
            if total && !t.totals {
                return Err(unsupported("totals formula without totals row", part));
            }
            t.columns.push(Column {
                name: n.to_owned(),
                calculated: false,
                total,
            });
            formulas.clear();
        } else if path.len() == 4
            && xml::path_is(
                &path[..3],
                xml::MAIN,
                &["table", "tableColumns", "tableColumn"],
            )
            && e.ns == xml::MAIN
            && matches!(
                e.local.as_str(),
                "calculatedColumnFormula" | "totalsRowFormula"
            )
        {
            attrs(&node, &["array"])?;
            if !matches!(node.value("array"), None | Some("0" | "false"))
                || !formulas.insert(e.local.clone())
            {
                return Err(unsupported("array/duplicate table formula", part));
            }
            let t = table.as_mut().expect("validated root");
            if e.local == "totalsRowFormula" && !t.totals {
                return Err(unsupported("totals formula without totals row", part));
            }
            let c = t
                .columns
                .last_mut()
                .ok_or_else(|| unsupported("misplaced table formula", part))?;
            if e.local == "calculatedColumnFormula" {
                c.calculated = true;
            } else {
                c.total = true;
            }
        } else {
            return Err(unsupported(
                format!("unsupported/misplaced table element {}", e.local),
                part,
            ));
        }
        Ok(())
    })?;
    let mut t = table.ok_or_else(|| unsupported("missing table root", part))?;
    if filter_active {
        let mut r = filter_ref.expect("validated table autoFilter");
        if r.first_row < t.rect.first_row
            || r.last_row > t.rect.last_row
            || r.first_col < t.rect.first_col
            || r.last_col > t.rect.last_col
        {
            return Err(unsupported(
                "active table autoFilter outside table bounds",
                part,
            ));
        }
        r.first_row += u32::from(t.header);
        if r.first_row <= r.last_row {
            t.active_filter = Some(r);
        }
    }
    if count != Some(t.columns.len()) {
        return Err(unsupported("table column count disagreement", part));
    }
    Ok(t)
}

pub(super) fn validate(
    table: &Table,
    cells: &[sheet::Cell],
    merges: &[sheet::SourceRect],
    bounds: (u32, u32),
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    let r = table.rect;
    if r.last_row > bounds.0 || r.last_col > bounds.1 {
        return Err(unsupported("table outside worksheet bounds", &table.name));
    }
    if merges.iter().any(|m| r.intersects(*m)) {
        return Err(unsupported("table overlaps merged cells", &table.name));
    }
    let mut covered = vec![0u32; table.columns.len()];
    let mut totals = vec![false; table.columns.len()];
    for cell in cells {
        checkpoint(&options.cancel)?;
        if cell.array_ref().is_some_and(|a| r.intersects(a)) {
            return Err(unsupported(
                "table intersects dynamic or CSE array footprint",
                &table.name,
            ));
        }
        if r.contains(cell.row, cell.col) {
            let col = (cell.col - r.first_col) as usize;
            if cell.row >= r.first_row + u32::from(table.header)
                && cell.row <= r.last_row - u32::from(table.totals)
                && matches!(cell.formula_kind, "normal" | "shared")
            {
                covered[col] += 1;
            }
            if table.totals
                && cell.row == r.last_row
                && matches!(cell.formula_kind, "normal" | "shared")
            {
                totals[col] = true;
            }
        }
    }
    let rows = r.last_row - r.first_row + 1 - u32::from(table.header) - u32::from(table.totals);
    for (i, c) in table.columns.iter().enumerate() {
        if (c.calculated && covered[i] != rows) || (c.total && !totals[i]) {
            return Err(unsupported(
                "table-managed formula is missing a worksheet <f>; write the formula into each row (and totals cell)",
                format!("table {} column {}", table.name, c.name),
            ));
        }
    }
    Ok(())
}
