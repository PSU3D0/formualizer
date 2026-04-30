use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use formualizer_bench_core::BenchmarkSuite;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType, parse};
use serde::Serialize;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    workbook: Option<PathBuf>,
    #[arg(long, default_value = "benchmarks/scenarios.yaml")]
    scenarios: PathBuf,
    #[arg(long)]
    scenario: Option<String>,
    #[arg(long, default_value = ".")]
    root: PathBuf,
}

#[derive(Debug, Clone)]
struct RawFormula {
    sheet: String,
    cell: String,
    row: u32,
    col: u32,
    formula: String,
    shared: bool,
    shared_index: Option<String>,
    shared_ref: Option<String>,
}

#[derive(Debug, Clone)]
struct ScannedFormula {
    raw: RawFormula,
    template_id: String,
    canonical: String,
    labels: BTreeSet<String>,
    parse_ok: bool,
}

#[derive(Debug, Serialize)]
struct TemplateSummary {
    template_id: String,
    canonical: String,
    cells: u64,
    first_cell: String,
    labels: Vec<String>,
    row_runs: u64,
    column_runs: u64,
    holes: u64,
    exceptions: u64,
    raw_formula_samples: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ScanTotals {
    formula_cells: u64,
    parse_ok: u64,
    parse_errors: u64,
    volatile_formula_cells: u64,
    dynamic_formula_cells: u64,
    unsupported_formula_cells: u64,
    shared_formula_tags: u64,
    shared_formula_anchor_tags: u64,
    shared_formula_indices: u64,
    templates: u64,
    repeated_templates: u64,
    repeated_template_cells: u64,
    row_runs: u64,
    column_runs: u64,
    holes: u64,
    exceptions: u64,
}

#[derive(Debug, Serialize)]
struct ScanOutput {
    workbook: String,
    totals: ScanTotals,
    templates: Vec<TemplateSummary>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let workbook = resolve_workbook(&cli)?;
    let raw = scan_ooxml_formulas(&workbook)?;
    let scanned = classify_formulas(raw);
    let output = summarize(workbook, scanned);
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn resolve_workbook(cli: &Cli) -> Result<PathBuf> {
    if let Some(path) = &cli.workbook {
        return Ok(path.clone());
    }
    let Some(scenario_id) = &cli.scenario else {
        bail!("provide --workbook <path> or --scenario <id>");
    };
    let suite = BenchmarkSuite::from_yaml_path(&cli.scenarios)
        .with_context(|| format!("load scenarios {}", cli.scenarios.display()))?;
    let scenario = suite
        .scenario(scenario_id)
        .with_context(|| format!("unknown scenario: {scenario_id}"))?;
    let p = PathBuf::from(&scenario.source.workbook_path);
    if p.is_absolute() {
        Ok(p)
    } else {
        Ok(cli.root.join(p))
    }
}

fn scan_ooxml_formulas(path: &Path) -> Result<Vec<RawFormula>> {
    let file = File::open(path).with_context(|| format!("open workbook {}", path.display()))?;
    let mut zip = zip::ZipArchive::new(file).context("open workbook zip")?;
    let sheet_names = workbook_sheet_names(&mut zip).unwrap_or_default();
    let rel_targets = workbook_relationship_targets(&mut zip).unwrap_or_default();
    let mut path_to_sheet = BTreeMap::new();
    for (rid, name) in sheet_names {
        if let Some(target) = rel_targets.get(&rid) {
            path_to_sheet.insert(normalize_target_path(target), name);
        }
    }

    let mut out = Vec::new();
    let mut entries = Vec::new();
    for i in 0..zip.len() {
        let name = zip.by_index(i)?.name().to_string();
        if name.starts_with("xl/worksheets/") && name.ends_with(".xml") {
            entries.push(name);
        }
    }
    entries.sort();

    for name in entries {
        let mut xml = String::new();
        zip.by_name(&name)?.read_to_string(&mut xml)?;
        let sheet = path_to_sheet.get(&name).cloned().unwrap_or(name.clone());
        scan_sheet_formulas(&sheet, &xml, &mut out);
    }
    Ok(out)
}

fn workbook_sheet_names<R: std::io::Read + std::io::Seek>(
    zip: &mut zip::ZipArchive<R>,
) -> Result<BTreeMap<String, String>> {
    let mut xml = String::new();
    zip.by_name("xl/workbook.xml")?.read_to_string(&mut xml)?;
    let mut out = BTreeMap::new();
    let mut pos = 0;
    while let Some(rel) = find_tag(&xml, "sheet", pos) {
        if let (Some(rid), Some(name)) = (attr(rel.tag, "r:id"), attr(rel.tag, "name")) {
            out.insert(rid, xml_unescape(&name));
        }
        pos = rel.end;
    }
    Ok(out)
}

fn workbook_relationship_targets<R: std::io::Read + std::io::Seek>(
    zip: &mut zip::ZipArchive<R>,
) -> Result<BTreeMap<String, String>> {
    let mut xml = String::new();
    zip.by_name("xl/_rels/workbook.xml.rels")?
        .read_to_string(&mut xml)?;
    let mut out = BTreeMap::new();
    let mut pos = 0;
    while let Some(rel) = find_tag(&xml, "Relationship", pos) {
        if let (Some(id), Some(target)) = (attr(rel.tag, "Id"), attr(rel.tag, "Target")) {
            out.insert(id, target);
        }
        pos = rel.end;
    }
    Ok(out)
}

fn normalize_target_path(target: &str) -> String {
    let target = target.trim_start_matches('/');
    if target.starts_with("xl/") {
        target.to_string()
    } else {
        format!("xl/{target}")
    }
}

struct TagRef<'a> {
    tag: &'a str,
    start: usize,
    end: usize,
}

fn find_tag<'a>(xml: &'a str, tag: &str, pos: usize) -> Option<TagRef<'a>> {
    let needle = format!("<{tag}");
    let rel_start = xml[pos..].find(&needle)?;
    let start = pos + rel_start;
    let after_name = start + needle.len();
    let next = xml.as_bytes().get(after_name).copied();
    if !matches!(next, Some(b' ') | Some(b'/') | Some(b'>')) {
        return find_tag(xml, tag, after_name);
    }
    let end = start + xml[start..].find('>')? + 1;
    Some(TagRef {
        tag: &xml[start..end],
        start,
        end,
    })
}

fn attr(tag: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=\"");
    if let Some(pos) = tag.find(&needle) {
        let start = pos + needle.len();
        let rest = &tag[start..];
        return rest.find('"').map(|end| rest[..end].to_string());
    }
    let needle = format!("{key}='");
    if let Some(pos) = tag.find(&needle) {
        let start = pos + needle.len();
        let rest = &tag[start..];
        return rest.find('\'').map(|end| rest[..end].to_string());
    }
    None
}

fn scan_sheet_formulas(sheet: &str, xml: &str, out: &mut Vec<RawFormula>) {
    let mut pos = 0;
    while let Some(f_tag) = find_tag(xml, "f", pos) {
        let content_start = f_tag.end;
        let Some(close_rel) = xml[content_start..].find("</f>") else {
            pos = f_tag.end;
            continue;
        };
        let content_end = content_start + close_rel;
        let formula = xml_unescape(&xml[content_start..content_end]);
        let cell_ref = preceding_cell_ref(xml, f_tag.start).unwrap_or_default();
        let (row, col) = parse_a1_cell(&cell_ref).unwrap_or((0, 0));
        let shared = attr(f_tag.tag, "t").as_deref() == Some("shared");
        out.push(RawFormula {
            sheet: sheet.to_string(),
            cell: cell_ref,
            row,
            col,
            formula,
            shared,
            shared_index: attr(f_tag.tag, "si"),
            shared_ref: attr(f_tag.tag, "ref"),
        });
        pos = content_end + 4;
    }
}

fn preceding_cell_ref(xml: &str, pos: usize) -> Option<String> {
    let cell_start = xml[..pos].rfind("<c ")?;
    let cell_end = cell_start + xml[cell_start..].find('>')? + 1;
    attr(&xml[cell_start..cell_end], "r")
}

fn xml_unescape(input: &str) -> String {
    input
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn parse_a1_cell(cell: &str) -> Option<(u32, u32)> {
    let mut col = 0u32;
    let mut row = 0u32;
    for ch in cell.chars() {
        if ch.is_ascii_alphabetic() {
            col = col * 26 + u32::from(ch.to_ascii_uppercase() as u8 - b'A' + 1);
        } else if ch.is_ascii_digit() {
            row = row * 10 + ch.to_digit(10)?;
        }
    }
    if row == 0 || col == 0 {
        None
    } else {
        Some((row, col))
    }
}

fn classify_formulas(raw: Vec<RawFormula>) -> Vec<ScannedFormula> {
    raw.into_iter()
        .map(|raw| {
            let mut labels = BTreeSet::new();
            if raw.shared {
                labels.insert("raw_ooxml_shared_formula".to_string());
                if raw.shared_ref.is_some() {
                    labels.insert("raw_ooxml_shared_anchor".to_string());
                }
            }
            let with_eq = if raw.formula.starts_with('=') {
                raw.formula.clone()
            } else {
                format!("={}", raw.formula)
            };
            match parse(&with_eq) {
                Ok(ast) => {
                    if ast.contains_volatile() {
                        labels.insert("volatile".to_string());
                    }
                    let canonical = canonical_ast(&ast, raw.row, raw.col, &mut labels);
                    let template_id = stable_id(&canonical, &labels);
                    ScannedFormula {
                        raw,
                        template_id,
                        canonical,
                        labels,
                        parse_ok: true,
                    }
                }
                Err(err) => {
                    labels.insert("unsupported_parse_error".to_string());
                    let canonical = format!("PARSE_ERROR:{}", err.to_string().replace('\n', " "));
                    let template_id = stable_id(&canonical, &labels);
                    ScannedFormula {
                        raw,
                        template_id,
                        canonical,
                        labels,
                        parse_ok: false,
                    }
                }
            }
        })
        .collect()
}

fn canonical_ast(
    ast: &ASTNode,
    anchor_row: u32,
    anchor_col: u32,
    labels: &mut BTreeSet<String>,
) -> String {
    match &ast.node_type {
        ASTNodeType::Literal(value) => format!("LIT:{:?}", value_kind(value)),
        ASTNodeType::Reference { reference, .. } => {
            canonical_reference(reference, anchor_row, anchor_col, labels)
        }
        ASTNodeType::UnaryOp { op, expr } => {
            format!(
                "UNARY({op},{})",
                canonical_ast(expr, anchor_row, anchor_col, labels)
            )
        }
        ASTNodeType::BinaryOp { op, left, right } => format!(
            "BIN({op},{},{})",
            canonical_ast(left, anchor_row, anchor_col, labels),
            canonical_ast(right, anchor_row, anchor_col, labels)
        ),
        ASTNodeType::Function { name, args } => {
            let upper = name.to_ascii_uppercase();
            if matches!(upper.as_str(), "OFFSET" | "INDIRECT") {
                labels.insert("dynamic_reference".to_string());
            }
            if matches!(upper.as_str(), "NOW" | "TODAY" | "RAND" | "RANDBETWEEN") {
                labels.insert("volatile".to_string());
            }
            let args = args
                .iter()
                .map(|arg| canonical_ast(arg, anchor_row, anchor_col, labels))
                .collect::<Vec<_>>()
                .join(",");
            format!("FN({upper},{args})")
        }
        ASTNodeType::Call { callee, args } => {
            labels.insert("dynamic_call".to_string());
            let args = args
                .iter()
                .map(|arg| canonical_ast(arg, anchor_row, anchor_col, labels))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "CALL({},{args})",
                canonical_ast(callee, anchor_row, anchor_col, labels)
            )
        }
        ASTNodeType::Array(rows) => {
            let rows = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|arg| canonical_ast(arg, anchor_row, anchor_col, labels))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect::<Vec<_>>()
                .join(";");
            format!("ARRAY({rows})")
        }
    }
}

fn value_kind(value: &formualizer_common::LiteralValue) -> &'static str {
    match value {
        formualizer_common::LiteralValue::Number(_) => "number",
        formualizer_common::LiteralValue::Int(_) => "int",
        formualizer_common::LiteralValue::Text(_) => "text",
        formualizer_common::LiteralValue::Boolean(_) => "bool",
        formualizer_common::LiteralValue::Error(_) => "error",
        formualizer_common::LiteralValue::Empty => "empty",
        formualizer_common::LiteralValue::Array(_) => "array",
        formualizer_common::LiteralValue::Date(_) => "date",
        formualizer_common::LiteralValue::DateTime(_) => "datetime",
        formualizer_common::LiteralValue::Time(_) => "time",
        formualizer_common::LiteralValue::Duration(_) => "duration",
        formualizer_common::LiteralValue::Pending => "pending",
    }
}

fn canonical_reference(
    reference: &ReferenceType,
    anchor_row: u32,
    anchor_col: u32,
    labels: &mut BTreeSet<String>,
) -> String {
    match reference {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => format!(
            "REF({}{},{})",
            sheet_prefix(sheet.as_deref()),
            coord_part("R", *row, anchor_row, *row_abs),
            coord_part("C", *col, anchor_col, *col_abs)
        ),
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => format!(
            "RANGE({}{}:{};{}:{})",
            sheet_prefix(sheet.as_deref()),
            opt_coord_part("R", *start_row, anchor_row, *start_row_abs),
            opt_coord_part("C", *start_col, anchor_col, *start_col_abs),
            opt_coord_part("R", *end_row, anchor_row, *end_row_abs),
            opt_coord_part("C", *end_col, anchor_col, *end_col_abs)
        ),
        ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => {
            labels.insert("unsupported_3d_reference".to_string());
            format!("UNSUPPORTED_REF:{reference:?}")
        }
        ReferenceType::External(_) => {
            labels.insert("unsupported_external_reference".to_string());
            format!("UNSUPPORTED_REF:{reference:?}")
        }
        ReferenceType::Table(_) => {
            labels.insert("unsupported_structured_reference".to_string());
            format!("UNSUPPORTED_REF:{reference:?}")
        }
        ReferenceType::NamedRange(name) => {
            labels.insert("named_reference".to_string());
            format!("NAME({})", name.to_ascii_uppercase())
        }
    }
}

fn sheet_prefix(sheet: Option<&str>) -> String {
    sheet
        .map(|s| format!("SHEET({})!", s.to_ascii_uppercase()))
        .unwrap_or_default()
}

fn coord_part(prefix: &str, value: u32, anchor: u32, absolute: bool) -> String {
    if absolute {
        format!("{prefix}${value}")
    } else {
        let delta = i64::from(value) - i64::from(anchor);
        format!("{prefix}{delta:+}")
    }
}

fn opt_coord_part(prefix: &str, value: Option<u32>, anchor: u32, absolute: bool) -> String {
    value
        .map(|v| coord_part(prefix, v, anchor, absolute))
        .unwrap_or_else(|| format!("{prefix}*"))
}

fn stable_id(canonical: &str, labels: &BTreeSet<String>) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in canonical
        .bytes()
        .chain(labels.iter().flat_map(|s| s.bytes()))
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("tpl_{hash:016x}")
}

fn summarize(workbook: PathBuf, scanned: Vec<ScannedFormula>) -> ScanOutput {
    let mut by_template: BTreeMap<String, Vec<ScannedFormula>> = BTreeMap::new();
    let mut cell_to_template = BTreeMap::new();
    for formula in scanned {
        cell_to_template.insert(
            (formula.raw.sheet.clone(), formula.raw.row, formula.raw.col),
            formula.template_id.clone(),
        );
        by_template
            .entry(formula.template_id.clone())
            .or_default()
            .push(formula);
    }

    let mut templates = Vec::new();
    let mut totals = ScanTotals {
        formula_cells: 0,
        parse_ok: 0,
        parse_errors: 0,
        volatile_formula_cells: 0,
        dynamic_formula_cells: 0,
        unsupported_formula_cells: 0,
        shared_formula_tags: 0,
        shared_formula_anchor_tags: 0,
        shared_formula_indices: 0,
        templates: by_template.len() as u64,
        repeated_templates: 0,
        repeated_template_cells: 0,
        row_runs: 0,
        column_runs: 0,
        holes: 0,
        exceptions: 0,
    };
    let mut shared_indices = BTreeSet::new();

    for (template_id, mut formulas) in by_template {
        formulas.sort_by(|a, b| {
            (&a.raw.sheet, a.raw.row, a.raw.col).cmp(&(&b.raw.sheet, b.raw.row, b.raw.col))
        });
        totals.formula_cells += formulas.len() as u64;
        totals.parse_ok += formulas.iter().filter(|f| f.parse_ok).count() as u64;
        totals.parse_errors += formulas.iter().filter(|f| !f.parse_ok).count() as u64;
        totals.volatile_formula_cells += formulas
            .iter()
            .filter(|f| f.labels.contains("volatile"))
            .count() as u64;
        totals.dynamic_formula_cells += formulas
            .iter()
            .filter(|f| f.labels.iter().any(|label| label.starts_with("dynamic_")))
            .count() as u64;
        totals.unsupported_formula_cells += formulas
            .iter()
            .filter(|f| {
                f.labels
                    .iter()
                    .any(|label| label.starts_with("unsupported_"))
            })
            .count() as u64;
        totals.shared_formula_tags += formulas.iter().filter(|f| f.raw.shared).count() as u64;
        totals.shared_formula_anchor_tags += formulas
            .iter()
            .filter(|f| f.raw.shared && f.raw.shared_ref.is_some())
            .count() as u64;
        for formula in &formulas {
            if let Some(index) = &formula.raw.shared_index {
                shared_indices.insert(index.clone());
            }
        }
        if formulas.len() > 1 {
            totals.repeated_templates += 1;
            totals.repeated_template_cells += formulas.len() as u64;
        }

        let labels = formulas
            .iter()
            .flat_map(|f| f.labels.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let canonical = formulas
            .first()
            .map(|f| f.canonical.clone())
            .unwrap_or_default();
        let first_cell = formulas
            .first()
            .map(|f| format!("{}!{}", f.raw.sheet, f.raw.cell))
            .unwrap_or_default();
        let raw_formula_samples = formulas
            .iter()
            .map(|f| f.raw.formula.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();
        let (row_runs, column_runs, holes, exceptions) =
            run_stats(&template_id, &formulas, &cell_to_template);
        totals.row_runs += row_runs;
        totals.column_runs += column_runs;
        totals.holes += holes;
        totals.exceptions += exceptions;
        templates.push(TemplateSummary {
            template_id,
            canonical,
            cells: formulas.len() as u64,
            first_cell,
            labels,
            row_runs,
            column_runs,
            holes,
            exceptions,
            raw_formula_samples,
        });
    }
    totals.shared_formula_indices = shared_indices.len() as u64;

    ScanOutput {
        workbook: workbook.display().to_string(),
        totals,
        templates,
    }
}

fn run_stats(
    template_id: &str,
    formulas: &[ScannedFormula],
    cell_to_template: &BTreeMap<(String, u32, u32), String>,
) -> (u64, u64, u64, u64) {
    let mut by_row: BTreeMap<(String, u32), Vec<u32>> = BTreeMap::new();
    let mut by_col: BTreeMap<(String, u32), Vec<u32>> = BTreeMap::new();
    for formula in formulas {
        by_row
            .entry((formula.raw.sheet.clone(), formula.raw.row))
            .or_default()
            .push(formula.raw.col);
        by_col
            .entry((formula.raw.sheet.clone(), formula.raw.col))
            .or_default()
            .push(formula.raw.row);
    }
    let (row_runs, row_holes, row_exceptions) =
        count_runs(by_row, template_id, cell_to_template, true);
    let (col_runs, col_holes, col_exceptions) =
        count_runs(by_col, template_id, cell_to_template, false);
    (
        row_runs,
        col_runs,
        row_holes + col_holes,
        row_exceptions + col_exceptions,
    )
}

fn count_runs(
    groups: BTreeMap<(String, u32), Vec<u32>>,
    template_id: &str,
    cell_to_template: &BTreeMap<(String, u32, u32), String>,
    row_major: bool,
) -> (u64, u64, u64) {
    let mut runs = 0u64;
    let mut holes = 0u64;
    let mut exceptions = 0u64;
    for ((sheet, fixed), mut values) in groups {
        values.sort_unstable();
        values.dedup();
        let mut current_len = 0u64;
        let mut prev = None;
        for value in &values {
            if prev.is_none_or(|p| *value == p + 1) {
                current_len += 1;
            } else {
                if current_len > 1 {
                    runs += 1;
                }
                current_len = 1;
            }
            prev = Some(*value);
        }
        if current_len > 1 {
            runs += 1;
        }
        let Some(min) = values.first().copied() else {
            continue;
        };
        let Some(max) = values.last().copied() else {
            continue;
        };
        let present = values.into_iter().collect::<BTreeSet<_>>();
        for value in min..=max {
            let key = if row_major {
                (sheet.clone(), fixed, value)
            } else {
                (sheet.clone(), value, fixed)
            };
            if present.contains(&value) {
                continue;
            }
            match cell_to_template.get(&key) {
                Some(other) if other != template_id => exceptions += 1,
                Some(_) => {}
                None => holes += 1,
            }
        }
    }
    (runs, holes, exceptions)
}
