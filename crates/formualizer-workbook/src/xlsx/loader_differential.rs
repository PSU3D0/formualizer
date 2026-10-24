//! External-boundary loader oracle. The two factories must create independent
//! readers and engines from the same bytes; never compare parser internals.
use crate::workbook::WBResolver;
use crate::{
    CalamineAdapter, IoError, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook,
    WorkbookConfig,
};
use formualizer_eval::engine::ingest::EngineLoadStream;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    Absent,
    Empty,
    Number(u64),
    Int(i64),
    Text(String),
    Boolean(bool),
    Error(String),
    Array(Vec<Vec<Value>>),
    Other(String),
}

fn value(v: Option<LiteralValue>) -> Value {
    match v {
        None => Value::Absent,
        Some(LiteralValue::Empty) => Value::Empty,
        Some(LiteralValue::Number(n)) => Value::Number(n.to_bits()),
        Some(LiteralValue::Int(n)) => Value::Int(n),
        Some(LiteralValue::Text(s)) => Value::Text(s),
        Some(LiteralValue::Boolean(b)) => Value::Boolean(b),
        Some(LiteralValue::Error(e)) => Value::Error(format!("{:?}", e.kind)),
        Some(LiteralValue::Array(rows)) => Value::Array(
            rows.into_iter()
                .map(|row| row.into_iter().map(|v| value(Some(v))).collect())
                .collect(),
        ),
        Some(v) => Value::Other(format!("{v:?}")),
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Coverage {
    files: usize,
    sheets: usize,
    cells: usize,
    formulas: usize,
    evaluated_formulas: usize,
    names: usize,
    diagnostics: usize,
}

/// Keys include the observation phase, sheet ordinal and coordinate, so the
/// first difference always carries its location. BTreeMap also removes hash
/// iteration order from external-cache snapshots.
#[derive(Debug, Default, PartialEq, Eq)]
struct Dump {
    records: BTreeMap<String, String>,
    coverage: Coverage,
    raw_reader_errors: BTreeMap<String, String>,
}
impl Dump {
    fn record(&mut self, key: impl Into<String>, v: impl Debug) {
        assert!(self.records.insert(key.into(), format!("{v:?}")).is_none());
    }
    fn reader_error(
        &mut self,
        key: impl Into<String>,
        error: impl std::fmt::Display + Into<IoError>,
    ) {
        let key = key.into();
        self.raw_reader_errors
            .insert(key.clone(), error.to_string());
        let error = error.into();
        self.record(key, (format!("{error:?}"), error.to_string()));
    }
    fn refusal(&mut self, phase: &str, error: IoError) {
        self.record(
            format!("{phase}/refusal"),
            (format!("{error:?}"), error.to_string()),
        );
    }
}

fn capture<B>(bytes: &[u8], open: impl Fn(Vec<u8>) -> Result<B, IoError>) -> Dump
where
    B: SpreadsheetReader + EngineLoadStream<WBResolver>,
    IoError:
        From<<B as EngineLoadStream<WBResolver>>::Error> + From<<B as SpreadsheetReader>::Error>,
{
    capture_with_source_inventory(bytes, open, true)
}

fn capture_with_source_inventory<B>(
    bytes: &[u8],
    open: impl Fn(Vec<u8>) -> Result<B, IoError>,
    baseline_available: bool,
) -> Dump
where
    B: SpreadsheetReader + EngineLoadStream<WBResolver>,
    IoError:
        From<<B as EngineLoadStream<WBResolver>>::Error> + From<<B as SpreadsheetReader>::Error>,
{
    capture_snapshot(bytes, open, baseline_available).finish(true)
}

type FormulaTarget = (usize, String, u32, u32);
struct PendingEvaluation {
    workbook: Workbook,
    formulas: Vec<FormulaTarget>,
}
struct Snapshot {
    dump: Dump,
    evaluation: Option<PendingEvaluation>,
}
impl Snapshot {
    fn refused(dump: Dump) -> Self {
        Self {
            dump,
            evaluation: None,
        }
    }
    fn accepted(&self) -> bool {
        self.evaluation.is_some()
    }
    fn finish(self, compare_evaluation: bool) -> Dump {
        let Self {
            mut dump,
            evaluation,
        } = self;
        let Some(evaluation) = evaluation else {
            return dump;
        };
        if !compare_evaluation {
            dump.record("evaluation/status", "not compared: other loader refused");
            return dump;
        }
        if std::env::var_os("FZ_LOADER_VALUES_ONLY").is_some() {
            return dump;
        }
        evaluate_snapshot(dump, evaluation.workbook, evaluation.formulas)
    }
}

fn capture_snapshot<B>(
    bytes: &[u8],
    open: impl Fn(Vec<u8>) -> Result<B, IoError>,
    baseline_available: bool,
) -> Snapshot
where
    B: SpreadsheetReader + EngineLoadStream<WBResolver>,
    IoError:
        From<<B as EngineLoadStream<WBResolver>>::Error> + From<<B as SpreadsheetReader>::Error>,
{
    let mut dump = Dump::default();
    dump.coverage.files = 1;
    // Metadata/random reads use an independent reader. They must not populate
    // the engine or alter the stats of the actual fast-stream load.
    let mut reader = match open(bytes.to_vec()) {
        Ok(reader) => reader,
        Err(error) => {
            dump.refusal("open", error);
            return Snapshot::refused(dump);
        }
    };
    dump.record("reader/capabilities", reader.capabilities());
    dump.record("reader/granularity", reader.access_granularity());
    dump.record("reader/calc", reader.calc_settings());
    match reader.defined_names() {
        Ok(names) => {
            dump.coverage.names = names.len();
            for (i, name) in names.into_iter().enumerate() {
                let definition = match name.definition {
                    crate::traits::DefinedNameDefinition::Literal { value: v } => {
                        format!("{:?}", value(Some(v)))
                    }
                    crate::traits::DefinedNameDefinition::Range { address } => {
                        format!("{address:?}")
                    }
                };
                dump.record(
                    format!("reader/name/{i:08}"),
                    (name.name, name.scope, name.scope_sheet, definition),
                );
            }
        }
        Err(e) => dump.reader_error("reader/names-error", e),
    }
    if let Some(links) = reader.cached_external_link_values() {
        dump.record("external/indices", &links.indices);
        let refused: BTreeMap<_, _> = links.refused_formulas.iter().collect();
        dump.record("external/refused-formulas", refused);
        dump.record(
            "external/refused-names",
            links.refused_names.iter().collect::<BTreeSet<_>>(),
        );
        dump.record("external/skipped-names", &links.skipped_names);
        for name in links.values.scalar_names().collect::<BTreeSet<_>>() {
            dump.record(
                format!("external/scalar/{name}"),
                value(links.values.scalar(name).cloned()),
            );
        }
        for name in links.values.range_names().collect::<BTreeSet<_>>() {
            use formualizer_eval::traits::Range;
            let range = links.values.range(name).unwrap();
            dump.record(
                format!("external/range/{name}/dimensions"),
                range.dimensions(),
            );
            let (rows, cols) = range.dimensions();
            for row in 0..rows {
                for col in 0..cols {
                    dump.record(
                        format!("external/range/{name}/{row:08}/{col:08}"),
                        value(Some(range.get(row, col).unwrap())),
                    );
                }
            }
        }
    }
    let names = match reader.sheet_names() {
        Ok(names) => names,
        Err(e) => {
            dump.reader_error("reader/sheets-error", e);
            return Snapshot::refused(dump);
        }
    };
    dump.record("reader/sheets", &names);
    // The generic reader can omit unresolved/shared source cells even when the
    // stream stores a formula there. Use the independent source-record API as
    // a common coordinate inventory, then union reader and Arrow coordinates.
    // A source refusal remains observable through the normal baseline load.
    let mut coordinates: BTreeMap<String, BTreeSet<(u32, u32)>> = BTreeMap::new();
    if baseline_available {
        use calamine::Reader;
        if let Ok(mut source) = calamine::Xlsx::new(std::io::Cursor::new(bytes)) {
            for name in &names {
                if let Ok(mut cells) = source.worksheet_cells_reader(name) {
                    while let Ok(Some(cell)) = cells.next_cell_with_formula_metadata() {
                        if cell.formula.is_some() {
                            coordinates
                                .entry(name.clone())
                                .or_default()
                                .insert((cell.pos.0 + 1, cell.pos.1 + 1));
                        }
                    }
                }
            }
        }
    }
    for (i, name) in names.iter().enumerate() {
        let prefix = format!("reader/sheet/{i:08}");
        dump.record(format!("{prefix}/bounds"), reader.sheet_bounds(name));
        dump.record(
            format!("{prefix}/loaded-before"),
            reader.is_loaded(name, None, None),
        );
        match reader.read_sheet(name) {
            Err(e) => dump.reader_error(format!("{prefix}/error"), e),
            Ok(sheet) => {
                dump.record(
                    format!("{prefix}/metadata"),
                    (
                        sheet.dimensions,
                        sheet.tables,
                        sheet.named_ranges,
                        sheet.date_system_1904,
                        sheet.merged_cells,
                        sheet.hidden,
                        sheet.row_hidden_manual,
                        sheet.row_hidden_filter,
                    ),
                );
                let mut coords = BTreeSet::new();
                for ((row, col), cell) in sheet.cells {
                    coords.insert((row, col));
                    dump.record(
                        format!("{prefix}/cell/{row:08}/{col:08}"),
                        (value(cell.value), cell.formula, cell.style),
                    );
                }
                let probes: BTreeSet<_> = [(0, 0)]
                    .into_iter()
                    .chain(coords.first().copied())
                    .chain(coords.last().copied())
                    .collect();
                for (row, col) in probes {
                    let key = format!("{prefix}/read-cell/{row:08}/{col:08}");
                    match reader.read_cell(name, row, col) {
                        Ok(cell) => dump.record(
                            key,
                            cell.map(|cell| (value(cell.value), cell.formula, cell.style)),
                        ),
                        Err(error) => dump.reader_error(key, error),
                    }
                    let key = format!("{prefix}/read-range/{row:08}/{col:08}");
                    match reader.read_range(
                        name,
                        (row, col),
                        (row.saturating_add(1), col.saturating_add(1)),
                    ) {
                        Ok(cells) => dump.record(
                            key,
                            cells
                                .into_iter()
                                .map(|(coord, cell)| {
                                    (coord, (value(cell.value), cell.formula, cell.style))
                                })
                                .collect::<BTreeMap<_, _>>(),
                        ),
                        Err(error) => dump.reader_error(key, error),
                    }
                }
                coordinates.entry(name.clone()).or_default().extend(coords);
            }
        }
        dump.record(
            format!("{prefix}/loaded-after"),
            reader.is_loaded(name, None, None),
        );
    }
    let backend = match open(bytes.to_vec()) {
        Ok(backend) => backend,
        Err(error) => {
            dump.refusal("stream-open", error);
            return Snapshot::refused(dump);
        }
    };
    let mut config = WorkbookConfig::ephemeral();
    config.eval.deterministic_mode = formualizer_eval::engine::DeterministicMode::Enabled {
        timestamp_utc: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        timezone: formualizer_eval::timezone::TimeZoneSpec::Utc,
    };
    let (wb, stats) =
        match Workbook::from_reader_with_adapter_stats(backend, LoadStrategy::EagerAll, config) {
            Ok(loaded) => loaded,
            Err(error) => {
                dump.refusal("load", error);
                return Snapshot::refused(dump);
            }
        };
    dump.record("loaded/stats", stats);
    dump.record("loaded/sheets", wb.sheet_names());
    dump.record("loaded/date-system", wb.engine().config.date_system);
    dump.record("loaded/name-diagnostics", wb.name_import_diagnostics());
    dump.record("loaded/sheet-diagnostics", wb.sheet_import_diagnostics());
    dump.record(
        "loaded/parse-diagnostics",
        wb.engine().formula_parse_diagnostics(),
    );
    dump.coverage.diagnostics = wb.name_import_diagnostics().len()
        + wb.sheet_import_diagnostics().len()
        + wb.engine().formula_parse_diagnostics().len();
    dump.record("loaded/tables", wb.engine().tables());
    for (i, name) in wb.engine().named_ranges_snapshot().into_iter().enumerate() {
        use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
        let scope = match name.scope {
            NameScope::Workbook => None,
            NameScope::Sheet(id) => Some(wb.engine().sheet_name(id).to_owned()),
        };
        let definition = match name.definition {
            NamedDefinition::Literal(v) => format!("{:?}", value(Some(v))),
            NamedDefinition::Formula { ast, .. } => format!("Formula({ast:?})"),
            other => format!("{other:?}"),
        };
        dump.record(
            format!("loaded/name/{i:08}"),
            (name.name, scope, definition),
        );
    }
    // Include engine-only populated Arrow coordinates too. Walking materialized
    // chunks (not the sheet's bounding rectangle) stays bounded for XFD1048576.
    for sheet in &wb.engine().sheet_store().sheets {
        let coords = coordinates.entry(sheet.name.to_string()).or_default();
        for column in &sheet.columns {
            for (index, chunk) in column
                .chunks
                .iter()
                .enumerate()
                .chain(column.sparse_chunks.iter().map(|(i, c)| (*i, c)))
            {
                let start = sheet
                    .chunk_starts
                    .get(index)
                    .copied()
                    .unwrap_or(index * sheet.chunk_rows);
                for (offset, tag) in chunk.type_tag.values().iter().enumerate() {
                    if *tag != formualizer_eval::arrow_store::TypeTag::Empty as u8 {
                        coords.insert(((start + offset + 1) as u32, column.index + 1));
                    }
                }
                for (offset, _) in chunk.overlay.iter().chain(chunk.computed_overlay.iter()) {
                    coords.insert(((start + offset + 1) as u32, column.index + 1));
                }
            }
        }
    }
    let mut formulas = Vec::new();
    for (i, name) in wb.sheet_names().into_iter().enumerate() {
        dump.coverage.sheets += 1;
        dump.record(
            format!("loaded/sheet/{i:08}/dimensions"),
            wb.sheet_dimensions(&name),
        );
        for &(row, col) in coordinates.get(&name).into_iter().flatten() {
            dump.coverage.cells += 1;
            let formula = wb.get_formula(&name, row, col);
            if formula.is_some() {
                formulas.push((i, name.clone(), row, col));
            }
            let sheet = wb.engine().sheet_store().sheet(&name);
            let format = sheet.and_then(|s| s.format_id((row - 1) as usize, (col - 1) as usize));
            use formualizer_eval::traits::EvaluationContext;
            let class = format.and_then(|id| wb.engine().format_class(id));
            let raw = sheet.map(|s| s.get_cell_value((row - 1) as usize, (col - 1) as usize));
            dump.record(
                format!("loaded/sheet/{i:08}/cell/{row:08}/{col:08}"),
                (
                    value(wb.get_value(&name, row, col)),
                    value(raw),
                    formula,
                    format,
                    class,
                ),
            );
        }
    }
    dump.coverage.formulas = formulas.len();
    Snapshot {
        dump,
        evaluation: Some(PendingEvaluation {
            workbook: wb,
            formulas,
        }),
    }
}

fn evaluate_snapshot(mut dump: Dump, mut wb: Workbook, formulas: Vec<FormulaTarget>) -> Dump {
    dump.coverage.evaluated_formulas = formulas.len();
    // Evaluate only after snapshotting every cell. Evaluation can materialize
    // spills and build deferred graphs; neither belongs in the load snapshot.
    // A single public batch evaluates every requested formula, without
    // rebuilding demand/preparation state separately for thousands of targets.
    // If the batch refuses, still visit each target to expose all cell results.
    let targets: Vec<_> = formulas
        .iter()
        .map(|(_, name, row, col)| (name.as_str(), *row, *col))
        .collect();
    match wb.evaluate_cells(&targets) {
        Ok(values) => {
            assert_eq!(values.len(), formulas.len());
            for ((i, _, row, col), result) in formulas.into_iter().zip(values) {
                dump.record(
                    format!("evaluated/sheet/{i:08}/{row:08}/{col:08}"),
                    Ok::<_, String>(value(Some(result))),
                );
            }
        }
        Err(error) => {
            dump.refusal("evaluate-batch", error);
            for (i, name, row, col) in formulas {
                let result = wb
                    .evaluate_cell(&name, row, col)
                    .map(|v| value(Some(v)))
                    .map_err(|e| format!("{e:?}: {e}"));
                dump.record(format!("evaluated/sheet/{i:08}/{row:08}/{col:08}"), result);
            }
        }
    }
    dump
}

fn first_difference(a: &Dump, b: &Dump) -> Option<String> {
    let keys: BTreeSet<_> = a.records.keys().chain(b.records.keys()).collect();
    // Acceptance asymmetry must show the actual refusal, not an arbitrary
    // missing cell that happens to sort before its phase-tagged error.
    let ordered = keys
        .iter()
        .copied()
        .filter(|key| key.ends_with("/refusal"))
        .chain(
            keys.iter()
                .copied()
                .filter(|key| !key.ends_with("/refusal")),
        );
    for key in ordered {
        if a.records.get(key) != b.records.get(key) {
            let context: Vec<_> = a.records.range(..=key.clone()).rev().take(3).collect();
            let formula_context = key
                .strip_prefix("evaluated/sheet/")
                .and_then(|suffix| suffix.split_once('/'))
                .map(|(sheet, coordinate)| {
                    let source_key = format!("loaded/sheet/{sheet}/cell/{coordinate}");
                    format!(
                        "\nFormula context: baseline={:?} native={:?}\nSheets: {:?}",
                        a.records.get(&source_key),
                        b.records.get(&source_key),
                        a.records.get("loaded/sheets")
                    )
                })
                .unwrap_or_default();
            return Some(format!(
                "{key}\nCalamine: {:?}\nCandidate: {:?}\nPrevious context: {context:?}{formula_context}",
                a.records.get(key),
                b.records.get(key)
            ));
        }
    }
    None
}

fn calamine(bytes: Vec<u8>) -> Result<CalamineAdapter, IoError> {
    CalamineAdapter::open_bytes(bytes).map_err(IoError::from)
}

/// Run every file even after a mismatch. Refusal asymmetry is data, not a
/// reason to lose coverage of the rest of the fixture set. Factories are
/// explicit so a self-check can never masquerade as a native comparison.
fn compare_files<A, B>(
    paths: &[std::path::PathBuf],
    baseline: impl Fn(Vec<u8>) -> Result<A, IoError> + Copy,
    candidate: impl Fn(Vec<u8>) -> Result<B, IoError> + Copy,
    output: impl std::io::Write,
) -> std::io::Result<(Coverage, usize)>
where
    A: SpreadsheetReader + EngineLoadStream<WBResolver>,
    B: SpreadsheetReader + EngineLoadStream<WBResolver>,
    IoError: From<<A as EngineLoadStream<WBResolver>>::Error>
        + From<<B as EngineLoadStream<WBResolver>>::Error>
        + From<<A as SpreadsheetReader>::Error>
        + From<<B as SpreadsheetReader>::Error>,
{
    compare_projected(
        paths,
        baseline,
        candidate,
        output,
        |dump| dump,
        &BTreeMap::new(),
    )
}

// Keep operator fixture locations out of the library. Each manifest line is an
// exact absolute path, a tab, and a nonempty one-line explanation; no wildcards.
fn baseline_unavailable_manifest() -> BTreeMap<std::path::PathBuf, String> {
    let Some(path) = std::env::var_os("FZ_LOADER_BASELINE_ALLOWLIST") else {
        return BTreeMap::new();
    };
    parse_baseline_unavailable_manifest(
        &std::fs::read_to_string(path).expect("allow-list manifest"),
    )
}
fn parse_baseline_unavailable_manifest(text: &str) -> BTreeMap<std::path::PathBuf, String> {
    let mut exclusions = BTreeMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let (path, reason) = line
            .split_once('\t')
            .expect("allow-list requires path<TAB>reason");
        let path = std::path::PathBuf::from(path);
        assert!(
            path.is_absolute() && !reason.trim().is_empty(),
            "allow-list requires absolute path and reason"
        );
        assert!(
            exclusions.insert(path, reason.to_owned()).is_none(),
            "duplicate allow-list path"
        );
    }
    exclusions
}

fn compare_projected<A, B>(
    paths: &[std::path::PathBuf],
    baseline: impl Fn(Vec<u8>) -> Result<A, IoError> + Copy,
    candidate: impl Fn(Vec<u8>) -> Result<B, IoError> + Copy,
    mut output: impl std::io::Write,
    project: impl Fn(Dump) -> Dump,
    exclusions: &BTreeMap<std::path::PathBuf, String>,
) -> std::io::Result<(Coverage, usize)>
where
    A: SpreadsheetReader + EngineLoadStream<WBResolver>,
    B: SpreadsheetReader + EngineLoadStream<WBResolver>,
    IoError: From<<A as EngineLoadStream<WBResolver>>::Error>
        + From<<B as EngineLoadStream<WBResolver>>::Error>
        + From<<A as SpreadsheetReader>::Error>
        + From<<B as SpreadsheetReader>::Error>,
{
    let mut coverage = Coverage::default();
    let mut divergences = 0;
    let mut skipped = 0;
    let mut acceptance_asymmetric = 0;
    for path in paths {
        if let Some(reason) = exclusions.get(path) {
            skipped += 1;
            writeln!(
                output,
                "SKIP BASELINE_UNAVAILABLE {}: {reason}",
                path.display()
            )?;
            output.flush()?;
            continue;
        }
        writeln!(output, "BEGIN {}", path.display())?;
        let bytes = std::fs::read(path)?;
        writeln!(output, "ARM baseline")?;
        output.flush()?;
        let a = capture_snapshot(&bytes, baseline, true);
        writeln!(output, "ARM native")?;
        output.flush()?;
        let b = capture_snapshot(&bytes, candidate, true);
        // An unstable evaluator result must not conceal a stable loaded-state,
        // metadata or generic-reader difference that sorts after evaluation.
        if a.dump.records == b.dump.records {
            writeln!(output, "SNAPSHOT MATCH")?;
        } else if let Some(difference) = first_difference(&a.dump, &b.dump) {
            writeln!(output, "SNAPSHOT DIFFERENCE\n{difference}")?;
        }
        let both_accepted = a.accepted() && b.accepted();
        if a.accepted() != b.accepted() {
            acceptance_asymmetric += 1;
            writeln!(output, "EVALUATION NOT COMPARED: acceptance asymmetry")?;
        }
        if both_accepted {
            writeln!(output, "EVALUATE baseline")?;
            output.flush()?;
        }
        let a = project(a.finish(both_accepted));
        if both_accepted {
            writeln!(output, "EVALUATE native")?;
            output.flush()?;
        }
        let b = project(b.finish(both_accepted));
        if !a.raw_reader_errors.is_empty() || !b.raw_reader_errors.is_empty() {
            writeln!(
                output,
                "RAW READER ERRORS baseline={:?} native={:?}",
                a.raw_reader_errors, b.raw_reader_errors
            )?;
        }
        coverage.files += 1;
        coverage.sheets += a.coverage.sheets;
        coverage.cells += a.coverage.cells;
        coverage.formulas += a.coverage.formulas;
        coverage.evaluated_formulas += a.coverage.evaluated_formulas;
        coverage.names += a.coverage.names;
        coverage.diagnostics += a.coverage.diagnostics;
        if let Some(difference) = first_difference(&a, &b) {
            divergences += 1;
            writeln!(output, "DIVERGENCE {}\n{difference}", path.display())?;
        } else {
            writeln!(output, "MATCH {} {:?}", path.display(), a.coverage)?;
        }
        output.flush()?;
    }
    writeln!(
        output,
        "TOTAL {coverage:?} divergent_files={divergences} skipped_files={skipped} acceptance_asymmetric_files={acceptance_asymmetric}"
    )?;
    Ok((coverage, divergences))
}

#[test]
#[ignore = "set FZ_LOADER_FILES and FZ_LOADER_REPORT; self-check, not native parity"]
fn listed_calamine_oracle_is_repeatable() {
    let list = std::env::var_os("FZ_LOADER_FILES").expect("fixture list");
    let report = std::env::var_os("FZ_LOADER_REPORT").expect("report path");
    let paths: Vec<_> = std::fs::read_to_string(list)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(std::path::PathBuf::from)
        .collect();
    assert!(!paths.is_empty());
    let (_, divergences) = compare_files(
        &paths,
        calamine,
        calamine,
        std::fs::File::create(report).unwrap(),
    )
    .unwrap();
    assert_eq!(divergences, 0, "Calamine oracle is not deterministic");
}

#[test]
fn baseline_unavailable_allowlist_is_exact_and_explicit() {
    let exclusions = parse_baseline_unavailable_manifest(
        "/fixtures/unavailable.xlsx\tbaseline reader returns no result\n",
    );
    let path = std::path::PathBuf::from("/fixtures/unavailable.xlsx");
    let mut report = Vec::new();
    let (coverage, differences) = compare_projected(
        &[path],
        calamine,
        native,
        &mut report,
        |dump| dump,
        &exclusions,
    )
    .unwrap();
    assert_eq!(coverage.files, 0);
    assert_eq!(differences, 0);
    let report = String::from_utf8(report).unwrap();
    assert!(report.contains("SKIP BASELINE_UNAVAILABLE"));
    assert!(report.contains("baseline reader returns no result"));
    assert!(!exclusions.contains_key(std::path::Path::new("/another/unavailable.xlsx")));
}

#[test]
#[ignore = "set FZ_LOADER_REPORT and FZ_LOADER_BASELINE_ALLOWLIST for native-only observations"]
fn listed_native_baseline_unavailable_observations() {
    use std::io::Write;
    let mut report =
        std::fs::File::create(std::env::var_os("FZ_LOADER_REPORT").expect("report path")).unwrap();
    let exclusions = baseline_unavailable_manifest();
    assert!(!exclusions.is_empty(), "explicit allow-list required");
    for (path, reason) in exclusions {
        writeln!(report, "NATIVE_ONLY {}: {reason}", path.display()).unwrap();
        report.flush().unwrap();
        let dump = capture_with_source_inventory(&std::fs::read(path).unwrap(), native, false);
        writeln!(report, "{dump:#?}").unwrap();
        report.flush().unwrap();
    }
}

fn native(bytes: Vec<u8>) -> Result<super::loader::NativeXlsxAdapter, IoError> {
    super::loader::NativeXlsxAdapter::open_bytes(bytes)
}

fn values_and_formulas(mut dump: Dump) -> Dump {
    dump.records.retain(|key, _| {
        (key.ends_with("/refusal") && !key.starts_with("evaluate"))
            || key == "loaded/sheets"
            || (key.starts_with("loaded/sheet/") && key.contains("/cell/"))
    });
    dump
}

#[test]
fn native_values_and_formulas_match_committed_fixture() {
    let bytes = include_bytes!("../../tests/fixtures/shared_formula_above_master.xlsx");
    let a = values_and_formulas(capture(bytes, calamine));
    let b = values_and_formulas(capture(bytes, native));
    assert_eq!(first_difference(&a, &b), None);
}

#[test]
fn native_formula_staging_matches_eager_deferred_and_authoritative_modes() {
    use formualizer_eval::engine::FormulaPlaneMode;
    let bytes = include_bytes!("../../tests/fixtures/shared_formula_above_master.xlsx");
    let mut source = calamine(bytes.to_vec()).unwrap();
    let name = source.sheet_names().unwrap().remove(0);
    let formulas: Vec<_> = source
        .read_sheet(&name)
        .unwrap()
        .cells
        .into_iter()
        .filter(|(_, cell)| cell.formula.is_some())
        .map(|(coord, _)| coord)
        .collect();
    for config in [
        WorkbookConfig::ephemeral(),
        WorkbookConfig::interactive().with_formula_plane_mode(FormulaPlaneMode::Off),
        WorkbookConfig::interactive()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
    ] {
        let mut a = Workbook::from_reader(
            calamine(bytes.to_vec()).unwrap(),
            LoadStrategy::EagerAll,
            config.clone(),
        )
        .unwrap();
        let mut b = Workbook::from_reader(
            native(bytes.to_vec()).unwrap(),
            LoadStrategy::EagerAll,
            config,
        )
        .unwrap();
        for &(row, col) in &formulas {
            assert_eq!(
                a.get_formula(&name, row, col),
                b.get_formula(&name, row, col)
            );
            assert_eq!(
                value(Some(a.evaluate_cell(&name, row, col).unwrap())),
                value(Some(b.evaluate_cell(&name, row, col).unwrap()))
            );
        }
    }
}

#[test]
fn native_ordinary_names_diagnostics_and_calc_settings_match() {
    use std::io::{Cursor, Write};
    let parts = [
        (
            "[Content_Types].xml",
            "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>",
        ),
        (
            "_rels/.rels",
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>",
        ),
        (
            "xl/_rels/workbook.xml.rels",
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>",
        ),
        (
            "xl/workbook.xml",
            "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets><definedNames><definedName name=\"Input\">Sheet1!$A$1</definedName><definedName name=\"Twice\">Input*2</definedName><definedName name=\"Bad\">Bad+1</definedName><definedName name=\"Local\" localSheetId=\"0\">3</definedName><definedName name=\"Bad\\Name\">#REF!</definedName></definedNames><calcPr iterate=\"1\" iterateCount=\"10\" iterateDelta=\"0.001\"/></workbook>",
        ),
        (
            "xl/worksheets/sheet1.xml",
            "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData><row r=\"1\"><c r=\"A1\"><v>4</v></c><c r=\"B1\"><f>Twice+Local</f><v>11</v></c></row></sheetData></worksheet>",
        ),
    ];
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, text) in parts {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(text.as_bytes()).unwrap();
    }
    let bytes = zip.finish().unwrap().into_inner();
    let mut a = capture(&bytes, calamine);
    let mut b = capture(&bytes, native);
    // Generic source-reader cache values are tested separately from engine load.
    a.records.retain(|key, _| !key.starts_with("reader/sheet/"));
    b.records.retain(|key, _| !key.starts_with("reader/sheet/"));
    assert_eq!(first_difference(&a, &b), None);
    assert!(a.coverage.names >= 2);
    assert_eq!(a.coverage.diagnostics, 2);
}

#[test]
#[ignore = "set FZ_LOADER_FILES and FZ_LOADER_REPORT for native differential"]
fn listed_native_loader_differential() {
    let list = std::env::var_os("FZ_LOADER_FILES").expect("fixture list");
    let report = std::env::var_os("FZ_LOADER_REPORT").expect("report path");
    let paths: Vec<_> = std::fs::read_to_string(list)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(std::path::PathBuf::from)
        .collect();
    assert!(!paths.is_empty());
    let values_only = std::env::var_os("FZ_LOADER_VALUES_ONLY").is_some();
    let (_, divergences) = compare_projected(
        &paths,
        calamine,
        native,
        std::fs::File::create(report).unwrap(),
        |dump| {
            if values_only {
                values_and_formulas(dump)
            } else {
                dump
            }
        },
        &baseline_unavailable_manifest(),
    )
    .unwrap();
    assert_eq!(divergences, 0, "native loader differential; see report");
}

#[test]
fn canonical_values_preserve_bits_and_error_kinds() {
    assert_ne!(
        value(Some(LiteralValue::Number(0.0))),
        value(Some(LiteralValue::Number(-0.0)))
    );
    assert_ne!(value(None), value(Some(LiteralValue::Empty)));
    let a = capture(b"not a ZIP", calamine);
    assert!(a.records.contains_key("open/refusal"));
    assert!(first_difference(&a, &capture(b"not a ZIP", calamine)).is_none());
    assert!(
        first_difference(&a, &Dump::default())
            .unwrap()
            .contains("open/refusal")
    );
}

#[test]
fn refusal_asymmetry_does_not_stop_later_files() {
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<_> = (0..2)
        .map(|i| dir.path().join(format!("{i}.xlsx")))
        .collect();
    for path in &paths {
        std::fs::write(
            path,
            include_bytes!("../../tests/fixtures/shared_formula_above_master.xlsx"),
        )
        .unwrap();
    }
    fn refused(_: Vec<u8>) -> Result<CalamineAdapter, IoError> {
        Err(IoError::Io(std::io::Error::other(
            "deliberate candidate refusal",
        )))
    }
    let mut report = Vec::new();
    let (coverage, divergences) = compare_files(&paths, calamine, refused, &mut report).unwrap();
    assert_eq!(coverage.files, 2);
    assert_eq!(
        coverage.evaluated_formulas, 0,
        "asymmetry must not evaluate the accepting arm"
    );
    assert_eq!(divergences, 2);
    let report = String::from_utf8(report).unwrap();
    assert_eq!(report.matches("DIVERGENCE ").count(), 2);
    assert!(report.contains("deliberate candidate refusal"));
    assert!(report.contains("acceptance_asymmetric_files=2"));
    assert_eq!(report.matches("EVALUATION NOT COMPARED").count(), 2);
    let (_, reverse) = compare_files(&paths, refused, calamine, Vec::new()).unwrap();
    assert_eq!(
        reverse, 2,
        "the opposite acceptance asymmetry is also a difference"
    );
}

#[test]
fn source_inventory_covers_formulas_omitted_by_generic_reader() {
    use std::io::{Cursor, Read, Write};
    let bytes = include_bytes!("../../tests/fixtures/shared_formula_above_master.xlsx");
    let mut input = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut output = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for i in 0..input.len() {
        let mut part = input.by_index(i).unwrap();
        let name = part.name().to_owned();
        output
            .start_file(&name, zip::write::SimpleFileOptions::default())
            .unwrap();
        if name == "xl/worksheets/sheet1.xml" {
            output.write_all(br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1"><f t="shared" si="0"/></c><c r="B1"><f t="shared" si="0" ref="A1:B1">1</f></c></row></sheetData></worksheet>"#).unwrap();
        } else {
            let mut data = Vec::new();
            part.read_to_end(&mut data).unwrap();
            output.write_all(&data).unwrap();
        }
    }
    let bytes = output.finish().unwrap().into_inner();
    let a = capture(&bytes, calamine);
    let b = capture(&bytes, native);
    let key = "evaluated/sheet/00000000/00000001/00000001";
    assert!(a.records.contains_key(key), "{a:?}");
    assert_eq!(a.records.get(key), b.records.get(key));
    assert_eq!(a.coverage.formulas, 2);
    assert_eq!(b.coverage.formulas, 2);
}

#[test]
fn calamine_oracle_is_repeatable() {
    let bytes = include_bytes!("../../tests/fixtures/shared_formula_above_master.xlsx");
    let a = capture(bytes, calamine);
    let b = capture(bytes, calamine);
    assert!(a.coverage.cells > 0, "{:?}", a.records);
    assert_eq!(a.coverage.evaluated_formulas, a.coverage.formulas);
    assert!(a.coverage.evaluated_formulas > 0);
    assert_eq!(first_difference(&a, &b), None);
}
