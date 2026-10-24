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
}
impl Dump {
    fn record(&mut self, key: impl Into<String>, v: impl Debug) {
        assert!(self.records.insert(key.into(), format!("{v:?}")).is_none());
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
    IoError: From<<B as EngineLoadStream<WBResolver>>::Error>,
{
    let mut dump = Dump::default();
    dump.coverage.files = 1;
    // Metadata/random reads use an independent reader. They must not populate
    // the engine or alter the stats of the actual fast-stream load.
    let mut reader = match open(bytes.to_vec()) {
        Ok(reader) => reader,
        Err(error) => {
            dump.refusal("open", error);
            return dump;
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
        Err(e) => dump.record("reader/names-error", e.to_string()),
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
            dump.record("reader/sheets-error", e.to_string());
            return dump;
        }
    };
    dump.record("reader/sheets", &names);
    let mut coordinates = BTreeMap::new();
    for (i, name) in names.iter().enumerate() {
        let prefix = format!("reader/sheet/{i:08}");
        dump.record(format!("{prefix}/bounds"), reader.sheet_bounds(name));
        dump.record(
            format!("{prefix}/loaded-before"),
            reader.is_loaded(name, None, None),
        );
        match reader.read_sheet(name) {
            Err(e) => dump.record(format!("{prefix}/error"), e.to_string()),
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
                coordinates.insert(name.clone(), coords);
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
            return dump;
        }
    };
    let mut config = WorkbookConfig::ephemeral();
    config.eval.deterministic_mode = formualizer_eval::engine::DeterministicMode::Enabled {
        timestamp_utc: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        timezone: formualizer_eval::timezone::TimeZoneSpec::Utc,
    };
    let (mut wb, stats) =
        match Workbook::from_reader_with_adapter_stats(backend, LoadStrategy::EagerAll, config) {
            Ok(loaded) => loaded,
            Err(error) => {
                dump.refusal("load", error);
                return dump;
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
    // Evaluate only after snapshotting every cell. Evaluation can materialize
    // spills and build deferred graphs; neither belongs in the load snapshot.
    for (i, name, row, col) in formulas {
        let result = wb
            .evaluate_cell(&name, row, col)
            .map(|v| value(Some(v)))
            .map_err(|e| format!("{e:?}: {e}"));
        dump.record(format!("evaluated/sheet/{i:08}/{row:08}/{col:08}"), result);
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
            return Some(format!(
                "{key}\nCalamine: {:?}\nCandidate: {:?}\nPrevious context: {context:?}",
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
    mut output: impl std::io::Write,
) -> std::io::Result<(Coverage, usize)>
where
    A: SpreadsheetReader + EngineLoadStream<WBResolver>,
    B: SpreadsheetReader + EngineLoadStream<WBResolver>,
    IoError: From<<A as EngineLoadStream<WBResolver>>::Error>
        + From<<B as EngineLoadStream<WBResolver>>::Error>,
{
    let mut coverage = Coverage::default();
    let mut divergences = 0;
    for path in paths {
        let bytes = std::fs::read(path)?;
        let a = capture(&bytes, baseline);
        let b = capture(&bytes, candidate);
        coverage.files += 1;
        coverage.sheets += a.coverage.sheets;
        coverage.cells += a.coverage.cells;
        coverage.formulas += a.coverage.formulas;
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
    writeln!(output, "TOTAL {coverage:?} divergent_files={divergences}")?;
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
    assert_eq!(divergences, 2);
    let report = String::from_utf8(report).unwrap();
    assert_eq!(report.matches("DIVERGENCE ").count(), 2);
    assert!(report.contains("deliberate candidate refusal"));
    let (_, reverse) = compare_files(&paths, refused, calamine, Vec::new()).unwrap();
    assert_eq!(
        reverse, 2,
        "the opposite acceptance asymmetry is also a difference"
    );
}

#[test]
fn calamine_oracle_is_repeatable() {
    let bytes = include_bytes!("../../tests/fixtures/shared_formula_above_master.xlsx");
    let a = capture(bytes, calamine);
    let b = capture(bytes, calamine);
    assert!(a.coverage.cells > 0, "{:?}", a.records);
    assert_eq!(first_difference(&a, &b), None);
}
