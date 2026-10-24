//! Compatible XLSX loading through the shared bounded source reader.

mod timing;

#[cfg(test)]
mod retained_tests {
    use super::*;
    use crate::{CalamineAdapter, Workbook};

    #[test]
    fn generic_and_stream_surfaces_share_retained_state() {
        let bytes = include_bytes!("../../tests/fixtures/zip_containers/libreoffice.xlsx");
        let mut baseline = CalamineAdapter::open_bytes(bytes.to_vec()).unwrap();
        let names = baseline.sheet_names().unwrap();
        let expected: Vec<_> = names
            .iter()
            .map(|name| baseline.read_sheet(name).unwrap())
            .collect();
        for stream_first in [false, true] {
            let mut native = NativeXlsxAdapter::open_bytes(bytes.to_vec()).unwrap();
            let mut workbook = Workbook::new();
            if stream_first {
                native.stream_into_engine(workbook.engine_mut()).unwrap();
            } else {
                for name in &names {
                    assert!(!native.is_loaded(name, None, None));
                    assert_eq!(native.sheet_bounds(name), baseline.sheet_bounds(name));
                    assert!(
                        !native.is_loaded(name, None, None),
                        "bounds must not mark loaded"
                    );
                }
            }
            // Any accidental rescan after this point fails. Both ordering paths
            // must use the same parsed Arrow/source records, not source XML.
            for sheet in &mut native.sheets {
                assert!(sheet.parsed.get().is_some());
                sheet.data.clear();
            }
            if !stream_first {
                native.stream_into_engine(workbook.engine_mut()).unwrap();
            }
            for (name, expected) in names.iter().zip(&expected) {
                let actual = native.read_sheet(name).unwrap();
                assert_eq!(format!("{expected:?}"), format!("{actual:?}"));
                assert_eq!(native.sheet_bounds(name), expected.dimensions);
                assert!(native.is_loaded(name, None, None));
                for (&(row, col), cell) in &expected.cells {
                    assert_eq!(
                        format!("{:?}", native.read_cell(name, row, col).unwrap()),
                        format!("{:?}", Some(cell))
                    );
                }
                assert!(native.read_range(name, (2, 2), (1, 1)).unwrap().is_empty());
            }
            assert_eq!(
                native.read_sheet("absent").unwrap_err().to_string(),
                IoError::from(baseline.read_sheet("absent").unwrap_err()).to_string()
            );
        }
    }
}
use super::{
    ingest::{
        formulas,
        replay::{FormulaReplayState, OwnedSpoolFormulaRecord},
        staging::WorkbookSpoolUsage,
        stream::{FormulaBatches, LoadConfig, SheetFormulas, install_values},
        values::{Context, ValueSink},
    },
    metadata::{SharedStringTable, Styles, read_source},
    options::{ReadOptions, ScanRetention},
    package,
    worksheet::{
        self, ScanRequest, ScanStrategy, Scanned,
        events::{Formula, FormulaKind, Literal, Sink},
    },
    xml,
};
use crate::{
    IoError, SpreadsheetReader,
    backends::{XlsxPathSource, calamine::XlsxSource},
    traits::{AccessGranularity, AdapterLoadStats, BackendCaps, CellData, SheetData},
};
use formualizer_common::{DateSystem, LiteralValue};
use formualizer_eval::{
    engine::{Engine, SourceCoord, SourceRect, ingest::EngineLoadStream},
    traits::EvaluationContext,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    io::{Cursor, Read},
    path::Path,
    sync::{Arc, OnceLock},
};

struct SheetSource {
    name: String,
    data: Vec<u8>,
    parsed: OnceLock<ParsedSheet>,
    reader: OnceLock<SheetData>,
}

/// Opt-in XLSX reader using the shared Compatible read policy.
pub struct NativeXlsxAdapter {
    source: XlsxSource,
    sheets: Vec<SheetSource>,
    strings: SharedStringTable,
    styles: Styles,
    options: ReadOptions,
    loaded: HashSet<String>,
    stats: AdapterLoadStats,
    names: super::ingest::names::CalculationNames,
    calc_settings: Option<crate::traits::CalcSettings>,
    name_diagnostics: Vec<crate::traits::NameImportDiagnostic>,
    sheet_diagnostics: Vec<crate::traits::SheetImportDiagnostic>,
    has_links: bool,
    external: Option<Arc<crate::CachedExternalLinkValues>>,
    admission_error: Option<IoError>,
    debug_load: bool,
    load_clock: timing::Timer,
    package_ms: f64,
    external_formulas_ms: f64,
}
impl NativeXlsxAdapter {
    /// Open an XLSX path using an explicitly selected retained backing source.
    pub fn open_path_with_source<P: AsRef<Path>>(
        path: P,
        source: XlsxPathSource,
    ) -> Result<Self, IoError> {
        Self::open_source(XlsxSource::open_path(path.as_ref(), source)?)
    }
    fn open_source(source: XlsxSource) -> Result<Self, IoError> {
        let debug_load = std::env::var_os("FZ_DEBUG_LOAD").is_some();
        let load_clock = timing::Timer::start(debug_load);
        let options = ReadOptions::default();
        let (sheets, strings, styles, names, calc_settings, has_links, sheet_diagnostics) = {
            let bytes = source.bytes(options.limits.max_input_bytes)?;
            let mut archive = package::admit_with(&bytes, &options, Ok)?;
            let root = package::relationships_read(&mut archive, "", &options)?;
            let workbook = root
                .values()
                .find(|r| r.kind.ends_with("/officeDocument"))
                .and_then(|r| r.target.clone())
                .unwrap_or_else(|| "xl/workbook.xml".into());
            let relationships = package::relationships_read(&mut archive, &workbook, &options)?;
            let workbook_xml =
                package::read_part(&mut archive, &workbook, options.limits.max_worksheet_bytes)?;
            let mut has_links = archive
                .file_names()
                .any(|part| part.starts_with("xl/externalLinks/"));
            let mut declared = Vec::new();
            read_source(&workbook_xml, &options, |path, node| {
                if xml::path_is(
                    path,
                    xml::MAIN,
                    &["workbook", "externalReferences", "externalReference"],
                ) && matches!(node.kind, xml::Kind::Open { .. })
                {
                    has_links = true;
                }
                if xml::path_is(path, xml::MAIN, &["workbook", "sheets", "sheet"])
                    && matches!(node.kind, xml::Kind::Open { .. })
                    && let (Some(name), Some(id)) =
                        (node.value("name"), node.attribute(xml::OFFICE, "id"))
                {
                    let (inert, kind, target) = if id.value.is_empty() {
                        (true, "module", String::new())
                    } else {
                        let relation = relationships.get(id.value.as_ref()).ok_or_else(|| {
                            super::options::unsupported(
                                "missing worksheet relationship",
                                "workbook XML",
                            )
                        })?;
                        let kind = if relation.kind == format!("{}/chartsheet", xml::OFFICE) {
                            "chartsheet"
                        } else if relation.kind == format!("{}/dialogsheet", xml::OFFICE) {
                            "dialogsheet"
                        } else if relation.kind == format!("{}/worksheet", xml::OFFICE) {
                            "worksheet"
                        } else {
                            return Err(super::options::unsupported(
                                "non-worksheet sheet",
                                "workbook XML",
                            ));
                        };
                        let target = relation.target.clone().ok_or_else(|| {
                            super::options::unsupported("external worksheet", "workbook XML")
                        })?;
                        (kind != "worksheet", kind, target)
                    };
                    declared.push(package::Sheet {
                        name: name.to_owned(),
                        part: target,
                        inert,
                        kind,
                        sheet_id: 0,
                        tables: BTreeMap::new(),
                    });
                }
                Ok(())
            })?;
            let mut sheets = Vec::new();
            for sheet in declared.iter().filter(|sheet| !sheet.inert) {
                sheets.push(SheetSource {
                    name: sheet.name.clone(),
                    parsed: OnceLock::new(),
                    reader: OnceLock::new(),
                    data: package::read_part(
                        &mut archive,
                        &sheet.part,
                        options.limits.max_worksheet_bytes,
                    )?,
                });
            }
            // The baseline reports omitted modules first, then other inert
            // sheet kinds, retaining declaration order within each group.
            let sheet_diagnostics = declared
                .iter()
                .filter(|sheet| sheet.kind == "module")
                .chain(
                    declared
                        .iter()
                        .filter(|sheet| sheet.inert && sheet.kind != "module"),
                )
                .map(|sheet| crate::traits::SheetImportDiagnostic {
                    name: sheet.name.clone(),
                    kind: sheet.kind.to_owned(),
                })
                .collect();
            let mut metadata = |kind: &str| -> Result<Vec<u8>, IoError> {
                let target = relationships
                    .values()
                    .find(|r| r.kind.ends_with(&format!("/{kind}")))
                    .and_then(|r| r.target.as_deref());
                target
                    .map(|part| {
                        package::read_part(&mut archive, part, options.limits.max_worksheet_bytes)
                    })
                    .transpose()
                    .map(Option::unwrap_or_default)
            };
            let string_bytes = metadata("sharedStrings")?;
            let style_bytes = metadata("styles")?;
            let strings = if string_bytes.is_empty() {
                SharedStringTable::default()
            } else {
                SharedStringTable::parse_read(&string_bytes, &options)?
            };
            let styles = if style_bytes.is_empty() {
                Styles::default()
            } else {
                Styles::parse_read(&style_bytes, &options)?
            };
            let names = super::ingest::names::CalculationNames::new(
                &workbook_xml,
                &declared,
                options.cancel.clone(),
                super::ingest::names::NameImportPolicy::COMPATIBLE_REFERENCED,
            );
            let calc_settings = crate::calc_pr::parse_calc_pr(&workbook_xml);
            (
                sheets,
                strings,
                styles,
                names,
                calc_settings,
                has_links,
                sheet_diagnostics,
            )
        };
        let package_ms = load_clock.elapsed_ms();
        Ok(Self {
            source,
            sheets,
            strings,
            styles,
            options,
            loaded: HashSet::new(),
            stats: AdapterLoadStats::default(),
            names,
            calc_settings,
            name_diagnostics: Vec::new(),
            sheet_diagnostics,
            has_links,
            external: None,
            admission_error: None,
            debug_load,
            load_clock,
            package_ms,
            external_formulas_ms: 0.0,
        })
    }
    fn prepare_external(&mut self) -> Result<Arc<crate::CachedExternalLinkValues>, IoError> {
        let mut formulas = Vec::new();
        for sheet in &self.sheets {
            let parsed = self.parsed_sheet(sheet)?;
            let formula_timer = timing::Timer::start(self.debug_load);
            let mut replay = FormulaReplayState::default();
            for record in &parsed.records {
                replay
                    .emit_record(
                        record.clone(),
                        &sheet.name,
                        |_, _| true,
                        |_, coord, text, _| {
                            formulas.push((
                                sheet.name.clone(),
                                coord.row + 1,
                                coord.col + 1,
                                text.to_owned(),
                            ));
                            Ok(())
                        },
                    )
                    .map_err(IoError::from)?;
            }
            self.external_formulas_ms += formula_timer.elapsed_ms();
        }
        let package_timer = timing::Timer::start(self.debug_load);
        let snapshot = {
            let bytes = self.source.bytes(self.options.limits.max_input_bytes)?;
            Arc::new(crate::cached_external_links::load(
                Cursor::new(bytes.as_ref()),
                &formulas,
                self.names.raw_calculation_names(),
                &self.options,
            ))
        };
        self.names.admit_external_snapshot(&snapshot);
        self.package_ms += package_timer.elapsed_ms();
        Ok(snapshot)
    }
    fn parsed_sheet<'a>(&self, sheet: &'a SheetSource) -> Result<&'a ParsedSheet, IoError> {
        if sheet.parsed.get().is_none() {
            let parsed = self.scan_sheet(sheet, &Default::default(), DateSystem::Excel1900)?;
            // Only successful state is retained; failures remain fallible reader results.
            let _ = sheet.parsed.set(parsed);
        }
        Ok(sheet.parsed.get().expect("initialized parsed sheet"))
    }
    fn source_sheet(&self, name: &str) -> Result<&SheetSource, IoError> {
        self.sheets
            .iter()
            .find(|sheet| sheet.name == name)
            .ok_or_else(|| {
                IoError::Calamine(calamine::Error::Xlsx(
                    calamine::XlsxError::WorksheetNotFound(name.to_owned()),
                ))
            })
    }
    fn reader_sheet<'a>(&self, sheet: &'a SheetSource) -> Result<&'a SheetData, IoError> {
        if let Some(reader) = sheet.reader.get() {
            return Ok(reader);
        }
        let parsed = self.parsed_sheet(sheet)?;
        if let Some(error) = &parsed.value_error {
            return Err(error.to_error());
        }
        let mut cells = BTreeMap::new();
        {
            let arrow = &parsed.reader_values.arrow;
            for (row, col) in populated_coordinates(arrow) {
                let mut value = arrow.get_cell_value(row as usize - 1, col as usize - 1);
                if let LiteralValue::Number(number) = value
                    && arrow
                        .format_id(row as usize - 1, col as usize - 1)
                        .is_some()
                {
                    // The ordinary reader's rich temporal surface always uses
                    // the 1900 epoch, independently of stream configuration.
                    value = LiteralValue::try_from_serial_number_for(DateSystem::Excel1900, number)
                        .unwrap_or_else(LiteralValue::Error);
                }
                if value != LiteralValue::Empty {
                    cells.insert((row, col), CellData::from_value(value));
                }
            }
        }
        let dimensions = value_extent(cells.keys().copied());
        let mut replay = FormulaReplayState::default();
        let mut anchors = HashSet::new();
        for record in parsed
            .records
            .iter()
            .filter(|_| parsed.formula_error.is_none())
        {
            match record {
                OwnedSpoolFormulaRecord::SharedAnchor { shared_index, .. } => {
                    anchors.insert(*shared_index);
                }
                OwnedSpoolFormulaRecord::SharedDescendant { shared_index, .. }
                    if !anchors.contains(shared_index) =>
                {
                    continue;
                }
                _ => {}
            }
            // The generic range API uses the declared rectangle's origin for
            // shared expansion, unlike the stream's actual source anchor. Keep
            // that legacy reader convention out of engine formula staging.
            let mut reader_record = record.clone();
            let anchor_coord = if let OwnedSpoolFormulaRecord::SharedAnchor {
                coord0,
                declared_range: Some(range),
                ..
            } = &mut reader_record
            {
                let actual = *coord0;
                *coord0 = range.start;
                Some(actual)
            } else {
                None
            };
            replay
                .emit_record(
                    reader_record,
                    &sheet.name,
                    |_, _| true,
                    |_, coord, text, _| {
                        let coord = anchor_coord.unwrap_or(coord);
                        if !text.is_empty() {
                            cells
                                .entry((coord.row + 1, coord.col + 1))
                                .or_insert_with(|| CellData {
                                    value: None,
                                    formula: None,
                                    style: None,
                                })
                                .formula = Some(if text.starts_with('=') {
                                text.to_owned()
                            } else {
                                format!("={text}")
                            });
                        }
                        Ok(())
                    },
                )
                .map_err(IoError::from)?;
        }
        let _ = sheet.reader.set(SheetData {
            cells,
            dimensions: Some(dimensions),
            tables: vec![],
            named_ranges: vec![],
            date_system_1904: false,
            merged_cells: vec![],
            hidden: false,
            row_hidden_manual: vec![],
            row_hidden_filter: vec![],
        });
        Ok(sheet.reader.get().expect("initialized reader sheet"))
    }
    fn scan_sheet(
        &self,
        sheet: &SheetSource,
        limits: &formualizer_eval::engine::WorkbookLoadLimits,
        date_system: DateSystem,
    ) -> Result<ParsedSheet, IoError> {
        let scan_timer = timing::Timer::start(self.debug_load);
        // A bounded dimension hint sizes Arrow; actual populated bounds win.
        let dimensions = super::ingest::values::dimension_hint(&sheet.data, &Default::default());
        let values = ValueSink::new(Context {
            name: &sheet.name,
            source: &sheet.data,
            strings: &self.strings,
            styles: &self.styles,
            dimensions,
            chunk_rows: 32 * 1024,
            date_system,
            limits,
        })?;
        let reader_values = ValueSink::new(Context {
            name: &sheet.name,
            source: &sheet.data,
            strings: &self.strings,
            styles: &self.styles,
            dimensions,
            chunk_rows: 32 * 1024,
            date_system,
            limits,
        })?;
        let mut sink = LoadSink {
            source: &sheet.data,
            values,
            reader_values,
            records: Vec::new(),
            value_error: None,
            formula_error: None,
            stream_error: None,
            last_formula_coord: None,
            skipped_values: 0,
        };
        let mut observed = 0;
        let mut logical = 0;
        let scanned = worksheet::scanner::scan_with(
            &sheet.data,
            &self.options,
            ScanRequest {
                retention: ScanRetention {
                    values: true,
                    formula_events: true,
                    marks: false,
                    geometry: false,
                },
                strategy: ScanStrategy::Plain,
            },
            &mut observed,
            &mut logical,
            &mut sink,
        );
        let bounds = match scanned {
            // The scanner validates all source sections, but the ordinary
            // reader consumes only the first sheetData. Sink coordinates own
            // the loaded extent; later ignored sections cannot enlarge it.
            Ok(Scanned::Done(_)) => (0, 0),
            Ok(_) => {
                return Err(IoError::from_backend(
                    "xlsx",
                    std::io::Error::other("unexpected compatible index request"),
                ));
            }
            Err(error) => {
                let error = DeferredReadError::capture(error)?;
                sink.stream_error.get_or_insert_with(|| error.clone());
                sink.value_error.get_or_insert(error);
                (0, 0)
            }
        };
        let scan_ms = scan_timer.elapsed_ms();
        let sinks_timer = timing::Timer::start(self.debug_load);
        let mut values = sink.values.finish(bounds)?;
        values.values_observed += sink.skipped_values;
        let reader_values = sink.reader_values.finish(bounds)?;
        let sinks_ms = sinks_timer.elapsed_ms();
        Ok(ParsedSheet {
            values,
            records: sink.records,
            reader_values,
            scan_ms,
            sinks_ms,
            value_error: sink.value_error,
            formula_error: sink.formula_error,
            stream_error: sink.stream_error,
        })
    }
}
fn populated_coordinates(
    arrow: &formualizer_eval::arrow_store::ArrowSheet,
) -> BTreeSet<(u32, u32)> {
    let mut coordinates = BTreeSet::new();
    for column in &arrow.columns {
        for (index, chunk) in column.chunks.iter().enumerate().chain(
            column
                .sparse_chunks
                .iter()
                .map(|(index, chunk)| (*index, chunk)),
        ) {
            let start = arrow
                .chunk_starts
                .get(index)
                .copied()
                .unwrap_or(index * arrow.chunk_rows);
            for (offset, tag) in chunk.type_tag.values().iter().enumerate() {
                if *tag != formualizer_eval::arrow_store::TypeTag::Empty as u8 {
                    coordinates.insert(((start + offset + 1) as u32, column.index + 1));
                }
            }
            for (offset, _) in chunk.overlay.iter() {
                coordinates.insert(((start + offset + 1) as u32, column.index + 1));
            }
        }
    }
    coordinates
}

fn value_extent(coordinates: impl Iterator<Item = (u32, u32)>) -> (u32, u32) {
    let mut extent: Option<(u32, u32, u32, u32)> = None;
    for (row, col) in coordinates {
        extent = Some(match extent {
            Some((r0, c0, r1, c1)) => (r0.min(row), c0.min(col), r1.max(row), c1.max(col)),
            None => (row, col, row, col),
        });
    }
    extent.map_or((0, 0), |(r0, c0, r1, c1)| (r1 - r0 + 1, c1 - c0 + 1))
}

// The generic reader parses values first and discards a refused formula view;
// streaming instead reports the first source refusal. Retain those independent
// outcomes without rereading the worksheet or cloning opaque backend errors.
#[derive(Clone)]
enum DeferredReadError {
    Unexpected(&'static str),
    CellType(String),
    Alphanumeric(u8),
    Unsupported { feature: String, context: String },
    Backend { backend: String, message: String },
}
impl DeferredReadError {
    fn capture(error: IoError) -> Result<Self, IoError> {
        match error {
            IoError::Calamine(calamine::Error::Xlsx(calamine::XlsxError::Unexpected(message))) => {
                Ok(Self::Unexpected(message))
            }
            IoError::Calamine(calamine::Error::Xlsx(calamine::XlsxError::CellTAttribute(kind))) => {
                Ok(Self::CellType(kind))
            }
            IoError::Calamine(calamine::Error::Xlsx(calamine::XlsxError::Alphanumeric(byte))) => {
                Ok(Self::Alphanumeric(byte))
            }
            IoError::Unsupported { feature, context } => Ok(Self::Unsupported { feature, context }),
            IoError::Backend { backend, message } => Ok(Self::Backend { backend, message }),
            other => Err(other),
        }
    }
    fn to_error(&self) -> IoError {
        let error = match self {
            Self::Backend { backend, message } => {
                return IoError::Backend {
                    backend: backend.clone(),
                    message: message.clone(),
                };
            }
            Self::Unexpected(message) => calamine::XlsxError::Unexpected(message),
            Self::CellType(kind) => calamine::XlsxError::CellTAttribute(kind.clone()),
            Self::Alphanumeric(byte) => calamine::XlsxError::Alphanumeric(*byte),
            Self::Unsupported { feature, context } => {
                return IoError::Unsupported {
                    feature: feature.clone(),
                    context: context.clone(),
                };
            }
        };
        IoError::Calamine(calamine::Error::Xlsx(error))
    }
}

struct ParsedSheet {
    scan_ms: f64,
    sinks_ms: f64,
    values: super::ingest::values::IngestedValues,
    records: Vec<OwnedSpoolFormulaRecord>,
    reader_values: super::ingest::values::IngestedValues,
    value_error: Option<DeferredReadError>,
    formula_error: Option<DeferredReadError>,
    stream_error: Option<DeferredReadError>,
}
struct LoadSink<'a> {
    source: &'a [u8],
    values: ValueSink<'a>,
    records: Vec<OwnedSpoolFormulaRecord>,
    reader_values: ValueSink<'a>,
    last_formula_coord: Option<(u32, u32)>,
    skipped_values: usize,
    value_error: Option<DeferredReadError>,
    formula_error: Option<DeferredReadError>,
    stream_error: Option<DeferredReadError>,
}
fn ordinary_coordinate(
    source: &[u8],
    span: Option<&std::ops::Range<usize>>,
) -> Result<(), IoError> {
    if let Some(span) = span
        && source[span.clone()].contains(&b'$')
    {
        return Err(IoError::Calamine(calamine::Error::Xlsx(
            calamine::XlsxError::Alphanumeric(b'$'),
        )));
    }
    Ok(())
}
fn ordinary_literal<'a>(source: &[u8], mut literal: Literal<'a>) -> Result<Literal<'a>, IoError> {
    ordinary_coordinate(source, literal.raw_coordinate_span.as_ref())?;
    if let Some(raw) = literal.raw_value(source)
        && matches!(literal.value, worksheet::events::Value::Text(_))
        && (raw.windows(9).any(|s| s == b"<![CDATA[")
            || raw.windows(2).any(|s| s == b"\xC2\x85")
            || raw.windows(3).any(|s| s == b"\xE2\x80\xA8"))
    {
        let mut fragment = b"<v>".to_vec();
        fragment.extend_from_slice(raw);
        fragment.extend_from_slice(b"</v>");
        let mut value = String::new();
        xml::walk_ignoring_cdata(&fragment, &ReadOptions::default(), |_, node| {
            if let xml::Kind::Text(text) = &node.kind {
                value.push_str(&worksheet::events::text(&fragment, &node, text.clone())?);
            }
            Ok(())
        })?;
        literal.value = worksheet::events::Value::Text(std::borrow::Cow::Owned(value));
    }
    Ok(literal)
}
impl Sink for LoadSink<'_> {
    fn retain_formula_cache(&self) -> bool {
        true
    }
    fn formula_cache(&mut self, literal: Literal<'_>) -> Result<(), IoError> {
        if self.value_error.is_none() {
            let result = ordinary_literal(self.source, literal)
                .and_then(|literal| self.reader_values.push(literal));
            self.retain_value_error(result)?;
        }
        Ok(())
    }
    fn literal(&mut self, literal: Literal<'_>) -> Result<(), IoError> {
        if self.value_error.is_none() {
            let result = ordinary_literal(self.source, literal).and_then(|literal| {
                let before = self.reader_values.observed_count();
                self.reader_values.push(literal.clone())?;
                if self.last_formula_coord == Some((literal.row, literal.col)) {
                    self.skipped_values += self.reader_values.observed_count() - before;
                    Ok(())
                } else {
                    self.values.push(literal)
                }
            });
            self.retain_value_error(result)?;
        }
        Ok(())
    }
    fn formula(&mut self, formula: Formula<'_>) -> Result<(), IoError> {
        if self.formula_error.is_none()
            && let Err(error) = self.source_formula(formula)
        {
            let error = DeferredReadError::capture(error)?;
            self.stream_error.get_or_insert_with(|| error.clone());
            self.formula_error = Some(error);
        }
        Ok(())
    }
}
impl LoadSink<'_> {
    fn retain_value_error(&mut self, result: Result<(), IoError>) -> Result<(), IoError> {
        if let Err(error) = result {
            let error = DeferredReadError::capture(error)?;
            self.stream_error.get_or_insert_with(|| error.clone());
            self.value_error = Some(error);
        }
        Ok(())
    }
    fn source_formula(&mut self, formula: Formula<'_>) -> Result<(), IoError> {
        ordinary_coordinate(self.source, formula.envelope.raw_coordinate.as_ref())?;
        if matches!(
            formula.kind,
            FormulaKind::SharedAnchor { .. } | FormulaKind::SharedDescendant { .. }
        ) && !formula
            .envelope
            .raw_shared_index
            .as_ref()
            .is_some_and(|span| {
                let raw = &self.source[span.clone()];
                !raw.is_empty() && raw.iter().all(u8::is_ascii_digit)
            })
        {
            return Err(IoError::Calamine(calamine::Error::Xlsx(
                calamine::XlsxError::Unexpected("si attribute must be a number"),
            )));
        }
        self.values.observe_formula(formula.row, formula.col)?;
        self.last_formula_coord = Some((formula.row, formula.col));
        let coord0 = SourceCoord {
            row: formula.row - 1,
            col: formula.col - 1,
        };
        let sequence = formula.sequence;
        let record = match formula.kind {
            FormulaKind::SharedAnchor {
                shared_index: Some(index),
                declared_range: range,
                text,
            } => OwnedSpoolFormulaRecord::SharedAnchor {
                sequence,
                coord0,
                shared_index: index as usize,
                declared_range: Some(SourceRect {
                    start: SourceCoord {
                        row: range.first_row - 1,
                        col: range.first_col - 1,
                    },
                    end: SourceCoord {
                        row: range.last_row - 1,
                        col: range.last_col - 1,
                    },
                }),
                text: text.to_owned(),
            },
            FormulaKind::SharedDescendant {
                shared_index: Some(index),
            } => OwnedSpoolFormulaRecord::SharedDescendant {
                sequence,
                coord0,
                shared_index: index as usize,
            },
            FormulaKind::Ordinary { text }
            | FormulaKind::Unsupported { text, .. }
            | FormulaKind::SharedAnchor { text, .. } => OwnedSpoolFormulaRecord::Ordinary {
                sequence,
                coord0,
                text: text.to_owned(),
            },
            FormulaKind::SharedDescendant { shared_index: None } => {
                return Err(IoError::Calamine(calamine::Error::Xlsx(
                    calamine::XlsxError::Unexpected("si attribute must be a number"),
                )));
            }
        };
        self.records.push(record);
        Ok(())
    }
}
impl SpreadsheetReader for NativeXlsxAdapter {
    type Error = IoError;
    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, IoError> {
        Self::open_path_with_source(path, XlsxPathSource::SharedFile)
    }
    fn open_bytes(data: Vec<u8>) -> Result<Self, IoError> {
        Self::open_source(XlsxSource::from_bytes(data))
    }
    fn open_reader(mut reader: Box<dyn Read + Send + Sync>) -> Result<Self, IoError> {
        let mut data = Vec::new();
        reader
            .by_ref()
            .take((ReadOptions::default().limits.max_input_bytes as u64).saturating_add(1))
            .read_to_end(&mut data)?;
        Self::open_bytes(data)
    }
    fn access_granularity(&self) -> AccessGranularity {
        AccessGranularity::Sheet
    }
    fn capabilities(&self) -> BackendCaps {
        BackendCaps {
            read: true,
            formulas: true,
            named_ranges: true,
            bytes_input: true,
            ..Default::default()
        }
    }
    fn sheet_names(&self) -> Result<Vec<String>, IoError> {
        Ok(self.sheets.iter().map(|s| s.name.clone()).collect())
    }
    fn cached_external_link_values(&mut self) -> Option<Arc<crate::CachedExternalLinkValues>> {
        if !self.has_links {
            return None;
        }
        if let Some(snapshot) = &self.external {
            return Some(snapshot.clone());
        }
        match self.prepare_external() {
            Ok(snapshot) => {
                self.external = Some(snapshot.clone());
                Some(snapshot)
            }
            Err(error) => {
                self.admission_error = Some(error);
                None
            }
        }
    }
    fn calc_settings(&self) -> Option<crate::traits::CalcSettings> {
        self.calc_settings.clone()
    }
    fn defined_names(&mut self) -> Result<Vec<crate::traits::DefinedName>, IoError> {
        self.names.defined_names().map_err(IoError::from)
    }
    fn sheet_import_diagnostics(&self) -> Vec<crate::traits::SheetImportDiagnostic> {
        self.sheet_diagnostics.clone()
    }
    fn name_import_diagnostics(&self) -> Vec<crate::traits::NameImportDiagnostic> {
        self.name_diagnostics.clone()
    }
    fn load_stats(&self) -> Option<AdapterLoadStats> {
        Some(self.stats.clone())
    }
    fn read_sheet(&mut self, name: &str) -> Result<SheetData, IoError> {
        let sheet = self.source_sheet(name)?;
        let data = self.reader_sheet(sheet)?.clone();
        self.loaded.insert(name.to_owned());
        Ok(data)
    }
    fn read_cell(&mut self, name: &str, row: u32, col: u32) -> Result<Option<CellData>, IoError> {
        let data = self
            .reader_sheet(self.source_sheet(name)?)?
            .cells
            .get(&(row, col))
            .cloned();
        self.loaded.insert(name.to_owned());
        Ok(data)
    }
    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, IoError> {
        let cells = self
            .reader_sheet(self.source_sheet(sheet)?)?
            .cells
            .iter()
            .filter(|((r, c), _)| *r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1)
            .map(|(coord, cell)| (*coord, cell.clone()))
            .collect();
        self.loaded.insert(sheet.to_owned());
        Ok(cells)
    }
    fn sheet_bounds(&self, name: &str) -> Option<(u32, u32)> {
        let sheet = self.sheets.iter().find(|s| s.name == name)?;
        self.reader_sheet(sheet).ok()?.dimensions
    }
    fn is_loaded(&self, sheet: &str, _row: Option<u32>, _col: Option<u32>) -> bool {
        self.loaded.contains(sheet)
    }
}
impl<R: EvaluationContext> EngineLoadStream<R> for NativeXlsxAdapter {
    type Error = IoError;
    fn stream_into_engine(&mut self, engine: &mut Engine<R>) -> Result<(), IoError> {
        if let Some(error) = self.admission_error.take() {
            return Err(error);
        }
        let names = self.sheet_names()?;
        engine.adopt_file_sheets(names.iter().map(String::as_str))?;
        let previous = LoadConfig::begin(
            engine,
            self.external.is_some(),
            self.external
                .as_ref()
                .is_some_and(|snapshot| !snapshot.refused_formulas.is_empty()),
        );
        let mut phases = timing::Phases {
            package_ms: self.package_ms,
            formulas_ms: self.external_formulas_ms,
            ..Default::default()
        };
        let result = (|| {
            let mut batches = FormulaBatches::default();
            let mut usage = WorkbookSpoolUsage::default();
            let mut values_observed = 0;
            let mut values_handed = 0;
            let mut formulas_observed = 0;
            let mut formulas_handed = 0;
            let mut shared = 0;
            for (index, sheet) in self.sheets.iter().enumerate() {
                let parsed = self.parsed_sheet(sheet)?;
                // External-cache preparation may already have populated these
                // sheets. Include their original scan once in the adapter lap.
                phases.scan_ms += parsed.scan_ms;
                phases.sinks_ms += parsed.sinks_ms;
                if let Some(error) = &parsed.stream_error {
                    return Err(error.to_error());
                }
                crate::load_limits::enforce_sheet_dimension_limits(
                    "calamine",
                    &sheet.name,
                    parsed.values.dimensions.0 as u32,
                    parsed.values.dimensions.1 as u32,
                    engine.workbook_load_limits(),
                )?;
                let populated = (parsed.values.values_observed as u64)
                    .saturating_add(parsed.records.len() as u64);
                if populated > engine.workbook_load_limits().max_sheet_logical_cells {
                    return Err(IoError::Calamine(super::ingest::staging::engine_error(
                        format!(
                            "Workbook load budget exceeded in calamine for sheet {}: observed populated cell count exceeds configured logical-cell budget of {}",
                            sheet.name,
                            engine.workbook_load_limits().max_sheet_logical_cells
                        ),
                    )));
                }
                // The source sink stores raw numeric serials, not converted chrono
                // values. Select the caller's epoch before these lanes are installed.
                let mut arrow = parsed.values.arrow.clone();
                arrow.date_system = engine.config.date_system;
                let refused: HashSet<_> = self
                    .external
                    .iter()
                    .flat_map(|snapshot| snapshot.refused_formulas.keys())
                    .filter(|(name, _, _)| name == &sheet.name)
                    .map(|(_, row, col)| (row - 1, col - 1))
                    .collect();
                values_observed += parsed.values.values_observed as u64;
                values_handed += parsed.values.values_handed_to_engine as u64;
                let install_timer = timing::Timer::start(self.debug_load);
                install_values(engine, &sheet.name, arrow);
                phases.install_ms += install_timer.elapsed_ms();
                let formula_timer = timing::Timer::start(self.debug_load);
                let staged = formulas::stage_records_with_refusals(
                    engine,
                    &sheet.name,
                    index as u32,
                    parsed.records.iter().cloned().map(Ok),
                    usage,
                    &self.options.cancel,
                    &refused,
                )?;
                usage.bytes += staged.spool_bytes;
                usage.files += u32::from(staged.spool_spilled);
                formulas_observed += staged.formulas_observed as u64;
                // Match the ordinary loader's handoff accounting, including
                // source formula records that produce no stored engine formula.
                formulas_handed += staged.formulas_observed as u64;
                shared += staged.shared_tags as u64;
                batches.push(
                    engine,
                    &sheet.name,
                    SheetFormulas {
                        records: staged.records,
                        report: staged.report,
                        families: staged.families,
                        partitioned: staged.partitioned,
                        preparation: staged.preparation,
                        deferred: staged.deferred,
                    },
                );
                phases.formulas_ms += formula_timer.elapsed_ms();
                self.loaded.insert(sheet.name.clone());
            }
            let install_timer = timing::Timer::start(self.debug_load);
            if !engine.config.defer_graph_building {
                batches.install_replay(engine).map_err(IoError::from)?;
            }
            batches.install_direct(engine).map_err(IoError::from)?;
            self.names.register(engine).map_err(IoError::from)?;
            self.name_diagnostics = self.names.import_diagnostics()?;
            for diagnostic in &self.name_diagnostics {
                use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
                if diagnostic.local_sheet_id.is_some() && diagnostic.scope_sheet.is_none() {
                    continue;
                }
                let scope = match diagnostic.scope_sheet.as_deref() {
                    Some(sheet) => {
                        let Some(id) = engine.sheet_id(sheet) else {
                            continue;
                        };
                        NameScope::Sheet(id)
                    }
                    None => NameScope::Workbook,
                };
                let error =
                    formualizer_common::ExcelError::new(formualizer_common::ExcelErrorKind::Name)
                        .with_message(format!(
                            "Defined name `{}` could not be evaluated",
                            diagnostic.name
                        ));
                if let Err(error) = engine.define_name(
                    &diagnostic.name,
                    NamedDefinition::Literal(LiteralValue::Error(error)),
                    scope,
                ) && !self
                    .names
                    .skippable_name_registration_error(&diagnostic.name, &error)
                {
                    return Err(IoError::Calamine(super::ingest::staging::engine_error(
                        error,
                    )));
                }
            }
            self.stats = AdapterLoadStats {
                formula_cells_observed: Some(formulas_observed),
                value_cells_observed: Some(values_observed),
                value_slots_handed_to_engine: Some(values_handed),
                formula_cells_handed_to_engine: Some(formulas_handed),
                shared_formula_tags_observed: Some(shared),
            };
            phases.install_ms += install_timer.elapsed_ms();
            Ok(())
        })();
        previous.restore(engine);
        if result.is_ok() {
            let install_timer = timing::Timer::start(self.debug_load);
            engine.finalize_all_sheet_indexes(names.iter().map(String::as_str));
            phases.install_ms += install_timer.elapsed_ms();
            if self.debug_load {
                phases.report(self.load_clock.elapsed_ms());
            }
        }
        result
    }
}
