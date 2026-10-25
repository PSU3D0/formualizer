//! Compatible XLSX loading through the shared bounded source reader.

pub(in crate::xlsx) mod cells;
mod timing;

#[cfg(test)]
mod retained_tests {
    use super::*;
    use crate::{CalamineAdapter, Workbook};

    #[test]
    fn bulk_unique_loader_inflates_consumed_parts_once() {
        let bytes = include_bytes!("../../tests/fixtures/zip_containers/libreoffice.xlsx");
        package::take_part_inflates();
        let mut native = NativeXlsxAdapter::open_bytes(bytes.to_vec()).unwrap();
        native
            .stream_into_engine(Workbook::new().engine_mut())
            .unwrap();
        for (name, count) in package::take_part_inflates() {
            assert_eq!(count, 1, "{name} must not be inflated again after audit");
        }
    }

    #[test]
    fn bulk_formula_producer_does_not_retain_a_second_record_vector() {
        let bytes = include_bytes!("../../tests/fixtures/shared_formula_above_master.xlsx");
        let native = NativeXlsxAdapter::open_bytes(bytes.to_vec()).unwrap();
        let limits = Default::default();
        let mut count = 0;
        let mut emit = |_| {
            count += 1;
            Ok(())
        };
        let parsed = native
            .scan_sheet_produced(
                &native.sheets[0],
                &limits,
                DateSystem::Excel1900,
                false,
                Some(&mut emit),
            )
            .unwrap();
        assert!(count > 0);
        assert!(parsed.records.is_empty());
        assert!(parsed.reader_values.is_none());
    }

    #[test]
    fn bulk_source_errors_precede_later_or_same_cell_staging_failures() {
        let bytes = include_bytes!("../../tests/fixtures/zip_containers/libreoffice.xlsx");
        let native = NativeXlsxAdapter::open_bytes(bytes.to_vec()).unwrap();
        for body in [
            "<c r=\"A1\" t=\"s\"><v>999999</v></c><c r=\"B1\"><f>1</f></c>",
            "<c r=\"A1\" t=\"s\"><f>1</f><v>999999</v></c>",
            "<c r=\"A1\"><f t=\"shared\" si=\"bad\"/></c><c r=\"B1\"><f>1</f></c>",
        ] {
            let sheet = SheetSource {
                name: "test".into(),
                data: format!("<worksheet><sheetData><row>{body}</row></sheetData></worksheet>")
                    .into_bytes(),
                parsed: OnceLock::new(),
                reader: OnceLock::new(),
                reader_parsed: OnceLock::new(),
            };
            let limits = Default::default();
            let mut emit = |_| panic!("source failure must prevent later staging failures");
            let parsed = native
                .scan_sheet_produced(
                    &sheet,
                    &limits,
                    DateSystem::Excel1900,
                    false,
                    Some(&mut emit),
                )
                .unwrap();
            assert!(parsed.stream_error.is_some(), "{body}");
        }
    }

    #[test]
    fn bulk_first_source_refusal_is_not_replaced_by_low_formula_budgets() {
        let bytes = include_bytes!("../../tests/fixtures/zip_containers/libreoffice.xlsx");
        let native = NativeXlsxAdapter::open_bytes(bytes.to_vec()).unwrap();
        for (budget, body) in [
            (
                1,
                "<c r=\"A1\" t=\"s\"><v>999999</v></c><c r=\"B1\"><f>1</f></c><c r=\"C1\"><f>1</f></c>",
            ),
            (0, "<c r=\"A1\" t=\"s\"><f>1</f><v>999999</v></c>"),
        ] {
            let sheet = SheetSource {
                name: "test".into(),
                data: format!("<worksheet><sheetData><row>{body}</row></sheetData></worksheet>")
                    .into_bytes(),
                parsed: OnceLock::new(),
                reader: OnceLock::new(),
                reader_parsed: OnceLock::new(),
            };
            let limits = formualizer_eval::engine::WorkbookLoadLimits {
                max_sheet_logical_cells: budget,
                ..Default::default()
            };
            let mut emit = |_| panic!("invalid source must stop before staging");
            let parsed = native
                .scan_sheet_produced(
                    &sheet,
                    &limits,
                    DateSystem::Excel1900,
                    false,
                    Some(&mut emit),
                )
                .unwrap();
            assert!(
                parsed
                    .stream_error
                    .unwrap()
                    .to_error()
                    .to_string()
                    .contains("shared strings table")
            );
        }
    }

    #[test]
    fn reader_constructor_preserves_the_incoming_cursor() {
        let bytes = include_bytes!("../../tests/fixtures/zip_containers/libreoffice.xlsx");
        let mut prefixed = b"prefix".to_vec();
        prefixed.extend_from_slice(bytes);
        let mut reader = Cursor::new(prefixed);
        reader.set_position(6);
        let mut baseline = CalamineAdapter::open_reader(Box::new(reader.clone())).unwrap();
        let mut native = NativeXlsxAdapter::open_reader(Box::new(reader)).unwrap();
        for name in baseline.sheet_names().unwrap() {
            assert_eq!(
                format!("{:?}", baseline.read_sheet(&name).unwrap()),
                format!("{:?}", native.read_sheet(&name).unwrap())
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn path_sources_keep_the_original_backing_after_path_replacement() {
        let bytes = include_bytes!("../../tests/fixtures/zip_containers/libreoffice.xlsx");
        for source in [XlsxPathSource::SharedFile, XlsxPathSource::DirectMmap] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("source.xlsx");
            std::fs::write(&path, bytes).unwrap();
            let mut baseline = CalamineAdapter::open_path_with_source(&path, source).unwrap();
            let mut native = NativeXlsxAdapter::open_path_with_source(&path, source).unwrap();
            // Replace the path, never truncate or mutate the retained/mapped
            // inode: neither adapter may reopen the replacement by pathname.
            std::fs::rename(&path, directory.path().join("retained.xlsx")).unwrap();
            std::fs::write(&path, b"not the original workbook").unwrap();
            for name in baseline.sheet_names().unwrap() {
                assert_eq!(
                    format!("{:?}", baseline.read_sheet(&name).unwrap()),
                    format!("{:?}", native.read_sheet(&name).unwrap())
                );
            }
        }
    }

    #[test]
    fn generic_and_stream_surfaces_cache_only_their_requested_state() {
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
            if stream_first {
                assert!(
                    native
                        .sheets
                        .iter()
                        .all(|sheet| sheet.reader_parsed.get().is_none()),
                    "engine loading must not eagerly build a second value plane"
                );
            }
            if !stream_first {
                native.stream_into_engine(workbook.engine_mut()).unwrap();
            }
            for name in &names {
                native.read_sheet(name).unwrap();
            }
            // Once each surface is requested, subsequent reads use its cache.
            for sheet in &mut native.sheets {
                assert!(
                    sheet.parsed.get().is_none(),
                    "engine handoff must consume its value plane"
                );
                assert!(sheet.reader_parsed.get().is_some());
                sheet.data.clear();
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
                baseline.read_sheet("absent").unwrap_err().to_string()
            );
        }
    }
}
use super::source::XlsxSource;
use super::{
    ingest::{
        formulas,
        replay::{FormulaReplayState, OwnedSpoolFormulaRecord},
        staging::WorkbookSpoolUsage,
        stream::{FormulaBatches, LoadConfig, SheetFormulas, install_values},
        values::{Context, ValueSink},
    },
    metadata::{SharedStringTable, Styles, read_source},
    options::{ReadOptions, XlsxLoadLimits},
    package, worksheet, xml,
};
use crate::{
    IoError, SpreadsheetReader,
    backends::XlsxPathSource,
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
    reader_parsed: OnceLock<ParsedSheet>,
}

/// Native XLSX reader using the shared Compatible read policy.
pub struct NativeXlsxAdapter {
    source: XlsxSource,
    sheets: Vec<SheetSource>,
    strings: SharedStringTable,
    styles: Styles,
    options: ReadOptions,
    budget: package::ConsumptionBudget,
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
        Self::open_path_with_source_and_limits(path, source, XlsxLoadLimits::default())
    }
    /// Open immutable XLSX bytes with cooperative cancellation during admission and ingestion.
    pub fn open_bytes_cancellable(
        bytes: Vec<u8>,
        cancel: crate::CancelToken,
    ) -> Result<Self, IoError> {
        Self::open_bytes_cancellable_with_limits(bytes, cancel, XlsxLoadLimits::default())
    }
    /// Open a retained path with limits validated before opening or mapping it.
    pub fn open_path_with_source_and_limits<P: AsRef<Path>>(
        path: P,
        source: XlsxPathSource,
        limits: XlsxLoadLimits,
    ) -> Result<Self, IoError> {
        let options = limits.read_options()?;
        let source = XlsxSource::open_path(path.as_ref(), source, options.limits.max_input_bytes)?;
        Self::open_source_with_options(source, options)
    }
    /// Open owned bytes with explicit ordinary package/XML limits.
    pub fn open_bytes_with_limits(bytes: Vec<u8>, limits: XlsxLoadLimits) -> Result<Self, IoError> {
        let options = limits.read_options()?;
        if bytes.len() > options.limits.max_input_bytes {
            return Err(super::options::unsupported(
                "input byte limit",
                "XLSX source",
            ));
        }
        Self::open_source_with_options(XlsxSource::from_bytes(bytes), options)
    }
    /// Open bytes with chunk-level decode cancellation. XML cancellation remains
    /// cooperative between parser events, not within an individual XML token.
    pub fn open_bytes_cancellable_with_limits(
        bytes: Vec<u8>,
        cancel: crate::CancelToken,
        limits: XlsxLoadLimits,
    ) -> Result<Self, IoError> {
        let mut options = limits.read_options()?;
        options.cancel = Some(cancel);
        super::options::checkpoint(&options.cancel)?;
        if bytes.len() > options.limits.max_input_bytes {
            return Err(super::options::unsupported(
                "input byte limit",
                "XLSX source",
            ));
        }
        Self::open_source_with_options(XlsxSource::from_bytes(bytes), options)
    }
    /// Read from the reader's current position, retaining the resulting bytes.
    /// Seeking is not required and the incoming cursor is never rewound.
    pub fn open_reader_with_limits(
        reader: Box<dyn Read + Send + Sync>,
        limits: XlsxLoadLimits,
    ) -> Result<Self, IoError> {
        let options = limits.read_options()?;
        let bytes = super::source::read_input(reader, &options)?;
        Self::open_source_with_options(XlsxSource::from_bytes(bytes), options)
    }
    fn open_source_with_options(source: XlsxSource, options: ReadOptions) -> Result<Self, IoError> {
        super::options::checkpoint(&options.cancel)?;
        let debug_load = std::env::var_os("FZ_DEBUG_LOAD").is_some();
        let load_clock = timing::Timer::start(debug_load);
        let (sheets, strings, styles, names, calc_settings, has_links, sheet_diagnostics, budget) = {
            let bytes = source.bytes(&options)?;
            let (mut archive, (workbook_xml, relationships, declared, has_links)) =
                package::admit_retaining(&bytes, &options, |archive| {
                    let root = package::relationships_read(archive, "", &options)?;
                    let workbook = root
                        .values()
                        .find(|r| r.kind.ends_with("/officeDocument"))
                        .and_then(|r| r.target.clone())
                        .unwrap_or_else(|| "xl/workbook.xml".into());
                    let relationships = package::relationships_read(archive, &workbook, &options)?;
                    let workbook_xml =
                        package::read_part(archive, &workbook, options.limits.max_worksheet_bytes)?;
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
                                let relation =
                                    relationships.get(id.value.as_ref()).ok_or_else(|| {
                                        super::options::unsupported(
                                            "missing worksheet relationship",
                                            "workbook XML",
                                        )
                                    })?;
                                let kind = if relation.kind == format!("{}/chartsheet", xml::OFFICE)
                                {
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
                                    super::options::unsupported(
                                        "external worksheet",
                                        "workbook XML",
                                    )
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
                    let mut needed: HashSet<_> = declared
                        .iter()
                        .filter(|s| !s.inert)
                        .map(|s| s.part.clone())
                        .collect();
                    for relation in relationships.values() {
                        if (relation.kind.ends_with("/sharedStrings")
                            || relation.kind.ends_with("/styles"))
                            && let Some(target) = &relation.target
                        {
                            needed.insert(target.clone());
                        }
                    }
                    Ok(((workbook_xml, relationships, declared, has_links), needed))
                })?;
            let mut sheets = Vec::new();
            for sheet in declared.iter().filter(|sheet| !sheet.inert) {
                sheets.push(SheetSource {
                    name: sheet.name.clone(),
                    parsed: OnceLock::new(),
                    reader: OnceLock::new(),
                    reader_parsed: OnceLock::new(),
                    data: package::take_part(
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
                        package::take_part(&mut archive, part, options.limits.max_worksheet_bytes)
                    })
                    .transpose()
                    .map(Option::unwrap_or_default)
            };
            let string_bytes = metadata("sharedStrings")?;
            let style_bytes = metadata("styles")?;
            let strings = if string_bytes.is_empty() {
                SharedStringTable::default()
            } else {
                SharedStringTable::parse_direct(&string_bytes, &options)?
            };
            let styles = if style_bytes.is_empty() {
                Styles::default()
            } else {
                Styles::parse_direct(&style_bytes, &options)?
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
                archive.take_consumption_budget(),
            )
        };
        let package_ms = load_clock.elapsed_ms();
        Ok(Self {
            source,
            sheets,
            strings,
            styles,
            options,
            budget,
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
    fn finish_read<T>(&self, result: Result<T, IoError>) -> Result<T, IoError> {
        if result.as_ref().is_err_and(super::options::is_safety) {
            self.budget.fail();
        }
        result
    }
    fn prepare_external(&mut self) -> Result<Arc<crate::CachedExternalLinkValues>, IoError> {
        self.budget.check()?;
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
            let bytes = self.source.bytes(&self.options)?;
            Arc::new(crate::cached_external_links::load(
                Cursor::new(bytes.as_ref()),
                &formulas,
                self.names.raw_calculation_names(),
                &self.options,
                &mut self.budget,
            )?)
        };
        self.names.admit_external_snapshot(&snapshot);
        self.package_ms += package_timer.elapsed_ms();
        Ok(snapshot)
    }
    fn parsed_sheet<'a>(&self, sheet: &'a SheetSource) -> Result<&'a ParsedSheet, IoError> {
        if sheet.parsed.get().is_none() {
            let parsed =
                self.scan_sheet(sheet, &Default::default(), DateSystem::Excel1900, false)?;
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
                IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                    crate::xlsx::types::XlsxError::WorksheetNotFound(name.to_owned()),
                ))
            })
    }
    fn reader_sheet<'a>(&self, sheet: &'a SheetSource) -> Result<&'a SheetData, IoError> {
        if let Some(reader) = sheet.reader.get() {
            return Ok(reader);
        }
        if sheet.reader_parsed.get().is_none() {
            let parsed =
                self.scan_sheet(sheet, &Default::default(), DateSystem::Excel1900, true)?;
            let _ = sheet.reader_parsed.set(parsed);
        }
        let parsed = sheet
            .reader_parsed
            .get()
            .expect("initialized reader values");
        if let Some(error) = &parsed.value_error {
            return Err(error.to_error());
        }
        let mut cells = BTreeMap::new();
        {
            let arrow = &parsed
                .reader_values
                .as_ref()
                .expect("reader values requested")
                .arrow;
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
        retain_reader_values: bool,
    ) -> Result<ParsedSheet, IoError> {
        self.scan_sheet_produced(sheet, limits, date_system, retain_reader_values, None)
    }
    fn scan_sheet_produced<'a>(
        &'a self,
        sheet: &'a SheetSource,
        limits: &'a formualizer_eval::engine::WorkbookLoadLimits,
        date_system: DateSystem,
        retain_reader_values: bool,
        emit: Option<&'a mut dyn FnMut(OwnedSpoolFormulaRecord) -> Result<(), IoError>>,
    ) -> Result<ParsedSheet, IoError> {
        let scan_timer = timing::Timer::start(self.debug_load);
        let mut xml = cells::CellXml::new(&sheet.data, &self.options);
        let dimensions = cells::header(&mut xml)?;
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
        let reader_values = if retain_reader_values {
            Some(ValueSink::new(Context {
                name: &sheet.name,
                source: &sheet.data,
                strings: &self.strings,
                styles: &self.styles,
                dimensions,
                chunk_rows: 32 * 1024,
                date_system,
                limits,
            })?)
        } else {
            None
        };
        let mut sink = LoadSink {
            strings: &self.strings,
            styles: &self.styles,
            values,
            reader_values,
            records: Vec::new(),
            record_count: 0,
            emit,
            value_error: None,
            formula_error: None,
            stream_error: None,
            last_formula_coord: None,
            skipped_values: 0,
        };
        let bounds = (0, 0);
        if let Err(error) = cells::scan(&mut xml, &self.options, &mut sink) {
            let error = DeferredReadError::capture(error)?;
            sink.stream_error.get_or_insert_with(|| error.clone());
            sink.value_error.get_or_insert(error);
        }
        let scan_ms = scan_timer.elapsed_ms();
        let sinks_timer = timing::Timer::start(self.debug_load);
        let mut values = sink.values.finish(bounds)?;
        values.values_observed += sink.skipped_values;
        let reader_values = sink
            .reader_values
            .map(|values| values.finish(bounds))
            .transpose()?;
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
    Xml(quick_xml_cells::Error),
    XmlEof(&'static str),
    UnexpectedNode(&'static str),
    ParseFloat(std::num::ParseFloatError),
    CellError(String),
    NoRow,
    NoColumn,
    DimensionCount(usize),
    NotAWorksheet(String),
    Unsupported { feature: String, context: String },
    Backend { backend: String, message: String },
}
impl DeferredReadError {
    fn capture(error: IoError) -> Result<Self, IoError> {
        match error {
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::Unexpected(message),
            )) => Ok(Self::Unexpected(message)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::CellTAttribute(kind),
            )) => Ok(Self::CellType(kind)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::Alphanumeric(byte),
            )) => Ok(Self::Alphanumeric(byte)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::Xml(e),
            )) => Ok(Self::Xml(e)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::XmlEof(e),
            )) => Ok(Self::XmlEof(e)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::UnexpectedNode(e),
            )) => Ok(Self::UnexpectedNode(e)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::ParseFloat(e),
            )) => Ok(Self::ParseFloat(e)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::CellError(e),
            )) => Ok(Self::CellError(e)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::RangeWithoutRowComponent,
            )) => Ok(Self::NoRow),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::RangeWithoutColumnComponent,
            )) => Ok(Self::NoColumn),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::DimensionCount(e),
            )) => Ok(Self::DimensionCount(e)),
            IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::NotAWorksheet(e),
            )) => Ok(Self::NotAWorksheet(e)),
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
            Self::Unexpected(message) => crate::xlsx::types::XlsxError::Unexpected(message),
            Self::CellType(kind) => crate::xlsx::types::XlsxError::CellTAttribute(kind.clone()),
            Self::Alphanumeric(byte) => crate::xlsx::types::XlsxError::Alphanumeric(*byte),
            Self::Xml(e) => crate::xlsx::types::XlsxError::Xml(e.clone()),
            Self::XmlEof(e) => crate::xlsx::types::XlsxError::XmlEof(e),
            Self::UnexpectedNode(e) => crate::xlsx::types::XlsxError::UnexpectedNode(e),
            Self::ParseFloat(e) => crate::xlsx::types::XlsxError::ParseFloat(e.clone()),
            Self::CellError(e) => crate::xlsx::types::XlsxError::CellError(e.clone()),
            Self::NoRow => crate::xlsx::types::XlsxError::RangeWithoutRowComponent,
            Self::NoColumn => crate::xlsx::types::XlsxError::RangeWithoutColumnComponent,
            Self::DimensionCount(e) => crate::xlsx::types::XlsxError::DimensionCount(*e),
            Self::NotAWorksheet(e) => crate::xlsx::types::XlsxError::NotAWorksheet(e.clone()),
            Self::Unsupported { feature, context } => {
                return IoError::Unsupported {
                    feature: feature.clone(),
                    context: context.clone(),
                };
            }
        };
        IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(error))
    }
}

struct ParsedSheet {
    scan_ms: f64,
    sinks_ms: f64,
    values: super::ingest::values::IngestedValues,
    records: Vec<OwnedSpoolFormulaRecord>,
    reader_values: Option<super::ingest::values::IngestedValues>,
    value_error: Option<DeferredReadError>,
    formula_error: Option<DeferredReadError>,
    stream_error: Option<DeferredReadError>,
}
struct LoadSink<'a> {
    strings: &'a SharedStringTable,
    styles: &'a Styles,
    values: ValueSink<'a>,
    records: Vec<OwnedSpoolFormulaRecord>,
    record_count: usize,
    emit: Option<&'a mut dyn FnMut(OwnedSpoolFormulaRecord) -> Result<(), IoError>>,
    reader_values: Option<ValueSink<'a>>,
    last_formula_coord: Option<(u32, u32)>,
    skipped_values: usize,
    value_error: Option<DeferredReadError>,
    formula_error: Option<DeferredReadError>,
    stream_error: Option<DeferredReadError>,
}
impl LoadSink<'_> {
    fn emit_cell(
        &mut self,
        row: u32,
        col: u32,
        value: worksheet::events::Value<'_>,
        format: Option<formualizer_eval::format::FormatId>,
        formula: Option<OwnedSpoolFormulaRecord>,
    ) -> Result<(), IoError> {
        if let Some(record) = formula {
            if self.value_error.is_none() {
                let result = if let Some(reader) = &mut self.reader_values {
                    reader.push_decoded(row, col, value, format)
                } else if let worksheet::events::Value::Shared(index) = value {
                    self.strings.get(index).map(|_| ())
                } else {
                    Ok(())
                };
                self.retain_value_error(result)?;
            }
            self.values.observe_formula(row, col)?;
            self.last_formula_coord = Some((row, col));
            self.record_count += 1;
            if let Some(emit) = &mut self.emit {
                // A later staging failure must not replace an earlier source
                // refusal. In particular validate this cell's cache first.
                if self.stream_error.is_none() {
                    emit(record)?;
                }
            } else {
                self.records.push(record);
            }
        } else if self.value_error.is_none() {
            let result = if let Some(reader) = &mut self.reader_values {
                reader.push_decoded(row, col, value, format)
            } else if self.last_formula_coord == Some((row, col)) {
                self.skipped_values +=
                    usize::from(!matches!(value, worksheet::events::Value::Empty));
                if let worksheet::events::Value::Shared(index) = value {
                    self.strings.get(index).map(|_| ())
                } else {
                    Ok(())
                }
            } else {
                self.values.push_decoded(row, col, value, format)
            };
            self.retain_value_error(result)?;
        }
        Ok(())
    }

    fn retain_value_error(&mut self, result: Result<(), IoError>) -> Result<(), IoError> {
        if let Err(error) = result {
            let error = DeferredReadError::capture(error)?;
            self.stream_error.get_or_insert_with(|| error.clone());
            self.value_error = Some(error);
            // Streaming has no second reader outcome to collect. Stop on its
            // first source refusal before a later resource/staging failure.
            if self.emit.is_some() {
                return Err(self
                    .stream_error
                    .as_ref()
                    .expect("captured error")
                    .to_error());
            }
        }
        Ok(())
    }
}
impl SpreadsheetReader for NativeXlsxAdapter {
    type Error = IoError;
    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, IoError> {
        Self::open_path_with_source(path, XlsxPathSource::SharedFile)
    }
    fn open_bytes(data: Vec<u8>) -> Result<Self, IoError> {
        Self::open_bytes_with_limits(data, XlsxLoadLimits::default())
    }
    fn open_reader(reader: Box<dyn Read + Send + Sync>) -> Result<Self, IoError> {
        Self::open_reader_with_limits(reader, XlsxLoadLimits::default())
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
        if self.budget.check().is_err() {
            return None;
        }
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
                self.budget.fail();
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
        self.budget.check()?;
        let sheet = self.source_sheet(name)?;
        let result = self.reader_sheet(sheet).cloned();
        let data = self.finish_read(result)?;
        self.loaded.insert(name.to_owned());
        Ok(data)
    }
    fn read_cell(&mut self, name: &str, row: u32, col: u32) -> Result<Option<CellData>, IoError> {
        self.budget.check()?;
        let result = self
            .reader_sheet(self.source_sheet(name)?)
            .map(|data| data.cells.get(&(row, col)).cloned());
        let data = self.finish_read(result)?;
        self.loaded.insert(name.to_owned());
        Ok(data)
    }
    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, IoError> {
        self.budget.check()?;
        let result = self.reader_sheet(self.source_sheet(sheet)?).map(|data| {
            data.cells
                .iter()
                .filter(|((r, c), _)| *r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1)
                .map(|(coord, cell)| (*coord, cell.clone()))
                .collect()
        });
        let cells = self.finish_read(result)?;
        self.loaded.insert(sheet.to_owned());
        Ok(cells)
    }
    fn sheet_bounds(&self, name: &str) -> Option<(u32, u32)> {
        self.budget.check().ok()?;
        let sheet = self.source_sheet(name).ok()?;
        self.finish_read(self.reader_sheet(sheet)).ok()?.dimensions
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
        self.budget.check()?;
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
            for index in 0..self.sheets.len() {
                let cached = self.sheets[index].parsed.take();
                let sheet = &self.sheets[index];
                let refused: HashSet<_> = self
                    .external
                    .iter()
                    .flat_map(|snapshot| snapshot.refused_formulas.keys())
                    .filter(|(name, _, _)| name == &sheet.name)
                    .map(|(_, row, col)| (row - 1, col - 1))
                    .collect();
                let limits = engine.workbook_load_limits().clone();
                let mut parsed_result = None;
                let formula_timer = timing::Timer::start(self.debug_load);
                let staged = formulas::stage_records_produced::<R, true>(
                    engine,
                    &sheet.name,
                    index as u32,
                    |emit| {
                        let mut parsed = if let Some(mut parsed) = cached {
                            if let Some(error) = &parsed.stream_error {
                                return Err(error.to_error());
                            }
                            for record in std::mem::take(&mut parsed.records) {
                                emit(record)?;
                            }
                            parsed
                        } else {
                            self.scan_sheet_produced(
                                sheet,
                                &limits,
                                DateSystem::Excel1900,
                                false,
                                Some(emit),
                            )?
                        };
                        if let Some(error) = &parsed.stream_error {
                            return Err(error.to_error());
                        }
                        // Record storage is consumed by staging, not retained as
                        // an unbounded parallel copy of the bounded replay spool.
                        parsed.records.clear();
                        parsed_result = Some(parsed);
                        Ok(())
                    },
                    usage,
                    &self.options.cancel,
                    &refused,
                )?;
                let parsed = parsed_result.expect("producer completed");
                let combined_ms = formula_timer.elapsed_ms();
                phases.formulas_ms += (combined_ms - parsed.scan_ms - parsed.sinks_ms).max(0.0);
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
                    .saturating_add(staged.formulas_observed as u64);
                if populated > engine.workbook_load_limits().max_sheet_logical_cells {
                    return Err(IoError::XlsxRead(super::ingest::staging::engine_error(
                        format!(
                            "Workbook load budget exceeded in calamine for sheet {}: observed populated cell count exceeds configured logical-cell budget of {}",
                            sheet.name,
                            engine.workbook_load_limits().max_sheet_logical_cells
                        ),
                    )));
                }
                // The source sink stores raw numeric serials, not converted chrono
                // values. Select the caller's epoch before these lanes are installed.
                let mut arrow = parsed.values.arrow;
                arrow.date_system = engine.config.date_system;

                values_observed += parsed.values.values_observed as u64;
                values_handed += parsed.values.values_handed_to_engine as u64;
                let install_timer = timing::Timer::start(self.debug_load);
                install_values(engine, &sheet.name, arrow);
                phases.install_ms += install_timer.elapsed_ms();
                let formula_timer = timing::Timer::start(self.debug_load);
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
            if !engine.config.defer_graph_building && !batches.replay.is_empty() {
                batches.install_replay(engine).map_err(IoError::from)?;
            }
            if !batches.direct.is_empty() {
                batches.install_direct(engine).map_err(IoError::from)?;
            }
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
                    return Err(IoError::XlsxRead(super::ingest::staging::engine_error(
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
        self.finish_read(result)
    }
}
