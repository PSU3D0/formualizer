//! Compatible XLSX loading through the shared bounded source reader.
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
    io::Read,
    path::Path,
};

struct SheetSource {
    name: String,
    data: Vec<u8>,
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
        let options = ReadOptions::default();
        let (sheets, strings, styles, names, calc_settings) = {
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
            let mut declared = Vec::new();
            read_source(&workbook_xml, &options, |path, node| {
                if xml::path_is(path, xml::MAIN, &["workbook", "sheets", "sheet"])
                    && matches!(node.kind, xml::Kind::Open { .. })
                    && let (Some(name), Some(id)) =
                        (node.value("name"), node.attribute(xml::OFFICE, "id"))
                    && let Some(relation) = relationships.get(id.value.as_ref())
                    && relation.kind.ends_with("/worksheet")
                    && let Some(target) = &relation.target
                {
                    declared.push((name.to_owned(), target.clone()));
                }
                Ok(())
            })?;
            let mut sheets = Vec::new();
            for (name, part) in declared {
                sheets.push(SheetSource {
                    name,
                    data: package::read_part(
                        &mut archive,
                        &part,
                        options.limits.max_worksheet_bytes,
                    )?,
                });
            }
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
            let scopes: Vec<_> = sheets
                .iter()
                .map(|sheet| package::Sheet {
                    name: sheet.name.clone(),
                    part: String::new(),
                    inert: false,
                    kind: "worksheet",
                    sheet_id: 0,
                    tables: BTreeMap::new(),
                })
                .collect();
            let names = super::ingest::names::CalculationNames::new(
                &workbook_xml,
                &scopes,
                options.cancel.clone(),
                super::ingest::names::NameImportPolicy::COMPATIBLE_REFERENCED,
            );
            let calc_settings = crate::calc_pr::parse_calc_pr(&workbook_xml);
            (sheets, strings, styles, names, calc_settings)
        };
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
        })
    }
    fn scan_sheet(
        &self,
        sheet: &SheetSource,
        limits: &formualizer_eval::engine::WorkbookLoadLimits,
        date_system: DateSystem,
        reader_coordinates: bool,
    ) -> Result<ParsedSheet, IoError> {
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
        let mut sink = LoadSink {
            values,
            records: Vec::new(),
            coordinates: reader_coordinates.then(BTreeSet::new),
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
        )?;
        let Scanned::Done(scan) = scanned else {
            return Err(IoError::from_backend(
                "xlsx",
                std::io::Error::other("unexpected compatible index request"),
            ));
        };
        Ok(ParsedSheet {
            values: sink.values.finish(scan.bounds)?,
            records: sink.records,
            coordinates: sink.coordinates.unwrap_or_default(),
        })
    }
}
struct ParsedSheet {
    values: super::ingest::values::IngestedValues,
    records: Vec<OwnedSpoolFormulaRecord>,
    coordinates: BTreeSet<(u32, u32)>,
}
struct LoadSink<'a> {
    values: ValueSink<'a>,
    records: Vec<OwnedSpoolFormulaRecord>,
    coordinates: Option<BTreeSet<(u32, u32)>>,
}
impl Sink for LoadSink<'_> {
    fn literal(&mut self, literal: Literal<'_>) -> Result<(), IoError> {
        if let Some(coordinates) = &mut self.coordinates {
            coordinates.insert((literal.row, literal.col));
        }
        self.values.push(literal)
    }
    fn formula(&mut self, formula: Formula<'_>) -> Result<(), IoError> {
        if let Some(coordinates) = &mut self.coordinates {
            coordinates.insert((formula.row, formula.col));
        }
        self.values.observe_formula(formula.row, formula.col)?;
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
                    calamine::XlsxError::Io(std::io::Error::other("missing shared formula index")),
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
    fn calc_settings(&self) -> Option<crate::traits::CalcSettings> {
        self.calc_settings.clone()
    }
    fn defined_names(&mut self) -> Result<Vec<crate::traits::DefinedName>, IoError> {
        self.names.defined_names().map_err(IoError::from)
    }
    fn name_import_diagnostics(&self) -> Vec<crate::traits::NameImportDiagnostic> {
        self.name_diagnostics.clone()
    }
    fn load_stats(&self) -> Option<AdapterLoadStats> {
        Some(self.stats.clone())
    }
    fn read_sheet(&mut self, name: &str) -> Result<SheetData, IoError> {
        let sheet = self.sheets.iter().find(|s| s.name == name).ok_or_else(|| {
            IoError::from_backend(
                "xlsx",
                std::io::Error::other(format!("sheet not found: {name}")),
            )
        })?;
        let parsed = self.scan_sheet(sheet, &Default::default(), DateSystem::Excel1900, true)?;
        let dimensions = (
            parsed.values.dimensions.0 as u32,
            parsed.values.dimensions.1 as u32,
        );
        let mut cells = BTreeMap::new();
        for (row, col) in parsed.coordinates {
            let v = parsed
                .values
                .arrow
                .get_cell_value((row - 1) as usize, (col - 1) as usize);
            if v != LiteralValue::Empty {
                cells.insert((row, col), CellData::from_value(v));
            }
        }
        let mut replay = FormulaReplayState::default();
        for record in parsed.records {
            replay
                .emit_record(
                    record,
                    name,
                    |_, _| true,
                    |_, coord, text, _| {
                        if !text.is_empty() {
                            cells
                                .entry((coord.row + 1, coord.col + 1))
                                .or_insert_with(|| CellData {
                                    value: None,
                                    formula: None,
                                    style: None,
                                })
                                .formula = Some(format!("={}", text.trim_start_matches('=')));
                        }
                        Ok(())
                    },
                )
                .map_err(IoError::from)?;
        }
        self.loaded.insert(name.to_owned());
        Ok(SheetData {
            cells,
            dimensions: Some(dimensions),
            tables: vec![],
            named_ranges: vec![],
            date_system_1904: false,
            merged_cells: vec![],
            hidden: false,
            row_hidden_manual: vec![],
            row_hidden_filter: vec![],
        })
    }
    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, IoError> {
        Ok(self
            .read_sheet(sheet)?
            .cells
            .into_iter()
            .filter(|((r, c), _)| *r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1)
            .collect())
    }
    fn sheet_bounds(&self, name: &str) -> Option<(u32, u32)> {
        let sheet = self.sheets.iter().find(|s| s.name == name)?;
        let parsed = self
            .scan_sheet(sheet, &Default::default(), DateSystem::Excel1900, false)
            .ok()?;
        Some((
            parsed.values.dimensions.0 as u32,
            parsed.values.dimensions.1 as u32,
        ))
    }
    fn is_loaded(&self, sheet: &str, _row: Option<u32>, _col: Option<u32>) -> bool {
        self.loaded.contains(sheet)
    }
}
impl<R: EvaluationContext> EngineLoadStream<R> for NativeXlsxAdapter {
    type Error = IoError;
    fn stream_into_engine(&mut self, engine: &mut Engine<R>) -> Result<(), IoError> {
        // Keep the opened source alive, including the explicit mapping contract.
        let _source = &self.source;
        let names = self.sheet_names()?;
        engine.adopt_file_sheets(names.iter().map(String::as_str))?;
        let previous = LoadConfig::begin(engine, false, false);
        let result = (|| {
            let mut batches = FormulaBatches::default();
            let mut usage = WorkbookSpoolUsage::default();
            let mut values_observed = 0;
            let mut values_handed = 0;
            let mut formulas_observed = 0;
            let mut formulas_handed = 0;
            let mut shared = 0;
            for (index, sheet) in self.sheets.iter().enumerate() {
                let parsed = self.scan_sheet(
                    sheet,
                    engine.workbook_load_limits(),
                    engine.config.date_system,
                    false,
                )?;
                values_observed += parsed.values.values_observed as u64;
                values_handed += parsed.values.values_handed_to_engine as u64;
                install_values(engine, &sheet.name, parsed.values.arrow);
                let staged = formulas::stage_records(
                    engine,
                    &sheet.name,
                    index as u32,
                    parsed.records.into_iter().map(Ok),
                    usage,
                    &self.options.cancel,
                )?;
                usage.bytes += staged.spool_bytes;
                usage.files += u32::from(staged.spool_spilled);
                formulas_observed += staged.formulas_observed as u64;
                formulas_handed += if engine.config.defer_graph_building {
                    staged.formulas_observed as u64
                } else {
                    staged.records.len() as u64
                };
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
                self.loaded.insert(sheet.name.clone());
            }
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
                engine.define_name(
                    &diagnostic.name,
                    NamedDefinition::Literal(LiteralValue::Error(error)),
                    scope,
                )?;
            }
            self.stats = AdapterLoadStats {
                formula_cells_observed: Some(formulas_observed),
                value_cells_observed: Some(values_observed),
                value_slots_handed_to_engine: Some(values_handed),
                formula_cells_handed_to_engine: Some(formulas_handed),
                shared_formula_tags_observed: Some(shared),
            };
            Ok(())
        })();
        previous.restore(engine);
        if result.is_ok() {
            engine.finalize_all_sheet_indexes(names.iter().map(String::as_str));
        }
        result
    }
}
