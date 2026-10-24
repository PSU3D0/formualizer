//! Fused source admission/value handoff. Reader-shaped value errors are
//! deferred until source admission/ownership have retained their old priority.
use super::{IoError, XlsxRecalculateOptions, metadata, package, sheet, values};
use formualizer_common::DateSystem;
use formualizer_eval::engine::WorkbookLoadLimits;

pub(super) fn load_limits(options: &XlsxRecalculateOptions) -> WorkbookLoadLimits {
    let mut limits = WorkbookLoadLimits::default();
    limits.max_sheet_cols = limits.max_sheet_cols.min(options.limits.max_columns);
    limits.max_sheet_logical_cells = limits
        .max_sheet_logical_cells
        .min(options.limits.max_cells as u64);
    limits.max_formula_spool_bytes_per_sheet = limits
        .max_formula_spool_bytes_per_sheet
        .min(options.limits.max_expanded_bytes as u64);
    limits.max_formula_spool_bytes_per_workbook = limits
        .max_formula_spool_bytes_per_workbook
        .min(options.limits.max_expanded_bytes as u64);
    limits
}
pub(super) struct Metadata {
    strings: metadata::SharedStringTable,
    styles: metadata::Styles,
}
impl Metadata {
    pub(super) fn read(
        archive: &mut package::Archive<'_>,
        options: &XlsxRecalculateOptions,
    ) -> Result<Self, IoError> {
        let strings = if archive
            .file_names()
            .any(|name| name == "xl/sharedStrings.xml")
        {
            metadata::SharedStringTable::parse(
                &package::take_part(
                    archive,
                    "xl/sharedStrings.xml",
                    options.limits.max_worksheet_bytes,
                )?,
                options,
            )?
        } else {
            Default::default()
        };
        let styles = if archive.file_names().any(|name| name == "xl/styles.xml") {
            metadata::Styles::parse(
                &package::take_part(archive, "xl/styles.xml", options.limits.max_worksheet_bytes)?,
                options,
            )?
        } else {
            Default::default()
        };
        if std::env::var("FZ_DEBUG_LOAD")
            .ok()
            .is_some_and(|value| value != "0")
        {
            eprintln!(
                "[fz][native] SST entries={} styles={}",
                strings.len(),
                styles.len()
            );
        }
        Ok(Self { strings, styles })
    }
}
pub(super) struct DeferredValueError {
    pub position: usize,
    pub error: IoError,
}
pub(super) type ValueResult = Result<values::IngestedValues, DeferredValueError>;
pub(super) enum ScannedValues {
    NeedsIndex,
    Done {
        scan: Box<sheet::Scan>,
        values: ValueResult,
    },
}
fn reader_error(byte: u8) -> IoError {
    IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
        crate::xlsx::types::XlsxError::Alphanumeric(byte),
    ))
}
fn coordinate(raw: &[u8], dimension: bool) -> Result<(), IoError> {
    match raw
        .iter()
        .find(|byte| !byte.is_ascii_alphanumeric() && !(dimension && **byte == b':'))
    {
        Some(byte) => Err(reader_error(*byte)),
        None => Ok(()),
    }
}
fn record(first: &mut Option<(usize, IoError)>, offset: usize, result: Result<(), IoError>) {
    if let Err(error) = result
        && first.as_ref().is_none_or(|(old, _)| offset < *old)
    {
        *first = Some((offset, error));
    }
}
pub(super) fn scan(
    data: &[u8],
    name: &str,
    date_system: DateSystem,
    metadata: Option<&Metadata>,
    options: &XlsxRecalculateOptions,
    mode: sheet::ScanStrategy,
    counts: (&mut usize, &mut u64),
) -> Result<ScannedValues, IoError> {
    let (observed, logical_cells) = counts;
    let limits = load_limits(options);
    let mut first = None;
    let mut sink = metadata.and_then(|metadata| {
        match values::ValueSink::new(values::Context {
            name,
            source: data,
            strings: &metadata.strings,
            styles: &metadata.styles,
            dimensions: values::dimension_hint(data, options),
            chunk_rows: 32 * 1024,
            date_system,
            limits: &limits,
        }) {
            Ok(sink) => Some(sink),
            Err(error) => {
                first = Some((0, error));
                None
            }
        }
    });
    let scanned = sheet::scan_emitting(data, options, mode, observed, logical_cells, |literal| {
        debug_assert_eq!(
            literal.style.is_some(),
            literal.raw_style_span.is_some(),
            "decoded/raw style provenance must agree"
        );
        if first.is_some() {
            return Ok(());
        }
        let offset = literal
            .raw_coordinate_span
            .as_ref()
            .map_or(0, |span| span.start);
        if let Some(span) = &literal.raw_coordinate_span {
            record(&mut first, offset, coordinate(&data[span.clone()], false));
        }
        if first.is_some() || literal.generated_child_hint {
            return Ok(());
        }
        if let Some(sink) = &mut sink {
            record(&mut first, offset, sink.push(literal));
        }
        Ok(())
    })?;
    let sheet::Scanned::Done(scan) = scanned else {
        return Ok(ScannedValues::NeedsIndex);
    };
    if let Some(span) = &scan.raw_dimension_span {
        record(
            &mut first,
            span.start,
            coordinate(&data[span.clone()], true),
        );
    }
    if let Some(span) = &scan.raw_row_refusal_span
        && let Some(byte) = data[span.clone()]
            .iter()
            .find(|byte| !byte.is_ascii_digit())
    {
        record(&mut first, span.start, Err(reader_error(*byte)));
    }
    for cell in &scan.cells {
        if first
            .as_ref()
            .is_some_and(|(offset, _)| cell.span.start >= *offset)
        {
            break;
        }
        if first.is_none()
            && let Some(sink) = &mut sink
        {
            record(
                &mut first,
                cell.span.end,
                sink.observe_formula(cell.row, cell.col),
            );
        }
    }
    let values = if let Some((position, error)) = first {
        Err(DeferredValueError { position, error })
    } else if let Some(sink) = sink {
        sink.finish(scan.bounds)
            .map_err(|error| DeferredValueError {
                position: usize::MAX,
                error,
            })
    } else {
        Err(DeferredValueError {
            position: 0,
            error: IoError::Backend {
                backend: "native-values".into(),
                message: "global metadata unavailable".into(),
            },
        })
    };
    Ok(ScannedValues::Done {
        scan: Box::new(scan),
        values,
    })
}
