//! Shared command implementation. Arguments include the program name.
//! No signal handlers, TTY assumptions or process exits occur here.
use clap::{Parser, Subcommand};
pub use formualizer_workbook::CancelToken;
use formualizer_workbook::{
    IoError, XlsxRecalculateOptions, XlsxRecalculateResult, recalculate_xlsx_bytes,
    recalculate_xlsx_file,
};
use serde::Serialize;
use std::{
    ffi::OsString,
    io::{Read, Write},
    path::PathBuf,
};

const SCHEMA: &str = "formualizer.recalc/1";
const DEFAULT_MAX_ERRORS: usize = 20;

#[derive(Parser)]
#[command(
    name = "formualizer",
    version,
    about = "Recalculate XLSX formula caches without reconstructing the workbook"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Recalculate formula caches using the strict source-preserving path.
    Recalc {
        input: PathBuf,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(long)]
        check: bool,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = DEFAULT_MAX_ERRORS)]
        max_errors: usize,
    },
}
#[derive(Serialize)]
struct ErrorCell {
    sheet: String,
    cell: String,
    error: String,
}
#[derive(Serialize)]
struct Refusal {
    feature: String,
    context: String,
}
#[derive(Serialize)]
struct Report {
    schema: &'static str,
    status: &'static str,
    input: Option<String>,
    output: Option<String>,
    written: bool,
    formula_cells: Option<usize>,
    cache_cells_changed: Option<usize>,
    worksheet_parts_changed: Option<usize>,
    evaluated: Option<usize>,
    error_cells: Option<usize>,
    errors: Option<Vec<ErrorCell>>,
    errors_truncated: Option<bool>,
    refusal: Option<Refusal>,
    message: String,
}
impl Report {
    fn new(status: &'static str, message: String) -> Self {
        Self {
            schema: SCHEMA,
            status,
            input: None,
            output: None,
            written: false,
            formula_cells: None,
            cache_cells_changed: None,
            worksheet_parts_changed: None,
            evaluated: None,
            error_cells: None,
            errors: None,
            errors_truncated: None,
            refusal: None,
            message,
        }
    }
    fn result(&mut self, result: &XlsxRecalculateResult, limit: usize) {
        self.formula_cells = Some(result.formula_cells);
        self.cache_cells_changed = Some(result.cache_cells_changed);
        self.worksheet_parts_changed = Some(result.worksheet_parts_changed);
        self.evaluated = Some(result.summary.evaluated);
        self.error_cells = Some(result.summary.errors);
        let errors: Vec<_> = result
            .summary
            .error_summary
            .iter()
            .flat_map(|(error, summary)| {
                summary.locations.iter().filter_map(move |location| {
                    // Sheet names themselves may contain '!'. The final separator is the cell.
                    let (sheet, cell) = location.rsplit_once('!')?;
                    Some(ErrorCell {
                        sheet: sheet.into(),
                        cell: cell.into(),
                        error: error.clone(),
                    })
                })
            })
            .take(limit)
            .collect();
        self.errors_truncated = Some(errors.len() < result.summary.errors);
        self.errors = Some(errors);
    }
}

fn emit(
    report: &Report,
    json: bool,
    code: i32,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> i32 {
    let result = if json {
        serde_json::to_writer(&mut *stdout, report)
            .map_err(std::io::Error::other)
            .and_then(|_| writeln!(stdout))
    } else if code == 0 || code == 3 {
        writeln!(stdout, "{}", report.message)
    } else {
        writeln!(stderr, "{}", report.message)
    };
    if result.is_err() { 1 } else { code }
}

// All structured strict-path refusals remain refusals. A structured category in
// the library would allow malformed packages and limits to be refined later.
fn classify(error: &IoError) -> (&'static str, i32) {
    match error {
        IoError::Engine(e) if e.kind == formualizer_common::ExcelErrorKind::Cancelled => {
            ("interrupted", 130)
        }
        IoError::Unsupported { .. } => ("refused", 2),
        _ => ("error", 1),
    }
}

/// Runs the command and returns its exit code. `args` includes `argv[0]`.
/// Bindings should disable default features, pass `None` or their own shared
/// cancellation token, and capture stdout/stderr independently. No process exit
/// or signal-handler installation occurs in this function.
pub fn run<I, T>(
    args: I,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    cancel: Option<CancelToken>,
) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    // Even clap failures must have machine-readable output when requested.
    let json_requested = args
        .iter()
        .take_while(|arg| *arg != "--")
        .any(|arg| arg == "--json");
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => {
            use clap::error::ErrorKind;
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) {
                return if write!(stdout, "{error}").is_ok() {
                    0
                } else {
                    1
                };
            }
            if !json_requested {
                return if write!(stderr, "{error}").is_ok() {
                    64
                } else {
                    1
                };
            }
            let message = error.to_string().replace('\n', " ").trim().to_owned();
            return emit(
                &Report::new("error", message),
                json_requested,
                64,
                stdout,
                stderr,
            );
        }
    };
    let Command::Recalc {
        input,
        output,
        check,
        json,
        max_errors,
    } = cli.command;
    let options = XlsxRecalculateOptions {
        cancel: cancel.clone(),
        error_location_limit: max_errors,
        ..Default::default()
    };
    let result = (|| -> Result<XlsxRecalculateResult, IoError> {
        if cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
            return Err(IoError::Engine(formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Cancelled,
            )));
        }
        let mut file = std::fs::File::open(&input)?;
        let mut prefix = [0; 4];
        if file.read_exact(&mut prefix).is_err() || prefix != *b"PK\x03\x04" {
            return Err(IoError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "not an xlsx (ZIP) file",
            )));
        }
        if check {
            let mut source = prefix.to_vec();
            file.take(
                (options.limits.max_input_bytes as u64)
                    .saturating_add(1)
                    .saturating_sub(4),
            )
            .read_to_end(&mut source)?;
            recalculate_xlsx_bytes(&source, options)
        } else {
            recalculate_xlsx_file(&input, output.as_deref(), options)
        }
    })();
    let mut report = Report::new("error", String::new());
    report.input = Some(input.to_string_lossy().into_owned());
    report.output = if check {
        None
    } else {
        Some(
            output
                .as_deref()
                .unwrap_or(&input)
                .to_string_lossy()
                .into_owned(),
        )
    };
    let code = match result {
        Ok(result) => {
            // Metadata publication only accompanies worksheet changes. Cache
            // counts alone would miss attribute-only dynamic-array changes.
            let changed = result.worksheet_parts_changed != 0;
            let code = if check && changed { 3 } else { 0 };
            report.written = !check && (output.is_some() || changed);
            report.status = if check {
                if changed { "stale" } else { "current" }
            } else if report.written {
                "written"
            } else {
                "unchanged"
            };
            report.result(&result, max_errors);
            let locations = report
                .errors
                .as_ref()
                .unwrap()
                .iter()
                .map(|e| format!("{}!{} {}", e.sheet, e.cell, e.error))
                .collect::<Vec<_>>()
                .join(", ");
            report.message = format!(
                "{}: recalculated {} formulas, {} cached values changed, {} error cells{} ({})",
                input.display(),
                result.formula_cells,
                result.cache_cells_changed,
                result.summary.errors,
                if locations.is_empty() {
                    String::new()
                } else {
                    format!(" ({locations})")
                },
                report.status
            );
            code
        }
        Err(error) => {
            let (status, code) = classify(&error);
            report.status = status;
            report.message = format!("{}: {error}. Nothing was written.", input.display());
            if let IoError::Unsupported { feature, context } = error {
                report.refusal = Some(Refusal { feature, context });
            }
            code
        }
    };
    emit(&report, json, code, stdout, stderr)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn structured_error_classification() {
        use formualizer_common::{ExcelError, ExcelErrorKind};
        for (error, status, code) in [
            (
                IoError::Unsupported {
                    feature: "table metadata".into(),
                    context: "Sheet1".into(),
                },
                "refused",
                2,
            ),
            (
                IoError::Unsupported {
                    feature: "ambiguous/missing ZIP footer".into(),
                    context: "XLSX package".into(),
                },
                "refused",
                2,
            ),
            (
                IoError::Unsupported {
                    feature: "input size limit".into(),
                    context: "XLSX package".into(),
                },
                "refused",
                2,
            ),
            (
                IoError::Engine(ExcelError::new(ExcelErrorKind::Cancelled)),
                "interrupted",
                130,
            ),
            (
                IoError::Engine(ExcelError::new(ExcelErrorKind::Value)),
                "error",
                1,
            ),
            (IoError::Io(std::io::Error::other("I/O")), "error", 1),
            (
                IoError::Backend {
                    backend: "zip".into(),
                    message: "invalid".into(),
                },
                "error",
                1,
            ),
        ] {
            assert_eq!(classify(&error), (status, code));
        }
    }
}
