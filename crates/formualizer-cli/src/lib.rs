//! Shared command implementation. Arguments include the program name.
//! No signal handlers, TTY assumptions or process exits occur here.
use chrono::{DateTime, FixedOffset, Local, SecondsFormat, Utc};
use clap::{Parser, Subcommand};
use formualizer_eval::{engine::DeterministicMode, timezone::TimeZoneSpec};
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
const RECALC_AFTER_HELP: &str = "\
Writes INPUT in place unless -o or --check is given.
Run recalc as the last step that writes the workbook: later edits leave its
caches stale.

RAND/RANDBETWEEN are reproducible run to run. Without --now, TODAY/NOW use
the host clock (local time unless --tz).

Exit codes:
  0    written, unchanged or current
  1    error; nothing written
  2    refused (unsupported input); nothing written
  3    --check: caches are stale
  64   usage error or invalid option value
  130  interrupted; nothing written";

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
    #[command(after_help = RECALC_AFTER_HELP)]
    Recalc {
        /// Workbook (.xlsx) to recalculate
        input: PathBuf,
        /// Write the result to PATH instead of replacing INPUT
        #[arg(short, long, value_name = "PATH")]
        output: Option<PathBuf>,
        /// Compute without writing; exit 3 if any cache is stale
        #[arg(long)]
        check: bool,
        /// Print one formualizer.recalc/1 JSON object on stdout
        #[arg(long)]
        json: bool,
        /// List at most N error-cell locations
        #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_ERRORS)]
        max_errors: usize,
        /// Fixed TODAY/NOW instant: RFC 3339 with an offset or Z
        #[arg(long, value_name = "TIMESTAMP", value_parser = parse_now)]
        now: Option<DateTime<FixedOffset>>,
        /// TODAY/NOW timezone: UTC or ±HH:MM [default: --now offset, else local]
        #[arg(long, value_name = "ZONE", value_parser = parse_tz, allow_hyphen_values = true)]
        tz: Option<TimeZoneSpec>,
        /// RAND/RANDBETWEEN seed [default: built-in seed]
        #[arg(long, value_name = "U64")]
        seed: Option<u64>,
    },
}
#[derive(Serialize)]
struct ErrorCell {
    sheet: String,
    cell: String,
    error: String,
    /// The engine's reason, e.g. `Unknown function: SPDVOL`.
    message: Option<String>,
}
/// A function the engine does not implement and the error cells naming it.
#[derive(Serialize)]
struct UnknownFunction {
    name: String,
    cells: usize,
}
#[derive(Serialize)]
struct Refusal {
    feature: String,
    context: String,
}
/// The clock a computed run used: enough, with `seed`, to replay it.
#[derive(Serialize)]
struct Clock {
    /// RFC 3339 instant TODAY/NOW observed, written in the UTC offset that
    /// was applied to it (so `--now <now>` alone replays it); null when the
    /// build has no system clock and `--now` was not given.
    now: Option<String>,
    /// `Local`, `UTC` or `±HH:MM`.
    timezone: String,
    /// True when the instant came from `--now`.
    fixed: bool,
}
/// External link values the calculation read: always the values Excel last
/// stored in the workbook, never refreshed from the linked files.
#[derive(Serialize)]
struct ExternalLinks {
    /// Links whose cached values a formula or used defined name read.
    links_used: usize,
    /// Always false: links are never refreshed.
    refreshed: bool,
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
    error_cells: Option<usize>,
    errors: Option<Vec<ErrorCell>>,
    errors_truncated: Option<bool>,
    unknown_functions: Option<Vec<UnknownFunction>>,
    refusal: Option<Refusal>,
    clock: Option<Clock>,
    seed: Option<u64>,
    /// Present only when a computed run read cached external link values.
    #[serde(skip_serializing_if = "Option::is_none")]
    external_links: Option<ExternalLinks>,
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
            error_cells: None,
            errors: None,
            errors_truncated: None,
            unknown_functions: None,
            refusal: None,
            clock: None,
            seed: None,
            external_links: None,
            message,
        }
    }
    fn result(&mut self, result: &XlsxRecalculateResult, limit: usize) {
        self.formula_cells = Some(result.formula_cells);
        self.cache_cells_changed = Some(result.cache_cells_changed);
        self.worksheet_parts_changed = Some(result.worksheet_parts_changed);
        self.error_cells = Some(result.summary.errors);
        self.external_links = (result.external_links_used > 0).then_some(ExternalLinks {
            links_used: result.external_links_used,
            refreshed: false,
        });
        let errors: Vec<_> = result
            .summary
            .error_summary
            .iter()
            .flat_map(|(error, summary)| {
                let messages = summary
                    .messages
                    .iter()
                    .map(Some)
                    .chain(std::iter::repeat(None));
                summary
                    .locations
                    .iter()
                    .zip(messages)
                    .filter_map(move |(location, message)| {
                        // Sheet names themselves may contain '!'. The final separator is the cell.
                        let (sheet, cell) = location.rsplit_once('!')?;
                        Some(ErrorCell {
                            sheet: sheet.into(),
                            cell: cell.into(),
                            error: error.clone(),
                            message: message.cloned().flatten(),
                        })
                    })
            })
            .take(limit)
            .collect();
        self.errors_truncated = Some(errors.len() < result.summary.errors);
        self.errors = Some(errors);
        self.unknown_functions = Some(
            result
                .summary
                .unknown_functions
                .iter()
                .map(|(name, cells)| UnknownFunction {
                    name: name.clone(),
                    cells: *cells,
                })
                .collect(),
        );
    }
}

fn parse_now(value: &str) -> Result<DateTime<FixedOffset>, String> {
    DateTime::parse_from_rfc3339(value).map_err(|_| {
        "expected an RFC 3339 timestamp with an offset or Z, e.g. 2026-01-31T09:00:00Z".into()
    })
}
fn offset_zone(seconds: i32) -> TimeZoneSpec {
    if seconds == 0 {
        TimeZoneSpec::Utc
    } else {
        TimeZoneSpec::FixedOffsetSeconds(seconds)
    }
}
fn parse_tz(value: &str) -> Result<TimeZoneSpec, String> {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return Ok(TimeZoneSpec::Utc);
    }
    let bytes = value.as_bytes();
    let digits = |r: std::ops::Range<usize>| {
        bytes[r.clone()]
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| value[r].parse::<i32>().ok())
            .flatten()
    };
    if bytes.len() == 6
        && matches!(bytes[0], b'+' | b'-')
        && bytes[3] == b':'
        && let (Some(h), Some(m)) = (digits(1..3), digits(4..6))
        && h <= 23
        && m <= 59
    {
        let seconds = (h * 60 + m) * 60;
        return Ok(offset_zone(if bytes[0] == b'-' {
            -seconds
        } else {
            seconds
        }));
    }
    Err("expected UTC or an offset ±HH:MM, e.g. +02:00 or -05:00".into())
}
/// The UTC offset `zone` applies at `now` (for `Local`, the host's offset
/// at that instant, as the engine's clock computes it).
fn applied_offset(now: DateTime<Utc>, zone: &TimeZoneSpec) -> FixedOffset {
    zone.fixed_offset()
        .unwrap_or_else(|| *now.with_timezone(&Local).offset())
}
fn zone_label(zone: &TimeZoneSpec) -> String {
    match zone {
        TimeZoneSpec::Local => "Local".into(),
        TimeZoneSpec::Utc => "UTC".into(),
        TimeZoneSpec::FixedOffsetSeconds(seconds) => {
            let sign = if *seconds < 0 { '-' } else { '+' };
            let s = seconds.unsigned_abs();
            let (h, m, rest) = (s / 3600, s / 60 % 60, s % 60);
            if rest == 0 {
                format!("{sign}{h:02}:{m:02}")
            } else {
                format!("{sign}{h:02}:{m:02}:{rest:02}")
            }
        }
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
        now,
        tz,
        seed,
    } = cli.command;
    let mut options = XlsxRecalculateOptions {
        cancel: cancel.clone(),
        error_location_limit: max_errors,
        ..Default::default()
    };
    // No flags leave the default configuration (system clock, local time,
    // built-in seed) untouched.
    if now.is_some() || tz.is_some() {
        let timezone = tz.unwrap_or_else(|| {
            now.map_or(TimeZoneSpec::Local, |n| {
                offset_zone(n.offset().local_minus_utc())
            })
        });
        options.eval_config.deterministic_mode = match now {
            Some(now) => DeterministicMode::Enabled {
                timestamp_utc: now.with_timezone(&Utc),
                timezone,
            },
            None => DeterministicMode::Disabled { timezone },
        };
    }
    if let Some(seed) = seed {
        options.eval_config.workbook_seed = seed;
    }
    let zone = options.eval_config.deterministic_mode.timezone().clone();
    let zone = &zone;
    let seed = options.eval_config.workbook_seed;
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
            report.clock = Some(Clock {
                now: result.clock_now_utc.map(|now| {
                    now.with_timezone(&applied_offset(now, zone))
                        .to_rfc3339_opts(SecondsFormat::AutoSi, true)
                }),
                timezone: zone_label(zone),
                fixed: now.is_some(),
            });
            report.seed = Some(seed);
            let locations = report
                .errors
                .as_ref()
                .unwrap()
                .iter()
                .map(|e| match &e.message {
                    Some(message) => format!("{}!{} {} ({message})", e.sheet, e.cell, e.error),
                    None => format!("{}!{} {}", e.sheet, e.cell, e.error),
                })
                .collect::<Vec<_>>()
                .join(", ");
            let unknown = report
                .unknown_functions
                .as_ref()
                .unwrap()
                .iter()
                .map(|f| {
                    format!(
                        "{} ({} cell{})",
                        f.name,
                        f.cells,
                        if f.cells == 1 { "" } else { "s" }
                    )
                })
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
            if !unknown.is_empty() {
                report.message.push_str(&format!(
                    "\nunknown functions (cells produce #NAME?): {unknown}"
                ));
            }
            if let Some(links) = &report.external_links {
                report.message.push_str(&format!(
                    "\nexternal links: used the values cached in the workbook for {} link{} (not refreshed)",
                    links.links_used,
                    if links.links_used == 1 { "" } else { "s" }
                ));
            }
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
