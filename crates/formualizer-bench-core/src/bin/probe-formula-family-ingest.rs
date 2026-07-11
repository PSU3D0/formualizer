#[cfg(feature = "formualizer_runner")]
use std::{
    fs,
    io::{Cursor, Write},
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

#[cfg(feature = "formualizer_runner")]
use anyhow::{Context, Result, bail};
#[cfg(feature = "formualizer_runner")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "formualizer_runner")]
use formualizer_eval::engine::{FormulaIngestReport, FormulaPlaneMode};
#[cfg(feature = "formualizer_runner")]
use formualizer_workbook::{
    AdapterLoadStats, CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};
#[cfg(feature = "formualizer_runner")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "formualizer_runner")]
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("This binary requires feature `formualizer_runner`");
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.child {
        println!("{}", serde_json::to_string(&probe_child(&cli)?)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&run_matrix(&cli)?)?);
    }
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
#[command(about = "Cold-process matrix for Calamine formula-family ingest")]
struct Cli {
    #[arg(long, value_enum, default_value_t = Scenario::LargeFamily)]
    scenario: Scenario,
    #[arg(long, value_enum, default_value_t = ProbeMode::All)]
    mode: ProbeMode,
    #[arg(long, default_value_t = 100)]
    members: u32,
    #[arg(long)]
    input: Option<PathBuf>,
    #[arg(long)]
    fixture_out: Option<PathBuf>,
    #[arg(long)]
    generate_only: bool,
    /// Number of independently launched cold children per disposition.
    #[arg(long, default_value_t = 1)]
    samples: usize,
    #[arg(long, hide = true)]
    child: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProbeMode {
    All,
    Off,
    Shadow,
    Authoritative,
    ForcedReplay,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum Scenario {
    LargeFamily,
    ManyTinyFamilies,
    ForwardAnchor,
    HoleException,
    FullSheetTwoPoint,
}

#[cfg(feature = "formualizer_runner")]
impl Scenario {
    fn label(self) -> &'static str {
        match self {
            Self::LargeFamily => "large_family",
            Self::ManyTinyFamilies => "many_tiny_families",
            Self::ForwardAnchor => "forward_anchor",
            Self::HoleException => "hole_exception",
            Self::FullSheetTwoPoint => "full_sheet_two_point",
        }
    }
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct MatrixReport {
    scenario: Scenario,
    members: u32,
    fixture: String,
    generated: bool,
    generation_ms: f64,
    fixture_bytes: u64,
    samples_per_disposition: usize,
    children: Vec<ChildMeasurement>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ChildMeasurement {
    disposition: String,
    sample: usize,
    maximum_resident_set_kib: u64,
    report: ModeReport,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Deserialize)]
struct ModeReport {
    disposition: String,
    total_elapsed_ms: f64,
    load_ms: f64,
    evaluate_ms: f64,
    collection_ms: Option<f64>,
    preparation_ms: Option<f64>,
    replay_ms: Option<f64>,
    adapter: AdapterCounters,
    ingest: IngestCounters,
    storage_kind: String,
    active_spans: usize,
    graph_formula_vertices: usize,
    allocator_or_accounted_high_water: AccountedHighWater,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Deserialize)]
struct AccountedHighWater {
    spool_memory_bytes: u64,
    evidence_bytes: u64,
    allocator_formula_heap_bytes: Option<u64>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Deserialize)]
struct AdapterCounters {
    formula_cells_observed: Option<u64>,
    formula_cells_handed_to_engine: Option<u64>,
    shared_formula_tags_observed: Option<u64>,
}
#[cfg(feature = "formualizer_runner")]
impl From<Option<AdapterLoadStats>> for AdapterCounters {
    fn from(stats: Option<AdapterLoadStats>) -> Self {
        let s = stats.unwrap_or_default();
        Self {
            formula_cells_observed: s.formula_cells_observed,
            formula_cells_handed_to_engine: s.formula_cells_handed_to_engine,
            shared_formula_tags_observed: s.shared_formula_tags_observed,
        }
    }
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Deserialize)]
struct IngestCounters {
    formula_cells_seen: u64,
    graph_formula_cells_materialized: u64,
    source_formula_events: u64,
    source_formula_records_spooled: u64,
    source_spool_encoded_bytes: u64,
    source_spool_peak_memory_bytes: u64,
    source_spool_spilled_bytes: u64,
    source_spool_replays: u64,
    source_ordinary_events: u64,
    source_shared_anchor_events: u64,
    source_shared_descendant_events: u64,
    source_families_seen: u64,
    source_family_cells_seen: u64,
    source_family_promoted: u64,
    source_family_promoted_cells: u64,
    source_family_fallback: u64,
    source_family_fallback_cells: u64,
    source_forward_descendants: u64,
    source_evidence_limit_fallbacks: u64,
    source_evidence_peak_bytes: u64,
    source_anchor_parses: u64,
    source_anchor_asts: u64,
    source_anchor_analyses: u64,
    source_descendant_records_avoided: Option<u64>,
    source_descendant_strings_avoided: u64,
    source_descendant_events_avoided: u64,
    source_descendant_asts_avoided: Option<u64>,
    source_descendant_analyses_avoided: u64,
    source_staging_entries_avoided: Option<u64>,
    source_placement_runs: Option<u64>,
    source_placement_exceptions: Option<u64>,
    fallback_reasons: std::collections::BTreeMap<String, u64>,
}
#[cfg(feature = "formualizer_runner")]
impl From<FormulaIngestReport> for IngestCounters {
    fn from(r: FormulaIngestReport) -> Self {
        Self {
            formula_cells_seen: r.formula_cells_seen,
            graph_formula_cells_materialized: r.graph_formula_cells_materialized,
            source_formula_events: r.source_formula_events,
            source_formula_records_spooled: r.source_formula_records_spooled,
            source_spool_encoded_bytes: r.source_spool_encoded_bytes,
            source_spool_peak_memory_bytes: r.source_spool_peak_memory_bytes,
            source_spool_spilled_bytes: r.source_spool_spilled_bytes,
            source_spool_replays: r.source_spool_replays,
            source_ordinary_events: r.source_ordinary_events,
            source_shared_anchor_events: r.source_shared_anchor_events,
            source_shared_descendant_events: r.source_shared_descendant_events,
            source_families_seen: r.source_families_seen,
            source_family_cells_seen: r.source_family_cells_seen,
            source_family_promoted: r.source_family_promoted,
            source_family_promoted_cells: r.source_family_promoted_cells,
            source_family_fallback: r.source_family_fallback,
            source_family_fallback_cells: r.source_family_fallback_cells,
            source_forward_descendants: r.source_forward_descendants,
            source_evidence_limit_fallbacks: r.source_evidence_limit_fallbacks,
            source_evidence_peak_bytes: r.source_evidence_peak_bytes,
            source_anchor_parses: r.source_anchor_parses,
            source_anchor_asts: r.source_anchor_asts,
            source_anchor_analyses: r.source_anchor_analyses,
            source_descendant_records_avoided: None,
            source_descendant_strings_avoided: r.source_descendant_strings_avoided,
            source_descendant_events_avoided: r.source_descendant_events_avoided,
            source_descendant_asts_avoided: None,
            source_descendant_analyses_avoided: r.source_descendant_analyses_avoided,
            source_staging_entries_avoided: None,
            source_placement_runs: None,
            source_placement_exceptions: None,
            fallback_reasons: r.fallback_reasons,
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn fixture_path(cli: &Cli) -> PathBuf {
    cli.input
        .clone()
        .or_else(|| cli.fixture_out.clone())
        .unwrap_or_else(|| {
            PathBuf::from("scratch/formula-family-anchor-once-bench").join(format!(
                "{}-{}.xlsx",
                cli.scenario.label(),
                cli.members
            ))
        })
}

#[cfg(feature = "formualizer_runner")]
fn run_matrix(cli: &Cli) -> Result<MatrixReport> {
    if cli.members == 0 {
        bail!("--members must be at least 1")
    }
    if cli.samples == 0 {
        bail!("--samples must be at least 1")
    }
    let fixture = fixture_path(cli);
    let generated = cli.input.is_none();
    let started = Instant::now();
    if generated {
        let bytes = generate_xlsx(cli.scenario, cli.members)?;
        if let Some(p) = fixture.parent() {
            fs::create_dir_all(p)?
        }
        fs::write(&fixture, bytes)?;
    }
    let generation_ms = started.elapsed().as_secs_f64() * 1000.0;
    let fixture_bytes = fs::metadata(&fixture)?.len();
    let mut children = Vec::new();
    if !cli.generate_only {
        for mode in [
            ProbeMode::Off,
            ProbeMode::Shadow,
            ProbeMode::Authoritative,
            ProbeMode::ForcedReplay,
        ] {
            if cli.mode != ProbeMode::All && cli.mode != mode {
                continue;
            }
            for sample in 1..=cli.samples {
                children.push(run_child(cli, &fixture, mode, sample)?);
            }
        }
    }
    Ok(MatrixReport {
        scenario: cli.scenario,
        members: cli.members,
        fixture: fixture.display().to_string(),
        generated,
        generation_ms,
        fixture_bytes,
        samples_per_disposition: cli.samples,
        children,
    })
}

#[cfg(feature = "formualizer_runner")]
fn run_child(
    cli: &Cli,
    fixture: &Path,
    mode: ProbeMode,
    sample: usize,
) -> Result<ChildMeasurement> {
    let exe = std::env::current_exe()?;
    let mode_arg = match mode {
        ProbeMode::Off => "off",
        ProbeMode::Shadow => "shadow",
        ProbeMode::Authoritative => "authoritative",
        ProbeMode::ForcedReplay => "forced-replay",
        ProbeMode::All => unreachable!(),
    };
    let output = Command::new("/usr/bin/time")
        .args(["-v"])
        .arg(exe)
        .args(["--child", "--input"])
        .arg(fixture)
        .args(["--members", &cli.members.to_string(), "--mode", mode_arg])
        .output()
        .context("launch timed cold child through /usr/bin/time -v")?;
    if !output.status.success() {
        bail!(
            "child {mode_arg} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let rss = parse_max_rss(&stderr).context("/usr/bin/time -v did not report maximum RSS")?;
    let report: ModeReport = serde_json::from_slice(&output.stdout).context("parse child JSON")?;
    Ok(ChildMeasurement {
        disposition: mode_arg.into(),
        sample,
        maximum_resident_set_kib: rss,
        report,
    })
}

#[cfg(feature = "formualizer_runner")]
fn parse_max_rss(s: &str) -> Option<u64> {
    s.lines().find_map(|l| {
        l.split_once("Maximum resident set size (kbytes):")
            .and_then(|(_, v)| v.trim().parse().ok())
    })
}

#[cfg(feature = "formualizer_runner")]
fn probe_child(cli: &Cli) -> Result<ModeReport> {
    let path = fixture_path(cli);
    let (mode, label, forced) = match cli.mode {
        ProbeMode::Off => (FormulaPlaneMode::Off, "off", false),
        ProbeMode::Shadow => (FormulaPlaneMode::Shadow, "shadow", false),
        ProbeMode::Authoritative => (
            FormulaPlaneMode::AuthoritativeExperimental,
            "authoritative",
            false,
        ),
        ProbeMode::ForcedReplay => (
            FormulaPlaneMode::AuthoritativeExperimental,
            "forced_replay",
            true,
        ),
        ProbeMode::All => bail!("child requires one disposition"),
    };
    if forced {
        unsafe { std::env::set_var("FORMUALIZER_BENCH_FORCE_FORMULA_FAMILY_REPLAY", "1") };
    }
    let total = Instant::now();
    let adapter = CalamineAdapter::open_path(&path)?;
    let load = Instant::now();
    let (mut workbook, adapter_stats) = Workbook::from_reader_with_adapter_stats(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::ephemeral().with_formula_plane_mode(mode),
    )?;
    let load_ms = load.elapsed().as_secs_f64() * 1000.0;
    let eval = Instant::now();
    workbook.evaluate_all()?;
    let evaluate_ms = eval.elapsed().as_secs_f64() * 1000.0;
    let stats = workbook.engine().baseline_stats();
    let ingest: IngestCounters = workbook
        .last_formula_ingest_report()
        .context("missing ingest report")?
        .into();
    if forced
        && (stats.formula_plane_active_span_count != 0
            || stats.graph_formula_vertex_count as u64 != ingest.formula_cells_seen)
    {
        bail!(
            "forced replay must materialize every formula as a graph vertex (spans={}, vertices={}, formulas={})",
            stats.formula_plane_active_span_count,
            stats.graph_formula_vertex_count,
            ingest.formula_cells_seen
        );
    }
    let storage_kind = if ingest.source_spool_spilled_bytes > 0 {
        "native_file"
    } else if ingest.source_formula_records_spooled > 0 {
        "memory"
    } else {
        "none"
    }
    .to_string();
    let high = AccountedHighWater {
        spool_memory_bytes: ingest.source_spool_peak_memory_bytes,
        evidence_bytes: ingest.source_evidence_peak_bytes,
        allocator_formula_heap_bytes: None,
    };
    Ok(ModeReport {
        disposition: label.into(),
        total_elapsed_ms: total.elapsed().as_secs_f64() * 1000.0,
        load_ms,
        evaluate_ms,
        collection_ms: None,
        preparation_ms: None,
        replay_ms: None,
        adapter: adapter_stats.into(),
        storage_kind,
        active_spans: stats.formula_plane_active_span_count,
        graph_formula_vertices: stats.graph_formula_vertex_count,
        allocator_or_accounted_high_water: high,
        ingest,
    })
}

#[cfg(feature = "formualizer_runner")]
fn generate_xlsx(scenario: Scenario, members: u32) -> Result<Vec<u8>> {
    let sheet = sheet_xml(scenario, members)?;
    let cursor = Cursor::new(Vec::new());
    let mut zip = ZipWriter::new(cursor);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    for (name, body) in [
        ("[Content_Types].xml", CONTENT_TYPES),
        ("_rels/.rels", ROOT_RELS),
        ("xl/workbook.xml", WORKBOOK),
        ("xl/_rels/workbook.xml.rels", WORKBOOK_RELS),
        ("xl/worksheets/sheet1.xml", sheet.as_str()),
    ] {
        zip.start_file(name, options)?;
        zip.write_all(body.as_bytes())?;
    }
    Ok(zip.finish()?.into_inner())
}

#[cfg(feature = "formualizer_runner")]
fn sheet_xml(scenario: Scenario, members: u32) -> Result<String> {
    let rows = match scenario {
        Scenario::FullSheetTwoPoint => 2,
        _ => members,
    };
    if rows > 1_048_576 {
        bail!("--members exceeds the XLSX row limit");
    }
    let mut xml = String::with_capacity(rows as usize * 100 + 256);
    xml.push_str(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">"#);
    let dimension_end = if matches!(scenario, Scenario::FullSheetTwoPoint) {
        1_048_576
    } else {
        rows
    };
    xml.push_str(&format!(
        r#"<dimension ref="A1:B{dimension_end}"/><sheetData>"#
    ));

    let tiny_width = 4_u32;
    for row in 1..=rows {
        xml.push_str(&format!(
            r#"<row r="{row}"><c r="A{row}"><v>{row}</v></c><c r="B{row}">"#
        ));
        let formula = format!("A{row}+1");
        match scenario {
            Scenario::ManyTinyFamilies => {
                let family_start = ((row - 1) / tiny_width) * tiny_width + 1;
                let family_end = (family_start + tiny_width - 1).min(rows);
                let si = (row - 1) / tiny_width;
                if row == family_start {
                    xml.push_str(&format!(
                        r#"<f t="shared" si="{si}" ref="B{family_start}:B{family_end}">{formula}</f>"#
                    ));
                } else {
                    xml.push_str(&format!(r#"<f t="shared" si="{si}"/>"#));
                }
            }
            Scenario::ForwardAnchor => {
                if rows == 1 || row == 2 {
                    xml.push_str(&format!(
                        r#"<f t="shared" si="1" ref="B1:B{rows}">{formula}</f>"#
                    ));
                } else {
                    xml.push_str(r#"<f t="shared" si="1"/>"#);
                }
            }
            Scenario::HoleException if row == (rows / 2).max(1) => {
                xml.push_str(&format!("<f>{formula}</f>"));
            }
            Scenario::FullSheetTwoPoint | Scenario::LargeFamily | Scenario::HoleException => {
                if row == 1 {
                    let end = if matches!(scenario, Scenario::FullSheetTwoPoint) {
                        1_048_576
                    } else {
                        rows
                    };
                    xml.push_str(&format!(
                        r#"<f t="shared" si="1" ref="B1:B{end}">{formula}</f>"#
                    ));
                } else {
                    xml.push_str(r#"<f t="shared" si="1"/>"#);
                }
            }
        }
        xml.push_str("<v/></c></row>");
    }
    xml.push_str("</sheetData></worksheet>");
    Ok(xml)
}

#[cfg(feature = "formualizer_runner")]
const CONTENT_TYPES: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#;
#[cfg(feature = "formualizer_runner")]
const ROOT_RELS: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#;
#[cfg(feature = "formualizer_runner")]
const WORKBOOK: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>"#;
#[cfg(feature = "formualizer_runner")]
const WORKBOOK_RELS: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#;

#[cfg(all(test, feature = "formualizer_runner"))]
mod tests {
    use super::*;
    use std::io::Read;
    use zip::ZipArchive;

    #[test]
    fn fixture_generation_is_deterministic_and_contains_shared_ooxml() {
        let first = generate_xlsx(Scenario::LargeFamily, 100).unwrap();
        let second = generate_xlsx(Scenario::LargeFamily, 100).unwrap();
        assert_eq!(first, second);
        let mut archive = ZipArchive::new(Cursor::new(first)).unwrap();
        let mut xml = String::new();
        archive
            .by_name("xl/worksheets/sheet1.xml")
            .unwrap()
            .read_to_string(&mut xml)
            .unwrap();
        assert!(xml.contains("t=\"shared\""));
        assert!(xml.contains("ref=\"B1:B100\""));
    }

    #[test]
    fn pathological_fixture_declares_full_sheet_without_materializing_it() {
        let xml = sheet_xml(Scenario::FullSheetTwoPoint, 2).unwrap();
        assert!(xml.contains("ref=\"B1:B1048576\""));
        assert_eq!(xml.matches("<row ").count(), 2);
    }

    #[test]
    fn parses_gnu_time_maximum_rss_without_combining_children() {
        let output = "\tMaximum resident set size (kbytes): 123456\n";
        assert_eq!(parse_max_rss(output), Some(123_456));
        assert_eq!(parse_max_rss("elapsed 1.0"), None);
    }
}
