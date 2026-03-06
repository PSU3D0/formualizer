use std::{
    fs::File,
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use formualizer_bench_core::{BenchmarkSuite, Scenario};

#[cfg(not(feature = "xlsx"))]
fn main() {
    eprintln!(
        "This binary requires the `xlsx` feature: cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "xlsx")]
fn main() -> Result<()> {
    run()
}

#[cfg(feature = "xlsx")]
#[derive(Debug, Parser)]
#[command(name = "generate-corpus")]
#[command(about = "Generate XLSX benchmark corpus files from scenarios.yaml")]
struct Cli {
    /// Path to scenarios.yaml
    #[arg(long, default_value = "benchmarks/scenarios.yaml")]
    scenarios: PathBuf,

    /// Repo root used to resolve relative workbook paths
    #[arg(long, default_value = ".")]
    root: PathBuf,

    /// Optional scenario id filter (repeatable)
    #[arg(long = "only")]
    only: Vec<String>,

    /// Print actions without writing files
    #[arg(long)]
    dry_run: bool,
}

#[cfg(feature = "xlsx")]
fn run() -> Result<()> {
    let cli = Cli::parse();

    let suite = BenchmarkSuite::from_yaml_path(&cli.scenarios)
        .with_context(|| format!("loading scenarios from {}", cli.scenarios.display()))?;

    let mut generated = 0usize;
    let root = cli.root.canonicalize().unwrap_or(cli.root.clone());

    for scenario in &suite.scenarios {
        if !cli.only.is_empty() && !cli.only.iter().any(|id| id == &scenario.id) {
            continue;
        }
        if scenario.source.kind != "generated" {
            continue;
        }

        let out = resolve_output_path(&root, &scenario.source.workbook_path);
        if cli.dry_run {
            println!("[dry-run] {} -> {}", scenario.id, out.display());
        } else {
            generate_scenario(&out, scenario)?;
            normalize_xlsx_styles_for_cross_engine(&out)?;
            println!("generated {} -> {}", scenario.id, out.display());
        }
        generated += 1;
    }

    if generated == 0 {
        bail!("no generated scenarios matched filters")
    }

    println!("done: generated {generated} workbook(s)");
    Ok(())
}

#[cfg(feature = "xlsx")]
fn resolve_output_path(root: &Path, workbook_path: &str) -> PathBuf {
    let p = PathBuf::from(workbook_path);
    if p.is_absolute() { p } else { root.join(p) }
}

#[cfg(feature = "xlsx")]
fn cfg_u32(s: &Scenario, pointer: &str, default: u32) -> u32 {
    s.source
        .config
        .as_ref()
        .and_then(|v| v.pointer(pointer))
        .and_then(|v| v.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(default)
}

#[cfg(feature = "xlsx")]
fn cfg_str(s: &Scenario, pointer: &str, default: &str) -> String {
    s.source
        .config
        .as_ref()
        .and_then(|v| v.pointer(pointer))
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

#[cfg(feature = "xlsx")]
fn generate_scenario(output: &Path, s: &Scenario) -> Result<()> {
    use formualizer_testkit::write_workbook;

    match s.id.as_str() {
        "headline_100k_single_edit" => {
            let rows = cfg_u32(s, "/sheets/0/rows", 100_000);
            formualizer_bench_core::corpus::generate_headline_single_edit_xlsx(output, rows)
        }
        "chain_100k" => {
            let rows = cfg_u32(s, "/sheets/0/rows", 100_000);
            write_workbook(output, |book| {
                let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                sh.get_cell_mut((1, 1)).set_value_number(1.0);
                for r in 2..=rows {
                    sh.get_cell_mut((1, r))
                        .set_formula(format!("=A{}+1", r - 1));
                }
            });
            Ok(())
        }
        "fanout_100k" => {
            let rows = cfg_u32(s, "/sheets/0/rows", 100_000);
            write_workbook(output, |book| {
                let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                sh.get_cell_mut((1, 1)).set_value_number(1.0);
                for r in 1..=rows {
                    sh.get_cell_mut((2, r)).set_formula(format!("=A$1*{r}"));
                }
            });
            Ok(())
        }
        "cross_sheet_mesh" => {
            let rows = cfg_u32(s, "/sheets/0/rows", 25_000);
            write_workbook(output, |book| {
                let _ = book.new_sheet("Inputs");
                let _ = book.new_sheet("CalcA");
                let _ = book.new_sheet("CalcB");

                let inputs = book.get_sheet_by_name_mut("Inputs").expect("Inputs exists");
                for r in 1..=rows {
                    inputs.get_cell_mut((1, r)).set_value_number(r as f64);
                    inputs.get_cell_mut((2, r)).set_value_number((r * 2) as f64);
                    inputs
                        .get_cell_mut((3, r))
                        .set_value_number((r % 10) as f64 + 1.0);
                }

                let calca = book.get_sheet_by_name_mut("CalcA").expect("CalcA exists");
                for r in 1..=rows {
                    calca
                        .get_cell_mut((1, r))
                        .set_formula(format!("=Inputs!A{r}+Inputs!B{r}"));
                }

                let calcb = book.get_sheet_by_name_mut("CalcB").expect("CalcB exists");
                for r in 1..=rows {
                    calcb
                        .get_cell_mut((1, r))
                        .set_formula(format!("=CalcA!A{r}*Inputs!C{r}"));
                }
            });
            Ok(())
        }
        "sparse_whole_column_refs" => {
            let rows = cfg_u32(s, "/sheets/0/rows", 1_000_000);
            let every = cfg_u32(s, "/layout/sparse_fill/every_n_rows", 1_000).max(1);
            let summary_formula = cfg_str(s, "/layout/summary_formula", "=SUM(A:A)");
            write_workbook(output, |book| {
                let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                let mut r = 1;
                while r <= rows {
                    sh.get_cell_mut((1, r)).set_value_number(r as f64);
                    r = r.saturating_add(every);
                }
                sh.get_cell_mut((3, 1)).set_formula(summary_formula);
            });
            Ok(())
        }
        "sumifs_fact_table_100k" => {
            let fact_rows = cfg_u32(s, "/sheets/0/rows", 100_000);
            let report_rows = cfg_u32(s, "/layout/report_rows", 1_000);
            let facts_revenue_formula = cfg_str(
                s,
                "/layout/formulas/facts_revenue_formula",
                "=D{row}*E{row}",
            );
            let report_sumifs_formula = cfg_str(
                s,
                "/layout/formulas/report_sumifs_formula",
                "=SUMIFS(Facts!$F:$F,Facts!$A:$A,A{row},Facts!$B:$B,B{row},Facts!$C:$C,C{row})",
            );

            let regions = ["North", "South", "East", "West"];
            let products = ["A", "B", "C", "D", "E"];
            let channels = ["Online", "Retail", "Partner"];

            write_workbook(output, |book| {
                let _ = book.new_sheet("Facts");
                let _ = book.new_sheet("Report");

                let facts = book.get_sheet_by_name_mut("Facts").expect("Facts exists");
                facts.get_cell_mut((1, 1)).set_value("Region");
                facts.get_cell_mut((2, 1)).set_value("Product");
                facts.get_cell_mut((3, 1)).set_value("Channel");
                facts.get_cell_mut((4, 1)).set_value("Qty");
                facts.get_cell_mut((5, 1)).set_value("Price");
                facts.get_cell_mut((6, 1)).set_value("Revenue");

                for i in 0..fact_rows {
                    let r = i + 2;
                    let idx = i as usize;
                    let region = regions[idx % regions.len()];
                    let product = products[(idx / regions.len()) % products.len()];
                    let channel =
                        channels[(idx / (regions.len() * products.len())) % channels.len()];
                    let qty = ((i % 17) + 1) as f64;
                    let price = ((i % 23) + 10) as f64;

                    facts.get_cell_mut((1, r)).set_value(region);
                    facts.get_cell_mut((2, r)).set_value(product);
                    facts.get_cell_mut((3, r)).set_value(channel);
                    facts.get_cell_mut((4, r)).set_value_number(qty);
                    facts.get_cell_mut((5, r)).set_value_number(price);
                    facts
                        .get_cell_mut((6, r))
                        .set_formula(facts_revenue_formula.replace("{row}", &r.to_string()));
                }

                let report = book.get_sheet_by_name_mut("Report").expect("Report exists");
                report.get_cell_mut((1, 1)).set_value("Region");
                report.get_cell_mut((2, 1)).set_value("Product");
                report.get_cell_mut((3, 1)).set_value("Channel");
                report.get_cell_mut((4, 1)).set_value("Revenue");

                for i in 0..report_rows {
                    let r = i + 2;
                    let idx = i as usize;
                    let region = regions[idx % regions.len()];
                    let product = products[(idx / regions.len()) % products.len()];
                    let channel =
                        channels[(idx / (regions.len() * products.len())) % channels.len()];

                    report.get_cell_mut((1, r)).set_value(region);
                    report.get_cell_mut((2, r)).set_value(product);
                    report.get_cell_mut((3, r)).set_value(channel);
                    report
                        .get_cell_mut((4, r))
                        .set_formula(report_sumifs_formula.replace("{row}", &r.to_string()));
                }
            });
            Ok(())
        }
        "structural_sheet_recovery" => {
            write_workbook(output, |book| {
                let _ = book.new_sheet("Sheet2");
                let s1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                s1.get_cell_mut((1, 1)).set_formula("=Sheet2!A1");
                let s2 = book.get_sheet_by_name_mut("Sheet2").expect("Sheet2 exists");
                s2.get_cell_mut((1, 1)).set_value_number(10.0);
            });
            Ok(())
        }
        other => bail!("no generator implemented for scenario id: {other}"),
    }
}

#[cfg(feature = "xlsx")]
fn normalize_xlsx_styles_for_cross_engine(path: &Path) -> Result<()> {
    let src =
        File::open(path).with_context(|| format!("open xlsx for normalize: {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(src)
        .with_context(|| format!("read xlsx zip for normalize: {}", path.display()))?;

    let mut files: Vec<(String, zip::CompressionMethod, Vec<u8>)> =
        Vec::with_capacity(archive.len());

    for idx in 0..archive.len() {
        let mut entry = archive.by_index(idx)?;
        let name = entry.name().to_string();
        let method = entry.compression();
        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;

        if name == "xl/styles.xml" {
            data = normalize_styles_xml(&data)?;
        } else if name.starts_with("xl/worksheets/sheet") && name.ends_with(".xml") {
            data = normalize_worksheet_formulas_xml(&data)?;
        }

        files.push((name, method, data));
    }
    drop(archive);

    let mut out_buf = Cursor::new(Vec::<u8>::new());
    {
        let mut writer = zip::ZipWriter::new(&mut out_buf);
        for (name, method, data) in files {
            let options = zip::write::FileOptions::default().compression_method(method);
            writer.start_file(name, options)?;
            writer.write_all(&data)?;
        }
        writer.finish()?;
    }

    std::fs::write(path, out_buf.into_inner())
        .with_context(|| format!("write normalized xlsx: {}", path.display()))?;
    Ok(())
}

#[cfg(feature = "xlsx")]
fn normalize_worksheet_formulas_xml(bytes: &[u8]) -> Result<Vec<u8>> {
    let xml = String::from_utf8(bytes.to_vec()).context("worksheet xml must be utf-8")?;
    let re =
        regex::Regex::new(r"(<f(?:\s+[^>]*)?>)=").context("compile worksheet formula regex")?;
    Ok(re.replace_all(&xml, "$1").into_owned().into_bytes())
}

#[cfg(feature = "xlsx")]
fn normalize_styles_xml(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut xml = String::from_utf8(bytes.to_vec()).context("styles.xml must be utf-8")?;

    if !xml.contains("<numFmts") {
        insert_after_stylesheet_open(&mut xml, "<numFmts count=\"0\"/>")?;
    }
    if !xml.contains("<cellStyleXfs") {
        insert_before_marker_or_stylesheet_end(
            &mut xml,
            "<cellXfs",
            "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>",
        )?;
    }
    if !xml.contains("<cellStyles") {
        insert_after_marker_or_stylesheet_open(
            &mut xml,
            "</cellXfs>",
            "<cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles>",
        )?;
    }

    Ok(xml.into_bytes())
}

#[cfg(feature = "xlsx")]
fn insert_after_stylesheet_open(xml: &mut String, snippet: &str) -> Result<()> {
    let open = xml
        .find("<styleSheet")
        .with_context(|| "styles.xml missing <styleSheet> root")?;
    let gt_rel = xml[open..]
        .find('>')
        .with_context(|| "styles.xml malformed <styleSheet> open tag")?;
    let insert_at = open + gt_rel + 1;
    xml.insert_str(insert_at, snippet);
    Ok(())
}

#[cfg(feature = "xlsx")]
fn insert_before_marker_or_stylesheet_end(
    xml: &mut String,
    marker: &str,
    snippet: &str,
) -> Result<()> {
    if let Some(pos) = xml.find(marker) {
        xml.insert_str(pos, snippet);
        return Ok(());
    }
    if let Some(end) = xml.find("</styleSheet>") {
        xml.insert_str(end, snippet);
        return Ok(());
    }
    bail!("styles.xml missing marker and closing styleSheet: {marker}")
}

#[cfg(feature = "xlsx")]
fn insert_after_marker_or_stylesheet_open(
    xml: &mut String,
    marker: &str,
    snippet: &str,
) -> Result<()> {
    if let Some(pos) = xml.find(marker) {
        let insert_at = pos + marker.len();
        xml.insert_str(insert_at, snippet);
        return Ok(());
    }
    insert_after_stylesheet_open(xml, snippet)
}
