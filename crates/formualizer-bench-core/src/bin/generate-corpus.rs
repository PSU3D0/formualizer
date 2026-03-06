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
fn render_formula_template(template: &str, row: u32, fact_last_row: Option<u32>) -> String {
    let mut formula = template.replace("{row}", &row.to_string());
    if let Some(fact_last_row) = fact_last_row {
        formula = formula.replace("{fact_last_row}", &fact_last_row.to_string());
    }
    formula
}

#[cfg(feature = "xlsx")]
fn lookup_key(prefix: &str, index: u32) -> String {
    format!("{prefix}{index:06}")
}

#[cfg(feature = "xlsx")]
fn dense_lookup_value(index: u32) -> f64 {
    (index * 7 + (index % 13)) as f64
}

#[cfg(feature = "xlsx")]
fn dense_lookup_checksum(index: u32) -> f64 {
    (index * 11 + (index % 17)) as f64
}

#[cfg(feature = "xlsx")]
fn fact_id(index: u32) -> String {
    format!("F{index:06}")
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
        "inc_sparse_dirty_region_1m" => {
            let rows = cfg_u32(s, "/sheets/0/rows", 1_000_000);
            let block_rows = [
                1, 125_001, 250_001, 375_001, 500_001, 625_001, 750_001, 875_001,
            ];
            write_workbook(output, |book| {
                let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                for (idx, row) in block_rows.into_iter().enumerate() {
                    let seed = ((idx as u32) + 1) * 10;
                    sh.get_cell_mut((1, row)).set_value_number(seed as f64);
                    sh.get_cell_mut((2, row)).set_formula(format!("=A{row}*2"));
                    sh.get_cell_mut((3, row)).set_formula(format!("=B{row}+5"));
                    sh.get_cell_mut((4, row))
                        .set_formula(format!("=SUM(B{row}:C{row})"));
                }
                sh.get_cell_mut((1, rows)).set_value_number(3.0);
                sh.get_cell_mut((2, rows))
                    .set_formula(format!("=A{rows}+1"));
            });
            Ok(())
        }
        "inc_cross_sheet_mesh_3x25k" => {
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
        "lookup_index_match_dense_50k" => {
            let lookup_rows = cfg_u32(s, "/layout/lookup_rows", 50_000);
            let query_rows = cfg_u32(s, "/layout/query_rows", 20_000);
            let key_prefix = cfg_str(s, "/layout/key_prefix", "K");
            let query_key_stride = cfg_u32(s, "/layout/query_key_stride", 7_919).max(1);
            let lookup_last_row = lookup_rows + 1;

            write_workbook(output, |book| {
                let _ = book.new_sheet("Lookup");
                let _ = book.new_sheet("Queries");

                let lookup = book.get_sheet_by_name_mut("Lookup").expect("Lookup exists");
                lookup.get_cell_mut((1, 1)).set_value("Key");
                lookup.get_cell_mut((2, 1)).set_value("Value");
                lookup.get_cell_mut((3, 1)).set_value("Checksum");
                for index in 1..=lookup_rows {
                    let row = index + 1;
                    lookup
                        .get_cell_mut((1, row))
                        .set_value(lookup_key(&key_prefix, index));
                    lookup
                        .get_cell_mut((2, row))
                        .set_value_number(dense_lookup_value(index));
                    lookup
                        .get_cell_mut((3, row))
                        .set_value_number(dense_lookup_checksum(index));
                }

                let queries = book
                    .get_sheet_by_name_mut("Queries")
                    .expect("Queries exists");
                queries.get_cell_mut((1, 1)).set_value("Key");
                queries.get_cell_mut((2, 1)).set_value("Value");
                queries.get_cell_mut((3, 1)).set_value("Checksum");

                for i in 0..query_rows {
                    let row = i + 2;
                    let default_lookup_index = ((i * query_key_stride) % lookup_rows) + 1;
                    let lookup_index = match row {
                        2 => 40_000,
                        10_000 => 123,
                        _ => default_lookup_index,
                    };

                    queries
                        .get_cell_mut((1, row))
                        .set_value(lookup_key(&key_prefix, lookup_index));
                    queries.get_cell_mut((2, row)).set_formula(format!(
                        "=INDEX(Lookup!$B$2:$B${lookup_last_row},MATCH(A{row},Lookup!$A$2:$A${lookup_last_row},0))"
                    ));
                    queries.get_cell_mut((3, row)).set_formula(format!(
                        "=INDEX(Lookup!$C$2:$C${lookup_last_row},MATCH(A{row},Lookup!$A$2:$A${lookup_last_row},0))"
                    ));
                }
            });
            Ok(())
        }
        "lookup_cross_sheet_dim_fact" => {
            let fact_rows = cfg_u32(s, "/layout/fact_rows", 50_000);
            let report_rows = cfg_u32(s, "/layout/report_rows", 5_000);
            let fact_last_row = fact_rows + 1;
            let regions = [
                ("North", 3.0),
                ("South", 5.0),
                ("East", 7.0),
                ("West", 11.0),
                ("Central", 13.0),
                ("Coastal", 17.0),
            ];
            let products = [
                ("Alpha", 2.0),
                ("Beta", 4.0),
                ("Gamma", 6.0),
                ("Delta", 8.0),
                ("Epsilon", 10.0),
            ];
            let region_last_row = regions.len() as u32 + 1;
            let product_last_row = products.len() as u32 + 1;

            write_workbook(output, |book| {
                let _ = book.new_sheet("RegionDim");
                let _ = book.new_sheet("ProductDim");
                let _ = book.new_sheet("Facts");
                let _ = book.new_sheet("Report");

                let region_dim = book
                    .get_sheet_by_name_mut("RegionDim")
                    .expect("RegionDim exists");
                region_dim.get_cell_mut((1, 1)).set_value("RegionKey");
                region_dim.get_cell_mut((2, 1)).set_value("RegionWeight");
                for (idx, (region, weight)) in regions.iter().enumerate() {
                    let row = idx as u32 + 2;
                    region_dim.get_cell_mut((1, row)).set_value(*region);
                    region_dim.get_cell_mut((2, row)).set_value_number(*weight);
                }

                let product_dim = book
                    .get_sheet_by_name_mut("ProductDim")
                    .expect("ProductDim exists");
                product_dim.get_cell_mut((1, 1)).set_value("ProductKey");
                product_dim.get_cell_mut((2, 1)).set_value("ProductFactor");
                for (idx, (product, factor)) in products.iter().enumerate() {
                    let row = idx as u32 + 2;
                    product_dim.get_cell_mut((1, row)).set_value(*product);
                    product_dim.get_cell_mut((2, row)).set_value_number(*factor);
                }

                let facts = book.get_sheet_by_name_mut("Facts").expect("Facts exists");
                facts.get_cell_mut((1, 1)).set_value("FactId");
                facts.get_cell_mut((2, 1)).set_value("RegionKey");
                facts.get_cell_mut((3, 1)).set_value("ProductKey");
                facts.get_cell_mut((4, 1)).set_value("Qty");
                facts.get_cell_mut((5, 1)).set_value("Price");
                facts.get_cell_mut((6, 1)).set_value("Revenue");
                for i in 0..fact_rows {
                    let row = i + 2;
                    let fact_index = i + 1;
                    let region = regions[(i as usize) % regions.len()].0;
                    let product = products[((i as usize) / regions.len()) % products.len()].0;
                    let qty = ((i % 11) + 1) as f64;
                    let price = ((i % 19) + 10) as f64;

                    facts.get_cell_mut((1, row)).set_value(fact_id(fact_index));
                    facts.get_cell_mut((2, row)).set_value(region);
                    facts.get_cell_mut((3, row)).set_value(product);
                    facts.get_cell_mut((4, row)).set_value_number(qty);
                    facts.get_cell_mut((5, row)).set_value_number(price);
                    facts
                        .get_cell_mut((6, row))
                        .set_formula(format!("=D{row}*E{row}"));
                }

                let report = book.get_sheet_by_name_mut("Report").expect("Report exists");
                report.get_cell_mut((1, 1)).set_value("FactId");
                report.get_cell_mut((2, 1)).set_value("Revenue");
                report.get_cell_mut((3, 1)).set_value("RegionKey");
                report.get_cell_mut((4, 1)).set_value("ProductKey");
                report.get_cell_mut((5, 1)).set_value("RegionWeight");
                report.get_cell_mut((6, 1)).set_value("ProductFactor");
                report.get_cell_mut((7, 1)).set_value("AdjustedRevenue");
                for i in 0..report_rows {
                    let row = i + 2;
                    let default_fact_index = ((i * 37) % fact_rows) + 1;
                    let fact_index = match row {
                        2 => 12_345,
                        3 => 12_351,
                        _ => default_fact_index,
                    };

                    report.get_cell_mut((1, row)).set_value(fact_id(fact_index));
                    report.get_cell_mut((2, row)).set_formula(format!(
                        "=INDEX(Facts!$D$2:$D${fact_last_row},MATCH(A{row},Facts!$A$2:$A${fact_last_row},0))*INDEX(Facts!$E$2:$E${fact_last_row},MATCH(A{row},Facts!$A$2:$A${fact_last_row},0))"
                    ));
                    report.get_cell_mut((3, row)).set_formula(format!(
                        "=INDEX(Facts!$B$2:$B${fact_last_row},MATCH(A{row},Facts!$A$2:$A${fact_last_row},0))"
                    ));
                    report.get_cell_mut((4, row)).set_formula(format!(
                        "=INDEX(Facts!$C$2:$C${fact_last_row},MATCH(A{row},Facts!$A$2:$A${fact_last_row},0))"
                    ));
                    report.get_cell_mut((5, row)).set_formula(format!(
                        "=INDEX(RegionDim!$B$2:$B${region_last_row},MATCH(C{row},RegionDim!$A$2:$A${region_last_row},0))"
                    ));
                    report.get_cell_mut((6, row)).set_formula(format!(
                        "=INDEX(ProductDim!$B$2:$B${product_last_row},MATCH(D{row},ProductDim!$A$2:$A${product_last_row},0))"
                    ));
                    report
                        .get_cell_mut((7, row))
                        .set_formula(format!("=B{row}*E{row}*F{row}"));
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
        "agg_countifs_multi_criteria_100k" => {
            let fact_rows = cfg_u32(s, "/sheets/0/rows", 100_000);
            let report_rows = cfg_u32(s, "/layout/report_rows", 1_000);
            let fact_last_row = fact_rows + 1;
            let report_countifs_formula = cfg_str(
                s,
                "/layout/formulas/report_countifs_formula",
                "=COUNTIFS(Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row},Facts!$E$2:$E${fact_last_row},\">=\"&E{row})",
            );

            let regions = ["North", "South", "East", "West"];
            let products = ["A", "B", "C", "D", "E"];
            let channels = ["Online", "Retail", "Partner"];
            let statuses = ["Open", "Closed", "Pending", "Escalated"];
            let min_qty_cycle = [3_u32, 6, 9, 12];

            write_workbook(output, |book| {
                let _ = book.new_sheet("Facts");
                let _ = book.new_sheet("Report");

                let facts = book.get_sheet_by_name_mut("Facts").expect("Facts exists");
                facts.get_cell_mut((1, 1)).set_value("Region");
                facts.get_cell_mut((2, 1)).set_value("Product");
                facts.get_cell_mut((3, 1)).set_value("Channel");
                facts.get_cell_mut((4, 1)).set_value("Status");
                facts.get_cell_mut((5, 1)).set_value("Qty");

                for i in 0..fact_rows {
                    let r = i + 2;
                    let idx = i as usize;
                    let region = regions[idx % regions.len()];
                    let product = products[(idx / regions.len()) % products.len()];
                    let channel =
                        channels[(idx / (regions.len() * products.len())) % channels.len()];
                    let status = statuses[(idx
                        / (regions.len() * products.len() * channels.len()))
                        % statuses.len()];
                    let qty = ((i / 240) % 12) + 1;

                    facts.get_cell_mut((1, r)).set_value(region);
                    facts.get_cell_mut((2, r)).set_value(product);
                    facts.get_cell_mut((3, r)).set_value(channel);
                    facts.get_cell_mut((4, r)).set_value(status);
                    facts.get_cell_mut((5, r)).set_value_number(qty as f64);
                }

                let report = book.get_sheet_by_name_mut("Report").expect("Report exists");
                report.get_cell_mut((1, 1)).set_value("Region");
                report.get_cell_mut((2, 1)).set_value("Product");
                report.get_cell_mut((3, 1)).set_value("Channel");
                report.get_cell_mut((4, 1)).set_value("Status");
                report.get_cell_mut((5, 1)).set_value("MinQty");
                report.get_cell_mut((6, 1)).set_value("Count");

                for i in 0..report_rows {
                    let r = i + 2;
                    let idx = i as usize;
                    let region = regions[idx % regions.len()];
                    let product = products[(idx / regions.len()) % products.len()];
                    let channel =
                        channels[(idx / (regions.len() * products.len())) % channels.len()];
                    let status = statuses[(idx
                        / (regions.len() * products.len() * channels.len()))
                        % statuses.len()];
                    let min_qty = min_qty_cycle[idx % min_qty_cycle.len()];

                    report.get_cell_mut((1, r)).set_value(region);
                    report.get_cell_mut((2, r)).set_value(product);
                    report.get_cell_mut((3, r)).set_value(channel);
                    report.get_cell_mut((4, r)).set_value(status);
                    report.get_cell_mut((5, r)).set_value_number(min_qty as f64);
                    report
                        .get_cell_mut((6, r))
                        .set_formula(render_formula_template(
                            &report_countifs_formula,
                            r,
                            Some(fact_last_row),
                        ));
                }
            });
            Ok(())
        }
        "agg_mixed_rollup_grid_2k_reports" => {
            let fact_rows = cfg_u32(s, "/sheets/0/rows", 10_000);
            let report_rows = cfg_u32(s, "/layout/report_rows", 500);
            let fact_last_row = fact_rows + 1;
            let facts_revenue_formula = cfg_str(
                s,
                "/layout/formulas/facts_revenue_formula",
                "=E{row}*F{row}",
            );
            let report_units_formula = cfg_str(
                s,
                "/layout/formulas/report_units_formula",
                "=SUMIFS(Facts!$E$2:$E${fact_last_row},Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row})",
            );
            let report_countifs_formula = cfg_str(
                s,
                "/layout/formulas/report_countifs_formula",
                "=COUNTIFS(Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row})",
            );
            let report_averageifs_formula = cfg_str(
                s,
                "/layout/formulas/report_averageifs_formula",
                "=AVERAGEIFS(Facts!$F$2:$F${fact_last_row},Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row})",
            );
            let report_price_total_formula = cfg_str(
                s,
                "/layout/formulas/report_price_total_formula",
                "=SUMIFS(Facts!$F$2:$F${fact_last_row},Facts!$A$2:$A${fact_last_row},A{row},Facts!$B$2:$B${fact_last_row},B{row},Facts!$C$2:$C${fact_last_row},C{row},Facts!$D$2:$D${fact_last_row},D{row})",
            );

            let regions = ["North", "South", "East", "West"];
            let products = ["A", "B", "C", "D", "E"];
            let channels = ["Online", "Retail", "Partner"];
            let quarters = ["Q1", "Q2", "Q3", "Q4"];

            write_workbook(output, |book| {
                let _ = book.new_sheet("Facts");
                let _ = book.new_sheet("Report");

                let facts = book.get_sheet_by_name_mut("Facts").expect("Facts exists");
                facts.get_cell_mut((1, 1)).set_value("Region");
                facts.get_cell_mut((2, 1)).set_value("Product");
                facts.get_cell_mut((3, 1)).set_value("Channel");
                facts.get_cell_mut((4, 1)).set_value("Quarter");
                facts.get_cell_mut((5, 1)).set_value("Units");
                facts.get_cell_mut((6, 1)).set_value("Price");
                facts.get_cell_mut((7, 1)).set_value("Revenue");

                for i in 0..fact_rows {
                    let r = i + 2;
                    let idx = i as usize;
                    let region_idx = idx % regions.len();
                    let product_idx = (idx / regions.len()) % products.len();
                    let channel_idx = (idx / (regions.len() * products.len())) % channels.len();
                    let quarter_idx =
                        (idx / (regions.len() * products.len() * channels.len())) % quarters.len();
                    let units = ((i / 240) % 9) + 1 + region_idx as u32;
                    let price = ((i / 2_160) % 15) + 10 + product_idx as u32 + quarter_idx as u32;

                    facts.get_cell_mut((1, r)).set_value(regions[region_idx]);
                    facts.get_cell_mut((2, r)).set_value(products[product_idx]);
                    facts.get_cell_mut((3, r)).set_value(channels[channel_idx]);
                    facts.get_cell_mut((4, r)).set_value(quarters[quarter_idx]);
                    facts.get_cell_mut((5, r)).set_value_number(units as f64);
                    facts.get_cell_mut((6, r)).set_value_number(price as f64);
                    facts
                        .get_cell_mut((7, r))
                        .set_formula(render_formula_template(&facts_revenue_formula, r, None));
                }

                let report = book.get_sheet_by_name_mut("Report").expect("Report exists");
                report.get_cell_mut((1, 1)).set_value("Region");
                report.get_cell_mut((2, 1)).set_value("Product");
                report.get_cell_mut((3, 1)).set_value("Channel");
                report.get_cell_mut((4, 1)).set_value("Quarter");
                report.get_cell_mut((5, 1)).set_value("Units");
                report.get_cell_mut((6, 1)).set_value("Orders");
                report.get_cell_mut((7, 1)).set_value("AvgPrice");
                report.get_cell_mut((8, 1)).set_value("PriceTotal");

                for i in 0..report_rows {
                    let r = i + 2;
                    let idx = i as usize;
                    let region = regions[idx % regions.len()];
                    let product = products[(idx / regions.len()) % products.len()];
                    let channel =
                        channels[(idx / (regions.len() * products.len())) % channels.len()];
                    let quarter = quarters[(idx
                        / (regions.len() * products.len() * channels.len()))
                        % quarters.len()];

                    report.get_cell_mut((1, r)).set_value(region);
                    report.get_cell_mut((2, r)).set_value(product);
                    report.get_cell_mut((3, r)).set_value(channel);
                    report.get_cell_mut((4, r)).set_value(quarter);
                    report
                        .get_cell_mut((5, r))
                        .set_formula(render_formula_template(
                            &report_units_formula,
                            r,
                            Some(fact_last_row),
                        ));
                    report
                        .get_cell_mut((6, r))
                        .set_formula(render_formula_template(
                            &report_countifs_formula,
                            r,
                            Some(fact_last_row),
                        ));
                    report
                        .get_cell_mut((7, r))
                        .set_formula(render_formula_template(
                            &report_averageifs_formula,
                            r,
                            Some(fact_last_row),
                        ));
                    report
                        .get_cell_mut((8, r))
                        .set_formula(render_formula_template(
                            &report_price_total_formula,
                            r,
                            Some(fact_last_row),
                        ));
                }
            });
            Ok(())
        }
        "struct_row_insert_middle_50k_refs" => {
            let rows = cfg_u32(s, "/sheets/0/rows", 50_000);
            let formula_pattern = cfg_str(s, "/layout/formula_pattern", "=A{row}*2");
            let rollup_formula = cfg_str(s, "/layout/rollup_formula", "=SUM(B1:B50000)");

            write_workbook(output, |book| {
                let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                for r in 1..=rows {
                    sh.get_cell_mut((1, r)).set_value_number(r as f64);
                    sh.get_cell_mut((2, r))
                        .set_formula(formula_pattern.replace("{row}", &r.to_string()));
                }
                sh.get_cell_mut((3, 1)).set_formula(rollup_formula);
            });
            Ok(())
        }
        "struct_sheet_rename_rebind" => {
            let input_rows = cfg_u32(s, "/sheets/0/rows", 25_000);
            let summary_rows = cfg_u32(s, "/sheets/1/rows", input_rows);
            let summary_formula_pattern =
                cfg_str(s, "/layout/summary_formula_pattern", "=Inputs!A{row}*3");
            let summary_rollup_formula =
                cfg_str(s, "/layout/summary_rollup_formula", "=SUM(A1:A25000)");
            let direct_cross_sheet_rollup_formula = cfg_str(
                s,
                "/layout/direct_cross_sheet_rollup_formula",
                "=SUM(Inputs!A1:A25000)",
            );
            let rebind_probe_formula =
                cfg_str(s, "/layout/rebind_probe_formula", "=Inputs!A12345+A12345");

            write_workbook(output, |book| {
                let _ = book.new_sheet("Inputs");
                let _ = book.new_sheet("Summary");

                let inputs = book.get_sheet_by_name_mut("Inputs").expect("Inputs exists");
                for r in 1..=input_rows {
                    inputs.get_cell_mut((1, r)).set_value_number(r as f64);
                }

                let summary = book
                    .get_sheet_by_name_mut("Summary")
                    .expect("Summary exists");
                for r in 1..=summary_rows {
                    summary
                        .get_cell_mut((1, r))
                        .set_formula(summary_formula_pattern.replace("{row}", &r.to_string()));
                }
                summary
                    .get_cell_mut((2, 1))
                    .set_formula(summary_rollup_formula);
                summary
                    .get_cell_mut((2, 2))
                    .set_formula(rebind_probe_formula);
                summary
                    .get_cell_mut((3, 1))
                    .set_formula(direct_cross_sheet_rollup_formula);
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
        "real_finance_model_v1" => generate_real_finance_model_v1(output),
        "real_ops_model_v1" => generate_real_ops_model_v1(output),
        other => bail!("no generator implemented for scenario id: {other}"),
    }
}

#[cfg(feature = "xlsx")]
fn generate_real_finance_model_v1(output: &Path) -> Result<()> {
    use formualizer_testkit::write_workbook;

    let segments = [
        ("Enterprise", 120.0, 5.0, 2_500.0, 0.34),
        ("SMB", 420.0, 12.0, 950.0, 0.46),
        ("Services", 60.0, 2.0, 4_200.0, 0.58),
    ];

    write_workbook(output, |book| {
        let _ = book.new_sheet("Assumptions");
        let _ = book.new_sheet("Forecast");
        let _ = book.new_sheet("Summary");

        let assumptions = book
            .get_sheet_by_name_mut("Assumptions")
            .expect("Assumptions exists");
        assumptions.get_cell_mut((1, 2)).set_value("Driver");
        assumptions.get_cell_mut((3, 2)).set_value("Value");
        assumptions.get_cell_mut((1, 4)).set_value("TaxRate");
        assumptions.get_cell_mut((3, 4)).set_value_number(0.24);
        assumptions.get_cell_mut((1, 5)).set_value("OpexRatio");
        assumptions.get_cell_mut((3, 5)).set_value_number(0.18);
        assumptions.get_cell_mut((1, 6)).set_value("CapexRatio");
        assumptions.get_cell_mut((3, 6)).set_value_number(0.05);
        assumptions
            .get_cell_mut((1, 7))
            .set_value("WorkingCapitalRatio");
        assumptions.get_cell_mut((3, 7)).set_value_number(0.02);
        assumptions.get_cell_mut((1, 8)).set_value("StartingCash");
        assumptions.get_cell_mut((3, 8)).set_value_number(150_000.0);
        assumptions.get_cell_mut((1, 9)).set_value("DebtPrincipal");
        assumptions.get_cell_mut((3, 9)).set_value_number(600_000.0);
        assumptions
            .get_cell_mut((1, 10))
            .set_value("AnnualInterestRate");
        assumptions.get_cell_mut((3, 10)).set_value_number(0.06);
        assumptions
            .get_cell_mut((1, 11))
            .set_value("MonthlyPrincipalPaydown");
        assumptions.get_cell_mut((3, 11)).set_value_number(25_000.0);
        assumptions
            .get_cell_mut((1, 12))
            .set_value("Year2PriceUplift");
        assumptions.get_cell_mut((3, 12)).set_value_number(0.015);
        assumptions.get_cell_mut((1, 15)).set_value("Segment");
        assumptions.get_cell_mut((2, 15)).set_value("BaseUnits");
        assumptions
            .get_cell_mut((3, 15))
            .set_value("MonthlyUnitAdd");
        assumptions.get_cell_mut((4, 15)).set_value("UnitPrice");
        assumptions.get_cell_mut((5, 15)).set_value("CostPct");
        for (idx, (segment, base_units, monthly_add, unit_price, cost_pct)) in
            segments.iter().enumerate()
        {
            let row = idx as u32 + 16;
            assumptions.get_cell_mut((1, row)).set_value(*segment);
            assumptions
                .get_cell_mut((2, row))
                .set_value_number(*base_units);
            assumptions
                .get_cell_mut((3, row))
                .set_value_number(*monthly_add);
            assumptions
                .get_cell_mut((4, row))
                .set_value_number(*unit_price);
            assumptions
                .get_cell_mut((5, row))
                .set_value_number(*cost_pct);
        }

        let forecast = book
            .get_sheet_by_name_mut("Forecast")
            .expect("Forecast exists");
        for (col, label) in [
            (1_u32, "Month"),
            (2, "FiscalYear"),
            (3, "Quarter"),
            (4, "Enterprise"),
            (5, "EnterpriseRevenue"),
            (6, "EnterpriseGrossProfit"),
            (7, "SMB"),
            (8, "SMBRevenue"),
            (9, "SMBGrossProfit"),
            (10, "Services"),
            (11, "ServicesRevenue"),
            (12, "ServicesGrossProfit"),
            (13, "TotalRevenue"),
            (14, "GrossProfit"),
            (15, "Opex"),
            (16, "EBITDA"),
            (17, "Capex"),
            (18, "WorkingCapital"),
            (19, "Interest"),
            (20, "Principal"),
            (21, "EndingDebt"),
            (22, "PretaxCash"),
            (23, "Taxes"),
            (24, "NetCash"),
            (25, "EndingCash"),
        ] {
            forecast.get_cell_mut((col, 1)).set_value(label);
        }

        for month in 1..=24_u32 {
            let row = month + 1;
            let fiscal_year = if month <= 12 { 2026 } else { 2027 };
            let quarter = format!("Q{}", ((month - 1) % 12) / 3 + 1);

            forecast
                .get_cell_mut((1, row))
                .set_value_number(month as f64);
            forecast
                .get_cell_mut((2, row))
                .set_value_number(fiscal_year as f64);
            forecast.get_cell_mut((3, row)).set_value(quarter);

            for (units_col, revenue_col, gp_col, segment_col) in [
                (4_u32, 5_u32, 6_u32, 'D'),
                (7, 8, 9, 'G'),
                (10, 11, 12, 'J'),
            ] {
                if month == 1 {
                    forecast.get_cell_mut((units_col, row)).set_formula(format!(
                        "=INDEX(Assumptions!$B$16:$B$18,MATCH({segment_col}$1,Assumptions!$A$16:$A$18,0))"
                    ));
                } else {
                    forecast.get_cell_mut((units_col, row)).set_formula(format!(
                        "={segment_col}{prev}+INDEX(Assumptions!$C$16:$C$18,MATCH({segment_col}$1,Assumptions!$A$16:$A$18,0))",
                        prev = row - 1,
                    ));
                }

                forecast.get_cell_mut((revenue_col, row)).set_formula(format!(
                    "={segment_col}{row}*INDEX(Assumptions!$D$16:$D$18,MATCH({segment_col}$1,Assumptions!$A$16:$A$18,0))*IF($B{row}=2026,1,1+Assumptions!$C$12)"
                ));
                forecast.get_cell_mut((gp_col, row)).set_formula(format!(
                    "={revenue_col_name}{row}*(1-INDEX(Assumptions!$E$16:$E$18,MATCH({segment_col}$1,Assumptions!$A$16:$A$18,0)))",
                    revenue_col_name = match revenue_col {
                        5 => "E",
                        8 => "H",
                        11 => "K",
                        _ => unreachable!(),
                    }
                ));
            }

            forecast
                .get_cell_mut((13, row))
                .set_formula(format!("=SUM(E{row},H{row},K{row})"));
            forecast
                .get_cell_mut((14, row))
                .set_formula(format!("=SUM(F{row},I{row},L{row})"));
            forecast
                .get_cell_mut((15, row))
                .set_formula(format!("=M{row}*Assumptions!$C$5"));
            forecast
                .get_cell_mut((16, row))
                .set_formula(format!("=N{row}-O{row}"));
            forecast
                .get_cell_mut((17, row))
                .set_formula(format!("=M{row}*Assumptions!$C$6"));
            forecast
                .get_cell_mut((18, row))
                .set_formula(format!("=M{row}*Assumptions!$C$7"));
            if month == 1 {
                forecast
                    .get_cell_mut((19, row))
                    .set_formula("=Assumptions!$C$9*Assumptions!$C$10/12");
                forecast
                    .get_cell_mut((20, row))
                    .set_formula("=MIN(Assumptions!$C$11,Assumptions!$C$9)");
                forecast
                    .get_cell_mut((21, row))
                    .set_formula("=Assumptions!$C$9-T2");
                forecast
                    .get_cell_mut((25, row))
                    .set_formula("=Assumptions!$C$8+X2");
            } else {
                forecast
                    .get_cell_mut((19, row))
                    .set_formula(format!("=U{}*Assumptions!$C$10/12", row - 1));
                forecast
                    .get_cell_mut((20, row))
                    .set_formula(format!("=MIN(Assumptions!$C$11,U{})", row - 1));
                forecast
                    .get_cell_mut((21, row))
                    .set_formula(format!("=U{}-T{row}", row - 1));
                forecast
                    .get_cell_mut((25, row))
                    .set_formula(format!("=Y{}+X{row}", row - 1));
            }
            forecast
                .get_cell_mut((22, row))
                .set_formula(format!("=P{row}-Q{row}-R{row}-S{row}"));
            forecast
                .get_cell_mut((23, row))
                .set_formula(format!("=IF(V{row}>0,V{row}*Assumptions!$C$4,0)"));
            forecast
                .get_cell_mut((24, row))
                .set_formula(format!("=V{row}-W{row}-T{row}"));
        }

        let summary = book
            .get_sheet_by_name_mut("Summary")
            .expect("Summary exists");
        summary.get_cell_mut((1, 1)).set_value("Metric");
        summary.get_cell_mut((2, 1)).set_value_number(2026.0);
        summary.get_cell_mut((3, 1)).set_value_number(2027.0);
        for (row, metric) in [
            (2_u32, "Revenue"),
            (3, "GrossProfit"),
            (4, "EBITDA"),
            (5, "Taxes"),
            (6, "NetCash"),
            (7, "EndingCash"),
            (8, "DSCR"),
            (9, "CovenantPass"),
            (10, "EndingDebt"),
            (11, "CashConversion"),
        ] {
            summary.get_cell_mut((1, row)).set_value(metric);
        }

        for (col, year_cell) in [(2_u32, 'B'), (3_u32, 'C')] {
            summary.get_cell_mut((col, 2)).set_formula(format!(
                "=SUMIFS(Forecast!$M$2:$M$25,Forecast!$B$2:$B$25,{year_cell}$1)"
            ));
            summary.get_cell_mut((col, 3)).set_formula(format!(
                "=SUMIFS(Forecast!$N$2:$N$25,Forecast!$B$2:$B$25,{year_cell}$1)"
            ));
            summary.get_cell_mut((col, 4)).set_formula(format!(
                "=SUMIFS(Forecast!$P$2:$P$25,Forecast!$B$2:$B$25,{year_cell}$1)"
            ));
            summary.get_cell_mut((col, 5)).set_formula(format!(
                "=SUMIFS(Forecast!$W$2:$W$25,Forecast!$B$2:$B$25,{year_cell}$1)"
            ));
            summary.get_cell_mut((col, 6)).set_formula(format!(
                "=SUMIFS(Forecast!$X$2:$X$25,Forecast!$B$2:$B$25,{year_cell}$1)"
            ));
            summary.get_cell_mut((col, 7)).set_formula(if col == 2 {
                "=Forecast!Y13".to_string()
            } else {
                "=Forecast!Y25".to_string()
            });
            summary.get_cell_mut((col, 8)).set_formula(format!(
                "=SUMIFS(Forecast!$P$2:$P$25,Forecast!$B$2:$B$25,{year_cell}$1)/(SUMIFS(Forecast!$S$2:$S$25,Forecast!$B$2:$B$25,{year_cell}$1)+SUMIFS(Forecast!$T$2:$T$25,Forecast!$B$2:$B$25,{year_cell}$1))"
            ));
            summary
                .get_cell_mut((col, 9))
                .set_formula(format!("=IF({year_cell}8>1.5,TRUE,FALSE)"));
            summary.get_cell_mut((col, 10)).set_formula(if col == 2 {
                "=Forecast!U13".to_string()
            } else {
                "=Forecast!U25".to_string()
            });
            summary
                .get_cell_mut((col, 11))
                .set_formula(format!("={year_cell}6/{year_cell}4"));
        }
    });

    Ok(())
}

#[cfg(feature = "xlsx")]
fn generate_real_ops_model_v1(output: &Path) -> Result<()> {
    use formualizer_testkit::write_workbook;

    let sites = ["Denver", "Phoenix", "Austin", "Boise"];
    let queues = [
        ("Install", "Field", 185.0, 92.0, 240.0),
        ("BreakFix", "Rapid", 210.0, 110.0, 200.0),
        ("Audit", "Compliance", 165.0, 80.0, 160.0),
        ("Depot", "Bench", 140.0, 70.0, 180.0),
    ];
    let priorities = [
        ("Critical", 4.0, 1.35),
        ("Expedited", 8.0, 1.15),
        ("Standard", 24.0, 1.0),
    ];
    let statuses = ["Open", "Scheduled", "Closed"];

    write_workbook(output, |book| {
        let _ = book.new_sheet("Assumptions");
        let _ = book.new_sheet("QueueConfig");
        let _ = book.new_sheet("PriorityConfig");
        let _ = book.new_sheet("WorkOrders");
        let _ = book.new_sheet("Dashboard");
        let _ = book.new_sheet("Summary");

        let assumptions = book
            .get_sheet_by_name_mut("Assumptions")
            .expect("Assumptions exists");
        assumptions
            .get_cell_mut((1, 4))
            .set_value("BacklogTargetHours");
        assumptions.get_cell_mut((3, 4)).set_value_number(160.0);
        assumptions
            .get_cell_mut((1, 5))
            .set_value("EscalationThreshold");
        assumptions.get_cell_mut((3, 5)).set_value_number(0.9);
        assumptions
            .get_cell_mut((1, 6))
            .set_value("LaborCostInflator");
        assumptions.get_cell_mut((3, 6)).set_value_number(1.08);

        let queue_config = book
            .get_sheet_by_name_mut("QueueConfig")
            .expect("QueueConfig exists");
        for (col, label) in [
            (1_u32, "Queue"),
            (2, "Team"),
            (3, "BillRate"),
            (4, "LaborRate"),
            (5, "WeeklyCapacity"),
        ] {
            queue_config.get_cell_mut((col, 9)).set_value(label);
        }
        for (idx, (queue, team, bill_rate, labor_rate, capacity)) in queues.iter().enumerate() {
            let row = idx as u32 + 10;
            queue_config.get_cell_mut((1, row)).set_value(*queue);
            queue_config.get_cell_mut((2, row)).set_value(*team);
            queue_config
                .get_cell_mut((3, row))
                .set_value_number(*bill_rate);
            queue_config
                .get_cell_mut((4, row))
                .set_value_number(*labor_rate);
            queue_config
                .get_cell_mut((5, row))
                .set_value_number(*capacity);
        }

        let priority_config = book
            .get_sheet_by_name_mut("PriorityConfig")
            .expect("PriorityConfig exists");
        for (col, label) in [
            (1_u32, "Priority"),
            (2, "ResponseHours"),
            (3, "LaborMultiplier"),
        ] {
            priority_config.get_cell_mut((col, 17)).set_value(label);
        }
        for (idx, (priority, response_hours, multiplier)) in priorities.iter().enumerate() {
            let row = idx as u32 + 18;
            priority_config.get_cell_mut((1, row)).set_value(*priority);
            priority_config
                .get_cell_mut((2, row))
                .set_value_number(*response_hours);
            priority_config
                .get_cell_mut((3, row))
                .set_value_number(*multiplier);
        }

        let work_orders = book
            .get_sheet_by_name_mut("WorkOrders")
            .expect("WorkOrders exists");
        for (col, label) in [
            (1_u32, "OrderId"),
            (2, "Site"),
            (3, "Queue"),
            (4, "Priority"),
            (5, "Status"),
            (6, "EstHours"),
            (7, "MaterialCost"),
            (8, "BillRate"),
            (9, "LaborRate"),
            (10, "LaborMultiplier"),
            (11, "ResponseHours"),
            (12, "BillableValue"),
            (13, "LaborCost"),
            (14, "GrossMargin"),
            (15, "SLARisk"),
            (16, "BacklogHours"),
            (17, "Team"),
        ] {
            work_orders.get_cell_mut((col, 1)).set_value(label);
        }

        for i in 0..1_500_u32 {
            let row = i + 2;
            let site = sites[(i as usize) % sites.len()];
            let queue_idx = ((i / 4) as usize) % queues.len();
            let priority_idx = ((i / 16) as usize) % priorities.len();
            let status_idx = ((i / 48) as usize) % statuses.len();
            let queue = queues[queue_idx].0;
            let priority = priorities[priority_idx].0;
            let status = statuses[status_idx];
            let est_hours = 2.0 + ((i % 8) * 2) as f64 + queue_idx as f64;
            let material_cost = 40.0 + ((i % 9) * 15) as f64 + ((i % 4) * 5) as f64;

            work_orders
                .get_cell_mut((1, row))
                .set_value(format!("WO{:05}", i + 1));
            work_orders.get_cell_mut((2, row)).set_value(site);
            work_orders.get_cell_mut((3, row)).set_value(queue);
            work_orders.get_cell_mut((4, row)).set_value(priority);
            work_orders.get_cell_mut((5, row)).set_value(status);
            work_orders
                .get_cell_mut((6, row))
                .set_value_number(est_hours);
            work_orders
                .get_cell_mut((7, row))
                .set_value_number(material_cost);
            work_orders.get_cell_mut((8, row)).set_formula(format!(
                "=INDEX(QueueConfig!$C$10:$C$13,MATCH(C{row},QueueConfig!$A$10:$A$13,0))"
            ));
            work_orders.get_cell_mut((9, row)).set_formula(format!(
                "=INDEX(QueueConfig!$D$10:$D$13,MATCH(C{row},QueueConfig!$A$10:$A$13,0))"
            ));
            work_orders.get_cell_mut((10, row)).set_formula(format!(
                "=INDEX(PriorityConfig!$C$18:$C$20,MATCH(D{row},PriorityConfig!$A$18:$A$20,0))*Assumptions!$C$6"
            ));
            work_orders.get_cell_mut((11, row)).set_formula(format!(
                "=INDEX(PriorityConfig!$B$18:$B$20,MATCH(D{row},PriorityConfig!$A$18:$A$20,0))"
            ));
            work_orders
                .get_cell_mut((12, row))
                .set_formula(format!("=F{row}*H{row}"));
            work_orders
                .get_cell_mut((13, row))
                .set_formula(format!("=F{row}*I{row}*J{row}"));
            work_orders
                .get_cell_mut((14, row))
                .set_formula(format!("=L{row}-M{row}-G{row}"));
            work_orders
                .get_cell_mut((15, row))
                .set_formula(format!("=IF(F{row}>K{row},1,0)"));
            work_orders
                .get_cell_mut((16, row))
                .set_formula(format!("=IF(E{row}=\"Closed\",0,F{row})"));
            work_orders.get_cell_mut((17, row)).set_formula(format!(
                "=INDEX(QueueConfig!$B$10:$B$13,MATCH(C{row},QueueConfig!$A$10:$A$13,0))"
            ));
        }

        let dashboard = book
            .get_sheet_by_name_mut("Dashboard")
            .expect("Dashboard exists");
        for (col, label) in [
            (1_u32, "Site"),
            (2, "Queue"),
            (3, "BacklogHours"),
            (4, "OpenOrders"),
            (5, "GrossMargin"),
            (6, "SLARiskCount"),
            (7, "Capacity"),
            (8, "StaffingGap"),
        ] {
            dashboard.get_cell_mut((col, 1)).set_value(label);
        }
        let mut dashboard_row = 2_u32;
        for site in sites {
            for (queue, _, _, _, _) in queues {
                dashboard.get_cell_mut((1, dashboard_row)).set_value(site);
                dashboard.get_cell_mut((2, dashboard_row)).set_value(queue);
                dashboard.get_cell_mut((3, dashboard_row)).set_formula(format!(
                    "=SUMIFS(WorkOrders!$P$2:$P$1501,WorkOrders!$B$2:$B$1501,A{dashboard_row},WorkOrders!$C$2:$C$1501,B{dashboard_row})"
                ));
                dashboard.get_cell_mut((4, dashboard_row)).set_formula(format!(
                    "=COUNTIFS(WorkOrders!$B$2:$B$1501,A{dashboard_row},WorkOrders!$C$2:$C$1501,B{dashboard_row},WorkOrders!$E$2:$E$1501,\"<>Closed\")"
                ));
                dashboard.get_cell_mut((5, dashboard_row)).set_formula(format!(
                    "=SUMIFS(WorkOrders!$N$2:$N$1501,WorkOrders!$B$2:$B$1501,A{dashboard_row},WorkOrders!$C$2:$C$1501,B{dashboard_row})"
                ));
                dashboard.get_cell_mut((6, dashboard_row)).set_formula(format!(
                    "=SUMIFS(WorkOrders!$O$2:$O$1501,WorkOrders!$B$2:$B$1501,A{dashboard_row},WorkOrders!$C$2:$C$1501,B{dashboard_row},WorkOrders!$E$2:$E$1501,\"<>Closed\")"
                ));
                dashboard.get_cell_mut((7, dashboard_row)).set_formula(format!(
                    "=INDEX(QueueConfig!$E$10:$E$13,MATCH(B{dashboard_row},QueueConfig!$A$10:$A$13,0))"
                ));
                dashboard
                    .get_cell_mut((8, dashboard_row))
                    .set_formula(format!("=C{dashboard_row}-G{dashboard_row}"));
                dashboard_row += 1;
            }
        }

        let summary = book
            .get_sheet_by_name_mut("Summary")
            .expect("Summary exists");
        summary.get_cell_mut((1, 2)).set_value("TotalBacklog");
        summary
            .get_cell_mut((2, 2))
            .set_formula("=SUM(Dashboard!C2:C17)");
        summary.get_cell_mut((1, 3)).set_value("TotalMargin");
        summary
            .get_cell_mut((2, 3))
            .set_formula("=SUM(Dashboard!E2:E17)");
        summary.get_cell_mut((1, 4)).set_value("TotalRisk");
        summary
            .get_cell_mut((2, 4))
            .set_formula("=SUM(Dashboard!F2:F17)");
        summary.get_cell_mut((1, 5)).set_value("WorstGap");
        summary
            .get_cell_mut((2, 5))
            .set_formula("=MAX(Dashboard!H2:H17)");
        summary
            .get_cell_mut((1, 6))
            .set_value("DenverInstallBacklog");
        summary.get_cell_mut((2, 6)).set_formula("=Dashboard!C2");
        summary.get_cell_mut((1, 7)).set_value("DenverDepotMargin");
        summary.get_cell_mut((2, 7)).set_formula("=Dashboard!E5");
    });

    Ok(())
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
