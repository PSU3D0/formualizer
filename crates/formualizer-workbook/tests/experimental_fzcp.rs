#![cfg(feature = "experimental-fzcp")]

use std::io::{Cursor, Write};
use std::sync::atomic::AtomicBool;

use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue, RangeAddress};
use formualizer_eval::engine::{CycleConfig, DateSystem};
#[cfg(feature = "umya")]
use formualizer_workbook::UmyaAdapter;
use formualizer_workbook::experimental_fzcp::{
    AdmissionDecision, BackendKind, DefinedNameSourceKind, DiscoveryError, DiscoveryLimits,
    PackageBuildOptions, PackageExpectation, PackageLimits, build_calculation_package,
    canonical_report_bytes, inspect_xlsx_source, load_calculation_package, manifest_digest,
};
#[cfg(feature = "umya")]
use formualizer_workbook::traits::SpreadsheetReader;
use formualizer_workbook::traits::{
    CellData, DefinedName, DefinedNameDefinition, DefinedNameScope, LoadStrategy,
    SpreadsheetWriter, TableDefinition,
};
use formualizer_workbook::{JsonAdapter, StoredCellValue, Workbook, WorkbookConfig};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

fn zip(entries: &[(&str, &str)], compression: CompressionMethod) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(compression);
    for (name, contents) in entries {
        writer.start_file(*name, options).unwrap();
        writer.write_all(contents.as_bytes()).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn workbook_xml(extra: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <workbookPr date1904="1"/>
  <sheets><sheet name="Visible" sheetId="1" r:id="rSheet1"/><sheet name="Hidden" sheetId="2" state="hidden" r:id="rSheet2"/></sheets>
  <definedNames>
    <definedName name="Good">Visible!$A$1:$A$2</definedName>
    <definedName name="Local" localSheetId="0">42</definedName>
    <definedName name="FormulaName">SUM(Visible!$A$1:$A$2)</definedName>
    <definedName name="ListName">Visible!$A$1,Visible!$A$2</definedName>
  </definedNames>{extra}
</workbook>"#
    )
}

const SHEET_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1" hidden="1"><c r="A1"><f>1+1</f><v>2</v></c></row>
    <row r="2"><c r="A2"><f>[1]Other!A1</f></c></row>
  </sheetData>
</worksheet>"#;

const CONTENT_TYPES: &str = r#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/></Types>"#;
const ROOT_RELS: &str = r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="root" Target="xl/workbook.xml" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"/></Relationships>"#;
const EMPTY_SHEET_XML: &str = r#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData/></worksheet>"#;

const MANIFEST_A: &str =
    "spec: fio\nspec_version: 0.3.0\nmanifest: {name: Spike, id: spike}\nports: []\n";
const MANIFEST_B: &str =
    "ports: []\nmanifest: {id: spike, name: Spike}\nspec_version: 0.3.0\nspec: fio\n";

#[test]
fn s0_reports_pre_adapter_facts_and_typed_refusals_deterministically() {
    let rels = r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rSheet1" Target="worksheets/sheet1.xml" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"/><Relationship Id="rSheet2" Target="worksheets/sheet2.xml" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"/><Relationship Id="r1" Target="https://example.test/book.xlsx" TargetMode="External" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLink"/></Relationships>"#;
    let workbook = workbook_xml("");
    let entries = [
        ("[Content_Types].xml", CONTENT_TYPES),
        ("_rels/.rels", ROOT_RELS),
        ("xl/worksheets/sheet1.xml", SHEET_XML),
        ("xl/worksheets/sheet2.xml", EMPTY_SHEET_XML),
        ("xl/workbook.xml", workbook.as_str()),
        ("xl/_rels/workbook.xml.rels", rels),
        ("xl/vbaProject.bin", "macro"),
        ("xl/externalLinks/externalLink1.xml", "<externalLink/>"),
        ("xl/connections.xml", "<connections/>"),
        ("xl/pivotTables/pivotTable1.xml", "<pivotTableDefinition/>"),
    ];
    let source = zip(&entries, CompressionMethod::Stored);
    let report = inspect_xlsx_source(
        &source,
        MANIFEST_A.as_bytes(),
        BackendKind::Calamine,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();

    assert!(report.date_1904);
    assert_eq!(report.formula_cells, 2);
    assert_eq!(report.formula_cells_with_cached_results, 1);
    assert_eq!(report.external_formula_references, 1);
    assert!(report.all_entry_paths_safe);
    assert_eq!(report.hidden_sheets, ["Hidden"]);
    assert_eq!(report.source_hidden_rows, 1);
    assert_eq!(report.defined_names.len(), 4);
    assert!(report.defined_names.iter().any(|name| {
        name.name == "FormulaName" && name.kind == DefinedNameSourceKind::UnsupportedFormula
    }));
    assert!(report.defined_names.iter().any(|name| {
        name.name == "ListName" && name.kind == DefinedNameSourceKind::UnsupportedList
    }));
    assert_eq!(report.macros_or_vba_parts, ["xl/vbaProject.bin"]);
    assert_eq!(
        report.external_relationship_targets,
        ["https://example.test/book.xlsx"]
    );
    let AdmissionDecision::Refused { reasons } = &report.decision else {
        panic!("active/lossy source must be refused");
    };
    let codes = reasons
        .iter()
        .map(|reason| reason.code.as_str())
        .collect::<Vec<_>>();
    assert!(codes.contains(&"date1904_not_transportable"));
    assert!(codes.contains(&"unsupported_defined_names"));
    assert!(codes.contains(&"literal_defined_names_lost"));
    assert!(codes.contains(&"macros_or_vba"));
    assert!(codes.contains(&"external_links"));
    assert!(codes.contains(&"connections"));
    assert!(codes.contains(&"unmodeled_active_parts"));

    let reordered = zip(
        &entries.iter().copied().rev().collect::<Vec<_>>(),
        CompressionMethod::Stored,
    );
    let report_reordered = inspect_xlsx_source(
        &reordered,
        MANIFEST_B.as_bytes(),
        BackendKind::Calamine,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(
        report.source_digest_sha256,
        report_reordered.source_digest_sha256
    );
    assert_eq!(
        report.manifest_digest_sha256,
        report_reordered.manifest_digest_sha256
    );
    assert_eq!(
        canonical_report_bytes(&report).unwrap(),
        canonical_report_bytes(&report_reordered).unwrap(),
        "independently inspected equivalent archives produce identical reports"
    );
}

#[test]
fn s0_fails_closed_for_paths_ratios_counts_and_cancellation() {
    let unsafe_zip = zip(&[("../evil.xml", "x")], CompressionMethod::Stored);
    assert!(matches!(
        inspect_xlsx_source(
            &unsafe_zip,
            MANIFEST_A.as_bytes(),
            BackendKind::Calamine,
            &DiscoveryLimits::default(),
            None,
        ),
        Err(DiscoveryError::UnsafePath { .. })
    ));

    let many = zip(&[("a", "1"), ("b", "2")], CompressionMethod::Stored);
    let count_limits = DiscoveryLimits {
        max_entries: 1,
        ..DiscoveryLimits::default()
    };
    assert!(matches!(
        inspect_xlsx_source(
            &many,
            MANIFEST_A.as_bytes(),
            BackendKind::Calamine,
            &count_limits,
            None,
        ),
        Err(DiscoveryError::EntryCount { .. })
    ));

    let repeated = "0".repeat(32 * 1024);
    let compressed = zip(
        &[("xl/workbook.xml", &repeated)],
        CompressionMethod::Deflated,
    );
    let ratio_limits = DiscoveryLimits {
        max_expansion_ratio: 2,
        ..DiscoveryLimits::default()
    };
    assert!(matches!(
        inspect_xlsx_source(
            &compressed,
            MANIFEST_A.as_bytes(),
            BackendKind::Calamine,
            &ratio_limits,
            None,
        ),
        Err(DiscoveryError::ExpansionRatio { .. })
    ));

    let manifest_limits = DiscoveryLimits {
        max_manifest_bytes: (MANIFEST_A.len() - 1) as u64,
        ..DiscoveryLimits::default()
    };
    assert!(matches!(
        inspect_xlsx_source(
            &many,
            MANIFEST_A.as_bytes(),
            BackendKind::Calamine,
            &manifest_limits,
            None,
        ),
        Err(DiscoveryError::ManifestBytes { .. })
    ));

    let cancelled = AtomicBool::new(true);
    assert_eq!(
        inspect_xlsx_source(
            &many,
            MANIFEST_A.as_bytes(),
            BackendKind::Calamine,
            &DiscoveryLimits::default(),
            Some(&cancelled),
        ),
        Err(DiscoveryError::Cancelled)
    );
}

#[test]
fn s0_reports_table_parts_and_refuses_only_lossy_backends() {
    let workbook = r#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="sheet"/></sheets></workbook>"#;
    let workbook_rels = r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="sheet" Target="worksheets/sheet1.xml" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"/></Relationships>"#;
    let sheet = r#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1"><f>SUM(Table1[Amount])</f></c></row></sheetData></worksheet>"#;
    let sheet_rels = r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="table" Target="../tables/table1.xml" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/table"/></Relationships>"#;
    let table = r#"<table xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" id="1" name="Table1" displayName="Table1" ref="A1:A2"/>"#;
    let entries = [
        ("[Content_Types].xml", CONTENT_TYPES),
        ("_rels/.rels", ROOT_RELS),
        ("xl/workbook.xml", workbook),
        ("xl/_rels/workbook.xml.rels", workbook_rels),
        ("xl/worksheets/sheet1.xml", sheet),
        ("xl/worksheets/_rels/sheet1.xml.rels", sheet_rels),
        ("xl/tables/table1.xml", table),
    ];
    let source = zip(&entries, CompressionMethod::Stored);

    let calamine = inspect_xlsx_source(
        &source,
        MANIFEST_A.as_bytes(),
        BackendKind::Calamine,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(calamine.table_part_count, 1);
    assert_eq!(calamine.table_parts, ["xl/tables/table1.xml"]);
    let AdmissionDecision::Refused { reasons } = calamine.decision else {
        panic!("Calamine must refuse a source whose tables it drops");
    };
    assert_eq!(reasons.len(), 1, "unexpected refusal reasons: {reasons:#?}");
    assert_eq!(reasons[0].code, "tables_lost");

    let umya = inspect_xlsx_source(
        &source,
        MANIFEST_A.as_bytes(),
        BackendKind::Umya,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(umya.table_part_count, 1);
    assert_eq!(umya.table_parts, ["xl/tables/table1.xml"]);
    assert_eq!(umya.decision, AdmissionDecision::Admitted);
}

fn modeled_workbook() -> Workbook {
    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Inputs").unwrap();
    adapter.create_sheet("Calc").unwrap();
    adapter.set_dimensions("Inputs", Some((8, 3)));
    adapter.set_dimensions("Calc", Some((4, 4)));
    adapter.set_date_system_1904("Inputs", true);
    adapter
        .write_cell("Inputs", 1, 1, CellData::from_value(LiteralValue::Int(7)))
        .unwrap();
    adapter
        .write_cell("Inputs", 2, 1, CellData::from_value(-0.0f64))
        .unwrap();
    adapter
        .write_cell("Inputs", 3, 1, CellData::from_value(true))
        .unwrap();
    adapter
        .write_cell("Inputs", 4, 1, CellData::from_value(""))
        .unwrap();
    adapter
        .write_cell(
            "Inputs",
            5,
            1,
            CellData::from_value(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))),
        )
        .unwrap();
    adapter
        .write_cell(
            "Inputs",
            6,
            1,
            CellData::from_value(LiteralValue::DateTime(
                chrono::NaiveDate::from_ymd_opt(2026, 7, 19)
                    .unwrap()
                    .and_hms_opt(12, 34, 56)
                    .unwrap(),
            )),
        )
        .unwrap();
    adapter
        .write_cell(
            "Inputs",
            7,
            1,
            CellData::from_value(LiteralValue::Duration(chrono::Duration::seconds(90))),
        )
        .unwrap();
    adapter
        .write_cell("Calc", 1, 2, CellData::from_formula("=Inputs!A1+2"))
        .unwrap();
    adapter
        .write_cell("Calc", 2, 2, CellData::from_formula("=SUM(Inputs!A1:A3)"))
        .unwrap();
    adapter.set_tables(
        "Inputs",
        vec![TableDefinition {
            name: "InputTable".to_string(),
            range: (1, 1, 5, 2),
            header_row: true,
            headers: vec!["Value".to_string(), "Label".to_string()],
            totals_row: true,
        }],
    );
    adapter.set_defined_names(vec![
        DefinedName {
            name: "InputRange".to_string(),
            scope: DefinedNameScope::Workbook,
            scope_sheet: None,
            definition: DefinedNameDefinition::Range {
                address: RangeAddress::new("Inputs", 1, 1, 3, 1).unwrap(),
            },
        },
        DefinedName {
            name: "InputRange".to_string(),
            scope: DefinedNameScope::Sheet,
            scope_sheet: Some("Calc".to_string()),
            definition: DefinedNameDefinition::Range {
                address: RangeAddress::new("Calc", 1, 2, 2, 2).unwrap(),
            },
        },
        DefinedName {
            name: "LocalLiteral".to_string(),
            scope: DefinedNameScope::Sheet,
            scope_sheet: Some("Calc".to_string()),
            definition: DefinedNameDefinition::Literal {
                value: LiteralValue::Int(11),
            },
        },
    ]);

    let mut config = WorkbookConfig::interactive();
    config.eval.date_system = DateSystem::Excel1904;
    config.eval.cycle = CycleConfig::iterate(25, 0.000_01);
    let mut workbook = Workbook::from_reader(adapter, LoadStrategy::EagerAll, config).unwrap();
    workbook.set_row_hidden("Inputs", 3, true).unwrap();
    workbook
}

fn mutate_package(bytes: &[u8], mutate: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
    const HEADER: &[u8] = b"FZCP_S1_EXPERIMENTAL\n";
    let mut document: serde_json::Value =
        serde_json::from_slice(bytes.strip_prefix(HEADER).unwrap()).unwrap();
    mutate(&mut document);
    let mut output = HEADER.to_vec();
    output.extend(serde_json::to_vec(&document).unwrap());
    output.push(b'\n');
    output
}

fn build(workbook: &Workbook, manifest: &serde_json::Value) -> Vec<u8> {
    build_calculation_package(
        workbook,
        PackageBuildOptions {
            source_digest_sha256: "source-digest",
            manifest,
            target_metadata: &serde_json::json!({"outputs":["Calc!B1","Calc!B2"]}),
            backend: BackendKind::Umya,
            limits: PackageLimits::default(),
            cancel: None,
        },
    )
    .unwrap()
}

#[test]
fn s1_typed_accessor_and_whole_workbook_package_are_deterministic_and_equivalent() {
    let manifest = serde_json::json!({"ports":[],"spec":"fio","spec_version":"0.3.0"});
    let mut source = modeled_workbook();

    assert_eq!(
        source.export_stored_cell("Inputs", 1, 1),
        Some(StoredCellValue::NumberBits(7.0f64.to_bits())),
        "Arrow ingest normalizes Int to Number"
    );
    assert_eq!(
        source.export_stored_cell("Inputs", 2, 1),
        Some(StoredCellValue::NumberBits((-0.0f64).to_bits()))
    );
    assert_eq!(
        source.export_stored_cell("Inputs", 4, 1),
        Some(StoredCellValue::Text(String::new())),
        "stored empty text survives while an absent cell returns None"
    );
    assert_eq!(
        source.export_stored_cell("Inputs", 5, 1),
        Some(StoredCellValue::ErrorCode(
            formualizer_eval::arrow_store::map_error_code(ExcelErrorKind::Na)
        ))
    );
    assert_eq!(source.export_stored_cell("Inputs", 8, 3), None);
    let source_datetime = source.export_stored_cell("Inputs", 6, 1);
    let source_duration = source.export_stored_cell("Inputs", 7, 1);
    assert!(matches!(
        &source_datetime,
        Some(StoredCellValue::DateTimeBits(_))
    ));
    assert!(matches!(
        &source_duration,
        Some(StoredCellValue::DurationBits(_))
    ));

    let before_eval = build(&source, &manifest);
    assert_eq!(
        before_eval,
        build(&modeled_workbook(), &manifest),
        "independently constructed workbooks must build byte-for-byte identically"
    );
    source.evaluate_all().unwrap();
    assert_eq!(
        source.get_value("Calc", 1, 2),
        Some(LiteralValue::Number(9.0))
    );
    let after_eval = build(&source, &manifest);
    assert_eq!(
        before_eval, after_eval,
        "computed formula overlays/cached results are excluded"
    );

    let expected_manifest_digest = manifest_digest(&manifest).unwrap();
    let source_sheet_names = source.sheet_names();
    let mut loaded = load_calculation_package(
        &before_eval,
        PackageExpectation {
            source_digest_sha256: Some("source-digest"),
            manifest_digest_sha256: Some(&expected_manifest_digest),
        },
    )
    .unwrap();
    assert_eq!(loaded.manifest, manifest);
    assert_eq!(
        loaded.target_metadata,
        serde_json::json!({"outputs":["Calc!B1","Calc!B2"]})
    );
    assert_eq!(loaded.workbook.sheet_names(), source_sheet_names);
    assert_eq!(loaded.workbook.sheet_dimensions("Inputs"), Some((8, 3)));
    assert_eq!(loaded.workbook.sheet_dimensions("Calc"), Some((4, 4)));
    assert_eq!(
        loaded.workbook.export_stored_cell("Inputs", 2, 1),
        Some(StoredCellValue::NumberBits((-0.0f64).to_bits()))
    );
    assert_eq!(
        loaded.workbook.export_stored_cell("Inputs", 4, 1),
        Some(StoredCellValue::Text(String::new()))
    );
    assert_eq!(
        loaded.workbook.export_stored_cell("Inputs", 6, 1),
        source_datetime
    );
    assert_eq!(
        loaded.workbook.export_stored_cell("Inputs", 7, 1),
        source_duration
    );
    assert_eq!(
        loaded.workbook.get_formula("Calc", 1, 2).as_deref(),
        Some("=Inputs!A1 + 2")
    );
    assert_eq!(
        loaded.workbook.named_range_address("InputRange"),
        Some(RangeAddress::new("Inputs", 1, 1, 3, 1).unwrap())
    );
    assert!(loaded.workbook.has_name("InputRange", Some("Calc")));
    assert!(loaded.workbook.has_name("LocalLiteral", Some("Calc")));
    let table = loaded.workbook.table_metadata("InputTable").unwrap();
    assert_eq!(
        (
            table.start_row,
            table.start_col,
            table.end_row,
            table.end_col
        ),
        (1, 1, 5, 2)
    );
    assert!(table.header_row);
    assert!(table.totals_row);
    assert_eq!(table.headers, ["Value", "Label"]);
    assert!(loaded.workbook.is_row_hidden("Inputs", 3).unwrap());
    assert_eq!(
        loaded.workbook.eval_config().date_system,
        DateSystem::Excel1904
    );
    assert_eq!(
        loaded.workbook.eval_config().cycle,
        CycleConfig::iterate(25, 0.000_01)
    );
    assert_eq!(
        build(&loaded.workbook, &manifest),
        before_eval,
        "load followed by a fresh build preserves the canonical modeled state"
    );

    loaded.workbook.evaluate_all().unwrap();
    for (row, expected) in [(1, 9.0), (2, 7.0)] {
        assert_eq!(
            loaded.workbook.get_value("Calc", row, 2),
            Some(LiteralValue::Number(expected))
        );
        assert_eq!(
            loaded.workbook.get_value("Calc", row, 2),
            source.get_value("Calc", row, 2),
            "source-load-evaluate and package-load-evaluate must match"
        );
    }
}

#[test]
fn s1_rejects_stale_binding_and_never_emits_on_exhaustion() {
    let workbook = modeled_workbook();
    let manifest = serde_json::json!({"ports":[]});
    let error = build_calculation_package(
        &workbook,
        PackageBuildOptions {
            source_digest_sha256: "source",
            manifest: &manifest,
            target_metadata: &serde_json::Value::Null,
            backend: BackendKind::Umya,
            limits: PackageLimits {
                max_logical_cells: 1,
                ..PackageLimits::default()
            },
            cancel: None,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("resource limit"));

    let mut volatile = Workbook::new();
    volatile.set_formula("Sheet1", 1, 1, "=NOW()").unwrap();
    let volatile_error = build_calculation_package(
        &volatile,
        PackageBuildOptions {
            source_digest_sha256: "source",
            manifest: &manifest,
            target_metadata: &serde_json::Value::Null,
            backend: BackendKind::Umya,
            limits: PackageLimits::default(),
            cancel: None,
        },
    )
    .unwrap_err();
    assert!(volatile_error.to_string().contains("volatile function"));

    let bytes = build(&workbook, &manifest);
    let error = load_calculation_package(
        &bytes,
        PackageExpectation {
            source_digest_sha256: Some("different"),
            manifest_digest_sha256: None,
        },
    )
    .err()
    .expect("stale binding must fail");
    assert!(error.to_string().contains("stale package binding"));
}

#[test]
fn s0_uses_relationships_allow_lists_and_strict_xml() {
    let workbook = r#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="sheet"/></sheets></workbook>"#;
    let workbook_rels = r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="sheet" Target="worksheets/sheet1.xml" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"/></Relationships>"#;
    let structured_sheet = r#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1"><f>SUM(Table1[Amount])</f></c><c r="A2"><f>SUM(Table1[2024])</f></c></row></sheetData></worksheet>"#;
    let entries = [
        ("[Content_Types].xml", CONTENT_TYPES),
        ("_rels/.rels", ROOT_RELS),
        ("xl/workbook.xml", workbook),
        ("xl/_rels/workbook.xml.rels", workbook_rels),
        ("xl/worksheets/sheet1.xml", structured_sheet),
    ];
    let admitted = inspect_xlsx_source(
        &zip(&entries, CompressionMethod::Stored),
        MANIFEST_A.as_bytes(),
        BackendKind::Umya,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(admitted.external_formula_references, 0);
    assert_eq!(admitted.decision, AdmissionDecision::Admitted);

    let relocated_root_rels = ROOT_RELS.replace("xl/workbook.xml", "parts/book.xml");
    let relocated_workbook_rels =
        workbook_rels.replace("worksheets/sheet1.xml", "../sheet-parts/one.xml");
    let relocated_entries = [
        ("[Content_Types].xml", CONTENT_TYPES),
        ("_rels/.rels", relocated_root_rels.as_str()),
        ("parts/book.xml", workbook),
        (
            "parts/_rels/book.xml.rels",
            relocated_workbook_rels.as_str(),
        ),
        ("sheet-parts/one.xml", structured_sheet),
    ];
    let relocated = inspect_xlsx_source(
        &zip(&relocated_entries, CompressionMethod::Stored),
        MANIFEST_A.as_bytes(),
        BackendKind::Umya,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(relocated.formula_cells, 2);
    assert_eq!(relocated.decision, AdmissionDecision::Admitted);

    let mut unknown = entries.to_vec();
    unknown.push(("customXml/item1.xml", "<root/>"));
    let report = inspect_xlsx_source(
        &zip(&unknown, CompressionMethod::Stored),
        MANIFEST_A.as_bytes(),
        BackendKind::Umya,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(report.unmodeled_active_parts, ["customXml/item1.xml"]);
    assert!(matches!(report.decision, AdmissionDecision::Refused { .. }));

    let missing_relationships = zip(
        &[
            ("[Content_Types].xml", CONTENT_TYPES),
            ("xl/workbook.xml", workbook),
        ],
        CompressionMethod::Stored,
    );
    let report = inspect_xlsx_source(
        &missing_relationships,
        MANIFEST_A.as_bytes(),
        BackendKind::Umya,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    let AdmissionDecision::Refused { reasons } = report.decision else {
        panic!("missing root relationships must refuse");
    };
    assert!(
        reasons
            .iter()
            .any(|reason| reason.code == "invalid_ooxml_relationships")
    );

    let escaping_rels = workbook_rels.replace("worksheets/sheet1.xml", "../../escape.xml");
    let escaping_entries = [
        ("[Content_Types].xml", CONTENT_TYPES),
        ("_rels/.rels", ROOT_RELS),
        ("xl/workbook.xml", workbook),
        ("xl/_rels/workbook.xml.rels", escaping_rels.as_str()),
        ("xl/worksheets/sheet1.xml", structured_sheet),
    ];
    let escaping = inspect_xlsx_source(
        &zip(&escaping_entries, CompressionMethod::Stored),
        MANIFEST_A.as_bytes(),
        BackendKind::Umya,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    assert!(matches!(
        escaping.decision,
        AdmissionDecision::Refused { .. }
    ));

    let malformed_sheet = structured_sheet.replace("r=\"1\"", "r=\"&broken\"");
    let malformed_entries = [
        ("[Content_Types].xml", CONTENT_TYPES),
        ("_rels/.rels", ROOT_RELS),
        ("xl/workbook.xml", workbook),
        ("xl/_rels/workbook.xml.rels", workbook_rels),
        ("xl/worksheets/sheet1.xml", malformed_sheet.as_str()),
    ];
    assert!(matches!(
        inspect_xlsx_source(
            &zip(&malformed_entries, CompressionMethod::Stored),
            MANIFEST_A.as_bytes(),
            BackendKind::Umya,
            &DiscoveryLimits::default(),
            None,
        ),
        Err(DiscoveryError::Xml { .. })
    ));

    assert!(matches!(
        inspect_xlsx_source(
            &zip(&entries, CompressionMethod::Stored),
            b"spec: fio\nports: []\nvalue: .nan\n",
            BackendKind::Umya,
            &DiscoveryLimits::default(),
            None,
        ),
        Err(DiscoveryError::Manifest(_))
    ));
}

#[test]
fn s1_loader_rejects_noncanonical_volatile_and_out_of_bounds_records() {
    let manifest = serde_json::json!({"ports":[]});
    let bytes = build(&modeled_workbook(), &manifest);

    let noncanonical = mutate_package(&bytes, |document| {
        document["workbook"]["sheets"][1]["cells"][0]["formula"] =
            serde_json::json!("=Inputs!A1+2");
    });
    assert!(load_calculation_package(&noncanonical, PackageExpectation::default()).is_err());

    let volatile = mutate_package(&bytes, |document| {
        document["workbook"]["sheets"][1]["cells"][0]["formula"] = serde_json::json!("=NOW()");
    });
    assert!(load_calculation_package(&volatile, PackageExpectation::default()).is_err());

    let degenerate_dimensions = mutate_package(&bytes, |document| {
        document["workbook"]["sheets"][0]["rows"] = serde_json::json!(1_048_576u32);
        document["workbook"]["sheets"][0]["cols"] = serde_json::json!(0u32);
    });
    assert!(
        load_calculation_package(&degenerate_dimensions, PackageExpectation::default()).is_err()
    );

    let reversed_name = mutate_package(&bytes, |document| {
        document["workbook"]["names"][0]["definition"]["start_row"] = serde_json::json!(3);
        document["workbook"]["names"][0]["definition"]["end_row"] = serde_json::json!(1);
    });
    assert!(load_calculation_package(&reversed_name, PackageExpectation::default()).is_err());

    let zero_table = mutate_package(&bytes, |document| {
        document["workbook"]["tables"][0]["start_col"] = serde_json::json!(0);
    });
    assert!(load_calculation_package(&zero_table, PackageExpectation::default()).is_err());
}

#[test]
fn s1_refuses_export_after_capped_formula_and_spill_compaction() {
    let manifest = serde_json::json!({"ports":[]});
    for formula in ["=1+1", "=SEQUENCE(3)"] {
        let mut config = WorkbookConfig::interactive();
        config.eval.max_overlay_memory_bytes = Some(0);
        let mut workbook = Workbook::new_with_config(config);
        workbook.set_formula("Sheet1", 1, 1, formula).unwrap();
        workbook.evaluate_all().unwrap();
        let error = build_calculation_package(
            &workbook,
            PackageBuildOptions {
                source_digest_sha256: "source",
                manifest: &manifest,
                target_metadata: &serde_json::Value::Null,
                backend: BackendKind::Umya,
                limits: PackageLimits::default(),
                cancel: None,
            },
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("provenance"),
            "{formula}: {error}"
        );
    }
}

#[cfg(feature = "umya")]
#[test]
fn real_xlsx_s0_to_s1_binding_load_evaluate_rebuild_parity() {
    let mut source_book = umya_spreadsheet::new_file();
    let source_sheet = source_book.get_sheet_by_name_mut("Sheet1").unwrap();
    source_sheet.get_cell_mut((1, 1)).set_value_number(7.0);
    source_sheet.get_cell_mut((2, 1)).set_formula("=A1+2");
    let mut xlsx = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&source_book, &mut xlsx).unwrap();

    let report = inspect_xlsx_source(
        &xlsx,
        MANIFEST_A.as_bytes(),
        BackendKind::Umya,
        &DiscoveryLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(report.decision, AdmissionDecision::Admitted, "{report:#?}");

    let adapter = UmyaAdapter::open_bytes(xlsx).unwrap();
    let mut source = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    source.evaluate_all().unwrap();
    assert_eq!(
        source.get_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(9.0))
    );

    let manifest = report.canonical_manifest_json.clone();
    let package = build_calculation_package(
        &source,
        PackageBuildOptions {
            source_digest_sha256: &report.source_digest_sha256,
            manifest: &manifest,
            target_metadata: &serde_json::json!({"outputs":["Sheet1!B1"]}),
            backend: BackendKind::Umya,
            limits: PackageLimits::default(),
            cancel: None,
        },
    )
    .unwrap();
    let mut loaded = load_calculation_package(
        &package,
        PackageExpectation {
            source_digest_sha256: Some(&report.source_digest_sha256),
            manifest_digest_sha256: Some(&report.manifest_digest_sha256),
        },
    )
    .unwrap();
    loaded.workbook.evaluate_all().unwrap();
    assert_eq!(
        loaded.workbook.get_value("Sheet1", 1, 2),
        source.get_value("Sheet1", 1, 2)
    );
    assert_eq!(
        build_calculation_package(
            &loaded.workbook,
            PackageBuildOptions {
                source_digest_sha256: &report.source_digest_sha256,
                manifest: &manifest,
                target_metadata: &serde_json::json!({"outputs":["Sheet1!B1"]}),
                backend: BackendKind::Umya,
                limits: PackageLimits::default(),
                cancel: None,
            },
        )
        .unwrap(),
        package
    );
}
