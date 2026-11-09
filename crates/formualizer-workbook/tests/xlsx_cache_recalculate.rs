#![cfg(feature = "xlsx-recalc")]
#[path = "support/cached_reader.rs"]
mod cached_reader;
use crate::cached_reader::{Data, Xlsx};
use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_bytes};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
};
use zip::{ZipArchive, ZipWriter};

const SHEET: &str = "xl/worksheets/sheet1.xml";
const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
fn parts(rows: &str) -> BTreeMap<String, String> {
    [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Default Extension=\"bin\" ContentType=\"application/octet-stream\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned()),
        ("_rels/.rels",format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml",format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        ("xl/_rels/workbook.xml.rels",format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        (SHEET,format!("<worksheet xmlns=\"{MAIN}\"><sheetData>{rows}</sheetData></worksheet>")),
        ("custom/opaque.bin","do not touch".to_owned()),
    ].into_iter().map(|(k,v)|(k.to_owned(),v)).collect()
}
fn pack(parts: &BTreeMap<String, String>) -> Vec<u8> {
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    z.set_comment("archive-comment")
        .expect("fixture archive comment");
    let options = zip::write::SimpleFileOptions::default()
        .unix_permissions(0o640)
        .last_modified_time(zip::DateTime::from_date_and_time(2020, 1, 2, 3, 4, 6).unwrap());
    for (name, body) in parts {
        z.start_file(name, options).unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}
fn single(formula: &str, cache: &str) -> BTreeMap<String, String> {
    parts(&format!(
        "<row r=\"1\"><c r=\"A1\"><f>{formula}</f>{cache}</c></row>"
    ))
}
fn fixture(formula: &str, cache: &str) -> Vec<u8> {
    pack(&single(formula, &format!("<v>{cache}</v>")))
}
fn member(bytes: &[u8], name: &str) -> String {
    let mut z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut s = String::new();
    z.by_name(name).unwrap().read_to_string(&mut s).unwrap();
    s
}
fn data(bytes: &[u8], row: u32) -> Data {
    let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
    x.worksheet_range("Sheet1")
        .unwrap()
        .get_value((row, 0))
        .cloned()
        .unwrap_or(Data::Empty)
}
fn reject(parts: &BTreeMap<String, String>) {
    assert!(recalculate_xlsx_bytes(&pack(parts), XlsxRecalculateOptions::default()).is_err());
}

#[test]
fn source_policy_boundary_strict_does_not_consume_affected_query_budget() {
    use formualizer_workbook::{CalculationDeferredReason, IoError, SourceCalculationCoverage};
    let input = fixture("SOME_MISSING_FN(1)", "99");
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_affected_regions = 0;
    options.limits.max_affected_work = 0;
    options.limits.max_affected_scratch_bytes = 0;
    let result = recalculate_xlsx_bytes(&input, options.clone()).unwrap();
    assert_eq!(result.coverage, SourceCalculationCoverage::Partial);
    options.require_complete = true;
    assert!(matches!(
        recalculate_xlsx_bytes(&input, options),
        Err(IoError::CalculationDeferred {
            reason: CalculationDeferredReason::KnownCoverageIncomplete,
            ..
        })
    ));
}

#[test]
fn source_policy_boundary_invalid_precision_never_claims_complete() {
    use formualizer_workbook::{CalculationDeferredReason, IoError, SourceCalculationCoverage};
    let mut source = single("1+1", "<v>99</v>");
    let workbook = source.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</workbook>",
        "<calcPr fullPrecision=\"maybe\"/></workbook>",
    );
    let bytes = pack(&source);
    let partial = recalculate_xlsx_bytes(&bytes, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(partial.coverage, SourceCalculationCoverage::Partial);
    assert!(
        partial
            .operation_diagnostics
            .workbook_features
            .iter()
            .any(|value| value == "invalid_full_precision")
    );
    let strict = XlsxRecalculateOptions {
        require_complete: true,
        ..Default::default()
    };
    assert!(matches!(
        recalculate_xlsx_bytes(&bytes, strict),
        Err(IoError::CalculationDeferred {
            reason: CalculationDeferredReason::KnownCoverageIncomplete,
            ..
        })
    ));
    assert!(matches!(
        recalculate_xlsx_bytes(&bytes, XlsxRecalculateOptions::workbook()),
        Err(IoError::CalculationDeferred {
            reason: CalculationDeferredReason::WorkbookPrecisionUnverified,
            ..
        })
    ));
}

#[test]
fn source_policy_boundary_modes_and_unverified_producers_prevent_false_complete() {
    use formualizer_workbook::{CalculationDeferredReason, IoError, SourceCalculationCoverage};
    for formula in [
        "CELL(\"format\",B1)",
        "IF(TRUE,SUM,AVERAGE)",
        "HSTACK(SUM,AVERAGE)",
        "TEXT(1,\"[>0]yyyy;0\")",
    ] {
        let input = fixture(formula, "99");
        let mut options = XlsxRecalculateOptions::workbook();
        options.error_location_limit = 0;
        let partial = recalculate_xlsx_bytes(&input, options.clone()).unwrap();
        assert_eq!(
            partial.coverage,
            SourceCalculationCoverage::Partial,
            "{formula}"
        );
        assert!(
            !partial.operation_diagnostics.function_modes.is_empty(),
            "{formula}"
        );
        assert!(partial.operation_diagnostics.samples.is_empty());
        assert_eq!(
            partial.operation_diagnostics.physical_function_dispatches,
            None
        );
        assert!(
            !member(&partial.bytes, SHEET).contains("<v>99</v>"),
            "{formula}"
        );
        options.require_complete = true;
        assert!(
            matches!(
                recalculate_xlsx_bytes(&input, options),
                Err(IoError::CalculationDeferred {
                    reason: CalculationDeferredReason::KnownCoverageIncomplete,
                    ..
                })
            ),
            "{formula}"
        );
    }
    for formula in ["1/0", "IFERROR(1/0,7)", "IF(FALSE,IF(TRUE,SUM,AVERAGE),7)"] {
        let result = recalculate_xlsx_bytes(
            &fixture(formula, "99"),
            XlsxRecalculateOptions {
                require_complete: true,
                ..XlsxRecalculateOptions::workbook()
            },
        )
        .unwrap();
        assert_eq!(
            result.coverage,
            SourceCalculationCoverage::Complete,
            "{formula}"
        );
        assert!(
            result.operation_diagnostics.function_modes.is_empty(),
            "{formula}"
        );
    }
}

#[test]
fn source_policy_boundary_independent_dynamic_cache_is_published() {
    use formualizer_workbook::SourceCalculationCoverage;
    let input = pack(&parts(
        "<row r=\"1\"><c r=\"A1\"><f>SOME_MISSING_FN(1)</f><v>99</v></c><c r=\"B1\"><f>INDIRECT(\"A1\")</f><v>88</v></c><c r=\"C1\"><f>B1+1</f><v>77</v></c><c r=\"D1\"><v>7</v></c><c r=\"E1\"><f>INDIRECT(\"D1\")</f><v>66</v></c><c r=\"F1\"><f>E1+1</f><v>55</v></c></row>",
    ));
    let output = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    assert_eq!(output.coverage, SourceCalculationCoverage::Partial);
    let xml = member(&output.bytes, SHEET);
    for stale in [
        "<v>99</v>",
        "<v>88</v>",
        "<v>77</v>",
        "<v>66</v>",
        "<v>55</v>",
    ] {
        assert!(!xml.contains(stale), "{xml}");
    }
    let mut reopened = Xlsx::new(Cursor::new(output.bytes)).unwrap();
    let range = reopened.worksheet_range("Sheet1").unwrap();
    assert_eq!(range.get_value((0, 4)), Some(&Data::Float(7.0)));
    assert_eq!(range.get_value((0, 5)), Some(&Data::Float(8.0)));
    let facts = output.operation_diagnostics.dependency_evidence.unwrap();
    assert_eq!(facts.unknown_facts, 0);
    assert!(
        facts.semantic_reads_unattested,
        "scope caveat must stay explicit"
    );
    assert_eq!(
        facts.source_domain,
        "admitted_workbook_and_fixed_cached_sources"
    );
}

#[test]
fn source_policy_boundary_evidence_and_query_failures_are_fatal_resources() {
    use formualizer_common::{ExcelErrorExtra, ResourceExhaustionReason};
    use formualizer_workbook::IoError;
    let input = fixture("SOME_MISSING_FN(1)", "99");
    for case in 0..5 {
        let mut options = XlsxRecalculateOptions::workbook();
        options.error_location_limit = 0;
        match case {
            0 => options.limits.max_operation_roots = 0,
            1 => options.limits.max_operation_text_bytes = 0,
            2 => options.limits.max_affected_work = 0,
            3 => options.limits.max_affected_regions = 0,
            _ => options.limits.max_affected_scratch_bytes = 0,
        }
        let error = recalculate_xlsx_bytes(&input, options).unwrap_err();
        let IoError::Engine(error) = error else {
            panic!("resource must remain fatal engine error: {error:?}")
        };
        let ExcelErrorExtra::Resource { detail } = error.extra() else {
            panic!("missing typed resource detail: {error:?}")
        };
        let expected = match case {
            0 => ResourceExhaustionReason::OperationEvidenceRoots,
            1 => ResourceExhaustionReason::OperationEvidenceTextBytes,
            2 => ResourceExhaustionReason::WorkUnits,
            3 => ResourceExhaustionReason::DependencyClosureOutputItems,
            _ => ResourceExhaustionReason::ScratchMemory,
        };
        assert_eq!(detail.reason, expected);
    }
    recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
}

#[test]
fn source_policy_boundary_optional_telemetry_is_measured_only_on_request() {
    let input = fixture("1+1", "99");
    let unmeasured = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    assert_eq!(
        unmeasured
            .operation_diagnostics
            .physical_function_dispatches,
        None
    );
    let measured = recalculate_xlsx_bytes(
        &input,
        XlsxRecalculateOptions {
            physical_telemetry: true,
            ..XlsxRecalculateOptions::workbook()
        },
    )
    .unwrap();
    assert_eq!(
        measured.operation_diagnostics.physical_function_dispatches,
        Some(0)
    );
    let called = recalculate_xlsx_bytes(
        &fixture("SUM(1,2)", "99"),
        XlsxRecalculateOptions {
            physical_telemetry: true,
            ..XlsxRecalculateOptions::workbook()
        },
    )
    .unwrap();
    assert!(
        called
            .operation_diagnostics
            .physical_function_dispatches
            .unwrap()
            > 0
    );
}

#[test]
fn source_policy_boundary_publication_membership_respects_shared_work_limit() {
    use formualizer_common::{ExcelErrorExtra, ResourceExhaustionReason};
    use formualizer_workbook::IoError;
    let input = pack(&parts(
        "<row r=\"1\"><c r=\"A1\"><f>SOME_MISSING_FN(1)</f><v>99</v></c><c r=\"B1\"><f>1+1</f><v>88</v></c></row>",
    ));
    let normal = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    let query_work = normal
        .operation_diagnostics
        .dependency_evidence
        .unwrap()
        .query_work;
    let mut limited = XlsxRecalculateOptions::workbook();
    limited.limits.max_affected_work = query_work;
    let error = recalculate_xlsx_bytes(&input, limited).unwrap_err();
    let IoError::Engine(error) = error else {
        panic!("membership bound must be fatal: {error:?}")
    };
    assert!(
        matches!(error.extra(),ExcelErrorExtra::Resource{detail} if detail.reason==ResourceExhaustionReason::WorkUnits)
    );
}

#[cfg(feature = "json")]
#[test]
fn source_policy_boundary_portable_policy_requires_records() {
    use formualizer_workbook::SourceCalculationPolicy;
    for input in [
        r#"[{}]"#,
        r#"{"iteration":["workbook"]}"#,
        r#"{"timezone":[0]}"#,
    ] {
        assert!(
            serde_json::from_str::<SourceCalculationPolicy>(input).is_err(),
            "{input}"
        );
    }
    for input in [
        r#"{}"#,
        r#"{"iteration":{"mode":"disabled"}}"#,
        r#"{"timezone":{"offsetSeconds":0}}"#,
    ] {
        serde_json::from_str::<SourceCalculationPolicy>(input).unwrap();
    }
}

#[test]
fn source_policy_boundary_unverified_actual_provider_is_reported() {
    use formualizer_workbook::{
        CalculationDeferredReason, IoError, SourceCalculationCoverage, SourceUnavailableOperation,
    };
    const CASE: &str = "source_policy_boundary_unverified_actual_provider_is_reported";
    if std::env::var("FZ_SOURCE_PROVIDER_CHILD").ok().as_deref() != Some(CASE) {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CASE, "--nocapture"])
            .env("FZ_SOURCE_PROVIDER_CHILD", CASE)
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    struct Provider;
    impl formualizer_eval::function::Function for Provider {
        fn name(&self) -> &'static str {
            "SOURCE_TEST_LOCAL_PROVIDER"
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _: &'c [formualizer_eval::traits::ArgumentHandle<'a, 'b>],
            _: &dyn formualizer_eval::traits::FunctionContext<'b>,
        ) -> Result<formualizer_eval::traits::CalcValue<'b>, formualizer_common::ExcelError>
        {
            Ok(formualizer_eval::traits::CalcValue::Scalar(
                formualizer_common::LiteralValue::Number(9.0),
            ))
        }
    }
    formualizer_eval::function_registry::try_register_function(std::sync::Arc::new(Provider))
        .unwrap();
    let input = fixture("SOURCE_TEST_LOCAL_PROVIDER()", "99");
    let result = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    assert_eq!(result.coverage, SourceCalculationCoverage::Partial);
    assert!(result.operation_diagnostics.samples.iter().any(|sample|matches!(
        &sample.operation,SourceUnavailableOperation::UnverifiedProvider{name} if name=="SOURCE_TEST_LOCAL_PROVIDER"
    )));
    assert!(matches!(
        recalculate_xlsx_bytes(
            &input,
            XlsxRecalculateOptions {
                require_complete: true,
                ..XlsxRecalculateOptions::workbook()
            }
        ),
        Err(IoError::CalculationDeferred {
            reason: CalculationDeferredReason::KnownCoverageIncomplete,
            ..
        })
    ));
}

#[test]
fn source_policy_boundary_unsupported_named_modes_preserve_admission_refusal() {
    use formualizer_workbook::IoError;
    let mut source = parts(
        "<row r=\"1\"><c r=\"A1\"><f>GapName+1</f><v>99</v></c><c r=\"B1\"><v>7</v></c></row>",
    );
    let workbook = source.get_mut("xl/workbook.xml").unwrap();
    *workbook=workbook.replace("</workbook>","<definedNames><definedName name=\"GapName\">CELL(\"format\",Sheet1!$B$1)</definedName></definedNames></workbook>");
    for options in [
        XlsxRecalculateOptions::default(),
        XlsxRecalculateOptions::workbook(),
    ] {
        assert!(matches!(recalculate_xlsx_bytes(&pack(&source),options),
            Err(IoError::Unsupported{feature,context}) if feature=="unsupported or cyclic calculation name"&&context=="GapName"));
    }
}

#[test]
fn source_policy_boundary_independent_dynamic_spill_is_published() {
    use formualizer_workbook::SourceCalculationCoverage;
    let input = pack(&parts(
        "<row r=\"1\"><c r=\"A1\"><f>SOME_MISSING_FN(1)</f><v>99</v></c><c r=\"B1\"><f>INDIRECT(\"A1\")</f><v>88</v></c></row><row r=\"3\"><c r=\"D3\"><v>7</v></c><c r=\"E3\"><f>INDIRECT(\"D3:D4\")</f><v>66</v></c></row><row r=\"4\"><c r=\"D4\"><v>8</v></c></row>",
    ));
    let result = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    assert_eq!(result.coverage, SourceCalculationCoverage::Partial);
    let mut reopened = Xlsx::new(Cursor::new(result.bytes)).unwrap();
    let range = reopened.worksheet_range("Sheet1").unwrap();
    assert_eq!(range.get_value((2, 4)), Some(&Data::Float(7.0)));
    assert_eq!(range.get_value((3, 4)), Some(&Data::Float(8.0)));
    assert!(result.operation_diagnostics.dependency_evidence.is_some());
}

#[cfg(not(feature = "system-clock"))]
#[test]
fn source_portable_fixed_clock_precedes_ambient_clock_admission() {
    use formualizer_workbook::{IoError, SourceCalculationCoverage, SourceCalculationPolicy};
    let input = fixture("NOW()", "99");
    let fixed = "2024-01-02T03:04:05Z"
        .parse::<chrono::DateTime<chrono::Utc>>()
        .unwrap();
    for mut options in [
        XlsxRecalculateOptions::default(),
        XlsxRecalculateOptions::workbook(),
    ] {
        options.calculation = Some(SourceCalculationPolicy {
            fixed_now_utc: Some(fixed),
            ..Default::default()
        });
        options.require_complete = true;
        let result = recalculate_xlsx_bytes(&input, options).unwrap();
        assert_eq!(result.coverage, SourceCalculationCoverage::Complete);
        assert_eq!(result.clock_now_utc, Some(fixed));
        assert_eq!(result.summary.errors, 0);
        assert!(
            matches!(data(&result.bytes,0),Data::Float(value) if value>40_000.0),
            "fixed instant must not become epoch fallback"
        );
    }
    assert!(
        matches!(recalculate_xlsx_bytes(&input,XlsxRecalculateOptions::workbook()),
        Err(IoError::Unsupported{feature,..}) if feature.contains("wall clock"))
    );
}

#[test]
fn portable_policy_reports_effective_settings_without_rewriting_workbook() {
    use formualizer_eval::engine::{CycleDetection, CyclePolicy};
    use formualizer_workbook::{
        CalculationSettingOrigin, SourceCalculationPolicy, SourceIterationPolicy,
    };
    let input = fixture("1+1", "99");
    let options = XlsxRecalculateOptions {
        calculation: Some(SourceCalculationPolicy {
            iteration: SourceIterationPolicy::Disabled,
            workbook_seed: Some(u64::MAX),
            ..Default::default()
        }),
        ..Default::default()
    };
    let out = recalculate_xlsx_bytes(&input, options).unwrap();
    let report = out.calculation.as_ref().unwrap();
    assert_eq!(report.cycle.detection, CycleDetection::Runtime);
    assert_eq!(report.cycle.policy, CyclePolicy::Error);
    assert_eq!(report.iteration_origin, CalculationSettingOrigin::Caller);
    assert_eq!(report.workbook_seed, u64::MAX);
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(
        member(&input, "xl/workbook.xml"),
        member(&out.bytes, "xl/workbook.xml")
    );
    assert!(
        recalculate_xlsx_bytes(&input, Default::default())
            .unwrap()
            .calculation
            .is_none()
    );
}

#[test]
fn workbook_profile_admits_sparse_declared_and_actual_grid_corners() {
    for (dimension, row, address) in [
        ("A1:XFD1048576", 1, "B1"),
        ("A1:IW1", 1, "IW1"),
        ("A1:XFD1048576", 1_048_576, "XFD1048576"),
    ] {
        let rows = if row == 1 {
            format!(
                "<row r=\"1\"><c r=\"A1\"><v>2</v></c><c r=\"{address}\"><f>A1+1</f><v>0</v></c></row>"
            )
        } else {
            format!(
                "<row r=\"1\"><c r=\"A1\"><v>2</v></c></row><row r=\"{row}\"><c r=\"{address}\"><f>A1+1</f><v>0</v></c></row>"
            )
        };
        let mut p = parts(&rows);
        let xml = p[SHEET].replace(
            "<sheetData>",
            &format!("<dimension ref=\"{dimension}\"/><sheetData>"),
        );
        p.insert(SHEET.into(), xml);
        let input = pack(&p);
        assert!(recalculate_xlsx_bytes(&input, Default::default()).is_err());
        let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
        let output = member(&out.bytes, SHEET);
        assert!(
            output.contains(&format!("<c r=\"{address}\"><f>A1+1</f><v>3</v></c>")),
            "{output}"
        );
        assert_eq!(out.cache_cells_changed, 1);
        let mut limited = XlsxRecalculateOptions::workbook();
        limited.limits.max_cells = 1;
        assert!(recalculate_xlsx_bytes(&input, limited).is_err());
    }
}

#[test]
fn source_iteration_requires_verified_state_or_explicit_cold_start() {
    use formualizer_workbook::{CalculationDeferredReason, IoError, SourceIterationInitialState};
    let mut p = single("A1+1", "<v>5</v>");
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</workbook>",
        "<calcPr iterate=\"1\" iterateCount=\"1\" iterateDelta=\"0.001\"/></workbook>",
    );
    let input = pack(&p);
    assert!(matches!(
        recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()),
        Err(IoError::CalculationDeferred {
            reason: CalculationDeferredReason::IterativeInitialStateUnverified,
            ..
        })
    ));
    let mut options = XlsxRecalculateOptions::workbook();
    options
        .calculation
        .as_mut()
        .unwrap()
        .iteration_initial_state = SourceIterationInitialState::ColdStart;
    let out = recalculate_xlsx_bytes(&input, options).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(1.0));
    assert_eq!(
        out.calculation.unwrap().iteration_initial_state,
        SourceIterationInitialState::ColdStart
    );
    let mut raw = XlsxRecalculateOptions::default();
    raw.eval_config.cycle = formualizer_eval::engine::CycleConfig::iterate(1, 0.001);
    assert!(recalculate_xlsx_bytes(&input, raw).is_err());
    p.get_mut(SHEET)
        .unwrap()
        .clone_from(&single("1+1", "<v>0</v>")[SHEET]);
    let out = recalculate_xlsx_bytes(&pack(&p), XlsxRecalculateOptions::workbook()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
}

#[test]
fn workbook_partial_invalidates_caught_gap_and_dependent_caches() {
    use formualizer_workbook::SourceCalculationCoverage;
    let input = pack(&parts(
        "<row r=\"1\"><c r=\"A1\"><f>IFERROR(GAP_ALPHA(1),7)</f><v>999</v></c><c r=\"B1\"><f>A1+1</f><v>999</v></c><c r=\"D1\"><f>2+2</f><v>0</v></c><c r=\"E1\"><f>IFERROR(1/0,7)</f><v>0</v></c></row>",
    ));
    let legacy = recalculate_xlsx_bytes(&input, Default::default()).unwrap();
    assert_eq!(legacy.coverage, SourceCalculationCoverage::Partial);
    assert_eq!(data(&legacy.bytes, 0), Data::Float(7.0));
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    let xml = member(&out.bytes, SHEET);
    assert_eq!(out.coverage, SourceCalculationCoverage::Partial);
    assert_eq!(out.caches_invalidated, 2);
    assert!(out.calculation_flags_changed);
    let flags = member(&out.bytes, "xl/workbook.xml");
    assert!(flags.contains("fullCalcOnLoad=\"1\"") && flags.contains("forceFullCalc=\"1\""));
    let repeat = recalculate_xlsx_bytes(&out.bytes, XlsxRecalculateOptions::workbook()).unwrap();
    assert_eq!(repeat.bytes, out.bytes);
    assert_eq!(repeat.caches_invalidated, 0);
    assert!(!repeat.calculation_flags_changed);
    assert_eq!(repeat.operation_diagnostics.logical_cell_reason_count, 1);
    assert!(
        xml.contains("<c r=\"A1\"><f>IFERROR(GAP_ALPHA(1),7)</f></c>"),
        "{xml}"
    );
    assert!(xml.contains("<c r=\"B1\"><f>A1+1</f></c>"));
    assert!(xml.contains("<c r=\"D1\"><f>2+2</f><v>4</v></c>"));
    assert!(xml.contains("<c r=\"E1\"><f>IFERROR(1/0,7)</f><v>7</v></c>"));
    for mut options in [
        XlsxRecalculateOptions::default(),
        XlsxRecalculateOptions::workbook(),
    ] {
        options.require_complete = true;
        assert!(recalculate_xlsx_bytes(&input, options).is_err());
        assert!(
            recalculate_xlsx_bytes(
                &fixture("1/0", "0"),
                XlsxRecalculateOptions {
                    require_complete: true,
                    ..Default::default()
                }
            )
            .is_ok()
        );
    }
}

#[test]
fn workbook_partial_data_table_preserves_descriptor_and_independent_result() {
    use formualizer_workbook::SourceCalculationCoverage;
    let input = pack(&parts(
        "<row r=\"1\"><c r=\"A1\"><v>1</v></c><c r=\"B1\"><f t=\"dataTable\" ref=\"B1:B2\" r1=\"A1\"/><v>123</v></c><c r=\"C1\"><f>SUM(B1:B2)</f><v>999</v></c><c r=\"D1\"><f>2+2</f><v>0</v></c></row><row r=\"2\"><c r=\"B2\"><v>456</v></c></row>",
    ));
    assert!(recalculate_xlsx_bytes(&input, Default::default()).is_err());
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    let xml = member(&out.bytes, SHEET);
    assert_eq!(out.coverage, SourceCalculationCoverage::Partial);
    assert!(xml.contains("<f t=\"dataTable\" ref=\"B1:B2\" r1=\"A1\"/>"));
    assert!(
        !xml.contains("123") && !xml.contains("456") && !xml.contains("999"),
        "{xml}"
    );
    assert!(xml.contains("<c r=\"D1\"><f>2+2</f><v>4</v></c>"));
    assert_eq!(out.caches_invalidated, 3);
}

#[test]
fn workbook_partial_uses_executed_roots_and_published_error_totals() {
    use formualizer_workbook::SourceCalculationCoverage;
    let mut options = XlsxRecalculateOptions::workbook();
    options.require_complete = true;
    let out = recalculate_xlsx_bytes(
        &fixture("IF(TRUE,MISSING_BINDING,GAP_INACTIVE(1))", "0"),
        options,
    )
    .unwrap();
    assert_eq!(out.coverage, SourceCalculationCoverage::Complete);
    assert_eq!(out.summary.errors, 1);
    assert!(
        out.summary
            .error_summary
            .values()
            .flat_map(|s| &s.messages)
            .all(|m| m.as_deref().is_none_or(|m| !m.contains("GAP_INACTIVE")))
    );
    assert!(out.summary.unknown_functions.is_empty());
    assert_eq!(out.operation_diagnostics.logical_cell_reason_count, 0);
    let input = pack(&parts(
        "<row r=\"1\"><c r=\"A1\"><v>1</v></c><c r=\"B1\"><f t=\"dataTable\" ref=\"B1:B2\" r1=\"A1\"/><v>123</v></c><c r=\"C1\"><f>IF(B2=456,GAP_STALE(1),42)</f><v>999</v></c><c r=\"D1\"><f>1/0</f><v>0</v></c></row><row r=\"2\"><c r=\"B2\"><v>456</v></c></row>",
    ));
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    assert_eq!(out.coverage, SourceCalculationCoverage::Partial);
    assert_eq!(
        out.operation_diagnostics.logical_cell_reason_count, 0,
        "saved generated cache must not select GAP_STALE"
    );
    assert_eq!(out.operation_diagnostics.source_regions, 1);
    assert_eq!(
        out.summary.evaluated, 2,
        "synthetic data-table sentinel is not a source calculation"
    );
    assert_eq!(
        out.summary.errors, 1,
        "only the published independent DIV/0 is a final error"
    );
    assert!(out.summary.unknown_functions.is_empty());
}

#[test]
fn workbook_partial_refuses_ambiguous_ownership_and_defers_affected_array() {
    use formualizer_workbook::{CalculationDeferredReason, IoError};
    for table in [
        "<c r=\"B1\"><f t=\"dataTable\" ref=\"B1:B2\"/><v>123</v></c></row><row r=\"2\"><c r=\"B2\"><f>1+1</f><v>456</v></c>",
        "<c r=\"B1\"><f t=\"dataTable\" ref=\"B2:B3\"/><v>123</v></c>",
        "<c r=\"B1\"><f t=\"dataTable\" ref=\"B1:B3\"/><v>123</v></c><c r=\"C1\"><f t=\"dataTable\" ref=\"C1:C2\"/><v>0</v></c></row><row r=\"2\"><c r=\"B2\"><f t=\"dataTable\" ref=\"B2:B4\"/><v>456</v></c>",
    ] {
        let input = pack(&parts(&format!("<row r=\"1\">{table}</row>")));
        let error = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap_err();
        assert!(
            !matches!(error, IoError::CalculationDeferred { .. }),
            "{error:?}"
        );
    }
    let error = recalculate_xlsx_bytes(
        &fixture("IFERROR(GAP_ARRAY(1),SEQUENCE(2))", "999"),
        XlsxRecalculateOptions::workbook(),
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            IoError::CalculationDeferred {
                reason: CalculationDeferredReason::AffectedArrayCoverageUnverified,
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn workbook_partial_flags_preserve_source_mode_and_namespace() {
    let mut input =
        parts("<row r=\"1\"><c r=\"A1\"><f>IFERROR(GAP_FLAG(1),7)</f><v>999</v></c></row>");
    let workbook = input["xl/workbook.xml"].clone();
    let workbook = workbook.replace("<workbook ", "<s:workbook ").replace("</workbook>", "</s:workbook>").replace("xmlns=", "xmlns:s=").replace("<sheets>", "<s:sheets>").replace("</sheets>", "</s:sheets>").replace("<sheet ", "<s:sheet ").replace("</s:workbook>", "<s:calcPr calcMode=\"manual\" iterate=\"0\" fullCalcOnLoad=\"0\" forceFullCalc=\"false\"/><s:extLst/></s:workbook>");
    input.insert("xl/workbook.xml".into(), workbook);
    let input = pack(&input);
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    let workbook = member(&out.bytes, "xl/workbook.xml");
    assert!(workbook.contains("calcMode=\"manual\"") && workbook.contains("iterate=\"0\""));
    assert!(workbook.contains("fullCalcOnLoad=\"1\"") && workbook.contains("forceFullCalc=\"1\""));
    assert_eq!(workbook.matches("calcPr").count(), 1);
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, XlsxRecalculateOptions::workbook())
            .unwrap()
            .bytes,
        out.bytes
    );
}

#[test]
fn workbook_partial_internal_cycle_and_zero_sample_scope_are_truthful() {
    use formualizer_workbook::SourceCalculationCoverage;
    let mut options = XlsxRecalculateOptions::workbook();
    options.error_location_limit = 0;
    let gap = recalculate_xlsx_bytes(
        &fixture("IFERROR(GAP_ZERO_SAMPLE(1),7)", "999"),
        options.clone(),
    )
    .unwrap();
    assert_eq!(gap.coverage, SourceCalculationCoverage::Partial);
    assert!(gap.operation_diagnostics.samples.is_empty());
    assert_eq!(gap.operation_diagnostics.samples_truncated, 1);
    assert_eq!(
        gap.operation_diagnostics.unavailable_functions["GAP_ZERO_SAMPLE"].cell_reason_count,
        1
    );
    assert_eq!(gap.summary.unknown_functions["GAP_ZERO_SAMPLE"], 1);
    let cycle = recalculate_xlsx_bytes(&fixture("A1+1", "999"), options).unwrap();
    assert_eq!(cycle.coverage, SourceCalculationCoverage::Partial);
    assert_eq!(cycle.caches_invalidated, 1);
    assert_eq!(cycle.operation_diagnostics.unavailable_result_cells, 1);
    assert_eq!(cycle.summary.errors, 0);
    assert!(!member(&cycle.bytes, SHEET).contains("<v>"));
}

#[test]
fn parse_refusal_precedes_any_coverage_deferral() {
    use formualizer_workbook::IoError;
    let input = pack(&parts(
        "<row r=\"1\"><c r=\"A1\"><f>IFERROR(GAP_ALPHA(1),7)</f><v>999</v></c><c r=\"B1\"><f>SUM((A1</f><v>999</v></c></row>",
    ));
    for require_complete in [false, true] {
        let options = XlsxRecalculateOptions {
            require_complete,
            ..XlsxRecalculateOptions::workbook()
        };
        let error = recalculate_xlsx_bytes(&input, options).unwrap_err();
        assert!(
            !matches!(error, IoError::CalculationDeferred { .. }),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("unparseable formula") && error.to_string().contains("B1"),
            "{error}"
        );
    }
}

#[test]
fn invalid_cycle_request_returns_typed_error_before_package_import() {
    use formualizer_eval::engine::{CycleConfig, CycleDetection, CyclePolicy};
    let mut options = XlsxRecalculateOptions::default();
    options.eval_config.cycle = CycleConfig {
        detection: CycleDetection::Static,
        policy: CyclePolicy::Iterate {
            max_iterations: 0,
            max_change: 0.0,
        },
    };
    assert!(matches!(
        recalculate_xlsx_bytes(&[], options),
        Err(formualizer_workbook::IoError::InvalidOptions { .. })
    ));
}

#[test]
fn defined_constant_is_calculated_not_published_as_name_error() {
    let mut p = single("Rate*2", "<v>99</v>");
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</workbook>",
        "<definedNames><definedName name=\"Rate\">0.07</definedName></definedNames></workbook>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(0.14));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    assert_eq!(out.summary.errors, 0);
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, Default::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}
fn with_names(mut p: BTreeMap<String, String>, names: &str) -> BTreeMap<String, String> {
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</workbook>",
        &format!("<definedNames>{names}</definedNames></workbook>"),
    );
    p
}

#[test]
fn grounded_formula_names_and_transitive_dependencies() {
    let p = with_names(
        parts(
            "<row r=\"1\"><c r=\"A1\"><v>3</v></c></row><row r=\"2\"><c r=\"A2\"><f>Answer</f><v>99</v></c></row>",
        ),
        "<definedName name=\"Answer\">DoubleBase</definedName><definedName name=\"DoubleBase\">SUM(Sheet1!$A$1,Sheet1!$A$1)</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 1), Data::Float(6.0));
    assert_eq!(data(&out.bytes, 0), Data::Float(3.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, Default::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}

#[test]
fn current_workbook_index_zero_resolves_names_and_keeps_formula_text() {
    // Excel stores `[0]!Name` for a workbook-scoped name qualified with the
    // current workbook (external links are numbered from 1).
    let p = with_names(
        parts(
            "<row r=\"1\"><c r=\"A1\"><v>7</v></c><c r=\"B1\"><f>INDEX([0]!MPRR,2,1)</f></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>8</v></c><c r=\"B2\"><f>SUM([0]!MPRR)+[0]Sheet1!A1</f></c></row>",
        ),
        "<definedName name=\"MPRR\">Sheet1!$A$1:$A$2</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0).to_string(), "7");
    let mut x = Xlsx::new(Cursor::new(&out.bytes)).unwrap();
    let range = x.worksheet_range("Sheet1").unwrap();
    assert_eq!(range.get_value((0, 1)), Some(&Data::Float(8.0)));
    assert_eq!(range.get_value((1, 1)), Some(&Data::Float(22.0)));
    let sheet = member(&out.bytes, SHEET);
    assert!(sheet.contains("<f>INDEX([0]!MPRR,2,1)</f>"), "{sheet}");
    assert!(
        sheet.contains("<f>SUM([0]!MPRR)+[0]Sheet1!A1</f>"),
        "{sheet}"
    );
}

#[test]
fn original_cross_sheet_formula_name_and_reference_control() {
    for (definition, formula) in [("Base!$A$1*2", "DoubleBase"), ("Base!$A$1", "DoubleBase*2")] {
        let mut p = with_names(
            single(formula, "<v>99</v>"),
            &format!("<definedName name=\"DoubleBase\">{definition}</definedName>"),
        );
        let workbook = p.get_mut("xl/workbook.xml").unwrap();
        *workbook = workbook.replace(
            "</sheets>",
            "<sheet name=\"Base\" sheetId=\"2\" r:id=\"rId2\"/></sheets>",
        );
        let relationships = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
        *relationships = relationships.replace("</Relationships>", &format!("<Relationship Id=\"rId2\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"));
        let types = p.get_mut("[Content_Types].xml").unwrap();
        *types = types.replace("</Types>", "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>");
        p.insert("xl/worksheets/sheet2.xml".into(), format!("<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\"><v>3</v></c></row></sheetData></worksheet>"));
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), Data::Float(6.0));
        assert_eq!(
            member(&out.bytes, "xl/worksheets/sheet2.xml"),
            p["xl/worksheets/sheet2.xml"]
        );
        assert_eq!(out.summary.errors, 0);
    }
}

#[test]
fn calculation_name_literal_types_and_legitimate_error() {
    for (definition, expected) in [
        ("TRUE", Data::Bool(true)),
        ("FALSE", Data::Bool(false)),
        ("&quot;a &amp; b&quot;", Data::String("a & b".into())),
        ("#N/A", Data::Error(cached_reader::CellErrorType::NA)),
        ("#DIV/0!", Data::Error(cached_reader::CellErrorType::Div0)),
        ("#VALUE!", Data::Error(cached_reader::CellErrorType::Value)),
        ("#REF!", Data::Error(cached_reader::CellErrorType::Ref)),
        (
            "Sheet1!#REF!",
            Data::Error(cached_reader::CellErrorType::Ref),
        ),
        ("#NUM!", Data::Error(cached_reader::CellErrorType::Num)),
        ("#NULL!", Data::Error(cached_reader::CellErrorType::Null)),
        ("#NAME?", Data::Error(cached_reader::CellErrorType::Name)),
        ("-0.07", Data::Float(-0.07)),
    ] {
        let p = with_names(
            single("ValueName", "<v>99</v>"),
            &format!("<definedName name=\"ValueName\">{definition}</definedName>"),
        );
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), expected, "{definition}");
        assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    }
}

#[test]
fn calculation_name_local_shadowing_and_local_formula_base() {
    let p = with_names(
        parts(
            "<row r=\"1\"><c r=\"A1\"><v>3</v></c></row><row r=\"2\"><c r=\"A2\"><f>ResultName</f><v>99</v></c></row>",
        ),
        "<definedName name=\"Rate\">10</definedName><definedName name=\"Rate\" localSheetId=\"0\">2</definedName><definedName name=\"ResultName\" localSheetId=\"0\">$A$1*Rate</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 1), Data::Float(6.0));
}

#[test]
fn unsupported_and_cyclic_calculation_names_refuse_publication() {
    for names in [
        "<definedName name=\"ResultName\">Sheet1!$A$1,Sheet1!$B$1</definedName>",
        "<definedName name=\"ResultName\">Sheet1!A1</definedName>",
        "<definedName name=\"ResultName\">$A$1*2</definedName>",
        "<definedName name=\"ResultName\">Sheet1!A1*2</definedName>",
        "<definedName name=\"ResultName\">INDIRECT(&quot;A1&quot;)</definedName>",
        "<definedName name=\"ResultName\">ROW()</definedName>",
        "<definedName name=\"ResultName\">OFFSET(#REF!,0,0,2,1)</definedName>",
        "<definedName name=\"ResultName\">OFFSET(Sheet1!$A$1,0,0,2,1)</definedName>",
        "<definedName name=\"ResultName\">[0]!X</definedName>",
        "<definedName name=\"ResultName\">{1,2}</definedName>",
        "<definedName name=\"ResultName\">[other.xlsx]Sheet1!$A$1</definedName>",
        "<definedName name=\"ResultName\">ResultName</definedName>",
        "<definedName name=\"ResultName\">OtherName</definedName><definedName name=\"OtherName\">ResultName</definedName>",
    ] {
        let p = with_names(single("ResultName", "<v>99</v>"), names);
        let input = pack(&p);
        assert!(
            matches!(
                recalculate_xlsx_bytes(&input, Default::default()),
                Err(formualizer_workbook::IoError::Unsupported { .. })
            ),
            "{names}"
        );
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.xlsx");
        let destination = dir.path().join("output.xlsx");
        std::fs::write(&source, &input).unwrap();
        std::fs::write(&destination, b"keep original destination").unwrap();
        assert!(
            formualizer_workbook::recalculate_xlsx_file(
                &source,
                Some(&destination),
                Default::default()
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&source).unwrap(), input);
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"keep original destination"
        );
    }
}

fn inert_fixture(kind: &str, first: bool, formula: &str) -> BTreeMap<String, String> {
    let mut p = parts(&format!(
        "<row r=\"1\"><c r=\"A1\"><f>{formula}</f><v>99</v></c></row>"
    ));
    let inert = if kind == "empty" {
        "<sheet name=\"Inert\" sheetId=\"3\" state=\"veryHidden\" r:id=\"\"/>".to_owned()
    } else {
        "<sheet name=\"Inert\" sheetId=\"3\" r:id=\"rId3\"/>".to_owned()
    };
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    if first {
        *workbook = workbook.replace("<sheets>", &format!("<sheets>{inert}"));
    } else {
        *workbook = workbook.replace("</sheets>", &format!("{inert}</sheets>"));
    }
    *workbook = workbook.replace(
        "</sheets>",
        "<sheet name=\"After\" sheetId=\"2\" r:id=\"rId2\"/></sheets>",
    );
    if kind == "empty" && !first {
        *workbook = workbook
            .replace(&inert, "")
            .replace("</sheets>", &format!("{inert}</sheets>"));
    }
    let scope = if kind == "empty" && !first { 1 } else { 2 };
    *workbook = workbook.replace("</workbook>", &format!("<definedNames><definedName name=\"Rate\" localSheetId=\"{scope}\">7</definedName></definedNames></workbook>"));
    let rels = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
    *rels = rels.replace("</Relationships>", &format!("<Relationship Id=\"rId2\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"));
    let types = p.get_mut("[Content_Types].xml").unwrap();
    *types = types.replace("</Types>", "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>");
    p.insert("xl/worksheets/sheet2.xml".into(), format!("<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\"><f>Rate</f><v>99</v></c></row></sheetData></worksheet>"));
    if kind != "empty" {
        let rels = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
        *rels = rels.replace("</Relationships>", &format!("<Relationship Id=\"rId3\" Type=\"{OFFICE}/{kind}\" Target=\"{kind}s/sheet3.xml\"/></Relationships>"));
        let types = p.get_mut("[Content_Types].xml").unwrap();
        *types = types.replace("</Types>", &format!("<Override PartName=\"/xl/{kind}s/sheet3.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.{kind}+xml\"/></Types>"));
        p.insert(
            format!("xl/{kind}s/sheet3.xml"),
            format!("<{kind} xmlns=\"{MAIN}\"/>"),
        );
        p.insert(format!("xl/{kind}s/_rels/sheet3.xml.rels"), format!("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"drawing\" Type=\"{OFFICE}/drawing\" Target=\"../drawings/drawing1.xml\"/></Relationships>"));
        p.insert("xl/drawings/drawing1.xml".into(), "<drawing/>".into());
        p.insert("xl/charts/chart1.xml".into(), "<chart/>".into());
    }
    p
}

#[test]
fn inert_sheets_keep_scope_order_and_original_parts() {
    let mut failures = Vec::new();
    for (kind, first) in [
        ("chartsheet", false),
        ("dialogsheet", false),
        ("empty", false),
        ("empty", true),
    ] {
        let p = inert_fixture(kind, first, "1+1");
        let out = match recalculate_xlsx_bytes(&pack(&p), Default::default()) {
            Ok(out) => out,
            Err(error) => {
                failures.push(format!("{kind}, first={first}: {error}"));
                continue;
            }
        };
        let expected = if first {
            vec!["Inert", "Sheet1", "After"]
        } else {
            vec!["Sheet1", "Inert", "After"]
        };
        if kind != "empty" {
            let mut x = Xlsx::new(Cursor::new(&out.bytes)).unwrap();
            assert_eq!(x.sheet_names(), expected);
            assert_eq!(
                x.worksheet_range("After").unwrap().get_value((0, 0)),
                Some(&Data::Float(7.0))
            );
        } else {
            assert!(member(&out.bytes, "xl/worksheets/sheet2.xml").contains("<v>7</v>"));
        }
        let zip = ZipArchive::new(Cursor::new(&out.bytes)).unwrap();
        assert_eq!(zip.len(), p.len());
        assert!(
            !zip.file_names()
                .any(|n| n.expect("fixture ZIP entry name").contains("__inert_sheet"))
        );
        for (name, original) in &p {
            if !name.starts_with("xl/worksheets/") {
                assert_eq!(member(&out.bytes, name), *original);
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn inert_sheet_direct_and_three_dimensional_references_refuse() {
    for formula in ["Inert!A1", "SUM(Sheet1:After!A1)"] {
        let p = inert_fixture("chartsheet", false, formula);
        match recalculate_xlsx_bytes(&pack(&p), Default::default()) {
            Err(formualizer_workbook::IoError::Unsupported { feature, .. }) => {
                assert_eq!(feature, "reference to a non-worksheet sheet")
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}

#[test]
fn inert_sheet_unicode_case_references_refuse() {
    for (kind, name, folded) in [
        ("chartsheet", "Ärger", "ärger"),
        ("empty", "MÓDULO", "módulo"),
    ] {
        for formula in [
            format!("'{folded}'!A1"),
            format!("SUM(Sheet1:'{folded}'!A1)"),
            format!("INDIRECT(&quot;'{folded}'!A1&quot;)"),
            format!("INDIRECT(&quot;'{folded}'!Missing&quot;)"),
            format!("SUM('{folded}'!A:A)"),
        ] {
            let mut p = inert_fixture(kind, false, &formula);
            let workbook = p.get_mut("xl/workbook.xml").unwrap();
            *workbook = workbook.replace("name=\"Inert\"", &format!("name=\"{name}\""));
            assert!(
                matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
                Err(formualizer_workbook::IoError::Unsupported { feature, .. })
                if feature == "reference to a non-worksheet sheet"),
                "{kind}: {formula}"
            );
        }
    }
}

#[test]
fn inert_sheet_indirect_references_are_guarded() {
    for formula in [
        "INDIRECT(&quot;Inert!A1&quot;)",
        "INDIRECT(&quot;Inert!R1C1&quot;,FALSE)",
        "INDIRECT(&quot;Sheet1:After!A1&quot;)",
        "_XLFN._XLWS.INDIRECT(&quot;Inert!A1&quot;)",
    ] {
        let p = inert_fixture("chartsheet", false, formula);
        assert!(
            matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "reference to a non-worksheet sheet")
        );
    }
    let p = inert_fixture(
        "chartsheet",
        false,
        "INDIRECT(&quot;After!&quot;&amp;&quot;A1&quot;)",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "nonliteral INDIRECT in a workbook with non-worksheet sheets")
    );
    let p = inert_fixture("chartsheet", false, "INDIRECT(&quot;After!A1&quot;)");
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(7.0));
}

#[test]
fn inert_sheet_positions_are_visible_to_metadata_functions() {
    for (formula, expected) in [
        ("SHEET(&quot;After&quot;)", 3.0),
        ("SHEETS()", 3.0),
        ("SHEET()", 1.0),
    ] {
        let p = inert_fixture("chartsheet", false, formula);
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), Data::Float(expected));
    }
}

#[test]
fn unused_local_name_does_not_shadow_other_sheet_scope() {
    let mut p = inert_fixture("chartsheet", false, "1+1");
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</definedNames>",
        "<definedName name=\"Rate\" localSheetId=\"0\">{1,2}</definedName></definedNames>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert!(member(&out.bytes, "xl/worksheets/sheet2.xml").contains("<v>7</v>"));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
}

#[test]
fn other_nonworksheet_sheet_types_remain_refused() {
    for kind in ["macrosheet", "intlmacrosheet"] {
        let p = inert_fixture(kind, false, "1+1");
        assert!(
            matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "non-worksheet sheet")
        );
    }
}

#[test]
fn unused_unevaluable_names_are_preserved() {
    let mut failures = Vec::new();
    for names in [
        "<definedName name=\"Unused\">{1,2}</definedName>",
        "<definedName name=\"Unused\">OFFSET(Sheet1!$A$1,0,0,2,1)</definedName>",
        "<definedName name=\"Unused\">OFFSET(#REF!,0,0,2,1)</definedName>",
        "<definedName name=\"Unused\">[0]!X</definedName>",
        "<definedName name=\"Unused\">Other</definedName><definedName name=\"Other\">Unused</definedName>",
        "<definedName name=\"Unused\">Other</definedName><definedName name=\"Other\">OFFSET(Unused,0,0)</definedName>",
        "<definedName name=\"Unused\">'#10'!$A$4:$AJ$23</definedName>",
    ] {
        let p = with_names(single("1+1", "<v>99</v>"), names);
        match recalculate_xlsx_bytes(&pack(&p), Default::default()) {
            Ok(out) => {
                assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
                assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
            }
            Err(error) => failures.push(format!("{names}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn unevaluable_name_readers_follow_engine_unicode_case_folding() {
    let p = with_names(
        single("äBAD", "<v>99</v>"),
        "<definedName name=\"Äbad\">{1,2}</definedName>",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
    );
}

#[test]
fn unregistrable_ordinary_names_are_kept_raw_only_when_unused() {
    let names = "<definedName name=\"a10intld\\\">7</definedName>";
    let p = with_names(single("1+1", "<v>99</v>"), names);
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
}

#[test]
fn unregistrable_ordinary_name_readers_refuse() {
    let names = "<definedName name=\"a10intld\\\">7</definedName>";
    for formula in ["a10intld\\", "INDIRECT(&quot;a10intld\\&quot;)"] {
        let p = with_names(single(formula, "<v>99</v>"), names);
        assert!(
            matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
        );
    }
    let p = with_names(
        single("1+1", "<v>99</v>"),
        "<definedName name=\"a10intld\\\">1+1</definedName><definedName name=\"Reader\">a10intld\\+1</definedName>",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
    );
}

#[test]
fn newly_admitted_workbooks_do_not_expose_masked_legacy_name_errors() {
    let legacy = "<definedName name=\"_xlnm.Auto_Open_hook\">[1]!Register.DClick</definedName>";
    let p = with_names(
        single("1+1", "<v>99</v>"),
        &format!("{legacy}<definedName name=\"Unused\">{{1,2}}</definedName>"),
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
    );
    let p = with_names(single("1+1", "<v>99</v>"), legacy);
    assert!(matches!(
        recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::XlsxRead(_))
    ));
    let mut p = inert_fixture("chartsheet", false, "1+1");
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace("</definedNames>", &format!("{legacy}</definedNames>"));
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
    );
}

#[test]
fn document_name_cycles_and_raw_readers_keep_legacy_refusals() {
    for names in [
        "<definedName name=\"_xlnm.Print_Area\">_xlnm.Print_Area</definedName>",
        "<definedName name=\"_xlnm.Print_Area\">Sheet1!$A$1,Sheet1!$B$1</definedName><definedName name=\"Unused\">OFFSET(_xlnm.Print_Area,0,0)</definedName>",
    ] {
        let p = with_names(single("1+1", "<v>99</v>"), names);
        assert!(matches!(
            recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { .. })
        ));
    }
}

#[test]
fn malformed_document_range_keeps_legacy_phantom_sheet_import() {
    let p = with_names(
        single("SHEETS()", "<v>99</v>"),
        "<definedName name=\"_xlnm.Print_Area\">Missing!$A$1</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
}

#[test]
fn generic_calamine_load_omits_unused_cycle() {
    use formualizer_eval::engine::ingest::EngineLoadStream;
    use formualizer_workbook::SpreadsheetReader;
    let p = with_names(
        single("1+1", "<v>99</v>"),
        "<definedName name=\"Unused\">Other</definedName><definedName name=\"Other\">Unused</definedName>",
    );
    let mut adapter = formualizer_workbook::CalamineAdapter::open_bytes(pack(&p)).unwrap();
    let mut engine = formualizer_eval::engine::Engine::new(
        formualizer_workbook::workbook::WBResolver::default(),
        Default::default(),
    );
    adapter.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(formualizer_common::LiteralValue::Number(2.0))
    );
    assert_eq!(adapter.name_import_diagnostics().len(), 2);
}

#[test]
fn literal_indirect_reads_unevaluable_names() {
    let p = with_names(
        single("INDIRECT(&quot;BadName&quot;)", "<v>99</v>"),
        "<definedName name=\"FirstBad\">{1,2}</definedName><definedName name=\"BadName\">OFFSET(Sheet1!$A$2,0,0)</definedName>",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, context }) if feature == "unsupported or cyclic calculation name" && context == "BadName")
    );
}

#[test]
fn nonliteral_indirect_is_guarded_only_for_newly_omitted_names() {
    let formula = "INDIRECT(&quot;A&quot;&amp;&quot;2&quot;)";
    let p = with_names(
        single(formula, "<v>99</v>"),
        "<definedName name=\"Unused\">{1,2}</definedName>",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "nonliteral INDIRECT in a workbook with unevaluable names")
    );
    let p = with_names(
        single(formula, "<v>99</v>"),
        "<definedName name=\"_xlnm.Print_Area\">Sheet1!$A$1,Sheet1!$B$1</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
}

#[test]
fn evaluated_name_cannot_read_an_unevaluable_name() {
    let p = with_names(
        single("1+1", "<v>99</v>"),
        "<definedName name=\"Unused\">OFFSET(#REF!,0,0,2,1)</definedName><definedName name=\"Reader\">Unused+1</definedName>",
    );
    assert!(recalculate_xlsx_bytes(&pack(&p), Default::default()).is_err());
}

#[test]
fn print_filter_metadata_is_preserved_but_referenced_unsupported_name_refuses() {
    let names = "<definedName name=\"_xlnm.Print_Area\" localSheetId=\"0\">Sheet1!$A$1,Sheet1!$B$1</definedName><definedName name=\"_xlnm._FilterDatabase\" localSheetId=\"0\" hidden=\"1\">Sheet1!$A$1:$B$2</definedName>";
    let p = with_names(single("1+1", "<v>99</v>"), names);
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    // Names are case-insensitive: any spelling of the reference refuses.
    for formula in ["SUM(_xlnm.Print_Area)", "SUM(_XLNM.PRINT_AREA)"] {
        let p = with_names(single(formula, "<v>99</v>"), names);
        assert!(matches!(
            recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { .. })
        ));
    }
}

#[test]
fn stale_cache_and_untouched_members_and_metadata() {
    let input = fixture("1+1", "99");
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(
        (
            out.formula_cells,
            out.cache_cells_changed,
            out.worksheet_parts_changed
        ),
        (1, 1, 1)
    );
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    let mut before = ZipArchive::new(Cursor::new(&input)).unwrap();
    let mut after = ZipArchive::new(Cursor::new(&out.bytes)).unwrap();
    assert_eq!(before.comment(), after.comment());
    assert_eq!(before.len(), after.len());
    for i in 0..before.len() {
        let a = before.by_index(i).unwrap();
        let b = after.by_index(i).unwrap();
        assert_eq!(
            a.name().expect("fixture ZIP entry name"),
            b.name().expect("fixture ZIP entry name")
        );
        assert_eq!(a.last_modified(), b.last_modified());
        assert_eq!(a.unix_mode(), b.unix_mode());
        assert_eq!(a.compression(), b.compression());
        if a.name().expect("fixture ZIP entry name") != SHEET {
            assert_eq!(
                &input[a.data_start().unwrap() as usize
                    ..(a.data_start().unwrap() + a.compressed_size()) as usize],
                &out.bytes[b.data_start().unwrap() as usize
                    ..(b.data_start().unwrap() + b.compressed_size()) as usize]
            );
        }
    }
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, XlsxRecalculateOptions::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}
#[test]
fn exact_noops() {
    for input in [
        fixture("1+1", "2"),
        pack(&parts("<row r=\"1\"><c r=\"A1\"><v>2</v></c></row>")),
        pack(&parts("")),
    ] {
        let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
        assert_eq!(out.bytes, input);
        assert_eq!(out.cache_cells_changed, 0);
    }
}
#[test]
fn missing_cache_and_typed_cache_repairs() {
    for (formula, expected, fragment) in [
        ("1+1", Data::Float(2.0), "<v>2</v>"),
        ("TRUE()", Data::Bool(true), "t=\"b\""),
        (
            "&quot;hi &amp; &lt; 💡&quot;",
            Data::String("hi & < 💡".into()),
            "<v>hi &amp; &lt; 💡</v>",
        ),
        ("&quot;&quot;", Data::String(String::new()), "<v></v>"),
        (
            "1/0",
            Data::Error(cached_reader::CellErrorType::Div0),
            "t=\"e\"",
        ),
    ] {
        let input = pack(&single(formula, ""));
        let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), expected, "{formula}");
        assert!(member(&out.bytes, SHEET).contains(fragment));
        assert_eq!(
            recalculate_xlsx_bytes(&out.bytes, XlsxRecalculateOptions::default())
                .unwrap()
                .bytes,
            out.bytes
        );
    }
}
#[test]
fn source_epoch_is_authoritative() {
    for (date1904, expected) in [("0", 1463.0), ("1", 1.0), ("true", 1.0)] {
        let mut p = single("DATE(1904,1,2)", "<v>99</v>");
        let wb = p.get_mut("xl/workbook.xml").unwrap();
        *wb = wb.replace(
            "<sheets>",
            &format!("<workbookPr date1904=\"{date1904}\"/><sheets>"),
        );
        let out = recalculate_xlsx_bytes(&pack(&p), XlsxRecalculateOptions::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), Data::Float(expected));
        assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    }
}
#[test]
fn shared_formula_text_is_untouched() {
    let p = parts(
        "<row r=\"1\"><c r=\"A1\"><f t=\"shared\" si=\"0\" ref=\"A1:A3\">ROW()</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><f t=\"shared\" si=\"0\"/><v>99</v></c></row><row r=\"3\"><c r=\"A3\"><f t=\"shared\" si=\"0\"/><v>99</v></c></row>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(out.formula_cells, 3);
    for r in 0..3 {
        assert_eq!(data(&out.bytes, r), Data::Float(f64::from(r + 1)));
    }
    let xml = member(&out.bytes, SHEET);
    assert!(xml.contains("<f t=\"shared\" si=\"0\" ref=\"A1:A3\">ROW()</f>"));
    assert_eq!(xml.matches("<f t=\"shared\" si=\"0\"/>").count(), 2);
}
#[test]
fn prefixes_quotes_and_unknown_children_survive() {
    let mut p = single("1+1", "<v>99</v>");
    p.insert(SHEET.into(),format!("<x:worksheet xmlns:x='{MAIN}' xmlns:u='urn:opaque'><x:sheetData><x:row r='1'><x:c r='A1' t='str' u:note='a&amp;&#13;'><x:f>1+1</x:f><x:v>old</x:v></x:c></x:row></x:sheetData><u:payload token='keep'/></x:worksheet>"));
    let out = recalculate_xlsx_bytes(&pack(&p), XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(
        member(&out.bytes, SHEET),
        p[SHEET]
            .replace("t='str'", "")
            .replace(">old</x:v>", ">2</x:v>")
    );
}
#[test]
fn malformed_and_unsupported_worksheet_matrix() {
    let original = single("1+1", "<v>99</v>");
    for xml in [
        original[SHEET].replace("<f>", "<f t=\"array\">"),
        original[SHEET].replace("<f>", "<f t=\"dataTable\">"),
        original[SHEET].replace("<f>", "<f t=\"shared\" si=\"8\">"),
        original[SHEET].replace("r=\"A1\"", "r=\"A0\""),
        original[SHEET].replace("r=\"A1\"", "r=\"XFE1\""),
        original[SHEET].replace("r=\"A1\"", "r=\"A2\""),
        original[SHEET].replace("r=\"A1\"", "r=\"$A$1\""),
        original[SHEET].replace("r=\"A1\"", "r=\"A1\" cm=\"1\""),
        original[SHEET].replace("<f>", "<f xmlns=\"urn:foreign\">"),
        original[SHEET].replace("<v>99</v>", "<v>99</v><v>1</v>"),
        original[SHEET].replace("<v>99</v>", "<v>&unknown;</v>"),
        original[SHEET].replace("<v>99</v>", "<v>&#0;</v>"),
        original[SHEET].replace("</row>", "</c>"),
        format!("<!DOCTYPE worksheet [<!ENTITY e 'x'>]>{}", original[SHEET]),
        original[SHEET].replace(
            "<sheetData>",
            "<dimension ref=\"A1:XFD1048576\"/><sheetData>",
        ),
        original[SHEET].replace(
            "<sheetData>",
            "<dimension ref=\"A1\"/><dimension ref=\"A1\"/><sheetData>",
        ),
        original[SHEET].replace("<sheetData>", "<dimension ref=\"B1\"/><sheetData>"),
    ] {
        let mut p = original.clone();
        p.insert(SHEET.into(), xml);
        reject(&p);
    }
}
#[test]
fn package_mapping_and_signature_rejections() {
    let original = single("1+1", "<v>99</v>");
    for (name, old, new) in [
        (
            "_rels/.rels",
            "Target=\"xl/workbook.xml\"",
            "Target=\"elsewhere.xml\"",
        ),
        (
            "xl/_rels/workbook.xml.rels",
            "Target=\"worksheets/sheet1.xml\"",
            "Target=\"../../../escape.xml\"",
        ),
        (
            "xl/_rels/workbook.xml.rels",
            "/worksheet\"",
            "/chartsheet\"",
        ),
        ("xl/workbook.xml", MAIN, "urn:foreign"),
        (
            "[Content_Types].xml",
            "spreadsheetml.worksheet+xml",
            "spreadsheetml.styles+xml",
        ),
    ] {
        let mut p = original.clone();
        let s = p.get_mut(name).unwrap();
        *s = s.replace(old, new);
        reject(&p);
    }
    let mut p = original;
    p.insert("_xmlsignatures/sig1.xml".into(), "<Signature/>".into());
    reject(&p);
}
#[test]
fn limits_and_precancellation() {
    let input = fixture("1+1", "99");
    for which in 0..7 {
        let mut o = XlsxRecalculateOptions::default();
        match which {
            0 => o.limits.max_input_bytes = 1,
            1 => o.limits.max_entries = 1,
            2 => o.limits.max_expanded_bytes = 1,
            3 => o.limits.max_worksheet_bytes = 1,
            4 => o.limits.max_formula_cells = 0,
            5 => o.limits.max_xml_depth = 1,
            _ => o.limits.max_cells = 0,
        };
        assert!(recalculate_xlsx_bytes(&input, o).is_err(), "limit {which}");
    }
    let cancel = formualizer_eval::engine::CancelToken::new();
    cancel.cancel();
    let o = XlsxRecalculateOptions {
        cancel: Some(cancel),
        ..Default::default()
    };
    assert!(recalculate_xlsx_bytes(&input, o).is_err());
}
#[test]
fn unsupported_spill_does_not_return_a_partial_package() {
    // Multi-cell spills are published since FORM211; one crossing a merge
    // is still refused as a whole.
    let mut p = single("SEQUENCE(2)", "<v>99</v>");
    let sheet = p.get_mut(SHEET).unwrap();
    *sheet = sheet.replace(
        "</sheetData>",
        "</sheetData><mergeCells count=\"1\"><mergeCell ref=\"A2:B2\"/></mergeCells>",
    );
    reject(&p);
    let out = recalculate_xlsx_bytes(&fixture("SEQUENCE(2)", "99"), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 1), Data::Float(2.0));
}
#[test]
fn error_locations_are_bounded() {
    let o = XlsxRecalculateOptions {
        error_location_limit: 0,
        ..Default::default()
    };
    let out = recalculate_xlsx_bytes(&fixture("1/0", "99"), o).unwrap();
    assert_eq!(out.summary.errors, 1);
    let error = &out.summary.error_summary["#DIV/0!"];
    assert_eq!(error.locations.len(), 0);
    assert_eq!(error.locations_truncated, 1);
}
#[test]
fn typed_text_controls_fail_instead_of_silent_corruption() {
    for formula in ["CHAR(1)", "&quot;_x0041_&quot;"] {
        assert!(
            recalculate_xlsx_bytes(&fixture(formula, "99"), XlsxRecalculateOptions::default())
                .is_err()
        );
    }
}
#[test]
fn modern_scalar_errors_are_cached_and_can_be_recalculated_again() {
    for (formula, token) in [
        ("SEQUENCE(2)", "#SPILL!"),
        ("FILTER(A2:A2,FALSE)", "#CALC!"),
    ] {
        let p = parts(&format!(
            "<row r=\"1\"><c r=\"A1\"><f>{formula}</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><v>7</v></c></row>"
        ));
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert_eq!(out.summary.errors, 1);
        assert!(member(&out.bytes, SHEET).contains(&format!("<v>{token}</v>")));
        let again = recalculate_xlsx_bytes(&out.bytes, Default::default()).unwrap();
        assert_eq!(again.bytes, out.bytes);
        assert_eq!(again.summary.errors, 1);
    }
}
#[test]
fn defined_names_are_evaluated_without_metadata_rewrite() {
    let mut p = parts(
        "<row r=\"1\"><c r=\"A1\"><f>Answer+1</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><v>7</v></c></row>",
    );
    let wb = p.get_mut("xl/workbook.xml").unwrap();
    *wb=wb.replace("</workbook>","<definedNames><definedName name=\"Answer\">Sheet1!$A$2</definedName></definedNames></workbook>");
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(8.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    let wb = p.get_mut("xl/workbook.xml").unwrap();
    *wb = wb.replace(
        "</definedNames>",
        "<definedName name=\"answer\">Sheet1!$A$1</definedName></definedNames>",
    );
    reject(&p);
}
#[test]
fn defined_name_range_endpoints_are_refused_not_written() {
    // Excel computes the bounding range (14 / 6 / 9 here); the evaluator does
    // not resolve name endpoints yet, so the package must be refused rather
    // than rewritten with a cached error.
    for formula in ["SUM(A1:Total)", "SUM(Start:A3)", "SUM(Start:Total)"] {
        let mut p = parts(&format!(
            "<row r=\"1\"><c r=\"A1\"><v>2</v></c><c r=\"B1\"><f>{formula}</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><v>3</v></c></row><row r=\"3\"><c r=\"A3\"><v>4</v></c></row>"
        ));
        let wb = p.get_mut("xl/workbook.xml").unwrap();
        *wb = wb.replace(
            "</workbook>",
            "<definedNames><definedName name=\"Start\">Sheet1!$A$2</definedName><definedName name=\"Total\">Sheet1!$A$3</definedName></definedNames></workbook>",
        );
        let Err(error) = recalculate_xlsx_bytes(&pack(&p), Default::default()) else {
            panic!("{formula}: recalculation wrote the package");
        };
        assert!(
            matches!(&error, formualizer_workbook::IoError::Unsupported { feature, context }
                if feature.contains("no approved XLSX cache encoding") && context == "#N/IMPL!"),
            "{formula}: unexpected error: {error:?}"
        );
    }
}
#[test]
fn nonportable_literal_errors_are_refused_and_empty_table_parts_are_inert() {
    let p = parts(
        "<row r=\"1\"><c r=\"A1\" t=\"e\"><v>#SPILL!</v></c><c r=\"B1\"><f>IFERROR(A1,0)</f><v>99</v></c></row>",
    );
    let error = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap_err();
    assert!(
        matches!(error,formualizer_workbook::IoError::Unsupported{feature,..} if feature.contains("literal error"))
    );
    let mut p = single("1+1", "<v>99</v>");
    let s = p.get_mut(SHEET).unwrap();
    *s = s.replace("</worksheet>", "<tableParts count=\"0\"/></worksheet>");
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
}
#[test]
fn engine_specific_errors_are_unsupported_results_not_invented_excel_tokens() {
    let error = recalculate_xlsx_bytes(&fixture("AGGREGATE(12,0,1)", "99"), Default::default())
        .unwrap_err();
    assert!(
        matches!(&error,formualizer_workbook::IoError::Unsupported{feature,context}
            if feature=="formula result is not current"
                || (feature.contains("no approved XLSX cache encoding") && context=="#N/IMPL!")),
        "unexpected error: {error:?}"
    );
}
#[test]
fn arbitrary_stale_error_cache_is_not_evaluator_authority() {
    let mut p = single("1+1", "<v>#FUTURE_ERROR!</v>");
    let s = p.get_mut(SHEET).unwrap();
    *s = s.replace("r=\"A1\"", "r=\"A1\" t=\"e\"");
    let output = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&output.bytes, 0), Data::Float(2.0));
}
#[test]
fn cached_text_empty_element_is_an_exact_noop() {
    let mut p = single("&quot;&quot;", "<v/>");
    let xml = p.get_mut(SHEET).unwrap();
    *xml = xml.replace("r=\"A1\"", "r=\"A1\" t=\"str\"");
    let input = pack(&p);
    let out = recalculate_xlsx_bytes(&input, Default::default()).unwrap();
    assert_eq!(out.bytes, input);
    assert_eq!(data(&out.bytes, 0), Data::String(String::new()));
}
#[test]
fn one_cell_dynamic_result_does_not_require_geometry_writeback() {
    let out = recalculate_xlsx_bytes(&fixture("SEQUENCE(1)", "99"), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(1.0));
}
#[test]
fn scalar_ingestion_cannot_silently_drop_xml_text() {
    for payload in ["1&#50;", "1<!--split-->2", "<![CDATA[12]]>"] {
        let p = parts(&format!(
            "<row r=\"1\"><c r=\"A1\"><v>{payload}</v></c><c r=\"B1\"><f>A1+1</f><v>99</v></c></row>"
        ));
        reject(&p);
    }
    let input = pack(&single("12", "<v>1&#50;</v>"));
    let out = recalculate_xlsx_bytes(&input, Default::default()).unwrap();
    assert_eq!(out.bytes, input);
    let p = single("1<![CDATA[+1]]>", "<v>99</v>");
    reject(&p);
}
#[test]
fn serial_egress_preserves_phantom_day_and_fractional_dates() {
    for value in ["60", "60.125", "-0.125"] {
        let mut p = single(value, "<v>99</v>");
        let worksheet = p.get_mut(SHEET).unwrap();
        *worksheet = worksheet.replace("r=\"A1\"", "r=\"A1\" s=\"0\"");
        p.insert("xl/styles.xml".into(),format!("<styleSheet xmlns=\"{MAIN}\"><cellXfs count=\"1\"><xf numFmtId=\"14\"/></cellXfs></styleSheet>"));
        let rel = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
        *rel=rel.replace("</Relationships>",&format!("<Relationship Id=\"style\" Type=\"{OFFICE}/styles\" Target=\"styles.xml\"/></Relationships>"));
        let ct = p.get_mut("[Content_Types].xml").unwrap();
        *ct=ct.replace("</Types>","<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/></Types>");
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert!(member(&out.bytes, SHEET).contains(&format!("<v>{value}</v>")));
        assert_eq!(member(&out.bytes, "xl/styles.xml"), p["xl/styles.xml"]);
    }
}
fn h16(b: &[u8], i: usize) -> usize {
    u16::from_le_bytes(b[i..i + 2].try_into().unwrap()) as usize
}
fn h32(b: &[u8], i: usize) -> usize {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize
}
fn directory(bytes: &[u8]) -> (Vec<usize>, usize) {
    let archive = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut at = archive.central_directory_start() as usize;
    let mut result = Vec::new();
    for _ in 0..archive.len() {
        result.push(at);
        at += 46 + h16(bytes, at + 28) + h16(bytes, at + 30) + h16(bytes, at + 32);
    }
    (result, at)
}
#[test]
fn zip_metadata_is_not_normalized_even_for_changed_members() {
    let mut input = fixture("1+1", "99");
    let (headers, _) = directory(&input);
    for &at in &headers {
        input[at + 4] = 20;
        input[at + 36] = 1;
        input[at + 38] |= 0x20;
    }
    let output = recalculate_xlsx_bytes(&input, Default::default())
        .unwrap()
        .bytes;
    let (after, _) = directory(&output);
    for (&a, &b) in headers.iter().zip(&after) {
        let length = 46 + h16(&input, a + 28) + h16(&input, a + 30) + h16(&input, a + 32);
        for i in 0..length {
            if !(16..28).contains(&i) && !(42..46).contains(&i) {
                assert_eq!(input[a + i], output[b + i], "central field {i}");
            }
        }
        let la = h32(&input, a + 42);
        let lb = h32(&output, b + 42);
        let local_length = 30 + h16(&input, la + 26) + h16(&input, la + 28);
        for i in 0..local_length {
            if !(14..26).contains(&i) {
                assert_eq!(input[la + i], output[lb + i], "local field {i}");
            }
        }
    }
}
#[test]
fn multiple_changed_members_relocate_growing_and_shrinking_payloads() {
    let old = (0..2048u32)
        .map(|n| format!("{:08x}", n.wrapping_mul(2_654_435_761)))
        .collect::<String>();
    let mut p = single("1+1", &format!("<v>{old}</v>"));
    let sheet = p.get_mut(SHEET).unwrap();
    *sheet = sheet.replace("r=\"A1\"", "r=\"A1\" t=\"str\"");
    let wb = p.get_mut("xl/workbook.xml").unwrap();
    *wb = wb.replace(
        "</sheets>",
        "<sheet name=\"Sheet2\" sheetId=\"2\" r:id=\"rId2\"/></sheets>",
    );
    let rel = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
    *rel=rel.replace("</Relationships>",&format!("<Relationship Id=\"rId2\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"));
    let ct = p.get_mut("[Content_Types].xml").unwrap();
    *ct=ct.replace("</Types>","<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>");
    p.insert("xl/worksheets/sheet2.xml".into(),format!("<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"str\"><f>REPT(&quot;x&quot;,32767)</f><v>q</v></c></row></sheetData></worksheet>"));
    p.insert("zz/opaque.bin".into(), "opaque tail".into());
    let input = pack(&p);
    let out = recalculate_xlsx_bytes(&input, Default::default()).unwrap();
    assert_eq!(out.worksheet_parts_changed, 2);
    let mut before = ZipArchive::new(Cursor::new(&input)).unwrap();
    let mut after = ZipArchive::new(Cursor::new(&out.bytes)).unwrap();
    assert!(
        before.by_name(SHEET).unwrap().compressed_size()
            > after.by_name(SHEET).unwrap().compressed_size()
    );
    assert!(
        before
            .by_name("xl/worksheets/sheet2.xml")
            .unwrap()
            .compressed_size()
            < after
                .by_name("xl/worksheets/sheet2.xml")
                .unwrap()
                .compressed_size()
    );
    let a = before.by_name("zz/opaque.bin").unwrap();
    let b = after.by_name("zz/opaque.bin").unwrap();
    assert_eq!(
        &input[a.data_start().unwrap() as usize
            ..(a.data_start().unwrap() + a.compressed_size()) as usize],
        &out.bytes[b.data_start().unwrap() as usize
            ..(b.data_start().unwrap() + b.compressed_size()) as usize]
    );
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, Default::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}
#[test]
fn duplicate_zip_names_are_not_hidden_by_archive_index() {
    let mut input = fixture("1+1", "99");
    let (headers, footer) = directory(&input);
    let a = headers[0];
    let len = 46 + h16(&input, a + 28) + h16(&input, a + 30) + h16(&input, a + 32);
    let copy = input[a..a + len].to_vec();
    input.splice(footer..footer, copy);
    let footer = footer + len;
    for offset in [8, 10] {
        input[footer + offset..footer + offset + 2]
            .copy_from_slice(&((headers.len() + 1) as u16).to_le_bytes());
    }
    let size = h32(&input, footer + 12) + len;
    input[footer + 12..footer + 16].copy_from_slice(&(size as u32).to_le_bytes());
    assert!(recalculate_xlsx_bytes(&input, Default::default()).is_err());
}
#[test]
fn missing_data_descriptors_and_inconsistent_headers_are_rejected() {
    let original = fixture("1+1", "99");
    let (headers, _) = directory(&original);
    let a = headers[0];
    let local = h32(&original, a + 42);
    let mut mismatch = original.clone();
    mismatch[local + 14] ^= 1;
    assert!(recalculate_xlsx_bytes(&mismatch, Default::default()).is_err());
    let mut descriptor = original;
    descriptor[a + 8] |= 8;
    descriptor[local + 6] |= 8;
    assert!(recalculate_xlsx_bytes(&descriptor, Default::default()).is_err());
}
#[test]
fn actual_expansion_and_output_limits_are_enforced() {
    let mut p = single("1+1", "<v>99</v>");
    p.insert("custom/opaque.bin".into(), "x".repeat(1 << 20));
    let input = pack(&p);
    let mut o = XlsxRecalculateOptions::default();
    o.limits.max_expanded_bytes = 1 << 16;
    assert!(recalculate_xlsx_bytes(&input, o).is_err());
    for cache in ["2", "99"] {
        let mut o = XlsxRecalculateOptions::default();
        o.limits.max_output_bytes = 1;
        assert!(recalculate_xlsx_bytes(&fixture("1+1", cache), o).is_err());
    }
    let mut p = single("1+1", "<v>99</v>");
    let xml = p.get_mut(SHEET).unwrap();
    *xml = xml.replace("<sheetData>", "<dimension ref=\"A1:XFD1\"/><sheetData>");
    reject(&p);
}
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn atomic_native_output_and_permissions() {
    use formualizer_workbook::recalculate_xlsx_file;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    let output = dir.path().join("out.xlsx");
    std::fs::write(&input, b"bad input").unwrap();
    std::fs::write(&output, b"existing output").unwrap();
    assert!(recalculate_xlsx_file(&input, Some(&output), Default::default()).is_err());
    assert_eq!(std::fs::read(&output).unwrap(), b"existing output");
    let source = fixture("1+1", "99");
    std::fs::write(&input, &source).unwrap();
    let out = recalculate_xlsx_file(&input, Some(&output), Default::default()).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), out.bytes);
    assert_eq!(std::fs::read(&input).unwrap(), source);
    recalculate_xlsx_file(&input, None, Default::default()).unwrap();
    assert_eq!(data(&std::fs::read(&input).unwrap(), 0), Data::Float(2.0));
}
#[test]
fn unparseable_stored_formula_is_a_refusal_naming_the_cell() {
    use formualizer_eval::engine::FormulaParsePolicy;
    use formualizer_workbook::IoError;
    // A second, valid formula keeps both eager and deferred ingestion busy.
    let mut p = parts(
        "<row r=\"1\"><c r=\"A1\"><v>1</v></c><c r=\"B1\"><f>A1+1</f><v>9</v></c><c r=\"C1\"><f>SUM((A1</f><v>9</v></c></row>",
    );
    let input = pack(&p);
    for defer in [false, true] {
        let mut o = XlsxRecalculateOptions::default();
        o.eval_config.defer_graph_building = defer;
        match recalculate_xlsx_bytes(&input, o) {
            Err(IoError::Unsupported { feature, context }) => {
                assert_eq!(feature, "unparseable formula");
                assert!(context.starts_with("Sheet1!C1: "), "{context}");
                assert!(context.contains("parenthesis"), "{context}");
            }
            other => panic!("defer={defer}: expected a refusal, got {other:?}"),
        }
    }
    // A caller's explicit non-strict policy keeps its meaning: the coerced
    // #ERROR! result has no XLSX cache encoding.
    let mut o = XlsxRecalculateOptions::default();
    o.eval_config.formula_parse_policy = FormulaParsePolicy::CoerceToError;
    match recalculate_xlsx_bytes(&input, o) {
        Err(IoError::Unsupported { feature, context }) => {
            assert!(
                feature.contains("no approved XLSX cache encoding"),
                "{feature}"
            );
            assert_eq!(context, "#ERROR!");
        }
        other => panic!("expected the coerced-error refusal, got {other:?}"),
    }
    // Valid formulas are unaffected.
    let sheet = p.get_mut(SHEET).unwrap();
    *sheet = sheet.replace("SUM((A1", "SUM((A1))");
    assert!(recalculate_xlsx_bytes(&pack(&p), Default::default()).is_ok());
}

#[test]
fn source_policy_boundary_function_panic_is_partial_and_require_complete_refuses() {
    use formualizer_workbook::{
        CalculationDeferredReason, IoError, SourceCalculationCoverage, SourceUnavailableOperation,
    };
    const CASE: &str =
        "source_policy_boundary_function_panic_is_partial_and_require_complete_refuses";
    if std::env::var("FZ_SOURCE_PROVIDER_CHILD").ok().as_deref() != Some(CASE) {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CASE, "--nocapture"])
            .env("FZ_SOURCE_PROVIDER_CHILD", CASE)
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    struct Provider;
    impl formualizer_eval::function::Function for Provider {
        fn name(&self) -> &'static str {
            "SOURCE_TEST_PANIC_PROVIDER"
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _: &'c [formualizer_eval::traits::ArgumentHandle<'a, 'b>],
            _: &dyn formualizer_eval::traits::FunctionContext<'b>,
        ) -> Result<formualizer_eval::traits::CalcValue<'b>, formualizer_common::ExcelError>
        {
            panic!("source panic payload")
        }
    }
    formualizer_eval::function_registry::try_register_function(std::sync::Arc::new(Provider))
        .unwrap();
    let input = fixture("SOURCE_TEST_PANIC_PROVIDER()", "99");
    let result = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::workbook()).unwrap();
    assert_eq!(result.coverage, SourceCalculationCoverage::Partial);
    assert!(result.operation_diagnostics.samples.iter().any(|sample|matches!(
        &sample.operation,SourceUnavailableOperation::FunctionPanicked{name} if name=="SOURCE_TEST_PANIC_PROVIDER"
    )));
    assert!(matches!(
        recalculate_xlsx_bytes(
            &input,
            XlsxRecalculateOptions {
                require_complete: true,
                ..XlsxRecalculateOptions::workbook()
            }
        ),
        Err(IoError::CalculationDeferred {
            reason: CalculationDeferredReason::KnownCoverageIncomplete,
            ..
        })
    ));
}
