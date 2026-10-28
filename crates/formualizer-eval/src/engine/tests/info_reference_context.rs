use super::common::abs_cell_ref;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::RangeRef;
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ReferenceType, parse};

fn err_kind(value: Option<LiteralValue>) -> Option<ExcelErrorKind> {
    match value {
        Some(LiteralValue::Error(e)) => Some(e.kind),
        _ => None,
    }
}

// Error inspectors consume spreadsheet errors identically across argument carriers.
#[test]
fn error_consumers_inline_reference_tree_arena() {
    let cases = [
        ("ISERROR", true),
        ("ISERR", true),
        ("ISNA", false),
        ("ISNUMBER", false),
        ("ISTEXT", false),
        ("ISLOGICAL", false),
        ("ISBLANK", false),
        ("ISNONTEXT", true),
    ];
    let mut failures = Vec::new();
    for (source, kind) in [
        ("MISSING_NAME", ExcelErrorKind::Name),
        ("1/0", ExcelErrorKind::Div),
        ("NA()", ExcelErrorKind::Na),
    ] {
        let wb = TestWorkbook::new().with_cell(
            "Sheet1",
            1,
            1,
            LiteralValue::Error(formualizer_common::ExcelError::new(kind)),
        );
        let tree = wb.interpreter();
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: false,
                enable_parallel: false,
                ..EvalConfig::default()
            },
        );
        engine
            .set_cell_formula("Sheet1", 1, 1, parse(&format!("={source}")).unwrap())
            .unwrap();
        for (idx, (name, name_expected)) in cases.iter().enumerate() {
            let expected = LiteralValue::Boolean(match *name {
                "ISERR" => kind != ExcelErrorKind::Na,
                "ISNA" => kind == ExcelErrorKind::Na,
                _ => *name_expected,
            });
            for (carrier, arg) in [("inline", source), ("reference", "A1")] {
                let formula = format!("={name}({arg})");
                let ast = parse(&formula).unwrap();
                let actual = tree.evaluate_ast(&ast).map(|v| v.into_literal());
                if actual != Ok(expected.clone()) {
                    failures.push(format!(
                        "tree {carrier} {formula}: {actual:?}, expected {expected:?}"
                    ));
                }
                let col = (idx * 2 + if carrier == "inline" { 2 } else { 3 }) as u32;
                engine.set_cell_formula("Sheet1", 1, col, ast).unwrap();
            }
        }
        for (i, arg) in [source, "A1"].iter().enumerate() {
            engine
                .set_cell_formula(
                    "Sheet1",
                    1,
                    20 + i as u32,
                    parse(&format!("=ERROR.TYPE({arg})")).unwrap(),
                )
                .unwrap();
            let actual = tree
                .evaluate_ast(&parse(&format!("=ERROR.TYPE({arg})")).unwrap())
                .map(|v| v.into_literal());
            let code = match kind {
                ExcelErrorKind::Name => 5,
                ExcelErrorKind::Div => 2,
                _ => 7,
            };
            let code_actual = actual.map(|v| match v {
                LiteralValue::Int(n) => n,
                LiteralValue::Number(n) => n as i64,
                _ => -1,
            });
            if code_actual != Ok(code) {
                failures.push(format!(
                    "tree ERROR.TYPE({arg}): {code_actual:?}, expected {code}"
                ));
            }
        }
        engine.evaluate_all().unwrap();
        for (idx, (name, name_expected)) in cases.iter().enumerate() {
            let expected = Some(LiteralValue::Boolean(match *name {
                "ISERR" => kind != ExcelErrorKind::Na,
                "ISNA" => kind == ExcelErrorKind::Na,
                _ => *name_expected,
            }));
            for col in [idx as u32 * 2 + 2, idx as u32 * 2 + 3] {
                let actual = engine.get_cell_value("Sheet1", 1, col);
                if actual != expected {
                    failures.push(format!(
                        "arena {name}({source}), col {col}: {actual:?}, expected {expected:?}"
                    ));
                }
            }
        }
        for col in [20, 21] {
            let expected = match kind {
                ExcelErrorKind::Name => 5.0,
                ExcelErrorKind::Div => 2.0,
                _ => 7.0,
            };
            if engine.get_cell_value("Sheet1", 1, col) != num(expected) {
                failures.push(format!(
                    "arena ERROR.TYPE({source}), col {col}: {:?}",
                    engine.get_cell_value("Sheet1", 1, col)
                ));
            }
        }
        for name in ["N", "T"] {
            for arg in [source, "A1"] {
                let actual = tree.evaluate_ast(&parse(&format!("={name}({arg})")).unwrap());
                let actual_kind = match actual {
                    Err(e) => Some(e.kind),
                    Ok(v) => err_kind(Some(v.into_literal())),
                };
                assert_eq!(actual_kind, Some(kind), "propagation control {name}({arg})");
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn registered_info_type_error_codes_tree_arena_family() {
    let mut failures = Vec::new();
    for family in [false, true] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: family,
                enable_parallel: false,
                ..super::common::arrow_eval_config()
            },
        );
        for row in 1..=24 {
            for (col, arg) in ["MISSING_NAME", "1/0", "NA()", "#VALUE!"]
                .iter()
                .enumerate()
            {
                engine
                    .set_cell_formula(
                        "Sheet1",
                        row,
                        col as u32 + 1,
                        parse(&format!("={arg}")).unwrap(),
                    )
                    .unwrap();
                engine
                    .set_cell_formula(
                        "Sheet1",
                        row,
                        col as u32 + 5,
                        parse(&format!("=TYPE({arg})")).unwrap(),
                    )
                    .unwrap();
                let letter = (b'A' + col as u8) as char;
                engine
                    .set_cell_formula(
                        "Sheet1",
                        row,
                        col as u32 + 9,
                        parse(&format!("=TYPE({letter}{row})")).unwrap(),
                    )
                    .unwrap();
            }
        }
        engine.evaluate_all().unwrap();
        for row in 1..=24 {
            for col in 5..=12 {
                if engine.get_cell_value("Sheet1", row, col) != num(16.0) {
                    failures.push(format!(
                        "arena/family={family} row={row} col={col}: {:?}",
                        engine.get_cell_value("Sheet1", row, col)
                    ));
                }
            }
        }
        let tree = crate::interpreter::Interpreter::new(&engine, "Sheet1");
        for arg in [
            "MISSING_NAME",
            "1/0",
            "NA()",
            "#VALUE!",
            "A1",
            "B1",
            "C1",
            "D1",
        ] {
            let result = tree
                .evaluate_ast(&parse(&format!("=TYPE({arg})")).unwrap())
                .map(|v| v.into_literal());
            if result != Ok(LiteralValue::Int(16)) {
                failures.push(format!("tree TYPE({arg}): {result:?}"));
            }
        }
        if family {
            assert!(engine.family_members_for_test() > 0);
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn registered_info_isformula_metadata_edits() {
    for family in [false, true] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: family,
                enable_parallel: false,
                ..super::common::arrow_eval_config()
            },
        );
        let sheet = engine.graph.sheet_id("Sheet1").unwrap();
        engine
            .define_name(
                "FormulaCell",
                NamedDefinition::Cell(abs_cell_ref(sheet, 1, 1)),
                NameScope::Workbook,
            )
            .unwrap();
        engine
            .define_name(
                "ConstantName",
                NamedDefinition::Literal(LiteralValue::Int(2)),
                NameScope::Workbook,
            )
            .unwrap();
        engine.add_sheet("Data").unwrap();
        engine
            .set_cell_formula("Data", 1, 1, parse("=1/0").unwrap())
            .unwrap();
        for row in 1..=24 {
            engine
                .set_cell_formula("Sheet1", row, 1, parse("=1+1").unwrap())
                .unwrap();
            engine
                .set_cell_formula(
                    "Sheet1",
                    row,
                    2,
                    parse(&format!("=ISFORMULA(A{row})")).unwrap(),
                )
                .unwrap();
            engine
                .set_cell_formula(
                    "Sheet1",
                    row,
                    3,
                    parse(&format!("=ISFORMULA(A{row}:A{row})")).unwrap(),
                )
                .unwrap();
        }
        let extras = [
            ("ISFORMULA(FormulaCell)", true),
            ("ISFORMULA(ConstantName)", false),
            ("ISFORMULA(1+1)", false),
            ("ISFORMULA(Z99)", false),
            ("ISFORMULA(Data!A1)", true),
            ("ISFORMULA(INDEX(A1:A2,1))", true),
        ];
        for (i, (formula, _)) in extras.iter().enumerate() {
            engine
                .set_cell_formula(
                    "Sheet1",
                    1,
                    i as u32 + 4,
                    parse(&format!("={formula}")).unwrap(),
                )
                .unwrap();
        }
        for phase in 0..3 {
            if phase > 0 {
                for row in 1..=24 {
                    if phase == 1 {
                        engine
                            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(2.0))
                            .unwrap();
                    } else {
                        engine
                            .set_cell_formula("Sheet1", row, 1, parse("=1+1").unwrap())
                            .unwrap();
                    }
                }
            }
            engine.evaluate_all().unwrap();
            let expected = phase != 1;
            for row in 1..=24 {
                for col in [2, 3] {
                    assert_eq!(
                        engine.get_cell_value("Sheet1", row, col),
                        Some(LiteralValue::Boolean(expected)),
                        "metadata-only same-value edit phase={phase}, family={family}, row={row}, col={col}"
                    );
                }
            }
            for (i, (formula, base)) in extras.iter().enumerate() {
                let expected = if i == 0 || i == 5 { expected } else { *base };
                assert_eq!(
                    engine.get_cell_value("Sheet1", 1, i as u32 + 4),
                    Some(LiteralValue::Boolean(expected)),
                    "{formula} phase={phase}, family={family}"
                );
                let tree = crate::interpreter::Interpreter::new(&engine, "Sheet1");
                assert_eq!(
                    tree.evaluate_ast(&parse(&format!("={formula}")).unwrap())
                        .unwrap()
                        .into_literal(),
                    LiteralValue::Boolean(expected),
                    "tree {formula} phase={phase}"
                );
            }
        }
        // Workbook metadata is ineligible for elementwise lifting; use its
        // ordinary reference-aware walk rather than broadening eligibility.
        assert_eq!(engine.lifted_members_for_test(), 0);
    }
}

fn num(value: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(value))
}

#[test]
fn context_sheet_metadata_tracks_add_rename_remove_without_name_cloning_api() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = engine.add_sheet("Data").unwrap();
    let scratch = engine.add_sheet("Scratch").unwrap();
    let summary = engine.add_sheet("Summary").unwrap();

    assert_eq!(engine.workbook_sheet_count(), Some(4));
    assert_eq!(engine.sheet_index_by_name("Sheet1"), Some(1));
    assert_eq!(engine.sheet_index_by_name("Data"), Some(2));
    assert_eq!(engine.sheet_index_by_name("Scratch"), Some(3));
    assert_eq!(engine.sheet_index_by_name("Summary"), Some(4));
    assert_eq!(engine.current_sheet_index("Summary"), Some(4));

    engine.rename_sheet(data, "Inputs").unwrap();
    assert_eq!(engine.sheet_index_by_name("Data"), None);
    assert_eq!(engine.sheet_index_by_name("Inputs"), Some(2));

    engine.remove_sheet(scratch).unwrap();
    assert_eq!(engine.workbook_sheet_count(), Some(3));
    assert_eq!(engine.sheet_index_by_name("Summary"), Some(3));
    assert_eq!(engine.sheet_name(summary), "Summary");
}

#[test]
fn inspect_reference_covers_cells_ranges_names_tables_and_3d_without_materializing_values() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = engine.add_sheet("Data").unwrap();
    engine.add_sheet("More").unwrap();

    let cell = ReferenceType::cell(Some("Data".to_string()), 2, 3);
    let info = engine.inspect_reference(&cell, "Sheet1").unwrap().unwrap();
    assert_eq!(info.first_sheet_index, Some(2));
    assert_eq!(info.sheet_count, Some(1));
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 2, 3)));

    let open_col = ReferenceType::range(Some("Data".to_string()), None, Some(2), None, Some(2));
    let info = engine
        .inspect_reference(&open_col, "Sheet1")
        .unwrap()
        .unwrap();
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 1, 2)));

    engine
        .define_name(
            "InputCell",
            NamedDefinition::Cell(abs_cell_ref(data, 4, 5)),
            NameScope::Workbook,
        )
        .unwrap();
    let info = engine
        .inspect_reference(&ReferenceType::NamedRange("InputCell".into()), "Sheet1")
        .unwrap()
        .unwrap();
    assert_eq!(info.first_sheet_index, Some(2));
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 4, 5)));

    engine
        .define_name(
            "ConstantName",
            NamedDefinition::Literal(LiteralValue::Int(7)),
            NameScope::Workbook,
        )
        .unwrap();
    let info = engine
        .inspect_reference(&ReferenceType::NamedRange("ConstantName".into()), "Sheet1")
        .unwrap()
        .unwrap();
    assert_eq!(info.first_cell, None);
    assert_eq!(info.sheet_count, None);

    engine
        .define_table(
            "SalesTable",
            RangeRef::new(abs_cell_ref(data, 10, 2), abs_cell_ref(data, 12, 4)),
            true,
            vec!["A".into(), "B".into(), "C".into()],
            false,
        )
        .unwrap();
    let info = engine
        .inspect_reference(
            &ReferenceType::Table(formualizer_parse::parser::TableReference {
                name: "SalesTable".into(),
                specifier: None,
            }),
            "Sheet1",
        )
        .unwrap()
        .unwrap();
    assert_eq!(info.first_sheet_index, Some(2));
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 10, 2)));

    let three_d = ReferenceType::Range3D {
        sheet_first: "Data".into(),
        sheet_last: "More".into(),
        start_row: Some(2),
        start_col: Some(1),
        end_row: Some(4),
        end_col: Some(3),
        start_row_abs: false,
        start_col_abs: false,
        end_row_abs: false,
        end_col_abs: false,
    };
    let info = engine
        .inspect_reference(&three_d, "Sheet1")
        .unwrap()
        .unwrap();
    assert_eq!(info.first_sheet_index, Some(2));
    assert_eq!(info.sheet_count, Some(2));
    assert_eq!(info.first_cell, Some(abs_cell_ref(data, 2, 1)));

    assert!(
        engine
            .inspect_reference(&ReferenceType::NamedRange("MissingName".into()), "Sheet1")
            .is_err()
    );
}

#[test]
fn formula_text_context_uses_staged_text_then_canonical_ast_and_tracks_edits() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = engine.add_sheet("Data").unwrap();
    let a1 = abs_cell_ref(data, 1, 1);
    let a2 = abs_cell_ref(data, 2, 1);
    let a3 = abs_cell_ref(data, 3, 1);

    engine
        .set_cell_formula("Data", 1, 1, parse("=1+2").unwrap())
        .unwrap();
    assert_eq!(
        engine.formula_text_at_cell(a1).unwrap(),
        Some("=1 + 2".into())
    );

    engine
        .set_cell_formula("Data", 1, 1, parse("=SUM(1,2,3)").unwrap())
        .unwrap();
    assert_eq!(
        engine.formula_text_at_cell(a1).unwrap(),
        Some("=SUM(1, 2, 3)".into())
    );

    engine
        .set_cell_value("Data", 2, 1, LiteralValue::Text("not formula".into()))
        .unwrap();
    assert_eq!(engine.formula_text_at_cell(a2).unwrap(), None);

    engine.stage_formula_text("Data", 3, 1, "SUM(A1:A2)".into());
    assert_eq!(
        engine.formula_text_at_cell(a3).unwrap(),
        Some("=SUM(A1:A2)".into())
    );

    engine.stage_formula_text("Data", 3, 1, "=$A$1".into());
    assert_eq!(
        engine.formula_text_at_cell(a3).unwrap(),
        Some("=$A$1".into())
    );
}

#[test]
fn formula_text_sheet_sheets_and_isref_functions_evaluate_directly() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let data = engine.add_sheet("Data").unwrap();
    engine.add_sheet("More").unwrap();

    engine
        .define_name(
            "InputCell",
            NamedDefinition::Cell(abs_cell_ref(data, 1, 1)),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .define_name(
            "ConstantName",
            NamedDefinition::Literal(LiteralValue::Int(7)),
            NameScope::Workbook,
        )
        .unwrap();

    engine
        .set_cell_formula("Data", 1, 1, parse("=1+2").unwrap())
        .unwrap();
    engine
        .set_cell_value("Data", 2, 1, LiteralValue::Int(42))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=FORMULATEXT(Data!A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=FORMULATEXT(Data!A2)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Data", 3, 1, parse("=SHEET()").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=SHEET(Data!A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=SHEET(\"More\")").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 3, parse("=SHEETS()").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 4, parse("=SHEETS(Data:More!A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=ISREF(Data!A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 2, parse("=ISREF(1+1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 3, parse("=ISREF(MissingName)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 4, parse("=ISREF(InputCell)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 5, parse("=ISREF(ConstantName)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("=1 + 2".into()))
    );
    assert_eq!(
        err_kind(engine.get_cell_value("Sheet1", 1, 2)),
        Some(ExcelErrorKind::Na)
    );
    assert_eq!(engine.get_cell_value("Data", 3, 1), num(2.0));
    assert_eq!(engine.get_cell_value("Sheet1", 2, 1), num(2.0));
    assert_eq!(engine.get_cell_value("Sheet1", 2, 2), num(3.0));
    assert_eq!(engine.get_cell_value("Sheet1", 2, 3), num(3.0));
    assert_eq!(engine.get_cell_value("Sheet1", 2, 4), num(2.0));
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Boolean(false))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Boolean(false))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 4),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 5),
        Some(LiteralValue::Boolean(false))
    );

    engine
        .set_cell_formula("Data", 1, 1, parse("=3+4").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("=3 + 4".into()))
    );
}
