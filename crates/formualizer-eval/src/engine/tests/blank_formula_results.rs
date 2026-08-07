use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

#[derive(Clone, Copy, Debug)]
enum Expected {
    Number(f64),
    Boolean(bool),
    Text(&'static str),
}

#[derive(Clone, Copy, Debug)]
struct OracleCase {
    row: u32,
    formula: &'static str,
    oracle: &'static str,
    expected: Expected,
}

const ORACLE_CASES: &[OracleCase] = &[
    OracleCase {
        row: 1,
        formula: "=ISBLANK(C1)",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Boolean(true),
    },
    OracleCase {
        row: 2,
        formula: "=C1&\"x\"",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Text("x"),
    },
    OracleCase {
        row: 3,
        formula: "=LEN(C1)",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 4,
        formula: "=C1=0",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Boolean(true),
    },
    OracleCase {
        row: 5,
        formula: "=C1=\"\"",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Boolean(true),
    },
    OracleCase {
        row: 6,
        formula: "=T(C1)",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Text(""),
    },
    OracleCase {
        row: 7,
        formula: "=N(C1)",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 8,
        formula: "=ISNUMBER(C1)",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Boolean(false),
    },
    OracleCase {
        row: 9,
        formula: "=COUNT(C1)",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 10,
        formula: "=COUNTA(C1)",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 11,
        formula: "=TEXT(C1,\"0\")",
        oracle: "oracle: lo-verified must-not-change",
        expected: Expected::Text("0"),
    },
    OracleCase {
        row: 12,
        formula: "=C1",
        oracle: "oracle: lo-verified must-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 13,
        formula: "=+C1",
        oracle: "oracle: lo-verified must-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 14,
        formula: "=IF(TRUE,C1,5)",
        oracle: "oracle: lo-verified must-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 15,
        formula: "=IFERROR(C1,5)",
        oracle: "oracle: lo-verified must-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 16,
        formula: "=CHOOSE(1,C1)",
        oracle: "oracle: lo-verified must-change",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 17,
        formula: "=C1",
        oracle: "oracle: lo-verified chain producer",
        expected: Expected::Number(0.0),
    },
    OracleCase {
        row: 18,
        formula: "=ISBLANK(A17)",
        oracle: "oracle: lo-verified chain consumer",
        expected: Expected::Boolean(false),
    },
    OracleCase {
        row: 19,
        formula: "=COUNT(A17)",
        oracle: "oracle: lo-verified chain consumer",
        expected: Expected::Number(1.0),
    },
    OracleCase {
        row: 20,
        formula: "=COUNTA(A17)",
        oracle: "oracle: lo-verified chain consumer",
        expected: Expected::Number(1.0),
    },
];

fn expected_literal(expected: Expected) -> LiteralValue {
    match expected {
        Expected::Number(value) => LiteralValue::Number(value),
        Expected::Boolean(value) => LiteralValue::Boolean(value),
        Expected::Text(value) => LiteralValue::Text(value.to_string()),
    }
}

#[test]
fn blank_formula_result_oracle_table() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for case in ORACLE_CASES {
        engine
            .set_cell_formula("Sheet1", case.row, 1, parse(case.formula).unwrap())
            .unwrap();
    }

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        None,
        "C1 must remain a truly blank stored cell"
    );
    for case in ORACLE_CASES {
        assert_eq!(
            engine.get_cell_value("Sheet1", case.row, 1),
            Some(expected_literal(case.expected)),
            "{} ({})",
            case.formula,
            case.oracle
        );
    }
}

#[derive(Clone, Copy)]
struct PlaneCase {
    col: u32,
    formula: fn(u32) -> String,
    expected: Expected,
}

fn row_formula(template: &str, row: u32) -> String {
    template.replace("{row}", &row.to_string())
}

fn build_plane_engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let cases = plane_cases();
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    let mut records = Vec::new();
    for case in &cases {
        for row in 1..=20 {
            let formula = (case.formula)(row);
            let ast = parse(&formula).unwrap();
            let ast_id = engine.intern_formula_ast(&ast);
            records.push(FormulaIngestRecord::new(
                row,
                case.col,
                ast_id,
                Some(Arc::<str>::from(formula)),
            ));
        }
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

fn plane_cases() -> [PlaneCase; 20] {
    [
        PlaneCase {
            col: 4,
            formula: |row| row_formula("=ISBLANK(C{row})", row),
            expected: Expected::Boolean(true),
        },
        PlaneCase {
            col: 5,
            formula: |row| row_formula("=C{row}&\"x\"", row),
            expected: Expected::Text("x"),
        },
        PlaneCase {
            col: 6,
            formula: |row| row_formula("=LEN(C{row})", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 7,
            formula: |row| row_formula("=C{row}=0", row),
            expected: Expected::Boolean(true),
        },
        PlaneCase {
            col: 8,
            formula: |row| row_formula("=C{row}=\"\"", row),
            expected: Expected::Boolean(true),
        },
        PlaneCase {
            col: 9,
            formula: |row| row_formula("=T(C{row})", row),
            expected: Expected::Text(""),
        },
        PlaneCase {
            col: 10,
            formula: |row| row_formula("=N(C{row})", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 11,
            formula: |row| row_formula("=ISNUMBER(C{row})", row),
            expected: Expected::Boolean(false),
        },
        PlaneCase {
            col: 12,
            formula: |row| row_formula("=COUNT(C{row})", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 13,
            formula: |row| row_formula("=COUNTA(C{row})", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 14,
            formula: |row| row_formula("=TEXT(C{row},\"0\")", row),
            expected: Expected::Text("0"),
        },
        PlaneCase {
            col: 15,
            formula: |row| row_formula("=C{row}", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 16,
            formula: |row| row_formula("=+C{row}", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 17,
            formula: |row| row_formula("=IF(TRUE,C{row},5)", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 18,
            formula: |row| row_formula("=IFERROR(C{row},5)", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 19,
            formula: |row| row_formula("=CHOOSE(1,C{row})", row),
            expected: Expected::Number(0.0),
        },
        PlaneCase {
            col: 20,
            formula: |row| row_formula("=ISBLANK(O{row})", row),
            expected: Expected::Boolean(false),
        },
        PlaneCase {
            col: 21,
            formula: |row| row_formula("=COUNT(O{row})", row),
            expected: Expected::Number(1.0),
        },
        PlaneCase {
            col: 22,
            formula: |row| row_formula("=COUNTA(O{row})", row),
            expected: Expected::Number(1.0),
        },
        PlaneCase {
            col: 23,
            formula: |_row| "=$C$1".to_string(),
            expected: Expected::Number(0.0),
        },
    ]
}

#[test]
fn formula_plane_blank_result_values_match_off() {
    let off = build_plane_engine(FormulaPlaneMode::Off);
    let authoritative = build_plane_engine(FormulaPlaneMode::AuthoritativeExperimental);
    assert!(
        authoritative
            .baseline_stats()
            .formula_plane_active_span_count
            > 0
    );

    for case in plane_cases() {
        for row in 1..=20 {
            let expected = Some(expected_literal(case.expected));
            assert_eq!(off.get_cell_value("Sheet1", row, case.col), expected);
            assert_eq!(
                authoritative.get_cell_value("Sheet1", row, case.col),
                off.get_cell_value("Sheet1", row, case.col),
                "FormulaPlane mismatch at ({row}, {})",
                case.col
            );
        }
    }
}

#[test]
fn blank_elements_in_spilled_formula_results_finalize_to_zero() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=C1:C2").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    for row in 1..=2 {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 1),
            Some(LiteralValue::Number(0.0))
        );
        assert_eq!(engine.get_cell_value("Sheet1", row, 3), None);
    }
}
