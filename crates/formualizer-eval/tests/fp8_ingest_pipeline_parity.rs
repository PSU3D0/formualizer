use std::collections::{BTreeMap, BTreeSet};

use formualizer_eval::engine::fp8_parity_test_support as fp8;
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::reference::{CellRef, Coord, RangeRef};
use formualizer_eval::test_workbook::TestWorkbook;

#[derive(Clone, Copy)]
struct Case {
    category: &'static str,
    formula: &'static str,
    row: u32,
    col: u32,
}

fn make_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheet1 = engine.sheet_id_mut("Sheet1");
    let sheet2 = engine.sheet_id_mut("Sheet2");
    let _sheet3 = engine.sheet_id_mut("Sheet3");

    engine
        .define_name(
            "Rate",
            NamedDefinition::Cell(fp8::cell(sheet1, 1, 6)),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .define_name(
            "LocalRate",
            NamedDefinition::Cell(fp8::cell(sheet2, 2, 6)),
            NameScope::Sheet(sheet2),
        )
        .unwrap();
    engine.define_source_scalar("ExtScalar", Some(1)).unwrap();
    engine.define_source_table("ExtTable", Some(1)).unwrap();

    let sales_range = RangeRef::new(
        CellRef::new(sheet1, Coord::from_excel(1, 1, true, true)),
        CellRef::new(sheet1, Coord::from_excel(6, 3, true, true)),
    );
    engine
        .define_table(
            "Sales",
            sales_range,
            true,
            vec!["Region".into(), "Amount".into(), "Tax".into()],
            false,
        )
        .unwrap();
    engine
}

fn cases() -> Vec<Case> {
    vec![
        // literals and scalar expressions
        Case {
            category: "literals",
            formula: "=1",
            row: 5,
            col: 5,
        },
        Case {
            category: "literals",
            formula: "=1+2*3",
            row: 5,
            col: 6,
        },
        Case {
            category: "literals",
            formula: "=\"hello\"",
            row: 5,
            col: 7,
        },
        Case {
            category: "literals",
            formula: "=TRUE",
            row: 5,
            col: 8,
        },
        Case {
            category: "literals",
            formula: "=-42",
            row: 5,
            col: 9,
        },
        // direct cell references with relative/absolute/mixed anchors
        Case {
            category: "cells",
            formula: "=A1",
            row: 2,
            col: 2,
        },
        Case {
            category: "cells",
            formula: "=A1+1",
            row: 2,
            col: 3,
        },
        Case {
            category: "cells",
            formula: "=$A$1",
            row: 10,
            col: 8,
        },
        Case {
            category: "cells",
            formula: "=A$1+$B2",
            row: 10,
            col: 9,
        },
        Case {
            category: "cells",
            formula: "=B2+C3-D4",
            row: 10,
            col: 10,
        },
        Case {
            category: "cells",
            formula: "=A1+Sheet2!B2",
            row: 7,
            col: 7,
        },
        Case {
            category: "cells",
            formula: "=Sheet2!A1+Sheet3!C4",
            row: 8,
            col: 8,
        },
        // ranges: expanded small ranges, compressed ranges, and open/whole-axis ranges
        Case {
            category: "ranges",
            formula: "=SUM(A1:A3)",
            row: 9,
            col: 4,
        },
        Case {
            category: "ranges",
            formula: "=SUM(A1:C3)",
            row: 9,
            col: 5,
        },
        Case {
            category: "ranges",
            formula: "=SUM(A1:A100)",
            row: 9,
            col: 6,
        },
        Case {
            category: "ranges",
            formula: "=SUM(A:A)",
            row: 9,
            col: 7,
        },
        Case {
            category: "ranges",
            formula: "=SUM(1:1)",
            row: 9,
            col: 8,
        },
        Case {
            category: "ranges",
            formula: "=SUM(Sheet2!A1:A100)",
            row: 9,
            col: 9,
        },
        Case {
            category: "ranges",
            formula: "=COUNT(A1:B2)",
            row: 9,
            col: 10,
        },
        // names, external sources, and unresolved names
        Case {
            category: "names",
            formula: "=Rate+1",
            row: 12,
            col: 2,
        },
        Case {
            category: "names",
            formula: "=MissingName+1",
            row: 12,
            col: 3,
        },
        Case {
            category: "names",
            formula: "=ExtScalar+1",
            row: 12,
            col: 4,
        },
        Case {
            category: "names",
            formula: "=LocalRate+1",
            row: 12,
            col: 5,
        },
        // table and structured references, including this-row rewrite inside Sales
        Case {
            category: "tables",
            formula: "=Sales[Amount]",
            row: 13,
            col: 2,
        },
        Case {
            category: "tables",
            formula: "=SUM(Sales[Amount])",
            row: 13,
            col: 3,
        },
        Case {
            category: "tables",
            formula: "=[@Amount]*2",
            row: 2,
            col: 3,
        },
        Case {
            category: "tables",
            formula: "=[@[Tax]]+[@Amount]",
            row: 3,
            col: 3,
        },
        // ordinary function calls
        Case {
            category: "functions",
            formula: "=ABS(A1)",
            row: 15,
            col: 2,
        },
        Case {
            category: "functions",
            formula: "=IF(A1>0,B1,C1)",
            row: 15,
            col: 3,
        },
        Case {
            category: "functions",
            formula: "=AND(A1>0,B1>0)",
            row: 15,
            col: 4,
        },
        Case {
            category: "functions",
            formula: "=ROUND(A1,2)",
            row: 15,
            col: 5,
        },
        Case {
            category: "functions",
            formula: "=INDEX(A1:B2,1,2)",
            row: 15,
            col: 6,
        },
        // volatile and dynamic dependency functions
        Case {
            category: "volatile_dynamic",
            formula: "=RAND()",
            row: 17,
            col: 2,
        },
        Case {
            category: "volatile_dynamic",
            formula: "=NOW()+A1",
            row: 17,
            col: 3,
        },
        Case {
            category: "volatile_dynamic",
            formula: "=OFFSET(A1,1,1)",
            row: 17,
            col: 4,
        },
        Case {
            category: "volatile_dynamic",
            formula: "=INDIRECT(\"A1\")",
            row: 17,
            col: 5,
        },
        // arrays and spill-like functions
        Case {
            category: "arrays",
            formula: "={1,2;3,4}",
            row: 19,
            col: 2,
        },
        Case {
            category: "arrays",
            formula: "=SUM({1,2,3})",
            row: 19,
            col: 3,
        },
        Case {
            category: "arrays",
            formula: "=SEQUENCE(3,1)",
            row: 19,
            col: 4,
        },
        // LET/LAMBDA local environment shapes
        Case {
            category: "let_lambda",
            formula: "=LET(x,1,2)",
            row: 21,
            col: 2,
        },
        Case {
            category: "let_lambda",
            formula: "=LET(x,A1,x+1)",
            row: 21,
            col: 3,
        },
        Case {
            category: "let_lambda",
            formula: "=LAMBDA(x,x+1)",
            row: 21,
            col: 4,
        },
        // probe-fp-scenarios equivalents: relative runs, anchor splits, names/tables, arrays, dynamic, cross-sheet
        Case {
            category: "probe_shapes",
            formula: "=A4*$H$1",
            row: 4,
            col: 3,
        },
        Case {
            category: "probe_shapes",
            formula: "=A5*$H$1",
            row: 5,
            col: 3,
        },
        Case {
            category: "probe_shapes",
            formula: "=$A4+H$1",
            row: 4,
            col: 4,
        },
        Case {
            category: "probe_shapes",
            formula: "=SUM(A4:B4)",
            row: 4,
            col: 5,
        },
        Case {
            category: "probe_shapes",
            formula: "=Rate*A4",
            row: 4,
            col: 6,
        },
        Case {
            category: "probe_shapes",
            formula: "=Sales[Tax]",
            row: 4,
            col: 7,
        },
        Case {
            category: "probe_shapes",
            formula: "=Sheet2!A4+A4",
            row: 4,
            col: 8,
        },
        Case {
            category: "probe_shapes",
            formula: "=RAND()+A4",
            row: 4,
            col: 9,
        },
        Case {
            category: "probe_shapes",
            formula: "=INDIRECT(\"A4\")",
            row: 4,
            col: 10,
        },
    ]
}

#[test]
fn fp8_ingest_pipeline_parity() {
    let mut observations = Vec::new();
    for case in cases() {
        let mut engine = make_engine();
        let sheet = if case.formula.contains("LocalRate") {
            engine.sheet_id_mut("Sheet2")
        } else {
            engine.sheet_id_mut("Sheet1")
        };
        let placement = fp8::cell(sheet, case.row, case.col);
        let observation = fp8::assert_case(&mut engine, case.formula, placement);
        observations.push((case, observation));
    }

    // Canonical hash parity checks the equivalence relation rather than the literal hash value:
    // formulas sharing an old canonical payload must share the new arena hash, and one arena hash
    // must not cover multiple old payload families inside this corpus.
    let mut payload_to_hashes: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
    let mut hash_to_payloads: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for (_case, observation) in &observations {
        payload_to_hashes
            .entry(observation.old_payload.clone())
            .or_default()
            .insert(observation.new_hash);
        hash_to_payloads
            .entry(observation.new_hash)
            .or_default()
            .insert(observation.old_payload.clone());
    }
    for (payload, hashes) in payload_to_hashes {
        assert_eq!(
            hashes.len(),
            1,
            "old payload mapped to multiple new hashes: {payload}\nhashes={hashes:?}"
        );
    }
    for (hash, payloads) in hash_to_payloads {
        assert_eq!(
            payloads.len(),
            1,
            "new hash mapped to multiple old payloads: {hash:016x}\npayloads={payloads:#?}"
        );
    }

    let mut by_category: BTreeMap<&'static str, usize> = BTreeMap::new();
    for (case, _) in observations {
        *by_category.entry(case.category).or_default() += 1;
    }
    assert!(by_category.values().sum::<usize>() >= 50);
}
