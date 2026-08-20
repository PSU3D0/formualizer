#![cfg(target_arch = "wasm32")]

use formualizer_wasm::Workbook;
use js_sys::{Array, Object, Reflect};
use serde_json::{Value, json};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

const FIXTURE: &str = r#"{
  "version": 1,
  "defined_names": [
    {
      "name": "MyInput",
      "scope": "workbook",
      "definition": {
        "type": "range",
        "address": {
          "sheet": "Model",
          "start_row": 1,
          "start_col": 1,
          "end_row": 1,
          "end_col": 1
        }
      }
    },
    {
      "name": "ConstName",
      "scope": "workbook",
      "definition": {"type": "literal", "value": {"type": "Int", "value": 7}}
    }
  ],
  "sheets": {
    "Model": {
      "cells": [
        {"row": 1, "col": 1, "value": {"type": "Int", "value": 10}},
        {"row": 2, "col": 1, "value": {"type": "Int", "value": 20}},
        {"row": 1, "col": 2, "formula": "=A1+1"},
        {"row": 2, "col": 2, "formula": "=SUM(A1:A2)"},
        {"row": 3, "col": 2, "formula": "=A1*2"},
        {"row": 1, "col": 3, "formula": "=MyInput+1"},
        {"row": 2, "col": 3, "formula": "=ConstName+1"},
        {"row": 1, "col": 4, "formula": "=SEQUENCE(2,2,1,1)"},
        {"row": 1, "col": 6, "formula": "=E2"},
        {"row": 1, "col": 7, "formula": "=H1+1"},
        {"row": 1, "col": 8, "formula": "=G1+1"},
        {"row": 1, "col": 9, "formula": "=A1"},
        {"row": 1, "col": 10, "formula": "=A1"},
        {"row": 1, "col": 11, "formula": "=I1+J1"},
        {"row": 4, "col": 1, "value": {"type": "Date", "value": "2025-01-15"}},
        {"row": 5, "col": 1, "formula": "=1/0"}
      ]
    }
  }
}"#;

const VARIANT_FIXTURE: &str = r#"{
  "version": 1,
  "defined_names": [
    {
      "name": "MyRange",
      "scope": "workbook",
      "definition": {
        "type": "range",
        "address": {
          "sheet": "Model",
          "start_row": 1,
          "start_col": 1,
          "end_row": 2,
          "end_col": 1
        }
      }
    }
  ],
  "sheets": {
    "Model": {
      "tables": [
        {
          "name": "Table1",
          "sheet": "Model",
          "range": [10, 1, 12, 2],
          "headers": ["Item", "Amount"],
          "header_row": true,
          "totals_row": false
        }
      ],
      "cells": [
        {"row": 1, "col": 1, "value": {"type": "Int", "value": 10}},
        {"row": 2, "col": 1, "value": {"type": "Int", "value": 20}},
        {"row": 1, "col": 3, "formula": "=SUM(MyRange)"},
        {"row": 2, "col": 3, "formula": "=NoSuchName+1"},
        {"row": 3, "col": 3, "formula": "=SUM(Table1[Amount])"},
        {"row": 6, "col": 1, "value": {"type": "DateTime", "value": "2025-01-15T12:34:56"}},
        {"row": 7, "col": 1, "value": {"type": "Duration", "value": 3661}},
        {"row": 8, "col": 1, "value": {"type": "Boolean", "value": true}},
        {"row": 9, "col": 1, "value": {"type": "Pending"}},
        {"row": 10, "col": 1, "value": {"type": "Text", "value": "Item"}},
        {"row": 10, "col": 2, "value": {"type": "Text", "value": "Amount"}},
        {"row": 11, "col": 1, "value": {"type": "Text", "value": "widget"}},
        {"row": 11, "col": 2, "value": {"type": "Int", "value": 5}},
        {"row": 12, "col": 1, "value": {"type": "Text", "value": "gadget"}},
        {"row": 12, "col": 2, "value": {"type": "Int", "value": 6}}
      ]
    },
    "Other": {"cells": [{"row": 1, "col": 1, "value": {"type": "Int", "value": 42}}]}
  }
}"#;

fn fixture() -> Workbook {
    let workbook = Workbook::from_json(FIXTURE.to_string()).unwrap();
    workbook.evaluate_all().unwrap();
    workbook
}

fn variant_fixture() -> Workbook {
    let workbook = Workbook::from_json(VARIANT_FIXTURE.to_string()).unwrap();
    workbook.evaluate_all().unwrap();
    workbook
        .set_formula("Model".to_string(), 4, 3, "='[1]Sheet1'!A1".to_string())
        .unwrap();
    workbook
        .set_formula(
            "Model".to_string(),
            5,
            3,
            "=SUM(Model:Other!A1)".to_string(),
        )
        .unwrap();
    workbook
}

fn set(object: &Object, key: &str, value: JsValue) {
    Reflect::set(object, &JsValue::from_str(key), &value).unwrap();
}

fn address(sheet: &str, row: u32, column: u32) -> JsValue {
    let object = Object::new();
    set(&object, "sheet", JsValue::from_str(sheet));
    set(&object, "row", JsValue::from_f64(row as f64));
    set(&object, "column", JsValue::from_f64(column as f64));
    object.into()
}

fn area(
    sheet: &str,
    start_row: Option<u32>,
    start_column: Option<u32>,
    end_row: Option<u32>,
    end_column: Option<u32>,
) -> JsValue {
    let object = Object::new();
    set(&object, "sheet", JsValue::from_str(sheet));
    for (key, value) in [
        ("startRow", start_row),
        ("startColumn", start_column),
        ("endRow", end_row),
        ("endColumn", end_column),
    ] {
        set(
            &object,
            key,
            value.map_or(JsValue::NULL, |value| JsValue::from_f64(value as f64)),
        );
    }
    object.into()
}

fn options(entries: &[(&str, JsValue)]) -> JsValue {
    let object = Object::new();
    for (key, value) in entries {
        set(&object, key, value.clone());
    }
    object.into()
}

fn json_value(value: JsValue) -> Value {
    let text = js_sys::JSON::stringify(&value)
        .unwrap()
        .as_string()
        .unwrap();
    serde_json::from_str(&text).unwrap()
}

fn assert_inspect_error(error: JsValue, code: &str) -> String {
    let error: js_sys::Error = error.dyn_into().unwrap();
    for (key, expected) in [
        ("kind", "InspectError"),
        ("code", code),
        ("inspect_code", code),
    ] {
        assert_eq!(
            Reflect::get(error.as_ref(), &JsValue::from_str(key))
                .unwrap()
                .as_string()
                .as_deref(),
            Some(expected)
        );
    }
    error.message().as_string().unwrap()
}

fn stamp_shape(value: &Value) {
    let stamp = &value["stamp"];
    assert_eq!(stamp.as_object().unwrap().len(), 2);
    assert!(stamp["mutationRevision"].as_str().is_some());
    assert!(stamp["recalcEpoch"].as_str().is_some());
    assert!(stamp["mutationRevision"].as_u64().is_none());
    assert!(stamp["recalcEpoch"].as_u64().is_none());
}

fn cell(sheet: &str, row: u32, column: u32) -> Value {
    json!({"sheet": sheet, "row": row, "column": column})
}

#[wasm_bindgen_test]
fn snapshot_precedent_dependent_and_range_page_reports_have_exact_shapes() {
    let workbook = fixture();

    let snapshot = json_value(
        workbook
            .inspect_cell_js(address("Model", 1, 2), None)
            .unwrap(),
    );
    stamp_shape(&snapshot);
    assert_eq!(
        snapshot["cell"],
        json!({
            "address": cell("Model", 1, 2),
            "formula": "=A1 + 1",
            "value": 11,
            "valueIncluded": true,
            "staleness": "current",
            "volatile": false,
            "spill": null
        })
    );

    let precedents = json_value(
        workbook
            .precedents_js(address("Model", 2, 2), None)
            .unwrap(),
    );
    stamp_shape(&precedents);
    assert_eq!(precedents["cell"], cell("Model", 2, 2));
    assert_eq!(
        precedents["precedents"],
        json!([{
            "reference": {
                "kind": "range",
                "declared": {
                    "sheet": "Model",
                    "startRow": 1,
                    "startColumn": 1,
                    "endRow": 2,
                    "endColumn": 1
                },
                "resolved": {
                    "sheet": "Model",
                    "startRow": 1,
                    "startColumn": 1,
                    "endRow": 2,
                    "endColumn": 1
                },
                "cellCount": 2
            },
            "provenance": "declared"
        }])
    );
    assert_eq!(
        precedents["truncation"],
        json!({"incomplete": false, "omitted": null})
    );
    let no_precedents = json_value(
        workbook
            .precedents_js(
                address("Model", 2, 2),
                Some(options(&[("maxLinks", JsValue::from_f64(0.0))])),
            )
            .unwrap(),
    );
    assert_eq!(no_precedents["precedents"], json!([]));
    assert_eq!(
        no_precedents["truncation"],
        json!({
            "incomplete": true,
            "omitted": {"kind": "atLeast", "count": "1"}
        })
    );

    let named = json_value(
        workbook
            .precedents_js(address("Model", 1, 3), None)
            .unwrap(),
    );
    assert_eq!(
        named["precedents"],
        json!([{
            "reference": {
                "kind": "name",
                "name": "MyInput",
                "resolution": {
                    "kind": "cell",
                    "address": cell("Model", 1, 1)
                }
            },
            "provenance": "declared"
        }])
    );
    let literal_name = json_value(
        workbook
            .precedents_js(address("Model", 2, 3), None)
            .unwrap(),
    );
    assert_eq!(
        literal_name["precedents"],
        json!([{
            "reference": {
                "kind": "name",
                "name": "ConstName",
                "resolution": {"kind": "literal", "value": 7}
            },
            "provenance": "declared"
        }])
    );

    let dependents = json_value(
        workbook
            .dependents_js(address("Model", 1, 1), None)
            .unwrap(),
    );
    stamp_shape(&dependents);
    assert_eq!(dependents["cell"], cell("Model", 1, 1));
    assert_eq!(
        dependents["dependents"],
        json!([
            {"cell": cell("Model", 1, 2), "via": []},
            {"cell": cell("Model", 1, 9), "via": []},
            {"cell": cell("Model", 1, 10), "via": []},
            {"cell": cell("Model", 2, 2), "via": []},
            {"cell": cell("Model", 3, 2), "via": []}
        ])
    );
    assert_eq!(
        dependents["truncation"],
        json!({"incomplete": false, "omitted": null})
    );

    let bounded = json_value(
        workbook
            .dependents_js(
                address("Model", 1, 1),
                Some(options(&[("maxResults", JsValue::from_f64(2.0))])),
            )
            .unwrap(),
    );
    assert_eq!(
        bounded["dependents"],
        json!([
            {"cell": cell("Model", 1, 2), "via": []},
            {"cell": cell("Model", 1, 9), "via": []}
        ])
    );
    assert_eq!(
        bounded["truncation"],
        json!({
            "incomplete": true,
            "omitted": {"kind": "atLeast", "count": "1"}
        })
    );

    let page = json_value(
        workbook
            .range_page_js(
                area("Model", Some(1), Some(1), Some(2), Some(2)),
                Some(options(&[("limit", JsValue::from_f64(3.0))])),
            )
            .unwrap(),
    );
    stamp_shape(&page);
    let page_stamp = page["stamp"].clone();
    assert_eq!(
        page,
        json!({
            "stamp": page_stamp,
            "declared": {
                "sheet": "Model", "startRow": 1, "startColumn": 1,
                "endRow": 2, "endColumn": 2
            },
            "resolved": {
                "sheet": "Model", "startRow": 1, "startColumn": 1,
                "endRow": 2, "endColumn": 2
            },
            "total": 4,
            "offset": 0,
            "items": [
                {
                    "address": cell("Model", 1, 1), "formula": null, "value": 10,
                    "valueIncluded": true, "staleness": "current", "volatile": false,
                    "spill": null
                },
                {
                    "address": cell("Model", 1, 2), "formula": "=A1 + 1", "value": 11,
                    "valueIncluded": true, "staleness": "current", "volatile": false,
                    "spill": null
                },
                {
                    "address": cell("Model", 2, 1), "formula": null, "value": 20,
                    "valueIncluded": true, "staleness": "current", "volatile": false,
                    "spill": null
                }
            ],
            "nextOffset": 3
        })
    );

    let spill = json_value(
        workbook
            .inspect_cell_js(address("Model", 2, 5), None)
            .unwrap(),
    );
    assert_eq!(
        spill["cell"]["spill"],
        json!({"role": "member", "anchor": cell("Model", 1, 4)})
    );
    assert_eq!(spill["cell"]["value"], 4.0);
    let anchor = json_value(
        workbook
            .inspect_cell_js(address("Model", 1, 4), None)
            .unwrap(),
    );
    assert_eq!(
        anchor["cell"]["spill"],
        json!({
            "role": "anchor",
            "extent": {
                "sheet": "Model", "startRow": 1, "startColumn": 4,
                "endRow": 2, "endColumn": 5
            }
        })
    );
    assert_eq!(anchor["cell"]["value"], 1.0);

    let date = json_value(
        workbook
            .inspect_cell_js(address("Model", 4, 1), None)
            .unwrap(),
    );
    assert_eq!(date["cell"]["value"], 45_672.0);
    let error = json_value(
        workbook
            .inspect_cell_js(address("Model", 5, 1), None)
            .unwrap(),
    );
    assert_eq!(error["cell"]["value"]["kind"], "error");
    assert_eq!(error["cell"]["value"]["code"], "#DIV/0!");
}

#[wasm_bindgen_test]
fn trace_cycle_dispositions_and_budget_tags_are_exact() {
    let workbook = fixture();
    let roots = Array::new();
    roots.push(&address("Model", 1, 7));
    let trace = json_value(
        workbook
            .trace_js(
                roots.into(),
                Some(options(&[
                    ("maxDepth", JsValue::from_f64(4.0)),
                    ("maxNodes", JsValue::from_f64(8.0)),
                    ("maxLinks", JsValue::from_f64(8.0)),
                ])),
            )
            .unwrap(),
    );
    stamp_shape(&trace);
    assert_eq!(trace["direction"], "precedents");
    assert_eq!(trace["roots"], json!([0]));
    assert_eq!(
        trace["truncation"],
        json!({"incomplete": false, "omitted": null})
    );
    assert_eq!(trace["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(trace["nodes"][0]["cell"]["address"], cell("Model", 1, 7));
    assert_eq!(trace["nodes"][1]["cell"]["address"], cell("Model", 1, 8));
    assert_eq!(
        trace["nodes"][0]["links"],
        json!([{
            "reference": {"kind": "cell", "address": cell("Model", 1, 8)},
            "kind": "formula",
            "provenance": "declared",
            "targets": [{"node": 1, "disposition": "cycle"}],
            "omitted": null
        }])
    );
    assert_eq!(
        trace["nodes"][1]["links"][0]["targets"],
        json!([{"node": 0, "disposition": "cycle"}])
    );

    let diamond_roots = Array::new();
    diamond_roots.push(&address("Model", 1, 11));
    let diamond = json_value(workbook.trace_js(diamond_roots.into(), None).unwrap());
    let a1_dispositions: Vec<_> = diamond["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|node| node["links"].as_array().unwrap())
        .filter(|link| link["reference"]["address"] == cell("Model", 1, 1))
        .map(|link| link["targets"][0]["disposition"].clone())
        .collect();
    assert_eq!(
        a1_dispositions,
        vec![json!("expanded"), json!("convergent")]
    );

    let spill_roots = Array::new();
    spill_roots.push(&address("Model", 2, 5));
    let spill_trace = json_value(workbook.trace_js(spill_roots.into(), None).unwrap());
    assert_eq!(spill_trace["nodes"][0]["links"][0]["kind"], "spillAnchor");
    assert!(
        spill_trace["nodes"][0]["links"][0]
            .get("provenance")
            .is_none()
    );

    let anchor_roots = Array::new();
    anchor_roots.push(&address("Model", 1, 4));
    let dependent_trace = json_value(
        workbook
            .trace_js(
                anchor_roots.into(),
                Some(options(&[("direction", JsValue::from_str("dependents"))])),
            )
            .unwrap(),
    );
    assert_eq!(dependent_trace["direction"], "dependents");
    assert_eq!(
        dependent_trace["nodes"][0]["links"][0]["kind"],
        "spillReader"
    );

    let range_roots = Array::new();
    range_roots.push(&address("Model", 2, 2));
    let range_limited = json_value(
        workbook
            .trace_js(
                range_roots.into(),
                Some(options(&[("rangeMemberBudget", JsValue::from_f64(1.0))])),
            )
            .unwrap(),
    );
    assert_eq!(
        range_limited["nodes"][0]["links"][0]["omitted"],
        json!({"kind": "exact", "count": "1"})
    );
    assert_eq!(
        range_limited["truncation"]["omitted"],
        json!({"kind": "exact", "count": "1"})
    );

    let roots = Array::new();
    roots.push(&address("Model", 1, 2));
    let elided = json_value(
        workbook
            .trace_js(
                roots.into(),
                Some(options(&[("maxDepth", JsValue::from_f64(0.0))])),
            )
            .unwrap(),
    );
    assert_eq!(
        elided["nodes"][0]["links"][0]["targets"][0]["disposition"],
        "elided"
    );
    assert_eq!(
        elided["truncation"],
        json!({"incomplete": true, "omitted": null})
    );
}

#[wasm_bindgen_test]
fn dirty_transition_snapshot_option_stamp_guard_and_common_errors_are_pinned() {
    let workbook = fixture();
    let before = json_value(
        workbook
            .inspect_cell_js(address("Model", 1, 2), None)
            .unwrap(),
    );
    workbook
        .set_value("Model".to_string(), 1, 1, JsValue::from_f64(99.0))
        .unwrap();
    let after = json_value(
        workbook
            .inspect_cell_js(
                address("Model", 1, 2),
                Some(options(&[("includeValues", JsValue::FALSE)])),
            )
            .unwrap(),
    );
    assert_eq!(before["cell"]["staleness"], "current");
    assert_eq!(after["cell"]["staleness"], "dirty");
    assert_eq!(after["cell"]["valueIncluded"], false);
    assert_eq!(after["cell"]["value"], Value::Null);
    assert_ne!(before["stamp"], after["stamp"]);
    stamp_shape(&after);

    workbook
        .set_formula("Model".to_string(), 1, 12, "=A1+5".to_string())
        .unwrap();
    let never = json_value(
        workbook
            .inspect_cell_js(address("Model", 1, 12), None)
            .unwrap(),
    );
    assert_eq!(never["cell"]["staleness"], "neverEvaluated");
    assert_eq!(never["cell"]["value"], Value::Null);

    let expected_stamp = Object::new();
    set(
        &expected_stamp,
        "mutationRevision",
        JsValue::from_str(before["stamp"]["mutationRevision"].as_str().unwrap()),
    );
    set(
        &expected_stamp,
        "recalcEpoch",
        JsValue::from_str(before["stamp"]["recalcEpoch"].as_str().unwrap()),
    );
    let error: js_sys::Error = workbook
        .range_page_js(
            area("Model", Some(1), Some(1), Some(1), Some(1)),
            Some(options(&[("expectedStamp", expected_stamp.into())])),
        )
        .unwrap_err()
        .dyn_into()
        .unwrap();
    assert_eq!(
        Reflect::get(error.as_ref(), &JsValue::from_str("code"))
            .unwrap()
            .as_string()
            .unwrap(),
        "REVISION_MISMATCH"
    );

    let error: js_sys::Error = workbook
        .inspect_cell_js(address("Missing", 1, 1), None)
        .unwrap_err()
        .dyn_into()
        .unwrap();
    assert_eq!(
        Reflect::get(error.as_ref(), &JsValue::from_str("kind"))
            .unwrap()
            .as_string()
            .unwrap(),
        "InspectError"
    );
    assert_eq!(
        Reflect::get(error.as_ref(), &JsValue::from_str("code"))
            .unwrap()
            .as_string()
            .unwrap(),
        "SHEET_NOT_FOUND"
    );
    assert!(error.message().as_string().unwrap().contains("Missing"));

    let error = workbook
        .range_page_js(
            area("Model", Some(1), Some(1), Some(1), Some(1)),
            Some(options(&[("limit", JsValue::from_f64(0.0))])),
        )
        .unwrap_err();
    assert!(assert_inspect_error(error, "INVALID_OPTIONS").contains("at least one"));
}

#[wasm_bindgen_test]
fn unknown_keys_are_rejected_for_every_inspection_input_object() {
    let workbook = fixture();

    let cases = [
        workbook
            .inspect_cell_js(
                address("Model", 1, 1),
                Some(options(&[("includevalues", JsValue::FALSE)])),
            )
            .unwrap_err(),
        workbook
            .precedents_js(
                address("Model", 1, 2),
                Some(options(&[("maxlinks", JsValue::from_f64(1.0))])),
            )
            .unwrap_err(),
        workbook
            .dependents_js(
                address("Model", 1, 1),
                Some(options(&[("maxresults", JsValue::from_f64(2.0))])),
            )
            .unwrap_err(),
        workbook
            .trace_js(
                Array::of1(&address("Model", 1, 2)).into(),
                Some(options(&[("maxdepth", JsValue::from_f64(0.0))])),
            )
            .unwrap_err(),
        workbook
            .range_page_js(
                area("Model", Some(1), Some(1), Some(2), Some(2)),
                Some(options(&[("Offset", JsValue::from_f64(2.0))])),
            )
            .unwrap_err(),
    ];
    for error in cases {
        assert!(assert_inspect_error(error, "INVALID_OPTIONS").contains("unknown field"));
    }

    let cell_with_extra = options(&[
        ("sheet", JsValue::from_str("Model")),
        ("row", JsValue::from_f64(1.0)),
        ("column", JsValue::from_f64(1.0)),
        ("rubbish", JsValue::from_f64(9.0)),
    ]);
    assert!(
        assert_inspect_error(
            workbook.inspect_cell_js(cell_with_extra, None).unwrap_err(),
            "INVALID_ADDRESS"
        )
        .contains("unknown field `rubbish`")
    );

    let area_with_extra = options(&[
        ("sheet", JsValue::from_str("Model")),
        ("startRow", JsValue::from_f64(1.0)),
        ("startColumn", JsValue::from_f64(1.0)),
        ("endRow", JsValue::from_f64(1.0)),
        ("endColumn", JsValue::from_f64(1.0)),
        ("zzz", JsValue::TRUE),
    ]);
    assert!(
        assert_inspect_error(
            workbook.range_page_js(area_with_extra, None).unwrap_err(),
            "INVALID_ADDRESS"
        )
        .contains("unknown field `zzz`")
    );

    let lower_case_stamp = options(&[
        ("mutationRevision", JsValue::from_str("999")),
        ("recalcEpoch", JsValue::from_str("1")),
    ]);
    let error = workbook
        .range_page_js(
            area("Model", Some(1), Some(1), Some(1), Some(1)),
            Some(options(&[("expectedstamp", lower_case_stamp)])),
        )
        .unwrap_err();
    assert!(assert_inspect_error(error, "INVALID_OPTIONS").contains("expectedstamp"));

    let stamp_with_extra = options(&[
        ("mutationRevision", JsValue::from_str("1")),
        ("recalcEpoch", JsValue::from_str("1")),
        ("extra", JsValue::TRUE),
    ]);
    let error = workbook
        .range_page_js(
            area("Model", Some(1), Some(1), Some(1), Some(1)),
            Some(options(&[("expectedStamp", stamp_with_extra)])),
        )
        .unwrap_err();
    assert!(assert_inspect_error(error, "INVALID_OPTIONS").contains("unknown field `extra`"));
}

#[wasm_bindgen_test]
fn binding_validation_errors_are_typed_and_coded() {
    let workbook = fixture();
    let oversized = |row: f64, column: f64| {
        options(&[
            ("sheet", JsValue::from_str("Model")),
            ("row", JsValue::from_f64(row)),
            ("column", JsValue::from_f64(column)),
        ])
    };

    for input in [
        address("Model", 0, 1),
        oversized(2_000_000.0, 1.0),
        oversized(1.0, 20_000.0),
        oversized(4_294_967_295.0, 1.0),
        oversized(4_294_967_296.0, 1.0),
        JsValue::NULL,
        JsValue::from_str("Model!A1"),
    ] {
        assert_inspect_error(
            workbook.inspect_cell_js(input, None).unwrap_err(),
            "INVALID_ADDRESS",
        );
    }

    for invalid_area in [
        area("Model", Some(0), Some(1), Some(1), Some(1)),
        area("Model", Some(5), Some(1), Some(1), Some(1)),
        options(&[
            ("startRow", JsValue::from_f64(1.0)),
            ("startColumn", JsValue::from_f64(1.0)),
            ("endRow", JsValue::from_f64(1.0)),
            ("endColumn", JsValue::from_f64(1.0)),
        ]),
    ] {
        assert_inspect_error(
            workbook.range_page_js(invalid_area, None).unwrap_err(),
            "INVALID_ADDRESS",
        );
    }

    let missing_sheet_roots = Array::of1(&address("Nope", 1, 1));
    assert_inspect_error(
        workbook
            .trace_js(missing_sheet_roots.into(), None)
            .unwrap_err(),
        "SHEET_NOT_FOUND",
    );
    let non_object_roots = Array::of1(&JsValue::from_str("A1"));
    assert_inspect_error(
        workbook
            .trace_js(non_object_roots.into(), None)
            .unwrap_err(),
        "INVALID_ADDRESS",
    );

    for roots in [
        JsValue::NULL,
        options(&[("0", address("Model", 1, 1))]),
        JsValue::from_str("A1"),
    ] {
        let message = assert_inspect_error(
            workbook.trace_js(roots, None).unwrap_err(),
            "INVALID_OPTIONS",
        );
        assert!(message.contains("expected an array"));
    }

    assert_inspect_error(
        workbook
            .precedents_js(
                address("Model", 1, 2),
                Some(options(&[("maxLinks", JsValue::from_f64(-1.0))])),
            )
            .unwrap_err(),
        "INVALID_OPTIONS",
    );
    assert_inspect_error(
        workbook
            .range_page_js(
                area("Model", Some(1), Some(1), Some(1), Some(1)),
                Some(options(&[("offset", JsValue::from_f64(-1.0))])),
            )
            .unwrap_err(),
        "INVALID_OPTIONS",
    );
    let numeric_stamp = options(&[
        ("mutationRevision", JsValue::from_f64(5.0)),
        ("recalcEpoch", JsValue::from_f64(1.0)),
    ]);
    let message = assert_inspect_error(
        workbook
            .range_page_js(
                area("Model", Some(1), Some(1), Some(1), Some(1)),
                Some(options(&[("expectedStamp", numeric_stamp)])),
            )
            .unwrap_err(),
        "INVALID_OPTIONS",
    );
    assert!(message.contains("range page options"));
}

#[wasm_bindgen_test]
fn reachable_inspect_error_codes_are_explicit() {
    let workbook = fixture();
    let current = json_value(
        workbook
            .inspect_cell_js(address("Model", 1, 1), None)
            .unwrap(),
    );
    let expected_stamp = options(&[
        ("mutationRevision", JsValue::from_str("0")),
        ("recalcEpoch", JsValue::from_str("0")),
    ]);
    let mut errors = vec![
        workbook
            .inspect_cell_js(address("Missing", 1, 1), None)
            .unwrap_err(),
        workbook
            .inspect_cell_js(address("Model", 0, 1), None)
            .unwrap_err(),
        workbook
            .range_page_js(
                area("Model", Some(1), Some(1), Some(1), Some(1)),
                Some(options(&[("limit", JsValue::from_f64(0.0))])),
            )
            .unwrap_err(),
        workbook
            .range_page_js(
                area("Model", Some(1), Some(1), Some(1), Some(1)),
                Some(options(&[("expectedStamp", expected_stamp)])),
            )
            .unwrap_err(),
    ];
    assert_ne!(current["stamp"]["mutationRevision"], "0");
    workbook
        .set_formula("Model".to_string(), 20, 1, "=A1+1".to_string())
        .unwrap();
    errors.push(
        workbook
            .dependents_js(address("Model", 1, 1), None)
            .unwrap_err(),
    );

    let mut codes: Vec<_> = errors
        .into_iter()
        .map(|error| {
            let code = Reflect::get(&error, &JsValue::from_str("code"))
                .unwrap()
                .as_string()
                .unwrap();
            assert_inspect_error(error, &code);
            code
        })
        .collect();
    codes.sort();
    assert_eq!(
        codes,
        vec![
            "DEPENDENCY_STATE_UNAVAILABLE",
            "INVALID_ADDRESS",
            "INVALID_OPTIONS",
            "REVISION_MISMATCH",
            "SHEET_NOT_FOUND",
        ]
    );
    // RESOURCE_EXHAUSTED requires an infeasible allocation failure today;
    // INSPECT_ERROR is the forward-compatible fallback for future core variants.
}

#[wasm_bindgen_test]
fn every_inspection_option_and_survivor_shape_is_pinned() {
    let workbook = fixture();
    workbook
        .set_formula(
            "Model".to_string(),
            8,
            12,
            "=SUM(Model!B10:C20)".to_string(),
        )
        .unwrap();
    workbook.evaluate_all().unwrap();
    let asymmetric = json_value(
        workbook
            .precedents_js(address("Model", 8, 12), None)
            .unwrap(),
    );
    assert_eq!(
        asymmetric["precedents"][0]["reference"]["declared"],
        json!({
            "sheet": "Model", "startRow": 10, "startColumn": 2,
            "endRow": 20, "endColumn": 3
        })
    );

    let spill_dependents = json_value(
        workbook
            .dependents_js(address("Model", 1, 4), None)
            .unwrap(),
    );
    assert_eq!(
        spill_dependents["dependents"],
        json!([{"cell": cell("Model", 1, 6), "via": [cell("Model", 2, 5)]}])
    );

    let roots = Array::new();
    roots.push(&address("Model", 1, 2));
    roots.push(&address("Model", 1, 2));
    roots.push(&address("Model", 1, 11));
    let multi_root = json_value(workbook.trace_js(roots.into(), None).unwrap());
    assert_eq!(multi_root["roots"], json!([0, 0, 1]));
    assert_eq!(
        multi_root["nodes"][0]["cell"]["address"],
        cell("Model", 1, 2)
    );
    assert_eq!(
        multi_root["nodes"][1]["cell"]["address"],
        cell("Model", 1, 11)
    );

    let precedent_work_zero = json_value(
        workbook
            .precedents_js(
                address("Model", 2, 2),
                Some(options(&[("maxWork", JsValue::from_f64(0.0))])),
            )
            .unwrap(),
    );
    assert_eq!(precedent_work_zero["precedents"], json!([]));
    assert_eq!(
        precedent_work_zero["truncation"],
        json!({"incomplete": true, "omitted": {"kind": "atLeast", "count": "1"}})
    );

    let dependent_work_zero = json_value(
        workbook
            .dependents_js(
                address("Model", 1, 1),
                Some(options(&[("maxWork", JsValue::from_f64(0.0))])),
            )
            .unwrap(),
    );
    assert_eq!(dependent_work_zero["dependents"], json!([]));
    assert_eq!(
        dependent_work_zero["truncation"],
        json!({"incomplete": true, "omitted": null})
    );

    let trace_case = |entries: &[(&str, JsValue)]| {
        let roots = Array::of1(&address("Model", 1, 11));
        json_value(
            workbook
                .trace_js(roots.into(), Some(options(entries)))
                .unwrap(),
        )
    };
    let node_limited = trace_case(&[("maxNodes", JsValue::from_f64(1.0))]);
    assert_eq!(node_limited["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(
        node_limited["truncation"],
        json!({"incomplete": true, "omitted": {"kind": "atLeast", "count": "2"}})
    );
    assert_eq!(node_limited["nodes"][0]["links"][0]["targets"], json!([]));
    assert_eq!(node_limited["nodes"][0]["links"][1]["targets"], json!([]));

    for field in ["maxLinks", "maxWork"] {
        let limited = trace_case(&[(field, JsValue::from_f64(0.0))]);
        assert_eq!(limited["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(limited["nodes"][0]["links"], json!([]));
        assert_eq!(
            limited["truncation"],
            json!({"incomplete": true, "omitted": {"kind": "atLeast", "count": "1"}})
        );
    }

    let roots = Array::of1(&address("Model", 1, 2));
    let without_values = json_value(
        workbook
            .trace_js(
                roots.into(),
                Some(options(&[("includeValues", JsValue::FALSE)])),
            )
            .unwrap(),
    );
    let value_shapes: Vec<_> = without_values["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| {
            json!([
                node["cell"]["valueIncluded"].clone(),
                node["cell"]["value"].clone()
            ])
        })
        .collect();
    assert_eq!(
        value_shapes,
        vec![json!([false, null]), json!([false, null])]
    );

    let offset_page = json_value(
        workbook
            .range_page_js(
                area("Model", Some(1), Some(1), Some(2), Some(2)),
                Some(options(&[
                    ("offset", JsValue::from_f64(2.0)),
                    ("limit", JsValue::from_f64(2.0)),
                ])),
            )
            .unwrap(),
    );
    assert_eq!(offset_page["offset"], 2);
    assert_eq!(offset_page["items"][0]["address"], cell("Model", 2, 1));
    assert_eq!(offset_page["items"][1]["address"], cell("Model", 2, 2));
    assert_eq!(offset_page["nextOffset"], Value::Null);

    let no_value_page = json_value(
        workbook
            .range_page_js(
                area("Model", Some(1), Some(1), Some(1), Some(2)),
                Some(options(&[("includeValues", JsValue::FALSE)])),
            )
            .unwrap(),
    );
    assert_eq!(
        no_value_page["items"],
        json!([
            {
                "address": cell("Model", 1, 1), "formula": null, "value": null,
                "valueIncluded": false, "staleness": "current", "volatile": false,
                "spill": null
            },
            {
                "address": cell("Model", 1, 2), "formula": "=A1 + 1", "value": null,
                "valueIncluded": false, "staleness": "current", "volatile": false,
                "spill": null
            }
        ])
    );
}

#[wasm_bindgen_test]
fn reachable_reference_and_value_variants_are_pinned() {
    let workbook = variant_fixture();

    let name_range = json_value(
        workbook
            .precedents_js(address("Model", 1, 3), None)
            .unwrap(),
    );
    assert_eq!(
        name_range["precedents"][0]["reference"]["resolution"],
        json!({
            "kind": "range",
            "declared": {
                "sheet": "Model", "startRow": 1, "startColumn": 1,
                "endRow": 2, "endColumn": 1
            },
            "resolved": {
                "sheet": "Model", "startRow": 1, "startColumn": 1,
                "endRow": 2, "endColumn": 1
            }
        })
    );
    let unresolved = json_value(
        workbook
            .precedents_js(address("Model", 2, 3), None)
            .unwrap(),
    );
    assert_eq!(
        unresolved["precedents"][0]["reference"]["resolution"],
        json!({"kind": "unresolved"})
    );
    for (row, expected_kind) in [(3, "table"), (4, "external"), (5, "unsupported")] {
        let report = json_value(
            workbook
                .precedents_js(address("Model", row, 3), None)
                .unwrap(),
        );
        assert_eq!(report["precedents"][0]["reference"]["kind"], expected_kind);
    }

    for (row, expected) in [
        (6, json!(45_672.524_259_259_26)),
        (7, json!(0.042_372_685_185_185_19)),
        (8, json!(true)),
        (9, json!({"kind": "pending"})),
        (30, Value::Null),
    ] {
        let report = json_value(
            workbook
                .inspect_cell_js(address("Model", row, 1), None)
                .unwrap(),
        );
        assert_eq!(report["cell"]["value"], expected);
        assert_eq!(report["cell"]["valueIncluded"], true);
    }
}
