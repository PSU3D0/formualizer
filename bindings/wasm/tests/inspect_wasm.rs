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

fn fixture() -> Workbook {
    let workbook = Workbook::from_json(FIXTURE.to_string()).unwrap();
    workbook.evaluate_all().unwrap();
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
fn inspection_reports_have_exact_public_shapes_and_semantics() {
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
    assert_eq!(
        page["declared"],
        json!({
            "sheet": "Model", "startRow": 1, "startColumn": 1,
            "endRow": 2, "endColumn": 2
        })
    );
    assert_eq!(page["resolved"], page["declared"]);
    assert_eq!(page["total"], 4.0);
    assert_eq!(page["offset"], 0.0);
    assert_eq!(page["nextOffset"], 3.0);
    let item_addresses: Vec<_> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["address"].clone())
        .collect();
    assert_eq!(
        item_addresses,
        vec![
            cell("Model", 1, 1),
            cell("Model", 1, 2),
            cell("Model", 2, 1)
        ]
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
    assert_eq!(
        date["cell"]["value"],
        json!({"kind": "date", "value": "2025-01-15"})
    );
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
                roots,
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
    let diamond = json_value(workbook.trace_js(diamond_roots, None).unwrap());
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
    let spill_trace = json_value(workbook.trace_js(spill_roots, None).unwrap());
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
                anchor_roots,
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
                range_roots,
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
                roots,
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
fn dirty_transition_options_stamps_and_error_mapping_are_pinned() {
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
}
