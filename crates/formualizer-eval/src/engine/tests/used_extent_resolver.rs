use super::common::arrow_eval_config;
use crate::arrow_store::OverlayValue;
use crate::engine::used_extent::{
    ExtentPolicy, OpenRangeBounds, ResolvedExtent, resolve_used_extent,
};
use crate::engine::{Engine, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ReferenceType, parse};
use proptest::prelude::*;

fn evaluation_extent(
    engine: &Engine<TestWorkbook>,
    sheet: &str,
    bounds: OpenRangeBounds,
) -> Option<ResolvedExtent> {
    let fallback = engine.sheet_bounds(sheet).map(|_| {
        (
            engine.config.max_open_ended_rows,
            engine.config.max_open_ended_cols,
        )
    });
    resolve_used_extent(
        bounds,
        ExtentPolicy::EvaluationCompat {
            fallback_row: fallback.map(|value| value.0),
            fallback_column: fallback.map(|value| value.1),
        },
        |first, last| engine.used_rows_for_columns(sheet, first, last),
        |first, last| engine.used_cols_for_rows(sheet, first, last),
    )
}

fn interpreter_probe_extent(
    engine: &Engine<TestWorkbook>,
    sheet: &str,
    bounds: OpenRangeBounds,
) -> Option<ResolvedExtent> {
    let fallback = engine.sheet_bounds(sheet);
    resolve_used_extent(
        bounds,
        ExtentPolicy::EvaluationCompat {
            fallback_row: fallback.map(|value| value.0),
            fallback_column: fallback.map(|value| value.1),
        },
        |first, last| engine.used_rows_for_columns(sheet, first, last),
        |first, last| engine.used_cols_for_rows(sheet, first, last),
    )
}

fn virtual_dependency_extent(
    engine: &Engine<TestWorkbook>,
    sheet: &str,
    bounds: OpenRangeBounds,
) -> Option<ResolvedExtent> {
    let fallback = engine.sheet_bounds(sheet).map(|_| {
        (
            engine.config.max_open_ended_rows,
            engine.config.max_open_ended_cols,
        )
    });
    resolve_used_extent(
        bounds,
        ExtentPolicy::VirtualDependencyCompat {
            fallback_row: fallback.map(|value| value.0),
            fallback_column: fallback.map(|value| value.1),
        },
        |first, last| engine.used_rows_for_columns(sheet, first, last),
        |first, last| engine.used_cols_for_rows(sheet, first, last),
    )
}

// Frozen from Engine::resolve_range_view at ccfeaf83. Keep this test-only copy
// independent of the shared resolver so it remains a differential oracle.
fn frozen_evaluation_extent_ccfeaf83(
    engine: &Engine<TestWorkbook>,
    sheet_name: &str,
    bounds: OpenRangeBounds,
) -> Option<ResolvedExtent> {
    let mut sr = bounds.start_row;
    let mut sc = bounds.start_column;
    let mut er = bounds.end_row;
    let mut ec = bounds.end_column;

    if sr.is_none() && er.is_none() {
        let scv = sc.unwrap_or(1);
        let ecv = ec.unwrap_or(scv);
        sr = Some(1);
        if let Some((_, max_r)) = engine.used_rows_for_columns(sheet_name, scv, ecv) {
            er = Some(max_r);
        } else if engine.sheet_bounds(sheet_name).is_some() {
            er = Some(engine.config.max_open_ended_rows);
        }
    }
    if sc.is_none() && ec.is_none() {
        let srv = sr.unwrap_or(1);
        let erv = er.unwrap_or(srv);
        sc = Some(1);
        if let Some((_, max_c)) = engine.used_cols_for_rows(sheet_name, srv, erv) {
            ec = Some(max_c);
        } else if engine.sheet_bounds(sheet_name).is_some() {
            ec = Some(engine.config.max_open_ended_cols);
        }
    }
    if sr.is_some() && er.is_none() {
        let scv = sc.unwrap_or(1);
        let ecv = ec.unwrap_or(scv);
        if let Some((_, max_r)) = engine.used_rows_for_columns(sheet_name, scv, ecv) {
            er = Some(max_r);
        } else if engine.sheet_bounds(sheet_name).is_some() {
            er = Some(engine.config.max_open_ended_rows);
        }
    }
    if er.is_some() && sr.is_none() {
        sr = Some(1);
    }
    if sc.is_some() && ec.is_none() {
        let srv = sr.unwrap_or(1);
        let erv = er.unwrap_or(srv);
        if let Some((_, max_c)) = engine.used_cols_for_rows(sheet_name, srv, erv) {
            ec = Some(max_c);
        } else if engine.sheet_bounds(sheet_name).is_some() {
            ec = Some(engine.config.max_open_ended_cols);
        }
    }
    if ec.is_some() && sc.is_none() {
        sc = Some(1);
    }

    let sr = sr.unwrap_or(1);
    let sc = sc.unwrap_or(1);
    let er = er.unwrap_or(sr.saturating_sub(1));
    let ec = ec.unwrap_or(sc.saturating_sub(1));
    (er >= sr && ec >= sc).then_some(ResolvedExtent {
        start_row: sr,
        start_column: sc,
        end_row: er,
        end_column: ec,
    })
}

// Frozen from RangeVirtualDepProvider::get_virtual_deps at ccfeaf83. Its used
// minimum behavior is deliberately different from runtime evaluation.
fn frozen_virtual_dependency_extent_ccfeaf83(
    engine: &Engine<TestWorkbook>,
    sheet_name: &str,
    bounds: OpenRangeBounds,
) -> Option<ResolvedExtent> {
    let mut sr = bounds.start_row;
    let mut sc = bounds.start_column;
    let mut er = bounds.end_row;
    let mut ec = bounds.end_column;

    if sr.is_none() && er.is_none() {
        let scv = sc.unwrap_or(1u32);
        let ecv = ec.unwrap_or(scv);
        if let Some((min_r, max_r)) = engine.used_rows_for_columns(sheet_name, scv, ecv) {
            sr = Some(min_r);
            er = Some(max_r);
        } else if engine.sheet_bounds(sheet_name).is_some() {
            sr = Some(1);
            er = Some(engine.config.max_open_ended_rows);
        }
    }
    if sc.is_none() && ec.is_none() {
        let srv = sr.unwrap_or(1u32);
        let erv = er.unwrap_or(srv);
        if let Some((min_c, max_c)) = engine.used_cols_for_rows(sheet_name, srv, erv) {
            sc = Some(min_c);
            ec = Some(max_c);
        } else if engine.sheet_bounds(sheet_name).is_some() {
            sc = Some(1);
            ec = Some(engine.config.max_open_ended_cols);
        }
    }
    if sr.is_some() && er.is_none() {
        let scv = sc.unwrap_or(1u32);
        let ecv = ec.unwrap_or(scv);
        if let Some((_, max_r)) = engine.used_rows_for_columns(sheet_name, scv, ecv) {
            er = Some(max_r);
        } else if engine.sheet_bounds(sheet_name).is_some() {
            er = Some(engine.config.max_open_ended_rows);
        }
    }
    if er.is_some() && sr.is_none() {
        let scv = sc.unwrap_or(1u32);
        let ecv = ec.unwrap_or(scv);
        if let Some((min_r, _)) = engine.used_rows_for_columns(sheet_name, scv, ecv) {
            sr = Some(min_r);
        } else {
            sr = Some(1);
        }
    }
    if sc.is_some() && ec.is_none() {
        let srv = sr.unwrap_or(1u32);
        let erv = er.unwrap_or(srv);
        if let Some((_, max_c)) = engine.used_cols_for_rows(sheet_name, srv, erv) {
            ec = Some(max_c);
        } else if engine.sheet_bounds(sheet_name).is_some() {
            ec = Some(engine.config.max_open_ended_cols);
        }
    }
    if ec.is_some() && sc.is_none() {
        let srv = sr.unwrap_or(1u32);
        let erv = er.unwrap_or(srv);
        if let Some((min_c, _)) = engine.used_cols_for_rows(sheet_name, srv, erv) {
            sc = Some(min_c);
        } else {
            sc = Some(1);
        }
    }

    let sr = sr.unwrap_or(1);
    let sc = sc.unwrap_or(1);
    let er = er.unwrap_or(sr.saturating_sub(1));
    let ec = ec.unwrap_or(sc.saturating_sub(1));
    (er >= sr && ec >= sc).then_some(ResolvedExtent {
        start_row: sr,
        start_column: sc,
        end_row: er,
        end_column: ec,
    })
}

fn range_matrix() -> [OpenRangeBounds; 7] {
    [
        OpenRangeBounds {
            start_row: None,
            start_column: Some(1),
            end_row: None,
            end_column: Some(3),
        },
        OpenRangeBounds {
            start_row: Some(2),
            start_column: None,
            end_row: Some(9),
            end_column: None,
        },
        OpenRangeBounds {
            start_row: Some(4),
            start_column: Some(2),
            end_row: None,
            end_column: Some(5),
        },
        OpenRangeBounds {
            start_row: None,
            start_column: Some(2),
            end_row: Some(20),
            end_column: Some(5),
        },
        OpenRangeBounds {
            start_row: Some(3),
            start_column: Some(3),
            end_row: Some(20),
            end_column: None,
        },
        OpenRangeBounds {
            start_row: Some(3),
            start_column: None,
            end_row: Some(20),
            end_column: Some(7),
        },
        OpenRangeBounds {
            start_row: Some(2),
            start_column: Some(2),
            end_row: Some(8),
            end_column: Some(6),
        },
    ]
}

fn populated_engine(
    base_cells: &[(u32, u32)],
    overlay_cells: &[(u32, u32)],
    formula_cells: &[(u32, u32)],
) -> Engine<TestWorkbook> {
    let mut config = arrow_eval_config();
    config.formula_plane_mode = FormulaPlaneMode::Off;
    config.enable_parallel = false;
    config.max_open_ended_rows = 64;
    config.max_open_ended_cols = 16;
    let mut engine = Engine::new(TestWorkbook::new(), config);
    let sheet = "Sheet1";

    {
        let mut ingest = engine.begin_bulk_ingest_arrow();
        ingest.add_sheet(sheet, 8, 4);
        for row in 1..=12 {
            let values = (1..=8)
                .map(|column| {
                    if base_cells.contains(&(row, column)) {
                        LiteralValue::Int(i64::from(row * 10 + column))
                    } else {
                        LiteralValue::Empty
                    }
                })
                .collect::<Vec<_>>();
            ingest.append_row(sheet, &values).unwrap();
        }
        ingest.finish().unwrap();
    }

    for &(row, column) in overlay_cells {
        let row0 = usize::try_from(row - 1).unwrap();
        let column0 = usize::try_from(column - 1).unwrap();
        let arrow_sheet = engine.sheet_store_mut().sheet_mut(sheet).unwrap();
        let (chunk, offset) = arrow_sheet.chunk_of_row(row0).unwrap();
        arrow_sheet.columns[column0].chunks[chunk]
            .overlay
            .set_scalar(offset, OverlayValue::Number(f64::from(row * 10 + column)));
    }

    for &(row, column) in formula_cells {
        engine
            .set_cell_formula(sheet, row, column, parse("=1+2").unwrap())
            .unwrap();
    }
    engine
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn randomized_sheet_extents_match_frozen_ccfeaf83_helpers(
        base_cells in prop::collection::vec((1u32..=12, 1u32..=8), 0..30),
        overlay_cells in prop::collection::vec((1u32..=12, 1u32..=8), 0..20),
        formula_cells in prop::collection::vec((1u32..=32, 1u32..=8), 0..20),
    ) {
        let engine = populated_engine(&base_cells, &overlay_cells, &formula_cells);
        for bounds in range_matrix() {
            prop_assert_eq!(
                evaluation_extent(&engine, "Sheet1", bounds),
                frozen_evaluation_extent_ccfeaf83(&engine, "Sheet1", bounds),
            );
            prop_assert_eq!(
                virtual_dependency_extent(&engine, "Sheet1", bounds),
                frozen_virtual_dependency_extent_ccfeaf83(&engine, "Sheet1", bounds),
            );
        }
    }
}

#[test]
fn differential_matrix_covers_empty_formula_only_overlay_only_and_logical_tail() {
    let empty = populated_engine(&[], &[], &[]);
    let formula_only = populated_engine(&[], &[], &[(31, 2)]);
    let overlay_only = populated_engine(&[], &[(11, 4)], &[]);
    let mixed = populated_engine(&[(2, 1), (9, 7)], &[(12, 3)], &[(30, 6)]);

    for engine in [&empty, &formula_only, &overlay_only, &mixed] {
        for bounds in range_matrix() {
            assert_eq!(
                evaluation_extent(engine, "Sheet1", bounds),
                frozen_evaluation_extent_ccfeaf83(engine, "Sheet1", bounds)
            );
            assert_eq!(
                virtual_dependency_extent(engine, "Sheet1", bounds),
                frozen_virtual_dependency_extent_ccfeaf83(engine, "Sheet1", bounds)
            );
        }
    }

    let whole_column =
        ReferenceType::range(Some("Sheet1".to_string()), None, Some(2), None, Some(2));
    let view = formula_only
        .resolve_range_view(&whole_column, "Sheet1")
        .unwrap();
    assert_eq!((view.start_row(), view.end_row()), (0, 30));
    assert_eq!(
        formula_only.sheet_store().sheet("Sheet1").unwrap().nrows,
        12
    );
}

#[test]
fn interpreter_probe_sheet_bounds_fallback_divergence_is_pinned_not_changed() {
    let engine = populated_engine(&[], &[], &[]);
    let whole_column = OpenRangeBounds {
        start_row: None,
        start_column: Some(1),
        end_row: None,
        end_column: Some(1),
    };

    let evaluation = evaluation_extent(&engine, "Sheet1", whole_column).unwrap();
    let interpreter_probe = interpreter_probe_extent(&engine, "Sheet1", whole_column).unwrap();
    assert_eq!(evaluation.end_row, 64);
    assert_eq!(interpreter_probe.end_row, 1_048_576);
}

#[test]
fn virtual_dependency_used_minimum_divergence_is_pinned_not_changed() {
    let engine = populated_engine(&[(8, 2)], &[], &[(20, 2)]);
    let whole_column = OpenRangeBounds {
        start_row: None,
        start_column: Some(2),
        end_row: None,
        end_column: Some(2),
    };
    let evaluation = evaluation_extent(&engine, "Sheet1", whole_column).unwrap();
    let virtual_dependency = virtual_dependency_extent(&engine, "Sheet1", whole_column).unwrap();
    assert_eq!((evaluation.start_row, evaluation.end_row), (1, 20));
    assert_eq!(
        (virtual_dependency.start_row, virtual_dependency.end_row),
        (8, 20)
    );
}
