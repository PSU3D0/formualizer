//! Shared test utilities for formualizer-workbook integration tests.
//!
//! Re-exported from `formualizer-testkit` so fixture helpers are reusable
//! across tests, benches, and future benchmark corpus generation.

#![allow(unused_imports)]
#![allow(dead_code)]

pub use formualizer_testkit::{build_numeric_grid, build_standard_grid, build_workbook};

/// Assertions shared by the issue #332 "phantom default sheet" load
/// regression tests across the calamine, umya and json backends.
pub mod sheet_load {
    use formualizer_eval::engine::Engine;
    use formualizer_eval::traits::EvaluationContext;
    use formualizer_parse::parser::parse;

    /// Sheet names in Arrow-store order — the same list `Workbook::sheet_names()`
    /// (and the Python/WASM bindings) expose.
    pub fn sheet_names<R: EvaluationContext>(engine: &Engine<R>) -> Vec<String> {
        engine
            .sheet_store()
            .sheets
            .iter()
            .map(|s| s.name.as_ref().to_string())
            .collect()
    }

    /// Assert the loaded engine's sheets are exactly `expected`, in that order,
    /// and that the three views of "which sheet is where" all agree:
    /// the Arrow store, the graph sheet registry, and `SHEET()`/`SHEETS()`.
    ///
    /// Scratch formulas are written far outside any fixture's data region.
    pub fn assert_sheet_layout<R: EvaluationContext>(engine: &mut Engine<R>, expected: &[&str]) {
        let names = sheet_names(engine);
        assert_eq!(
            names,
            expected.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            "Arrow-store sheet list"
        );

        // Graph sheet registry ids must follow the same order, starting at 0.
        for (idx, name) in expected.iter().enumerate() {
            assert_eq!(
                engine.sheet_id(name),
                Some(idx as u16),
                "graph sheet id for {name}"
            );
        }

        for name in expected {
            engine
                .set_cell_formula(name, 900, 20, parse("=SHEET()").unwrap())
                .unwrap();
            engine
                .set_cell_formula(name, 901, 20, parse("=SHEETS()").unwrap())
                .unwrap();
        }
        engine.evaluate_all().unwrap();
        for (idx, name) in expected.iter().enumerate() {
            assert_eq!(
                engine.get_cell_value(name, 900, 20),
                Some(formualizer_common::LiteralValue::Number((idx + 1) as f64)),
                "SHEET() on {name}"
            );
            assert_eq!(
                engine.get_cell_value(name, 901, 20),
                Some(formualizer_common::LiteralValue::Number(
                    expected.len() as f64
                )),
                "SHEETS() on {name}"
            );
        }
    }
}
