//! A workbook-local function registered in place of LOOKUP
//! (`allow_override_builtin`) never runs the builtin, so its calls have no
//! LOOKUP footprint (#586): a call inside the cells the builtin would read
//! past its result's end is not circular, whether it was written before or
//! after the override. Registering or removing the override rebuilds the
//! dependencies of the formulas that call LOOKUP, so the footprint exists
//! exactly while the builtin runs.

use std::sync::Arc;

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_workbook::{CustomFnOptions, Workbook};

fn workbook() -> Workbook {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    for r in 1..=3u32 {
        wb.set_value("S", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        wb.set_value("S", r, 2, LiteralValue::Number(f64::from(10 * r)))
            .unwrap();
    }
    wb
}

fn options() -> CustomFnOptions {
    CustomFnOptions {
        min_args: 3,
        max_args: Some(3),
        allow_override_builtin: true,
        ..Default::default()
    }
}

fn override_lookup(wb: &mut Workbook) {
    wb.register_custom_function(
        "LOOKUP",
        options(),
        Arc::new(|_: &[LiteralValue]| Ok(LiteralValue::Number(42.0))),
    )
    .unwrap();
}

fn is_circ(value: LiteralValue) -> bool {
    matches!(value, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Circ)
}

#[test]
fn a_local_override_written_inside_its_footprint_is_not_circular() {
    let mut wb = workbook();
    // The builtin reads B3 past the end of B1:B2.
    wb.set_formula("S", 1, 4, "=LOOKUP(3,A1:A3,B1:B2)").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 4).unwrap(),
        LiteralValue::Number(30.0)
    );
    override_lookup(&mut wb);
    // In B3, inside the footprint the builtin would have: not circular.
    wb.set_formula("S", 3, 2, "=LOOKUP(3,A1:A3,B1:B2)").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 3, 2).unwrap(),
        LiteralValue::Number(42.0)
    );
}

#[test]
fn installing_and_removing_an_override_rebuilds_the_footprints() {
    let mut wb = workbook();
    // Written before the override, the builtin in B3 reads B3: circular.
    wb.set_formula("S", 3, 2, "=LOOKUP(3,A1:A3,B1:B2)").unwrap();
    assert!(is_circ(wb.evaluate_cell("S", 3, 2).unwrap()));

    // The override has no footprint: the cycle goes, and stays gone after
    // an edit of an input.
    override_lookup(&mut wb);
    assert_eq!(
        wb.evaluate_cell("S", 3, 2).unwrap(),
        LiteralValue::Number(42.0)
    );
    wb.set_value("S", 1, 1, LiteralValue::Number(0.0)).unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 3, 2).unwrap(),
        LiteralValue::Number(42.0)
    );

    // Without the override, the builtin gets its footprint back: a LOOKUP
    // elsewhere reads B3 past the end of B1:B2 and follows its edits.
    wb.unregister_custom_function("LOOKUP").unwrap();
    wb.set_value("S", 3, 2, LiteralValue::Number(30.0)).unwrap();
    wb.set_formula("S", 1, 4, "=LOOKUP(3,A1:A3,B1:B2)").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 4).unwrap(),
        LiteralValue::Number(30.0)
    );
    wb.set_value("S", 3, 2, LiteralValue::Number(99.0)).unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 4).unwrap(),
        LiteralValue::Number(99.0)
    );
}

#[test]
fn a_formula_written_under_an_override_reads_past_the_end_once_it_goes() {
    let mut wb = workbook();
    override_lookup(&mut wb);
    wb.set_formula("S", 1, 4, "=LOOKUP(3,A1:A3,B1:B2)").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 4).unwrap(),
        LiteralValue::Number(42.0)
    );
    wb.unregister_custom_function("LOOKUP").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 4).unwrap(),
        LiteralValue::Number(30.0)
    );
    wb.set_value("S", 3, 2, LiteralValue::Number(99.0)).unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 4).unwrap(),
        LiteralValue::Number(99.0)
    );
}

/// A formula-valued name inside its own footprint: `MyLookup` reads S!B3
/// past the end of S!B1:B2, and S!B3 reads `MyLookup`. With the builtin the pair is
/// circular; installing an override rebuilds the name's dependencies too.
#[test]
fn installing_an_override_rebuilds_a_named_formula() {
    let mut wb = workbook();
    wb.engine_mut()
        .define_name(
            "MyLookup",
            NamedDefinition::Formula {
                ast: formualizer_parse::parser::parse("=LOOKUP(3,S!A1:A3,S!B1:B2)").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();
    wb.set_formula("S", 3, 2, "=MyLookup").unwrap();
    assert!(is_circ(wb.evaluate_cell("S", 3, 2).unwrap()));

    override_lookup(&mut wb);
    assert_eq!(
        wb.evaluate_cell("S", 3, 2).unwrap(),
        LiteralValue::Number(42.0)
    );
}
