use std::sync::Arc;

use proptest::prelude::*;

use crate::engine::inspect::*;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{CellAddress, LiteralValue, RangeArea};
use formualizer_parse::parse;

const SHEET: &str = "Model";
const SPAN_ROWS: u32 = 120;
const ZERO_STAMP: StateStamp = StateStamp {
    mutation_revision: 0,
    recalc_epoch: 0,
};

fn address(row: u32, column: u32) -> CellAddress {
    CellAddress::new(SHEET, row, column).unwrap()
}

fn engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig::default()
            .with_formula_plane_mode(mode)
            .with_parallel(false),
    )
}

fn ingest_span_families(engine: &mut Engine<TestWorkbook>, literal: u32) {
    let mut records = Vec::with_capacity((SPAN_ROWS * 2) as usize);
    for row in 1..=SPAN_ROWS {
        for (column, formula) in [
            (3, format!("=B{row}+A{row}+SUM($A$1:$A$3)+{literal}")),
            (4, format!("=SUM($B:$B)+A{row}+{literal}")),
        ] {
            let ast = parse(&formula).unwrap();
            let ast_id = engine.intern_formula_ast(&ast);
            records.push(FormulaIngestRecord::new(
                row,
                column,
                ast_id,
                Some(Arc::<str>::from(formula)),
            ));
        }
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, records)])
        .unwrap();
}

fn build_pair(literal: u32) -> (Engine<TestWorkbook>, Engine<TestWorkbook>) {
    let mut off = engine(FormulaPlaneMode::Off);
    let mut authoritative = engine(FormulaPlaneMode::AuthoritativeExperimental);
    for engine in [&mut off, &mut authoritative] {
        for row in 1..=SPAN_ROWS + 8 {
            engine
                .set_cell_value(SHEET, row, 1, LiteralValue::Number(f64::from(row)))
                .unwrap();
            engine
                .set_cell_value(SHEET, row, 2, LiteralValue::Number(f64::from(row * 2)))
                .unwrap();
        }
        engine
            .define_name(
                "Rate",
                NamedDefinition::Literal(LiteralValue::Number(0.25)),
                NameScope::Workbook,
            )
            .unwrap();
        let sheet = engine.sheet_id(SHEET).unwrap();
        engine
            .define_table(
                "Sales",
                RangeRef::new(
                    CellRef::new(sheet, Coord::from_excel(1, 7, true, true)),
                    CellRef::new(sheet, Coord::from_excel(3, 8, true, true)),
                ),
                true,
                vec!["Region".into(), "Amount".into()],
                false,
            )
            .unwrap();
        ingest_span_families(engine, literal);
        engine
            .set_cell_formula(SHEET, 5, 10, parse("=INDIRECT(\"A1\")").unwrap())
            .unwrap();
        engine
            .set_cell_formula(SHEET, 6, 10, parse("=Rate+A6").unwrap())
            .unwrap();
        engine
            .set_cell_formula(SHEET, 2, 10, parse("=Sales[@Amount]").unwrap())
            .unwrap();
        engine
            .set_cell_formula(SHEET, 1, 12, parse("={1,2;3,4}").unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        engine
            .set_cell_formula(SHEET, 1, 14, parse("=M2").unwrap())
            .unwrap();
    }

    let off_stats = off.baseline_stats();
    let authoritative_stats = authoritative.baseline_stats();
    assert_eq!(
        authoritative_stats.formula_plane_active_span_count, 2,
        "authoritative parity fixture did not retain both FormulaPlane families"
    );
    assert_eq!(off_stats.formula_plane_active_span_count, 0);
    assert_ne!(
        off_stats.graph_formula_vertex_count, authoritative_stats.graph_formula_vertex_count,
        "parity fixture used identical formula representations"
    );
    (off, authoritative)
}

fn assert_snapshot_parity(
    off: &Engine<TestWorkbook>,
    authoritative: &Engine<TestWorkbook>,
    cell: CellAddress,
    options: SnapshotOptions,
) {
    let mut left = off.inspect_cell(&cell, &options).unwrap();
    let mut right = authoritative.inspect_cell(&cell, &options).unwrap();
    left.stamp = ZERO_STAMP;
    right.stamp = ZERO_STAMP;
    assert_eq!(left, right, "inspect_cell parity at {cell}");
}

fn assert_precedent_parity(
    off: &Engine<TestWorkbook>,
    authoritative: &Engine<TestWorkbook>,
    cell: CellAddress,
    options: PrecedentOptions,
) {
    let mut left = off.precedents(&cell, &options).unwrap();
    let mut right = authoritative.precedents(&cell, &options).unwrap();
    left.stamp = ZERO_STAMP;
    right.stamp = ZERO_STAMP;
    assert_eq!(left, right, "precedents parity at {cell}");
}

fn assert_dependent_parity(
    off: &Engine<TestWorkbook>,
    authoritative: &Engine<TestWorkbook>,
    cell: CellAddress,
    options: DependentsOptions,
) {
    let mut left = off.dependents(&cell, &options).unwrap();
    let mut right = authoritative.dependents(&cell, &options).unwrap();
    left.stamp = ZERO_STAMP;
    right.stamp = ZERO_STAMP;
    assert_eq!(left, right, "dependents parity at {cell}");
}

fn assert_trace_parity(
    off: &Engine<TestWorkbook>,
    authoritative: &Engine<TestWorkbook>,
    roots: &[CellAddress],
    options: TraceOptions,
) {
    let mut left = off.trace(roots, &options).unwrap();
    let mut right = authoritative.trace(roots, &options).unwrap();
    left.stamp = ZERO_STAMP;
    right.stamp = ZERO_STAMP;
    assert_eq!(left, right, "trace parity at {roots:?}");
}

fn assert_page_parity(
    off: &Engine<TestWorkbook>,
    authoritative: &Engine<TestWorkbook>,
    area: &RangeArea,
    options: RangePageOptions,
) {
    let mut left = off.range_page(area, &options).unwrap();
    let mut right = authoritative.range_page(area, &options).unwrap();
    left.stamp = ZERO_STAMP;
    right.stamp = ZERO_STAMP;
    assert_eq!(left, right, "range_page parity at {area:?}");
}

fn assert_all_five_public_apis_match(
    off: &Engine<TestWorkbook>,
    authoritative: &Engine<TestWorkbook>,
) {
    for cell in [
        address(1, 3),
        address(64, 3),
        address(120, 3),
        address(64, 4),
        address(2, 10),
        address(5, 10),
        address(6, 10),
        address(1, 12),
        address(2, 13),
        address(125, 15),
    ] {
        assert_snapshot_parity(off, authoritative, cell.clone(), SnapshotOptions::default());
        assert_precedent_parity(off, authoritative, cell, PrecedentOptions::default());
    }
    for cell in [
        address(1, 1),
        address(64, 1),
        address(64, 2),
        address(1, 12),
    ] {
        assert_dependent_parity(off, authoritative, cell, DependentsOptions::default());
    }
    assert_trace_parity(
        off,
        authoritative,
        &[address(64, 3), address(2, 13)],
        TraceOptions::default(),
    );
    assert_trace_parity(
        off,
        authoritative,
        &[address(64, 1)],
        TraceOptions::default().with_direction(TraceDirection::Dependents),
    );
    assert_page_parity(
        off,
        authoritative,
        &RangeArea::new(SHEET, Some(62), Some(1), Some(66), Some(4)).unwrap(),
        RangePageOptions::default().with_limit(13),
    );
}

#[test]
fn formula_plane_span_adapter_uses_plane_formula_per_placement_dependency_templates_and_source_order()
 {
    let (off, authoritative) = build_pair(9);
    let cell = address(64, 3);
    let off_cell = off
        .inspect_cell(&cell, &SnapshotOptions::default())
        .unwrap();
    let authoritative_cell = authoritative
        .inspect_cell(&cell, &SnapshotOptions::default())
        .unwrap();
    assert_eq!(authoritative_cell.cell.formula, off_cell.cell.formula);
    assert!(authoritative_cell.cell.formula.is_some());

    let report = authoritative
        .precedents(&cell, &PrecedentOptions::default())
        .unwrap();
    assert_eq!(report.precedents.len(), 3);
    assert_eq!(
        report.precedents[0].reference,
        SemanticReference::Cell(address(64, 2))
    );
    assert_eq!(
        report.precedents[1].reference,
        SemanticReference::Cell(address(64, 1))
    );
    assert!(matches!(
        &report.precedents[2].reference,
        SemanticReference::Range { declared, .. }
            if declared.start_row == Some(1) && declared.end_row == Some(3)
                && declared.start_column == Some(1) && declared.end_column == Some(1)
    ));
}

#[test]
fn legacy_and_formula_plane_all_five_inspection_apis_are_field_exact_across_spans_fallbacks_spills_whole_columns_and_dirty_state()
 {
    let (mut off, mut authoritative) = build_pair(7);
    assert_all_five_public_apis_match(&off, &authoritative);

    off.set_cell_value(SHEET, 64, 1, LiteralValue::Number(999.0))
        .unwrap();
    authoritative
        .set_cell_value(SHEET, 64, 1, LiteralValue::Number(999.0))
        .unwrap();
    assert_all_five_public_apis_match(&off, &authoritative);
    let dirty = authoritative
        .inspect_cell(&address(64, 3), &SnapshotOptions::default())
        .unwrap();
    assert_eq!(dirty.cell.staleness, Staleness::Dirty);
}

#[test]
fn formula_plane_per_placement_literal_bindings_preserve_canonical_formula_inspection_parity() {
    let build = |mode| {
        let mut engine = engine(mode);
        let mut records = Vec::with_capacity(SPAN_ROWS as usize);
        for row in 1..=SPAN_ROWS {
            engine
                .set_cell_value(SHEET, row, 1, LiteralValue::Number(f64::from(row)))
                .unwrap();
            let formula = format!("=A{row}+{row}");
            let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
            records.push(FormulaIngestRecord::new(
                row,
                3,
                ast_id,
                Some(Arc::<str>::from(formula)),
            ));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, records)])
            .unwrap();
        engine.evaluate_all().unwrap();
        engine
    };
    let off = build(FormulaPlaneMode::Off);
    let authoritative = build(FormulaPlaneMode::AuthoritativeExperimental);
    assert_eq!(
        authoritative
            .baseline_stats()
            .formula_plane_active_span_count,
        1
    );
    for row in [1, 64, SPAN_ROWS] {
        assert_snapshot_parity(
            &off,
            &authoritative,
            address(row, 3),
            SnapshotOptions::default(),
        );
    }
}

#[test]
fn legacy_and_formula_plane_budget_truncation_and_missing_sheet_errors_are_identical() {
    let (off, authoritative) = build_pair(3);
    assert_precedent_parity(
        &off,
        &authoritative,
        address(64, 3),
        PrecedentOptions::default().with_max_links(1),
    );
    assert_dependent_parity(
        &off,
        &authoritative,
        address(64, 1),
        DependentsOptions::default().with_max_work(0),
    );
    assert_trace_parity(
        &off,
        &authoritative,
        &[address(64, 3)],
        TraceOptions::default()
            .with_max_links(1)
            .with_range_member_budget(0),
    );

    let missing = CellAddress::new("Missing", 1, 1).unwrap();
    assert_eq!(
        off.inspect_cell(&missing, &SnapshotOptions::default()),
        authoritative.inspect_cell(&missing, &SnapshotOptions::default())
    );
    assert_eq!(
        off.precedents(&missing, &PrecedentOptions::default()),
        authoritative.precedents(&missing, &PrecedentOptions::default())
    );
    assert_eq!(
        off.dependents(&missing, &DependentsOptions::default()),
        authoritative.dependents(&missing, &DependentsOptions::default())
    );
    assert_eq!(
        off.trace(&[missing], &TraceOptions::default()),
        authoritative.trace(
            &[CellAddress::new("Missing", 1, 1).unwrap()],
            &TraceOptions::default()
        )
    );
    let missing_area = RangeArea::new("Missing", Some(1), Some(1), Some(2), Some(2)).unwrap();
    assert_eq!(
        off.range_page(&missing_area, &RangePageOptions::default()),
        authoritative.range_page(&missing_area, &RangePageOptions::default())
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    #[test]
    fn randomized_ingested_workbooks_keep_all_five_public_inspection_apis_plane_independent(
        literal in 0u32..50,
        dirty_row in 1u32..=SPAN_ROWS,
        dirty_value in -10_000i32..10_000,
    ) {
        let (mut off, mut authoritative) = build_pair(literal);
        off.set_cell_value(SHEET, dirty_row, 1, LiteralValue::Number(f64::from(dirty_value))).unwrap();
        authoritative.set_cell_value(SHEET, dirty_row, 1, LiteralValue::Number(f64::from(dirty_value))).unwrap();

        assert_all_five_public_apis_match(&off, &authoritative);
        assert_snapshot_parity(&off, &authoritative, address(dirty_row, 3), SnapshotOptions::default());
        assert_precedent_parity(&off, &authoritative, address(dirty_row, 3), PrecedentOptions::default());
        assert_dependent_parity(&off, &authoritative, address(dirty_row, 1), DependentsOptions::default());
    }
}
