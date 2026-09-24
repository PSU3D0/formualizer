//! Shape memo differential tests (FORM-000133): memoized and unmemoized
//! preparation must produce identical `IngestedFormula` values, including
//! dependency plans and placement inputs, on witness families and adversarial
//! cases. Memo hits are additionally re-derived by the in-pipeline verifier.

use crate::engine::ingest_pipeline::{
    FormulaAstInput, IngestedFormula, ingested_formula_difference,
};
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::shape_memo::MemoCounts;
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::ExcelError;
use formualizer_parse::parser::parse;

type Cells = Vec<(String, u32, u32, String)>;

fn engine() -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.graph.sheet_id_mut("Sheet1");
    engine.graph.sheet_id_mut("Sheet2");
    engine
}

fn family(
    sheet: &str,
    col: u32,
    rows: std::ops::RangeInclusive<u32>,
    f: impl Fn(u32) -> String,
) -> Cells {
    rows.map(|r| (sheet.to_string(), r, col, f(r))).collect()
}

/// Run `f` until no global function registration (from concurrently running
/// tests) lands during it, and return that run's result. Any registration
/// clears the memo, so memo counts are only meaningful for such a run.
fn with_stable_registry<T>(mut f: impl FnMut() -> T) -> T {
    for _ in 0..50 {
        let epoch = crate::function_registry::semantic_epoch_lock_free();
        let result = f();
        if crate::function_registry::semantic_epoch_lock_free() == epoch {
            return result;
        }
    }
    panic!("global function registry kept changing");
}

/// Ingest `cells` (interned as arena ASTs) through one pipeline with and one
/// without the memo, compare every result, and return the memo's counts from
/// a run without concurrent registrations.
fn differential(engine: &mut Engine<TestWorkbook>, cells: &Cells) -> MemoCounts {
    with_stable_registry(|| differential_once(engine, cells))
}

fn differential_once(engine: &mut Engine<TestWorkbook>, cells: &Cells) -> MemoCounts {
    let inputs: Vec<_> = cells
        .iter()
        .map(|(sheet, row, col, text)| {
            let ast = parse(text).unwrap_or_else(|e| panic!("parse {text}: {e}"));
            let ast_id = engine.intern_formula_ast(&ast);
            let sheet_id = engine.graph.sheet_id_mut(sheet);
            (
                ast_id,
                CellRef::new(sheet_id, Coord::from_excel(*row, *col, true, true)),
                text.clone(),
            )
        })
        .collect();
    let run = |engine: &mut Engine<TestWorkbook>, memo: bool| {
        let mut pipeline = engine.ingest_pipeline();
        if !memo {
            pipeline = pipeline.without_shape_memo();
        }
        let results: Vec<Result<IngestedFormula, ExcelError>> = inputs
            .iter()
            .map(|(ast_id, placement, text)| {
                pipeline.ingest_formula(
                    FormulaAstInput::RawArena(*ast_id),
                    *placement,
                    Some(text.as_str().into()),
                )
            })
            .collect();
        (results, pipeline.memo_counts())
    };
    let (memoized, counts) = run(engine, true);
    let (reference, _) = run(engine, false);
    for (((sheet, row, col, text), memoized), reference) in
        cells.iter().zip(&memoized).zip(&reference)
    {
        match (memoized, reference) {
            (Ok(a), Ok(b)) => {
                if let Some(field) = ingested_formula_difference(a, b) {
                    panic!("{sheet}!R{row}C{col} `{text}`: memoized result differs in {field}");
                }
            }
            (Err(a), Err(b)) => assert_eq!(
                (a.kind, &a.message),
                (b.kind, &b.message),
                "{sheet}!R{row}C{col} `{text}`: errors differ"
            ),
            (a, b) => panic!(
                "{sheet}!R{row}C{col} `{text}`: memo ok={} reference ok={}",
                a.is_ok(),
                b.is_ok()
            ),
        }
    }
    counts
}

#[test]
fn witness_families_are_identical_and_hit() {
    let n = 64;
    type Template = Box<dyn Fn(u32) -> String>;
    let families: Vec<(&str, Template, u32)> = vec![
        ("independent", Box::new(|r| format!("=A{r}*2+1")), 2),
        ("window", Box::new(|r| format!("=SUM(A{r}:A{})", r + 7)), 3),
        ("reverse", Box::new(|r| format!("=B{}+A{r}", r + 1)), 2),
        (
            "lookup",
            Box::new(move |r| format!("=VLOOKUP({r},$D$1:$E${n},2,FALSE)")),
            2,
        ),
        ("chain", Box::new(|r| format!("=B{}+A{r}", r.max(2) - 1)), 2),
        (
            "stride",
            Box::new(|r| format!("=B{}+A{r}", r.max(13) - 12)),
            2,
        ),
        (
            "coupled",
            Box::new(|r| format!("=C{}+A{r}", r.max(2) - 1)),
            2,
        ),
        ("fixed", Box::new(move |_| format!("=SUM($A$1:$A${n})")), 2),
        (
            "same-relative",
            Box::new(move |_| format!("=SUM(A1:A{n})")),
            2,
        ),
        ("expanding", Box::new(|r| format!("=SUM(A$1:A{r})")), 2),
        ("irregular", Box::new(|r| format!("=A1+A{r}+{r}*{r}")), 2),
    ];
    for (id, f, col) in families {
        let mut engine = engine();
        let cells = family("Sheet1", col, 2..=n, f);
        let counts = differential(&mut engine, &cells);
        if matches!(id, "irregular" | "lookup" | "same-relative") {
            // A literal differs per row (`{r}`), or identical text has a
            // different relative offset per row: every cell is its own shape.
            assert_eq!(counts.specialization_hits, 0, "{id}: {counts:?}");
        } else {
            // Boundary rows (clamped offsets) are separate shapes.
            assert!(
                counts.specialization_hits >= u64::from(n) - 14,
                "{id}: {counts:?}"
            );
            assert!(counts.specialization_misses <= 12, "{id}: {counts:?}");
        }
    }
    // Horizontal copies share one shape across columns.
    let mut engine = engine();
    let cells: Cells = (2..=40)
        .map(|c| ("Sheet1".to_string(), 2, c, format!("={}1*2+1", col_name(c))))
        .collect();
    let counts = differential(&mut engine, &cells);
    assert!(counts.specialization_hits >= 36, "{counts:?}");
}

fn col_name(mut col: u32) -> String {
    let mut out = Vec::new();
    while col > 0 {
        let rem = (col - 1) % 26;
        out.push(b'A' + rem as u8);
        col = (col - 1) / 26;
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

#[test]
fn threshold_crossing_ranges_rechoose_expansion_per_placement() {
    // `SUM($A$1:A{r})` is one shape; its area crosses the expansion limit (64),
    // so the dependency plan switches from expanded cells to a range.
    let mut engine = engine();
    let cells = family("Sheet1", 2, 1..=200, |r| format!("=SUM($A$1:A{r})"));
    let counts = differential(&mut engine, &cells);
    assert!(counts.specialization_hits >= 197, "{counts:?}");
    let sheet = engine.graph.sheet_id_mut("Sheet1");
    let shapes: Vec<_> = [10u32, 64, 65, 200]
        .into_iter()
        .map(|r| {
            let ast = parse(format!("=SUM($A$1:A{r})")).unwrap();
            (r, engine.intern_formula_ast(&ast))
        })
        .collect();
    let hits = with_stable_registry(|| {
        let mut pipeline = engine.ingest_pipeline();
        for &(r, ast_id) in &shapes {
            let plan = pipeline
                .ingest_formula(
                    FormulaAstInput::RawArena(ast_id),
                    CellRef::new(sheet, Coord::from_excel(r, 2, true, true)),
                    None,
                )
                .unwrap()
                .dep_plan;
            if r <= 64 {
                assert_eq!(plan.direct_cell_deps.len(), r as usize, "row {r} expands");
                assert!(plan.range_deps.is_empty(), "row {r}");
            } else {
                assert!(plan.direct_cell_deps.is_empty(), "row {r} subscribes");
                assert_eq!(plan.range_deps.len(), 1, "row {r}");
            }
        }
        pipeline.memo_counts().specialization_hits
    });
    // Four cells: first-formula skip, first sighting, traced miss, hit.
    assert_eq!(hits, 1);
}

#[test]
fn reversed_mixed_ranges_error_per_placement() {
    // `SUM($A$10:A{r})` has one relative shape, but rows below 10 are
    // reversed (#REF!) and must fail exactly like the per-cell path.
    let mut engine = engine();
    let cells = family("Sheet1", 2, 1..=30, |r| format!("=SUM($A$10:A{r})"));
    let counts = differential(&mut engine, &cells);
    assert!(counts.specialization_hits >= 17, "{counts:?}");
}

#[test]
fn grid_edge_offsets_are_identical() {
    let mut engine = engine();
    let mut cells = family("Sheet1", 1, 2..=20, |r| format!("=A{}+1", r - 1));
    cells.extend(family("Sheet1", 16_384, 1_048_560..=1_048_576, |r| {
        format!("=XFC{r}+XFD{}", r - 1)
    }));
    cells.extend(family("Sheet1", 2, 1..=10, |r| {
        format!("=A{r}+$XFD$1048576")
    }));
    differential(&mut engine, &cells);
}

#[test]
fn names_scopes_and_late_definitions_are_identical() {
    let mut engine = engine();
    let mut cells = family("Sheet1", 2, 1..=20, |r| format!("=A{r}*Rate"));
    cells.extend(family("Sheet2", 2, 1..=20, |r| format!("=A{r}*Rate")));
    cells.extend(family("Sheet1", 3, 1..=20, |r| format!("=SUM(Block)+A{r}")));

    // Names used before they are defined.
    let before = differential(&mut engine, &cells);
    assert!(before.specialization_hits > 0, "{before:?}");

    let sheet1 = engine.graph.sheet_id_mut("Sheet1");
    let sheet2 = engine.graph.sheet_id_mut("Sheet2");
    let cell = |sheet, row, col| CellRef::new(sheet, Coord::from_excel(row, col, true, true));
    engine
        .define_name(
            "Rate",
            NamedDefinition::Cell(cell(sheet1, 1, 10)),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .define_name(
            "Rate",
            NamedDefinition::Cell(cell(sheet2, 2, 10)),
            NameScope::Sheet(sheet2),
        )
        .unwrap();
    engine
        .define_name(
            "Block",
            NamedDefinition::Range(RangeRef::new(cell(sheet1, 1, 12), cell(sheet1, 10, 12))),
            NameScope::Workbook,
        )
        .unwrap();

    // A later pipeline sees the definitions; sheet-scoped `Rate` shadows the
    // workbook name on Sheet2 only.
    let after = differential(&mut engine, &cells);
    assert!(after.specialization_hits > 0, "{after:?}");
    let resolved = |engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32| {
        let text = format!("=A{row}*Rate");
        let ast_id = engine.intern_formula_ast(&parse(&text).unwrap());
        let sheet_id = engine.graph.sheet_id_mut(sheet);
        let formula = engine
            .ingest_pipeline()
            .ingest_formula(
                FormulaAstInput::RawArena(ast_id),
                CellRef::new(sheet_id, Coord::from_excel(row, 2, true, true)),
                None,
            )
            .unwrap();
        (
            formula.dep_plan.resolved_named_refs,
            formula.read_projections,
        )
    };
    let s1 = resolved(&mut engine, "Sheet1", 5);
    let s2 = resolved(&mut engine, "Sheet2", 5);
    assert_eq!(s1.0, vec!["Rate".to_string()]);
    assert_ne!(
        s1.1, s2.1,
        "sheet scope must change the resolved projection"
    );
}

#[test]
fn table_this_row_references_bypass() {
    let mut engine = engine();
    let sheet1 = engine.graph.sheet_id_mut("Sheet1");
    let cell = |row, col| CellRef::new(sheet1, Coord::from_excel(row, col, true, true));
    engine
        .define_table(
            "T",
            RangeRef::new(cell(1, 1), cell(21, 3)),
            true,
            vec!["Qty".into(), "Price".into(), "Total".into()],
            false,
        )
        .unwrap();
    let mut cells = family("Sheet1", 3, 2..=21, |_| "=[@Qty]*[@Price]".to_string());
    cells.extend(family("Sheet1", 5, 2..=21, |r| {
        format!("=SUM(T[Qty])+A{r}")
    }));
    let counts = differential(&mut engine, &cells);
    assert_eq!(counts.specialization_hits, 0, "{counts:?}");
    assert_eq!(counts.bypasses, cells.len() as u64, "{counts:?}");
}

#[test]
fn position_and_dynamic_functions_are_identical() {
    let mut engine = engine();
    let mut cells = family("Sheet1", 2, 1..=20, |r| format!("=ROW()+A{r}"));
    cells.extend(family("Sheet1", 3, 1..=20, |r| format!("=COLUMN()*A{r}")));
    cells.extend(family("Sheet1", 4, 1..=20, |r| format!("=RAND()+A{r}")));
    cells.extend(family("Sheet1", 5, 1..=20, |_| {
        "=INDIRECT(\"A\"&ROW())".to_string()
    }));
    cells.extend(family("Sheet1", 6, 1..=20, |r| {
        format!("=OFFSET(A{r},0,1)")
    }));
    cells.extend(family("Sheet1", 7, 1..=20, |r| format!("=NOW()-A{r}")));
    let counts = differential(&mut engine, &cells);
    assert!(counts.specialization_hits > 100, "{counts:?}");
}

#[test]
fn local_bindings_sheets_literals_and_open_ranges_are_identical() {
    let mut engine = engine();
    let mut cells = family("Sheet1", 2, 1..=20, |r| format!("=LET(x,A{r},x*2)"));
    cells.extend(family("Sheet1", 3, 1..=20, |r| {
        format!("=LET(A1,5,A1+A{r})")
    }));
    cells.extend(family("Sheet1", 4, 1..=20, |r| format!("=Sheet2!A{r}+1")));
    cells.extend(family("Sheet1", 5, 1..=20, |r| format!("=Missing!A{r}+1")));
    cells.extend(family("Sheet1", 6, 1..=20, |r| format!("=\"x\"&A{r}&TRUE")));
    cells.extend(family("Sheet1", 7, 1..=20, |r| {
        format!("=IF(A{r}>0,#N/A,1)")
    }));
    cells.extend(family("Sheet1", 8, 1..=20, |r| format!("=SUM(A:A)+A{r}")));
    cells.extend(family("Sheet1", 9, 1..=20, |r| format!("=SUM(A{r}:A)")));
    cells.extend(family("Sheet1", 10, 1..=20, |r| {
        format!("=SUM(Sheet1:Sheet2!A{r})")
    }));
    cells.extend(family("Sheet1", 11, 1..=20, |r| {
        format!("=IF(A{r},B{r},C{r})")
    }));
    cells.extend(family("Sheet1", 12, 1..=20, |r| format!("={{1,2}}+A{r}")));
    differential(&mut engine, &cells);
}

#[test]
fn repeated_pipelines_do_not_share_memo_state() {
    // Each public request builds a new pipeline; a name defined between two
    // requests must be observed by the second one.
    let mut engine = engine();
    let cells = family("Sheet1", 2, 1..=10, |r| format!("=A{r}+Later"));
    let first = differential(&mut engine, &cells);
    let sheet1 = engine.graph.sheet_id_mut("Sheet1");
    engine
        .define_name(
            "Later",
            NamedDefinition::Cell(CellRef::new(sheet1, Coord::from_excel(1, 9, true, true))),
            NameScope::Workbook,
        )
        .unwrap();
    let second = differential(&mut engine, &cells);
    assert_eq!(first.specialization_misses, 1);
    assert_eq!(second.specialization_misses, 1);
}

/// A volatile user function registered in the global registry mid-pipeline.
struct MidPipelineFn;

impl crate::function::Function for MidPipelineFn {
    fn name(&self) -> &'static str {
        "FORMMIDPIPELINEREG"
    }
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::VOLATILE
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
        static SCHEMA: std::sync::LazyLock<Vec<crate::args::ArgSchema>> =
            std::sync::LazyLock::new(|| vec![crate::args::ArgSchema::any()]);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(
            formualizer_common::LiteralValue::Number(1.0),
        ))
    }
}

// FORM-000139: a function registered in the global registry while a pipeline
// is running must not let later same-shape cells reuse specializations
// derived before the registration. The global-registry provider's planning
// revision does not move, so only the registry epoch in the memo's validity
// token catches it.
#[test]
fn function_registered_mid_pipeline_invalidates_memo() {
    let mut engine = engine();
    let sheet = engine.graph.sheet_id_mut("Sheet1");
    let inputs: Vec<_> = (1..=12u32)
        .map(|r| {
            let text = format!("=FORMMIDPIPELINEREG(A{r})+1");
            let ast_id = engine.intern_formula_ast(&parse(&text).unwrap());
            (
                ast_id,
                CellRef::new(sheet, Coord::from_excel(r, 2, true, true)),
            )
        })
        .collect();
    let ingest =
        |pipeline: &mut crate::engine::ingest_pipeline::IngestPipeline<'_>,
         (ast_id, placement): (crate::engine::arena::AstNodeId, CellRef)| {
            pipeline
                .ingest_formula(FormulaAstInput::RawArena(ast_id), placement, None)
                .unwrap()
        };

    let mut pipeline = engine.ingest_pipeline();
    // First formula skip, first sighting, traced miss, then hits: the memo
    // holds the unknown-function specialization (not volatile). Concurrent
    // tests' registrations may clear it; keep ingesting until it hits.
    let mut before = pipeline.memo_counts();
    for (i, &input) in inputs[..6].iter().cycle().enumerate() {
        assert!(!ingest(&mut pipeline, input).dep_plan.volatile);
        before = pipeline.memo_counts();
        if i >= 5 && before.specialization_hits >= 2 {
            break;
        }
        assert!(i < 300, "memo never hit: {before:?}");
    }

    crate::function_registry::register_function(std::sync::Arc::new(MidPipelineFn));

    // Same pipeline, same shape: every later cell sees the registered
    // (volatile) function, like the per-cell path does.
    let memoized: Vec<_> = inputs[6..]
        .iter()
        .map(|&input| ingest(&mut pipeline, input))
        .collect();
    let after = pipeline.memo_counts();
    assert!(
        after.specialization_misses > before.specialization_misses,
        "registration must invalidate: {before:?} -> {after:?}"
    );
    drop(pipeline);
    let mut reference_pipeline = engine.ingest_pipeline().without_shape_memo();
    for (memoized, &input) in memoized.iter().zip(&inputs[6..]) {
        assert!(memoized.dep_plan.volatile, "{:?}", memoized.placement);
        let reference = ingest(&mut reference_pipeline, input);
        if let Some(field) = ingested_formula_difference(memoized, &reference) {
            panic!(
                "{:?}: memoized result differs in {field}",
                memoized.placement
            );
        }
    }
}

// FORM-000139: adversarial full hash collisions (every shape in one bucket)
// keep results identical and degrade to the per-cell path.
#[test]
fn colliding_shape_hashes_degrade_to_per_cell_path() {
    crate::engine::shape_memo::FORCED_HASH.with(|hash| hash.set(Some(42)));
    let mut engine = engine();
    let mut cells = Cells::new();
    for col in 2..=40u32 {
        let offset = col;
        cells.extend(family("Sheet1", col, 1..=20, move |r| {
            format!("=A{r}*{offset}+1")
        }));
    }
    let counts = differential(&mut engine, &cells);
    crate::engine::shape_memo::FORCED_HASH.with(|hash| hash.set(None));
    // At most MAX_BUCKET_SHAPES shapes ever materialize; all other columns
    // bypass instead of being compared against an ever-growing bucket.
    let bucket = crate::engine::shape_memo::MAX_BUCKET_SHAPES as u64;
    assert!(counts.specialization_misses <= bucket, "{counts:?}");
    assert!(counts.specialization_hits > 0, "{counts:?}");
    assert!(
        counts.bypasses >= cells.len() as u64 - bucket * 20,
        "{counts:?}"
    );
}

/// Non-global provider whose `FORMFLIP` flips from nonvolatile to volatile
/// through interior mutability, optionally bumping a planning revision.
struct FlipProvider {
    volatile: std::sync::atomic::AtomicBool,
    revision: Option<std::sync::atomic::AtomicU64>,
}

impl FlipProvider {
    fn new(revisioned: bool) -> Self {
        Self {
            volatile: std::sync::atomic::AtomicBool::new(false),
            revision: revisioned.then(|| std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn flip(&self) {
        use std::sync::atomic::Ordering;
        self.volatile.store(true, Ordering::SeqCst);
        if let Some(revision) = &self.revision {
            revision.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn flip_fn(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        if name.eq_ignore_ascii_case("FORMFLIP") {
            let volatile = self.volatile.load(std::sync::atomic::Ordering::SeqCst);
            return Some(std::sync::Arc::new(FlipFn(volatile)));
        }
        crate::function_registry::get(ns, name)
    }
}

impl crate::traits::FunctionProvider for FlipProvider {
    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        self.flip_fn(ns, name)
    }
    fn get_function_for_planning(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        self.flip_fn(ns, name)
    }
    fn planning_semantic_revision(&self) -> Option<u64> {
        self.revision
            .as_ref()
            .map(|revision| revision.load(std::sync::atomic::Ordering::SeqCst))
    }
}

struct FlipFn(bool);

impl crate::function::Function for FlipFn {
    fn name(&self) -> &'static str {
        "FORMFLIP"
    }
    fn caps(&self) -> crate::function::FnCaps {
        if self.0 {
            crate::function::FnCaps::VOLATILE
        } else {
            crate::function::FnCaps::PURE
        }
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
        static SCHEMA: std::sync::LazyLock<Vec<crate::args::ArgSchema>> =
            std::sync::LazyLock::new(|| vec![crate::args::ArgSchema::any()]);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(
            formualizer_common::LiteralValue::Number(1.0),
        ))
    }
}

fn flip_inputs(
    engine: &mut Engine<TestWorkbook>,
    rows: std::ops::RangeInclusive<u32>,
) -> Vec<(crate::engine::arena::AstNodeId, CellRef)> {
    let sheet = engine.graph.sheet_id_mut("Sheet1");
    rows.map(|r| {
        let ast_id = engine.intern_formula_ast(&parse(format!("=FORMFLIP(A{r})+1")).unwrap());
        (
            ast_id,
            CellRef::new(sheet, Coord::from_excel(r, 2, true, true)),
        )
    })
    .collect()
}

// FORM-000139: a provider without a planning revision cannot prove its
// semantics unchanged between cells, so it gets no memo reuse at all: a
// nonvolatile -> volatile flip mid-pipeline is observed by the next cell.
#[test]
fn unrevisioned_provider_never_reuses_memo() {
    let mut engine = engine();
    let inputs = flip_inputs(&mut engine, 1..=24);
    let provider = FlipProvider::new(false);
    let mut pipeline = engine.graph.ingest_pipeline(&provider);
    for (i, &(ast_id, placement)) in inputs.iter().enumerate() {
        if i == 12 {
            provider.flip();
        }
        let formula = pipeline
            .ingest_formula(FormulaAstInput::RawArena(ast_id), placement, None)
            .unwrap();
        assert_eq!(formula.dep_plan.volatile, i >= 12, "cell {i}");
    }
    let counts = pipeline.memo_counts();
    assert_eq!(counts.specialization_hits, 0, "{counts:?}");
    assert_eq!(counts.specialization_misses, 0, "{counts:?}");
    assert_eq!(counts.bypasses, inputs.len() as u64, "{counts:?}");
}

// FORM-000139: a revisioned provider gets memo hits, and bumping its revision
// together with a semantic change invalidates the memo mid-pipeline.
#[test]
fn provider_revision_bump_invalidates_memo() {
    let mut engine = engine();
    let inputs = flip_inputs(&mut engine, 1..=300);
    let provider = FlipProvider::new(true);
    let mut pipeline = engine.graph.ingest_pipeline(&provider);
    let ingest =
        |pipeline: &mut crate::engine::ingest_pipeline::IngestPipeline<'_>,
         (ast_id, placement): (crate::engine::arena::AstNodeId, CellRef)| {
            pipeline
                .ingest_formula(FormulaAstInput::RawArena(ast_id), placement, None)
                .unwrap()
        };
    // Concurrent tests' registrations may clear the memo; keep going until it
    // has hit.
    let mut next = 0;
    let before = loop {
        assert!(next < 250, "memo never hit");
        assert!(!ingest(&mut pipeline, inputs[next]).dep_plan.volatile);
        next += 1;
        let counts = pipeline.memo_counts();
        if counts.specialization_hits >= 2 {
            break counts;
        }
    };
    provider.flip();
    for &input in &inputs[next..next + 10] {
        assert!(
            ingest(&mut pipeline, input).dep_plan.volatile,
            "{:?}",
            input.1
        );
    }
    let after = pipeline.memo_counts();
    assert!(
        after.specialization_misses > before.specialization_misses,
        "revision bump must invalidate: {before:?} -> {after:?}"
    );
}

fn scratch_is_bounded(pipeline: &crate::engine::ingest_pipeline::IngestPipeline<'_>) {
    use crate::engine::shape_memo::{MAX_SHAPE_REFS, MAX_SHAPE_TOKENS};
    let memo = pipeline.shape_memo_state().expect("memo allocated");
    assert!(
        memo.tokens.len() <= MAX_SHAPE_TOKENS,
        "{}",
        memo.tokens.len()
    );
    assert!(
        memo.tokens.capacity() <= MAX_SHAPE_TOKENS,
        "{}",
        memo.tokens.capacity()
    );
    assert!(memo.refs.len() <= MAX_SHAPE_REFS, "{}", memo.refs.len());
    assert!(
        memo.refs.capacity() <= MAX_SHAPE_REFS,
        "{}",
        memo.refs.capacity()
    );
}

/// Ingest `cells` through a memoized and an unmemoized pipeline; assert every
/// result is identical and the memo's scratch stays bounded after each cell.
/// Returns the memo counts and retained specialization count.
fn differential_bounded(engine: &mut Engine<TestWorkbook>, cells: &Cells) -> (MemoCounts, usize) {
    let inputs: Vec<_> = cells
        .iter()
        .map(|(sheet, row, col, text)| {
            let ast_id = engine.intern_formula_ast(&parse(text).unwrap());
            let sheet_id = engine.graph.sheet_id_mut(sheet);
            (
                ast_id,
                CellRef::new(sheet_id, Coord::from_excel(*row, *col, true, true)),
            )
        })
        .collect();
    let mut memoized = Vec::new();
    let (counts, retained) = {
        let mut pipeline = engine.ingest_pipeline();
        for (i, &(ast_id, placement)) in inputs.iter().enumerate() {
            memoized.push(
                pipeline
                    .ingest_formula(FormulaAstInput::RawArena(ast_id), placement, None)
                    .unwrap(),
            );
            if i > 0 {
                scratch_is_bounded(&pipeline);
            }
        }
        let memo = pipeline.shape_memo_state().expect("memo allocated");
        (pipeline.memo_counts(), memo.retained_specializations())
    };
    let mut reference = engine.ingest_pipeline().without_shape_memo();
    for (memoized, &(ast_id, placement)) in memoized.iter().zip(&inputs) {
        let expected = reference
            .ingest_formula(FormulaAstInput::RawArena(ast_id), placement, None)
            .unwrap();
        if let Some(field) = ingested_formula_difference(memoized, &expected) {
            panic!("{placement:?}: memoized result differs in {field}");
        }
    }
    (counts, retained)
}

// FORM-000139: huge real ASTs (thousands of function arguments, a large flat
// array literal) stop the key walk at the bound instead of filling scratch,
// fall back to the per-cell path and leave scratch capacity bounded.
#[test]
fn huge_asts_stop_key_walk_and_keep_scratch_bounded() {
    let mut engine = engine();
    let many_refs = |r: u32| {
        let args: Vec<String> = (0..3_000).map(|i| format!("A{}", r + i % 50)).collect();
        format!("=SUM({})", args.join(","))
    };
    let many_literals = |r: u32| {
        let items: Vec<String> = (0..6_000).map(|i| i.to_string()).collect();
        format!("=SUM({{{}}})+A{r}", items.join(","))
    };
    let mut cells = family("Sheet1", 2, 1..=4, many_refs);
    cells.extend(family("Sheet1", 3, 1..=4, many_literals));
    // Ordinary families interleaved: the memo still works around them.
    for r in 1..=4u32 {
        cells.push(("Sheet1".into(), r, 4, format!("=A{r}*2")));
    }
    let (counts, _) = differential_bounded(&mut engine, &cells);
    assert_eq!(counts.bypasses, 8 + 1, "{counts:?}");
}

// FORM-000139: variable-length payloads (long text literals, sheet names,
// undefined names and function names) are charged by retained bytes; an
// oversized specialization is not retained, and results stay identical to the
// per-cell path.
#[test]
fn oversized_payloads_are_not_retained() {
    let mut large = engine();
    let big = "x".repeat(300_000);
    let long_sheet = format!("S{}", "q".repeat(200_000));
    let long_name = format!("N{}", "n".repeat(200_000));
    large.graph.sheet_id_mut(&long_sheet);
    // Defined names are length-limited, but references to undefined names
    // and unknown functions are not.
    let long_fn = format!("F{}", "f".repeat(200_000));
    let mut cells = family("Sheet1", 2, 1..=6, |r| format!("=\"{big}\"&A{r}"));
    cells.extend(family("Sheet1", 3, 1..=6, |r| {
        format!("='{long_sheet}'!A{r}+1")
    }));
    cells.extend(family("Sheet1", 4, 1..=6, |r| format!("={long_name}+A{r}")));
    cells.extend(family("Sheet1", 5, 1..=6, |r| format!("={long_fn}(A{r})")));
    let (counts, retained) = differential_bounded(&mut large, &cells);
    // Each family was derived (specialization miss) but not retained.
    assert_eq!(counts.specialization_misses, 4, "{counts:?}");
    assert_eq!(retained, 0, "{counts:?}");
    assert_eq!(counts.specialization_hits, 0, "{counts:?}");

    // Ordinary shapes are still retained and hit.
    let mut small = engine();
    let cells = family("Sheet1", 2, 1..=6, |r| format!("=\"short\"&A{r}"));
    let (counts, retained) = differential_bounded(&mut small, &cells);
    assert_eq!(retained, 1, "{counts:?}");
    assert!(counts.specialization_hits > 0, "{counts:?}");
}

/// Ingest `cells` through a memoized and an unmemoized pipeline, assert
/// every result is identical, and return the memo counts plus the memo's
/// cumulative key-walk count after each cell (from a run without concurrent
/// registrations).
fn differential_key_walks(
    engine: &mut Engine<TestWorkbook>,
    cells: &Cells,
) -> (MemoCounts, Vec<u64>) {
    let inputs: Vec<_> = cells
        .iter()
        .map(|(sheet, row, col, text)| {
            let ast_id = engine.intern_formula_ast(&parse(text).unwrap());
            let sheet_id = engine.graph.sheet_id_mut(sheet);
            (
                ast_id,
                CellRef::new(sheet_id, Coord::from_excel(*row, *col, true, true)),
            )
        })
        .collect();
    let mut memoized = Vec::new();
    let (counts, walks) = with_stable_registry(|| {
        memoized.clear();
        let mut pipeline = engine.ingest_pipeline();
        let mut walks = Vec::with_capacity(inputs.len());
        for &(ast_id, placement) in &inputs {
            memoized.push(
                pipeline
                    .ingest_formula(FormulaAstInput::RawArena(ast_id), placement, None)
                    .unwrap(),
            );
            walks.push(pipeline.shape_memo_state().map_or(0, |memo| memo.key_walks));
        }
        (pipeline.memo_counts(), walks)
    });
    let mut reference = engine.ingest_pipeline().without_shape_memo();
    for (memoized, &(ast_id, placement)) in memoized.iter().zip(&inputs) {
        let expected = reference
            .ingest_formula(FormulaAstInput::RawArena(ast_id), placement, None)
            .unwrap();
        if let Some(field) = ingested_formula_difference(memoized, &expected) {
            panic!("{placement:?}: memoized result differs in {field}");
        }
    }
    (counts, walks)
}

fn literal_distinct(col: u32, rows: std::ops::RangeInclusive<u32>) -> Cells {
    family("Sheet1", col, rows, |r| {
        format!("=VLOOKUP({r},$D$1:$E$100,2,FALSE)")
    })
}

// No-reuse cutoff: a literal-distinct family never hits, so after the warmup
// and the doubling windows only one formula in MAX_PROBE_STRIDE is walked.
#[test]
fn literal_distinct_family_backs_off_key_walks() {
    use crate::engine::shape_memo::{MAX_PROBE_STRIDE, PROBE_WARMUP};
    let mut engine = engine();
    let n = 4_096u32;
    let cells = literal_distinct(2, 1..=n);
    let (counts, walks) = differential_key_walks(&mut engine, &cells);
    assert_eq!(counts.specialization_hits, 0, "{counts:?}");
    let total = *walks.last().unwrap();
    // The warmup and the five halving windows before the cap walk
    // 64 + 32 + ... + 2 formulas; the first capped window adds one.
    let prefix = (PROBE_WARMUP * 7) as usize;
    assert!(walks[prefix] <= 2 * PROBE_WARMUP, "{}", walks[prefix]);
    let tail = total - walks[prefix];
    let tail_len = (walks.len() - 1 - prefix) as u64;
    assert!(
        tail <= tail_len / MAX_PROBE_STRIDE + 1,
        "{tail} of {tail_len}"
    );
    assert!(total < u64::from(n) / 16, "{total} of {n}");
    // Every skipped formula is a bypass.
    assert_eq!(
        counts.bypasses,
        u64::from(n) - counts.shape_misses,
        "{counts:?}"
    );
}

// A family that starts after a long literal-distinct prefix is still
// discovered: three probes (first sighting, traced miss, hit) at the capped
// stride, after which full probing resumes.
#[test]
fn late_family_after_literal_distinct_prefix_still_hits() {
    use crate::engine::shape_memo::MAX_PROBE_STRIDE;
    let mut engine = engine();
    let prefix = 4_096u32;
    let late = 1_024u32;
    let mut cells = literal_distinct(2, 1..=prefix);
    cells.extend(family("Sheet1", 3, 1..=late, |r| format!("=A{r}*2+1")));
    let (counts, walks) = differential_key_walks(&mut engine, &cells);
    let bound = 3 * MAX_PROBE_STRIDE;
    assert!(
        counts.specialization_hits >= u64::from(late) - bound,
        "{counts:?}"
    );
    // After discovery every formula of the family is walked again.
    let tail = (prefix + late - 1) as usize;
    let from = tail - (late as usize - bound as usize - 1);
    assert_eq!(
        walks[tail] - walks[from],
        (tail - from) as u64,
        "{counts:?}"
    );
}

// Interleaved families: a patterned family interleaved row by row with a
// literal-distinct one keeps hitting, so probing never backs off; and more
// round-robin families than the warmup covers are still found after the
// cutoff engages.
#[test]
fn interleaved_families_keep_hitting_under_cutoff() {
    let mut first = engine();
    let n = 1_024u32;
    let mut cells = Cells::new();
    for r in 1..=n {
        cells.push((
            "Sheet1".into(),
            r,
            2,
            format!("=VLOOKUP({r},$D$1:$E$100,2,FALSE)"),
        ));
        cells.push(("Sheet1".into(), r, 3, format!("=A{r}*2+1")));
    }
    let (counts, walks) = differential_key_walks(&mut first, &cells);
    assert!(counts.specialization_hits >= u64::from(n) - 3, "{counts:?}");
    // No formula after the first skipped the key walk.
    assert_eq!(*walks.last().unwrap(), cells.len() as u64 - 1, "{counts:?}");

    // 48 families round-robin need 2 * 48 attempts before the first hit,
    // more than the warmup: the cutoff engages and still finds them.
    let mut second = engine();
    let families = 48u32;
    let rows = 40u32;
    let mut cells = Cells::new();
    for r in 1..=rows {
        for f in 0..families {
            cells.push(("Sheet1".into(), r, 2 + f, format!("=A{r}*{f}+1")));
        }
    }
    let (counts, _) = differential_key_walks(&mut second, &cells);
    assert!(
        counts.specialization_hits >= u64::from(families * (rows - 3)),
        "{counts:?}"
    );
}
