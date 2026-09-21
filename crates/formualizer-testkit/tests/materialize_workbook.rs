#![cfg(feature = "workbook")]
use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_testkit::{
    materialize::{Artifact, Materialize, WorkbookApi, WorkbookRoute, Xlsx},
    patch_part,
    shape::{Extent, Range, Scale, Shape, r#gen},
};
use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

fn independent(rows: u32) -> Shape {
    Shape::new().scale(Scale::rows(rows)).sheet("S", |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "independent",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=A{r}*2+1",
        );
    })
}
fn coupled(rows: u32) -> Shape {
    Shape::new().scale(Scale::rows(rows)).sheet("S", |s| {
        s.values(
            Range::col("A", Extent::Abs(1)..=Extent::Rows),
            r#gen::index_f64(),
        );
        s.family(
            "recur",
            Range::col("B", Extent::Abs(1)..=Extent::Rows),
            "=C{r-1}+A{r}",
        )
        .boundary("B1", "=A1");
        s.family(
            "half",
            Range::col("C", Extent::Abs(1)..=Extent::Rows),
            "=B{r}/2",
        );
    })
}
fn values(mut workbook: Workbook, rows: u32, coupled: bool) -> Vec<f64> {
    workbook.evaluate_all().unwrap();
    let mut out = Vec::new();
    for row in 1..=rows {
        for col in if coupled { 1..=3 } else { 1..=2 } {
            match workbook.get_value("S", row, col).unwrap() {
                LiteralValue::Number(n) => out.push(n),
                other => panic!("unexpected {other:?}"),
            }
        }
    }
    out
}
fn oracle(rows: u32, is_coupled: bool) -> Vec<f64> {
    let mut out = Vec::new();
    let mut prior_c = 0.0;
    for row in 1..=rows {
        let a = row as f64;
        if is_coupled {
            let b = if row == 1 { a } else { prior_c + a };
            let c = b / 2.0;
            out.extend([a, b, c]);
            prior_c = c;
        } else {
            out.extend([a, a * 2.0 + 1.0]);
        }
    }
    out
}
#[test]
fn scale_relative_ranges_resize_the_same_shape() {
    let shape = independent(16);
    assert_eq!(shape.render().unwrap().len(), 32);
    let shape = shape.scale(Scale::rows(4096));
    assert_eq!(shape.render().unwrap().len(), 8192);
}
#[test]
fn all_routes_match_oracles_in_both_formula_plane_modes() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        for (shape, coupled) in [(coupled(256), true), (independent(256), false)] {
            for route in [
                WorkbookRoute::SetValuesSetFormulas,
                WorkbookRoute::WriteRange,
                WorkbookRoute::PerCell,
            ] {
                let mut config = WorkbookConfig::ephemeral().with_formula_plane_mode(mode);
                config.eval.enable_parallel = false;
                let Artifact::Workbook(workbook) = WorkbookApi::new(config)
                    .route(route)
                    .materialize(&shape)
                    .unwrap()
                else {
                    unreachable!()
                };
                assert_eq!(
                    values(workbook, 256, coupled),
                    oracle(256, coupled),
                    "{mode:?} {route:?}"
                );
            }
        }
    }
}
#[test]
fn xlsx_calamine_roundtrip_and_patch_evaluate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shape.xlsx");
    let shape = independent(16);
    let Artifact::Xlsx(path) = Xlsx::new(&path).materialize(&shape).unwrap() else {
        unreachable!()
    };
    let load = |path: &std::path::Path| {
        let backend = CalamineAdapter::open_path(path).unwrap();
        Workbook::from_reader(backend, LoadStrategy::EagerAll, WorkbookConfig::ephemeral()).unwrap()
    };
    assert_eq!(values(load(&path), 16, false), oracle(16, false));
    patch_part(&path, "xl/worksheets/sheet1.xml", |xml| {
        format!("<!-- P2a -->{xml}")
    })
    .unwrap();
    assert_eq!(values(load(&path), 16, false), oracle(16, false));
}
