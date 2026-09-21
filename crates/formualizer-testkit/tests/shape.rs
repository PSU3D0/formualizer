use formualizer_testkit::shape::{Cell, Range, Scale, Shape, render_template};

#[test]
fn templates_handle_boundaries_and_columns() {
    let scale = Scale::new(10, 3);
    assert_eq!(
        render_template("=A{r}+{c+1}{r-1}+{n}+{m}+{end}", 2, 26, scale, 10).unwrap(),
        "=A2+AA1+10+3+10"
    );
    assert!(render_template("=A{r-1}", 1, 1, scale, 1).is_err());
    assert!(render_template("={c-1}1", 1, 1, scale, 1).is_err());
}

#[test]
fn family_combinators_are_deterministic_at_scale() {
    let shape = Shape::new().scale(Scale::rows(4096)).sheet("S", |s| {
        s.family("f", Range::col("B", 1..=4096), "=A{r}")
            .gap_every(128)
            .blocks(256, 1)
            .except(Range::cells([(1, 2)]))
            .override_cell("B2", Cell::formula("=42"));
    });
    let cells = shape.render().unwrap();
    assert!(cells.iter().any(|c| c.row == 2));
    assert!(!cells.iter().any(|c| c.row == 1));
    assert!(cells.iter().all(|c| c.col == 2));
}

#[test]
fn row_and_column_ranges_render_transposed_templates() {
    let col = Shape::new()
        .sheet("S", |s| {
            s.family("x", Range::col("B", 1..=2), "=A{r}");
        })
        .render()
        .unwrap();
    let row = Shape::new()
        .sheet("S", |s| {
            s.family("x", Range::row(2, 1..=2), "={c}1");
        })
        .render()
        .unwrap();
    assert_eq!(
        col.iter()
            .map(|c| format!("{:?}", c.cell))
            .collect::<Vec<_>>(),
        vec!["Formula(\"=A1\")", "Formula(\"=A2\")"]
    );
    assert_eq!(
        row.iter()
            .map(|c| format!("{:?}", c.cell))
            .collect::<Vec<_>>(),
        vec!["Formula(\"=A1\")", "Formula(\"=B1\")"]
    );
}
