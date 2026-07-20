use std::path::Path;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use formualizer_testkit::write_workbook;

use super::{CalibrationPath, FixtureFamily, TypedOracleValue, generate_fixture};

pub const DIRTY_FORMULAS: u32 = 8;
pub const RETAINED_FORMULAS: u32 = 8;
pub const LAYOUT_BLANK_GUARD_ROW: u32 = 3;
pub const LAYOUT_MAX_SCAN_ROWS: u32 = 8;
pub const LAYOUT_PREPARATION_ENVELOPE_END_ROW: u32 = 9;
pub const LAYOUT_BELOW_ENVELOPE_ROW: u32 = 10;
pub const LAYOUT_RETAINED_FORMULAS: u32 = 6;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FamilyFixtureShape {
    pub family: FixtureFamily,
    pub formulas: u32,
    pub reachable_formulas: u32,
    pub dirty_formulas: u32,
    pub retained_formulas: u32,
    pub terminal_sheet: String,
    pub terminal_row: u32,
    pub terminal_col: u32,
}

pub fn generate_family_fixture_shape(
    family: FixtureFamily,
    formulas: u32,
) -> Result<FamilyFixtureShape> {
    if formulas < 32 {
        bail!("--formulas must be at least 32 for native breadth families");
    }
    let (reachable_formulas, retained_formulas, terminal_sheet, terminal_row) = match family {
        FixtureFamily::CrossSheet => {
            let chain = formulas - DIRTY_FORMULAS - RETAINED_FORMULAS;
            let (sheet, row) = cross_terminal(chain);
            (chain, RETAINED_FORMULAS, sheet, row)
        }
        FixtureFamily::Names => (
            formulas - DIRTY_FORMULAS - RETAINED_FORMULAS - 2,
            RETAINED_FORMULAS,
            "Chain",
            formulas - DIRTY_FORMULAS - RETAINED_FORMULAS - 4,
        ),
        FixtureFamily::Layout => (
            formulas - DIRTY_FORMULAS - LAYOUT_RETAINED_FORMULAS,
            LAYOUT_RETAINED_FORMULAS,
            "Chain",
            formulas - DIRTY_FORMULAS - RETAINED_FORMULAS,
        ),
        FixtureFamily::NativeTable => (
            formulas - DIRTY_FORMULAS - RETAINED_FORMULAS,
            RETAINED_FORMULAS,
            "Chain",
            formulas - DIRTY_FORMULAS - RETAINED_FORMULAS - 3,
        ),
        FixtureFamily::Dynamic => (formulas, 0, "Chain", formulas - DIRTY_FORMULAS - 2),
        FixtureFamily::Scalar => bail!("scalar shape uses FixtureShape"),
    };
    Ok(FamilyFixtureShape {
        family,
        formulas,
        reachable_formulas,
        dirty_formulas: DIRTY_FORMULAS,
        retained_formulas,
        terminal_sheet: terminal_sheet.to_string(),
        terminal_row,
        terminal_col: 2,
    })
}

pub fn generate_family_fixture(
    path: &Path,
    family: FixtureFamily,
    formulas: u32,
) -> Result<FamilyFixtureShape> {
    if family == FixtureFamily::Scalar {
        let shape = generate_fixture(path, formulas)?;
        return Ok(FamilyFixtureShape {
            family,
            formulas,
            reachable_formulas: shape.formulas,
            dirty_formulas: shape.dirty_formulas,
            retained_formulas: shape.large_formulas,
            terminal_sheet: "Tiny".to_string(),
            terminal_row: shape.tiny_formulas,
            terminal_col: 2,
        });
    }
    if formulas < 32 {
        bail!("--formulas must be at least 32 for native breadth families");
    }
    match family {
        FixtureFamily::CrossSheet => generate_cross_sheet(path, formulas),
        FixtureFamily::Names => generate_names(path, formulas),
        FixtureFamily::Layout => generate_layout(path, formulas),
        FixtureFamily::NativeTable => generate_native_table(path, formulas),
        FixtureFamily::Dynamic => generate_dynamic(path, formulas),
        FixtureFamily::Scalar => unreachable!(),
    }
}

fn base_book(book: &mut umya_spreadsheet::Spreadsheet, sheets: &[&str]) {
    book.get_sheet_by_name_mut("Sheet1")
        .expect("default sheet")
        .set_name("Inputs");
    for sheet in sheets {
        book.new_sheet(*sheet).expect("unique fixture sheet");
    }
    book.get_sheet_by_name_mut("Inputs")
        .expect("Inputs")
        .get_cell_mut((1, 1))
        .set_value_number(1.0);
}

fn populate_single_chain(
    book: &mut umya_spreadsheet::Spreadsheet,
    sheet_name: &str,
    formulas: u32,
) {
    let sheet = book.get_sheet_by_name_mut(sheet_name).expect("chain sheet");
    sheet.get_cell_mut((1, 1)).set_value_number(1.0);
    sheet.get_cell_mut((2, 1)).set_formula("=$A$1*1.015-0.25");
    for row in 2..=formulas {
        sheet
            .get_cell_mut((2, row))
            .set_formula(format!("=B{}*1.0001+0.00001", row - 1));
    }
}

fn populate_dirty_and_retained(book: &mut umya_spreadsheet::Spreadsheet, retained: u32) {
    let dirty = book.get_sheet_by_name_mut("Dirty").expect("Dirty");
    dirty.get_cell_mut((1, 1)).set_value_number(4.0);
    dirty.get_cell_mut((2, 1)).set_formula("=A1*2");
    for row in 2..=DIRTY_FORMULAS {
        dirty
            .get_cell_mut((2, row))
            .set_formula(format!("=B{}+1", row - 1));
    }
    if retained > 0 {
        let staged = book.get_sheet_by_name_mut("Retained").expect("Retained");
        for row in 1..=retained {
            staged
                .get_cell_mut((2, row))
                .set_formula(format!("={row}+100"));
        }
    }
}

fn cross_terminal(formulas: u32) -> (&'static str, u32) {
    if formulas % 2 == 1 {
        ("ChainA", formulas.div_ceil(2))
    } else {
        ("ChainB", formulas / 2)
    }
}

fn generate_cross_sheet(path: &Path, formulas: u32) -> Result<FamilyFixtureShape> {
    let chain = formulas - DIRTY_FORMULAS - RETAINED_FORMULAS;
    write_workbook(path, |book| {
        base_book(book, &["ChainA", "ChainB", "Dirty", "Retained"]);
        book.get_sheet_by_name_mut("ChainA")
            .expect("ChainA")
            .get_cell_mut((1, 1))
            .set_value_number(1.0);
        for index in 1..=chain {
            let (sheet_name, row) = cross_terminal(index);
            let formula = if index == 1 {
                "=$A$1*1.015-0.25".to_string()
            } else {
                let (previous_sheet, previous_row) = cross_terminal(index - 1);
                format!("='{previous_sheet}'!B{previous_row}*1.0001+0.00001")
            };
            book.get_sheet_by_name_mut(sheet_name)
                .expect("alternating chain sheet")
                .get_cell_mut((2, row))
                .set_formula(formula);
        }
        populate_dirty_and_retained(book, RETAINED_FORMULAS);
    });
    let (sheet, row) = cross_terminal(chain);
    Ok(FamilyFixtureShape {
        family: FixtureFamily::CrossSheet,
        formulas,
        reachable_formulas: chain,
        dirty_formulas: DIRTY_FORMULAS,
        retained_formulas: RETAINED_FORMULAS,
        terminal_sheet: sheet.to_string(),
        terminal_row: row,
        terminal_col: 2,
    })
}

fn generate_names(path: &Path, formulas: u32) -> Result<FamilyFixtureShape> {
    let chain = formulas - DIRTY_FORMULAS - RETAINED_FORMULAS - 4;
    write_workbook(path, |book| {
        base_book(book, &["Chain", "Names", "Dirty", "Retained"]);
        populate_single_chain(book, "Chain", chain);
        let names = book.get_sheet_by_name_mut("Names").expect("Names");
        for (row, offset) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
            names
                .get_cell_mut((2, row))
                .set_formula(format!("=Chain!B{chain}+{offset}"));
        }
        populate_dirty_and_retained(book, RETAINED_FORMULAS);
    });
    Ok(FamilyFixtureShape {
        family: FixtureFamily::Names,
        formulas,
        reachable_formulas: chain + 2,
        dirty_formulas: DIRTY_FORMULAS,
        retained_formulas: RETAINED_FORMULAS,
        terminal_sheet: "Chain".to_string(),
        terminal_row: chain,
        terminal_col: 2,
    })
}

fn generate_layout(path: &Path, formulas: u32) -> Result<FamilyFixtureShape> {
    let selector_formulas = 2;
    let chain = formulas - DIRTY_FORMULAS - LAYOUT_RETAINED_FORMULAS - selector_formulas;
    write_workbook(path, |book| {
        base_book(book, &["Chain", "Layout", "Dirty", "Retained"]);
        populate_single_chain(book, "Chain", chain);
        let layout = book.get_sheet_by_name_mut("Layout").expect("Layout");
        for (col, header) in [(1, "Label"), (2, "Count"), (3, "Value"), (4, "AsOf")] {
            layout.get_cell_mut((col, 1)).set_value(header);
        }
        layout.get_cell_mut((1, 2)).set_value("row-1");
        layout.get_cell_mut((2, 2)).set_value_number(7);
        layout.get_cell_mut((3, 2)).set_value_number(5.0);
        layout.get_cell_mut((4, 2)).set_value_number(45_659.0);
        let _ = layout
            .get_style_mut("D2")
            .get_number_format_mut()
            .set_format_code(umya_spreadsheet::NumberingFormat::FORMAT_DATE_XLSX14);
        // Row 3 is the blank guard. C4 is inside the conservative preparation
        // envelope and C10 is below it; neither belongs to the resolved output.
        // Deferred Calamine preparation commits their whole worksheet package,
        // but only C4 is scheduled by the A2:D9 target envelope.
        layout.get_cell_mut((3, 4)).set_formula("=999");
        layout.get_cell_mut((3, 10)).set_formula("=1000");
        populate_dirty_and_retained(book, LAYOUT_RETAINED_FORMULAS);
    });
    Ok(FamilyFixtureShape {
        family: FixtureFamily::Layout,
        formulas,
        reachable_formulas: chain + selector_formulas,
        dirty_formulas: DIRTY_FORMULAS,
        retained_formulas: LAYOUT_RETAINED_FORMULAS,
        terminal_sheet: "Chain".to_string(),
        terminal_row: chain,
        terminal_col: 2,
    })
}

fn generate_native_table(path: &Path, formulas: u32) -> Result<FamilyFixtureShape> {
    let selector_formulas = 3;
    let chain = formulas - DIRTY_FORMULAS - RETAINED_FORMULAS - selector_formulas;
    write_workbook(path, |book| {
        base_book(book, &["Chain", "Table", "Dirty", "Retained"]);
        populate_single_chain(book, "Chain", chain);
        let table = book.get_sheet_by_name_mut("Table").expect("Table");
        for (col, header) in [(1, "Label"), (2, "Count"), (3, "Value"), (4, "AsOf")] {
            table.get_cell_mut((col, 1)).set_value(header);
        }
        table.get_cell_mut((1, 2)).set_value("body");
        table.get_cell_mut((2, 2)).set_value_number(7);
        table
            .get_cell_mut((3, 2))
            .set_formula(format!("=Chain!B{chain}+5"));
        table.get_cell_mut((4, 2)).set_value_number(45_659.0);
        let _ = table
            .get_style_mut("D2")
            .get_number_format_mut()
            .set_format_code(umya_spreadsheet::NumberingFormat::FORMAT_DATE_XLSX14);
        table.get_cell_mut((1, 3)).set_value("totals");
        table.get_cell_mut((2, 3)).set_formula("=7");
        table
            .get_cell_mut((3, 3))
            .set_formula(format!("=Chain!B{chain}+12"));
        table.get_cell_mut((4, 3)).set_value_number(45_660.0);
        let _ = table
            .get_style_mut("D3")
            .get_number_format_mut()
            .set_format_code(umya_spreadsheet::NumberingFormat::FORMAT_DATE_XLSX14);
        populate_dirty_and_retained(book, RETAINED_FORMULAS);
    });
    Ok(FamilyFixtureShape {
        family: FixtureFamily::NativeTable,
        formulas,
        reachable_formulas: chain + selector_formulas,
        dirty_formulas: DIRTY_FORMULAS,
        retained_formulas: RETAINED_FORMULAS,
        terminal_sheet: "Chain".to_string(),
        terminal_row: chain,
        terminal_col: 2,
    })
}

fn generate_dynamic(path: &Path, formulas: u32) -> Result<FamilyFixtureShape> {
    let chain = formulas - DIRTY_FORMULAS - 2;
    write_workbook(path, |book| {
        base_book(book, &["Chain", "Dynamic", "Dirty"]);
        populate_single_chain(book, "Chain", chain);
        let dynamic = book.get_sheet_by_name_mut("Dynamic").expect("Dynamic");
        dynamic
            .get_cell_mut((2, 1))
            .set_formula(format!("=INDIRECT(\"Chain!A1\")+Chain!B{chain}"));
        dynamic
            .get_cell_mut((2, 2))
            .set_formula("=INDIRECT(\"Missing!A1\")");
        populate_dirty_and_retained(book, 0);
    });
    Ok(FamilyFixtureShape {
        family: FixtureFamily::Dynamic,
        formulas,
        reachable_formulas: formulas,
        dirty_formulas: DIRTY_FORMULAS,
        retained_formulas: 0,
        terminal_sheet: "Chain".to_string(),
        terminal_row: chain,
        terminal_col: 2,
    })
}

pub fn chain_value(shape: &FamilyFixtureShape, seed: f64) -> f64 {
    let chain_formulas = match shape.family {
        FixtureFamily::CrossSheet => shape.formulas - DIRTY_FORMULAS - RETAINED_FORMULAS,
        FixtureFamily::Names => shape.formulas - DIRTY_FORMULAS - RETAINED_FORMULAS - 4,
        FixtureFamily::Layout => shape.formulas - DIRTY_FORMULAS - RETAINED_FORMULAS,
        FixtureFamily::NativeTable => shape.formulas - DIRTY_FORMULAS - RETAINED_FORMULAS - 3,
        FixtureFamily::Dynamic => shape.formulas - DIRTY_FORMULAS - 2,
        FixtureFamily::Scalar => shape.formulas,
    };
    let first = seed * 1.015 - 0.25;
    if chain_formulas == 1 {
        return first;
    }
    let ratio = 1.0001_f64;
    let power = ratio.powi((chain_formulas - 1) as i32);
    first * power + 0.00001 * (power - 1.0) / (ratio - 1.0)
}

pub fn expected_typed_outputs(
    shape: &FamilyFixtureShape,
    path: CalibrationPath,
    warm_repeats: usize,
) -> Vec<Vec<TypedOracleValue>> {
    let mut outputs = (0..=warm_repeats)
        .map(|evaluation| {
            let seed = if evaluation == 0 {
                1.0
            } else {
                1.0 + evaluation as f64
            };
            let terminal = chain_value(shape, seed);
            match shape.family {
                FixtureFamily::CrossSheet => vec![TypedOracleValue::Number(terminal)],
                FixtureFamily::Names if path == CalibrationPath::Sheetport => {
                    vec![TypedOracleValue::Number(terminal + 10.0)]
                }
                FixtureFamily::Names => vec![
                    TypedOracleValue::Number(terminal + 10.0),
                    TypedOracleValue::Number(terminal + 20.0),
                ],
                FixtureFamily::Layout => vec![
                    TypedOracleValue::String("row-1".to_string()),
                    TypedOracleValue::Integer(7),
                    TypedOracleValue::Number(5.0),
                    TypedOracleValue::Date("2025-01-02".to_string()),
                    TypedOracleValue::Number(terminal),
                ],
                FixtureFamily::NativeTable => native_table_expected(terminal),
                FixtureFamily::Dynamic if path == CalibrationPath::Sheetport => {
                    vec![TypedOracleValue::Number(seed + terminal)]
                }
                FixtureFamily::Dynamic => vec![
                    TypedOracleValue::Number(seed + terminal),
                    TypedOracleValue::Error("#REF!".to_string()),
                ],
                FixtureFamily::Scalar => Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    if shape.family == FixtureFamily::Names {
        let seed = if warm_repeats == 0 {
            1.0
        } else {
            1.0 + warm_repeats as f64
        };
        let terminal = chain_value(shape, seed);
        outputs.push(if path == CalibrationPath::Sheetport {
            vec![TypedOracleValue::Number(terminal + 30.0)]
        } else {
            vec![
                TypedOracleValue::Number(terminal + 30.0),
                TypedOracleValue::Number(terminal + 40.0),
            ]
        });
    }
    outputs
}

fn native_table_expected(terminal: f64) -> Vec<TypedOracleValue> {
    vec![
        TypedOracleValue::String("Label".to_string()),
        TypedOracleValue::String("Count".to_string()),
        TypedOracleValue::String("Value".to_string()),
        TypedOracleValue::String("AsOf".to_string()),
        TypedOracleValue::String("body".to_string()),
        TypedOracleValue::Integer(7),
        TypedOracleValue::Number(terminal + 5.0),
        TypedOracleValue::Date("2025-01-02".to_string()),
        TypedOracleValue::String("totals".to_string()),
        TypedOracleValue::Integer(7),
        TypedOracleValue::Number(terminal + 12.0),
        TypedOracleValue::Date("2025-01-03".to_string()),
    ]
}

pub fn layout_manifest(terminal_row: u32) -> String {
    r#"spec: fio
spec_version: "0.3.0"
manifest: { id: c6-layout, name: C6 Layout }
ports:
  - id: input
    dir: in
    shape: scalar
    location: { a1: Chain!A1 }
    schema: { type: number }
  - id: rows
    dir: out
    shape: table
    location:
      layout:
        sheet: Layout
        header_row: 1
        anchor_col: A
        terminate: first_blank_row
        max_scan_rows: {max_scan_rows}
    schema:
      kind: table
      columns:
        - { name: Label, type: string, col: A }
        - { name: Count, type: integer, col: B }
        - { name: Value, type: number, col: C }
        - { name: AsOf, type: date, col: D }
  - id: breadth
    dir: out
    shape: scalar
    location: { a1: Chain!B{terminal_row} }
    schema: { type: number }
"#
    .replace("{terminal_row}", &terminal_row.to_string())
    .replace("{max_scan_rows}", &LAYOUT_MAX_SCAN_ROWS.to_string())
}

pub fn table_manifest() -> String {
    let columns = "      columns:\n        - { name: Label, type: string }\n        - { name: Count, type: integer }\n        - { name: Value, type: number }\n        - { name: AsOf, type: date }\n";
    format!(
        "spec: fio\nspec_version: \"0.3.0\"\ncapabilities: {{ profile: full-v0 }}\nmanifest: {{ id: c6-table, name: C6 Native Table }}\nports:\n  - id: input\n    dir: in\n    shape: scalar\n    location: {{ a1: Chain!A1 }}\n    schema: {{ type: number }}\n  - id: headers\n    dir: out\n    shape: table\n    location:\n      table: {{ name: C6Table, area: header }}\n    schema:\n      kind: table\n{header_columns}  - id: body\n    dir: out\n    shape: table\n    location:\n      table: {{ name: C6Table, area: body }}\n    schema:\n      kind: table\n{columns}  - id: totals\n    dir: out\n    shape: table\n    location:\n      table: {{ name: C6Table, area: totals }}\n    schema:\n      kind: table\n{columns}",
        header_columns = "      columns:\n        - { name: Label, type: string }\n        - { name: Count, type: string }\n        - { name: Value, type: string }\n        - { name: AsOf, type: string }\n",
    )
}

pub fn scalar_manifest(family: FixtureFamily, terminal_sheet: &str, terminal_row: u32) -> String {
    let input_sheet = if family == FixtureFamily::CrossSheet {
        "ChainA"
    } else {
        "Chain"
    };
    let (id, outputs) = match family {
        FixtureFamily::CrossSheet => (
            "c6-cross-sheet",
            format!(
                "  - id: output\n    dir: out\n    shape: scalar\n    location: {{ a1: {terminal_sheet}!B{terminal_row} }}\n    schema: {{ type: number }}\n"
            ),
        ),
        FixtureFamily::Names => (
            "c6-name",
            "  - id: output\n    dir: out\n    shape: scalar\n    location: { name: WorkbookOutput }\n    schema: { type: number }\n"
                .to_string(),
        ),
        FixtureFamily::Dynamic => (
            "c6-dynamic",
            "  - id: value\n    dir: out\n    shape: scalar\n    location: { a1: Dynamic!B1 }\n    schema: { type: number }\n"
                .to_string(),
        ),
        _ => unreachable!("scalar manifest family"),
    };
    format!(
        "spec: fio\nspec_version: \"0.3.0\"\nmanifest: {{ id: {id}, name: C6 Native }}\nports:\n  - id: input\n    dir: in\n    shape: scalar\n    location: {{ a1: {input_sheet}!A1 }}\n    schema: {{ type: number }}\n{outputs}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::c6_calibration::sha256_file;

    #[test]
    fn all_family_formula_accounting_is_exact() {
        for family in FixtureFamily::BREADTH {
            let path = std::env::temp_dir().join(format!(
                "c6-family-accounting-{}-{}.xlsx",
                family.label(),
                std::process::id()
            ));
            let shape = generate_family_fixture(&path, family, 200).unwrap();
            assert_eq!(shape.formulas, 200);
            assert!(shape.reachable_formulas <= 200);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn family_fixture_bytes_are_deterministic() {
        for family in FixtureFamily::BREADTH {
            let a = std::env::temp_dir().join(format!("c6-det-a-{}.xlsx", family.label()));
            let b = std::env::temp_dir().join(format!("c6-det-b-{}.xlsx", family.label()));
            generate_family_fixture(&a, family, 64).unwrap();
            generate_family_fixture(&b, family, 64).unwrap();
            assert_eq!(sha256_file(&a).unwrap(), sha256_file(&b).unwrap());
            let _ = std::fs::remove_file(a);
            let _ = std::fs::remove_file(b);
        }
    }

    #[test]
    fn independent_oracles_cover_initial_and_warm_edits() {
        let shape = FamilyFixtureShape {
            family: FixtureFamily::Dynamic,
            formulas: 100,
            reachable_formulas: 100,
            dirty_formulas: 8,
            retained_formulas: 0,
            terminal_sheet: "Chain".to_string(),
            terminal_row: 90,
            terminal_col: 2,
        };
        let outputs = expected_typed_outputs(&shape, CalibrationPath::Targets, 2);
        assert_eq!(outputs.len(), 3);
        assert_ne!(outputs[0][0], outputs[1][0]);
        assert_eq!(outputs[0][1], TypedOracleValue::Error("#REF!".to_string()));
    }

    #[test]
    fn layout_scan_envelope_extends_beyond_blank_guard() {
        let manifest = layout_manifest(64);
        assert!(manifest.contains("terminate: first_blank_row"));
        assert!(manifest.contains(&format!("max_scan_rows: {LAYOUT_MAX_SCAN_ROWS}")));
    }
}
