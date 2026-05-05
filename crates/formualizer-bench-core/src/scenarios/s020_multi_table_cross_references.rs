use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const SALES_ROWS: u32 = 1_000;
const PRODUCT_ROWS: u32 = 100;

pub struct S020MultiTableCrossReferences {
    scale: ScaleState,
}

impl Default for S020MultiTableCrossReferences {
    fn default() -> Self {
        Self::new()
    }
}

impl S020MultiTableCrossReferences {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S020MultiTableCrossReferences {
    fn id(&self) -> &'static str {
        "s020-multi-table-cross-references"
    }

    fn description(&self) -> &'static str {
        "Sales and Products native tables consumed by cross-sheet structured-reference formulas."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::StructuredRefs,
            ScenarioTag::CrossSheet,
            ScenarioTag::SingleCellEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            book.new_sheet("Sales").expect("Sales sheet");
            book.new_sheet("Products").expect("Products sheet");

            let sales = book.get_sheet_by_name_mut("Sales").expect("Sales exists");
            sales.get_cell_mut((1, 1)).set_value("SaleID");
            sales.get_cell_mut((2, 1)).set_value("ProductID");
            sales.get_cell_mut((3, 1)).set_value("Amount");
            for r in 2..=SALES_ROWS + 1 {
                let sale_id = r - 1;
                sales.get_cell_mut((1, r)).set_value_number(sale_id as f64);
                sales
                    .get_cell_mut((2, r))
                    .set_value_number(product_id_for_sale(sale_id) as f64);
                sales
                    .get_cell_mut((3, r))
                    .set_value_number(initial_amount_for_sale(sale_id));
            }
            let mut sales_table =
                umya_spreadsheet::structs::Table::new("SalesTable", ("A1", "C1001"));
            sales_table.add_column(umya_spreadsheet::structs::TableColumn::new("SaleID"));
            sales_table.add_column(umya_spreadsheet::structs::TableColumn::new("ProductID"));
            sales_table.add_column(umya_spreadsheet::structs::TableColumn::new("Amount"));
            sales.add_table(sales_table);

            let products = book
                .get_sheet_by_name_mut("Products")
                .expect("Products exists");
            products.get_cell_mut((1, 1)).set_value("ProductID");
            products.get_cell_mut((2, 1)).set_value("ProductName");
            products.get_cell_mut((3, 1)).set_value("Category");
            for r in 2..=PRODUCT_ROWS + 1 {
                let product_id = r - 1;
                products
                    .get_cell_mut((1, r))
                    .set_value_number(product_id as f64);
                products
                    .get_cell_mut((2, r))
                    .set_value(format!("Product{product_id}"));
                products
                    .get_cell_mut((3, r))
                    .set_value(format!("Category{}", product_id % 5));
            }
            let mut products_table =
                umya_spreadsheet::structs::Table::new("ProductsTable", ("A1", "C101"));
            products_table.add_column(umya_spreadsheet::structs::TableColumn::new("ProductID"));
            products_table.add_column(umya_spreadsheet::structs::TableColumn::new("ProductName"));
            products_table.add_column(umya_spreadsheet::structs::TableColumn::new("Category"));
            products.add_table(products_table);

            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for product_id in 1..=PRODUCT_ROWS {
                sheet.get_cell_mut((1, product_id)).set_formula(format!(
                    "=SUMIFS(SalesTable[Amount], SalesTable[ProductID], {product_id}) \
                     + COUNTIF(ProductsTable[ProductID], {product_id}) * 0"
                ));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: SALES_ROWS + 1,
                cols: 3,
                sheets: 3,
                formula_cells: PRODUCT_ROWS,
                value_cells: 3 + SALES_ROWS.saturating_mul(3) + 3 + PRODUCT_ROWS.saturating_mul(3),
                has_named_ranges: false,
                has_tables: true,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 5,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = Vec::with_capacity(12);
        for sheet in ["Sales", "Products", "Sheet1"] {
            invariants.push(ScenarioInvariant::NoErrorCells {
                sheet: sheet.to_string(),
            });
        }
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in [1, 50, 100] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 1,
                    expected: numeric(sum_for_product(row, cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = ((cycle * 37) % SALES_ROWS as usize) as u32 + 2;
    wb.set_value("Sales", row, 3, LiteralValue::Number(edited_amount(cycle)))?;
    Ok("sales_amount")
}

fn product_id_for_sale(sale_id: u32) -> u32 {
    (sale_id - 1) % PRODUCT_ROWS + 1
}

fn initial_amount_for_sale(sale_id: u32) -> f64 {
    sale_id as f64 * 10.0
}

fn edited_amount(cycle: usize) -> f64 {
    10_000.0 + cycle as f64
}

fn edited_sale_id(cycle: usize) -> u32 {
    ((cycle * 37) % SALES_ROWS as usize) as u32 + 1
}

fn amount_for_sale(sale_id: u32, completed_cycles: usize) -> f64 {
    let mut amount = initial_amount_for_sale(sale_id);
    for cycle in 0..completed_cycles {
        if sale_id == edited_sale_id(cycle) {
            amount = edited_amount(cycle);
        }
    }
    amount
}

fn sum_for_product(product_id: u32, completed_cycles: usize) -> f64 {
    (1..=SALES_ROWS)
        .filter(|sale_id| product_id_for_sale(*sale_id) == product_id)
        .map(|sale_id| amount_for_sale(sale_id, completed_cycles))
        .sum()
}
