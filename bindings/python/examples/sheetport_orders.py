"""SheetPort table example shown in the Python README; run by the test suite."""

# region readme
import formualizer as fz
from formualizer.sheetport import Input, Manifest, Output, column, layout, table_of

wb = fz.Workbook()
wb.add_sheet("Orders")
for col, header in enumerate(["sku", "quantity", "unit_price"], start=1):
    wb.set_value("Orders", 1, col, header)
wb.set_formula("Orders", 1, 6, "=SUMPRODUCT(B2:B1000, C2:C1000)")  # F1: order total

manifest = Manifest(
    "order-total",
    ports=[
        Input(
            "lines",
            # Rows below the header in A1, until the first blank row.
            layout(
                sheet="Orders",
                header_row=1,
                anchor_col="A",
                terminate="first_blank_row",
            ),
            schema=table_of(
                columns=[
                    column("sku", "string"),
                    column("quantity", "integer"),
                    column("unit_price", "number"),
                ],
                keys=["sku"],
            ),
        ),
        Output("total", "Orders!F1", type="number"),
    ],
)

session = fz.SheetPortSession(wb, manifest)
session.write_inputs(
    {
        "lines": [
            {"sku": "A-100", "quantity": 3, "unit_price": 12.5},
            {"sku": "B-200", "quantity": 1, "unit_price": 40.0},
        ]
    }
)
print(session.evaluate_once()["total"])  # 77.5
# endregion

assert session.evaluate_once()["total"] == 77.5
session.write_inputs({"lines": [{"sku": "C-300", "quantity": 2, "unit_price": 5.0}]})
assert session.evaluate_once()["total"] == 10.0, "shorter table must clear old rows"
