from formualizer import SheetPortSession, Workbook
from formualizer.sheetport import Input, Manifest, Output, column, layout, table_of


def main():
    wb = Workbook()
    wb.add_sheet("Orders")
    wb.set_value("Orders", 1, 1, "sku")
    wb.set_value("Orders", 1, 2, "quantity")
    selector = layout(
        sheet="Orders", header_row=1, anchor_col="A", terminate="first_blank_row"
    )
    schema = table_of(
        columns=[column("sku", "string"), column("quantity", "integer")], keys=["sku"]
    )
    manifest = Manifest(
        "orders",
        ports=[
            Input("orders", selector, schema=schema),
            Output("result", selector, schema=schema),
        ],
    )
    session = SheetPortSession(wb, manifest)
    rows = [{"sku": "ABC", "quantity": 3}, {"sku": "DEF", "quantity": 7}]
    session.write_inputs({"orders": rows})
    assert session.evaluate_once()["result"] == rows


if __name__ == "__main__":
    main()
