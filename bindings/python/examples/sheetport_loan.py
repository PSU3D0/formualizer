from formualizer import SheetPortSession, Workbook
from formualizer.sheetport import Input, Manifest, Output


def main():
    wb = Workbook()
    wb.add_sheet("Calculator")
    wb.set_formula("Calculator", 4, 2, "=-PMT(B2/12,360,B1)")
    manifest = Manifest(
        "loan-calculator",
        ports=[
            Input("principal", "Calculator!B1", type="number", constraints={"min": 0}),
            Input("rate", "Calculator!B2", type="number"),
            Output("payment", "Calculator!B4", type="number"),
        ],
    )
    assert (
        Manifest.from_yaml(manifest.to_yaml()).to_dict()["ports"][0]["id"]
        == "principal"
    )
    for session in (
        SheetPortSession(wb, manifest),
        SheetPortSession.from_manifest_yaml(manifest.to_yaml(), wb),
    ):
        session.write_inputs({"principal": 250_000, "rate": 0.065})
        assert abs(session.evaluate_once()["payment"] - 1580.1700587324134) < 1e-8
    canonical = SheetPortSession(wb, manifest.to_dict())
    assert canonical.read_inputs()["principal"] == 250_000


if __name__ == "__main__":
    main()
