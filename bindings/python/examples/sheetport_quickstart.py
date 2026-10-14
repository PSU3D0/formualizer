"""SheetPort quickstart shown in the READMEs; run by the test suite."""

from pathlib import Path

# region readme
import formualizer as fz
from formualizer.sheetport import Input, Manifest, Output

# The model: B4 computes the monthly payment from B1:B3.
wb = fz.Workbook()  # or fz.load_workbook("loan.xlsx")
wb.add_sheet("Calculator")
wb.set_formula("Calculator", 4, 2, "=PMT(B2/12, B3, -B1)")

manifest = Manifest(
    "loan-calculator",
    name="Loan calculator",
    ports=[
        Input("principal", "Calculator!B1", type="number", constraints={"min": 0}),
        Input("rate", "Calculator!B2", type="number"),
        Input("term_months", "Calculator!B3", type="integer"),
        Output("monthly_payment", "Calculator!B4", type="number"),
    ],
)

session = fz.SheetPortSession(wb, manifest)
session.write_inputs({"principal": 250_000, "rate": 0.045, "term_months": 360})
outputs = session.evaluate_once(freeze_volatile=True)
print(outputs["monthly_payment"])  # 1266.71...
# endregion

assert abs(outputs["monthly_payment"] - 1266.7132745647) < 1e-6

try:
    session.write_inputs({"principal": -1})
    raise AssertionError("constraint not enforced")
except fz.SheetPortConstraintError:
    pass
# A rejected update writes nothing, including its valid ports.
try:
    session.write_inputs({"rate": 0.05, "principal": -1})
    raise AssertionError("constraint not enforced")
except fz.SheetPortConstraintError:
    pass
assert session.read_inputs() == {
    "principal": 250_000,
    "rate": 0.045,
    "term_months": 360,
}

yaml_text = (
    Path(__file__).resolve().parents[3] / "examples/sheetport/loan-calculator.fio.yaml"
).read_text()
# region yaml
session = fz.SheetPortSession.from_manifest_yaml(yaml_text, wb)
# endregion
assert Manifest.from_yaml(yaml_text).to_dict() == manifest.to_dict()
session.write_inputs({"principal": 250_000, "rate": 0.045, "term_months": 360})
assert session.evaluate_once()["monthly_payment"] == outputs["monthly_payment"]
