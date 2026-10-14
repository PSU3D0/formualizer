use formualizer_common::LiteralValue;
use formualizer_sheetport::{EvalOptions, InputUpdate, PortValue, SheetPort};
use formualizer_workbook::Workbook;
use sheetport_spec::{Manifest, Port, ValueType};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut workbook = Workbook::new();
    workbook.add_sheet("Calculator")?;
    workbook.set_formula("Calculator", 4, 2, "=-PMT(B2/12,360,B1)")?;
    let manifest = Manifest::new("loan-calculator")
        .port(Port::input("principal", "Calculator!B1", ValueType::Number))
        .port(Port::input("rate", "Calculator!B2", ValueType::Number))
        .port(Port::output("payment", "Calculator!B4", ValueType::Number));
    // The same manifest may be shipped as YAML instead.
    let yaml = manifest.to_yaml()?;
    for manifest in [manifest, Manifest::from_yaml_str(&yaml)?] {
        let mut session = SheetPort::new(&mut workbook, manifest)?;
        let mut inputs = InputUpdate::new();
        inputs.insert(
            "principal",
            PortValue::Scalar(LiteralValue::Number(250_000.0)),
        );
        inputs.insert("rate", PortValue::Scalar(LiteralValue::Number(0.065)));
        session.write_inputs(inputs)?;
        let outputs = session.evaluate_once(EvalOptions::default())?;
        assert!(
            matches!(outputs.get("payment"), Some(PortValue::Scalar(LiteralValue::Number(n))) if (*n - 1_580.170_058_732_413_4).abs() < 1e-8)
        );
    }
    Ok(())
}
