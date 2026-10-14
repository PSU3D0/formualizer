//! SheetPort quickstart shown in the READMEs; run by `tests/examples.rs`.

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    // region readme
    use formualizer::sheetport::{EvalOptions, InputUpdate, PortValue, SheetPortSession};
    use formualizer::sheetport_spec::{Constraints, Manifest, Port, ValueType};
    use formualizer::{LiteralValue, Workbook};

    // The model: B4 computes the monthly payment from B1:B3.
    let mut wb = Workbook::new(); // or load one from an .xlsx
    wb.add_sheet("Calculator")?;
    wb.set_formula("Calculator", 4, 2, "=PMT(B2/12, B3, -B1)")?;

    let manifest = Manifest::new("loan-calculator")
        .with_name("Loan calculator")
        .port(
            Port::input("principal", "Calculator!B1", ValueType::Number).with_constraints(
                Constraints {
                    min: Some(0.0),
                    ..Default::default()
                },
            ),
        )
        .port(Port::input("rate", "Calculator!B2", ValueType::Number))
        .port(Port::input(
            "term_months",
            "Calculator!B3",
            ValueType::Integer,
        ))
        .port(Port::output(
            "monthly_payment",
            "Calculator!B4",
            ValueType::Number,
        ));

    let mut session = SheetPortSession::new(wb, manifest)?;
    session.write_inputs(InputUpdate::from_iter([
        ("principal", LiteralValue::Number(250_000.0)),
        ("rate", LiteralValue::Number(0.045)),
        ("term_months", LiteralValue::Int(360)),
    ]))?;
    let outputs = session.evaluate_once(EvalOptions {
        freeze_volatile: true,
        ..Default::default()
    })?;
    let payment = outputs
        .get("monthly_payment")
        .and_then(PortValue::as_scalar);
    println!("{payment:?}"); // Some(Number(1266.71...))
    // endregion

    let Some(LiteralValue::Number(payment)) = payment else {
        return Err(format!("unexpected payment {payment:?}").into());
    };
    assert!((payment - 1_266.713_274_564_7).abs() < 1e-6);

    let negative = session.write_inputs(InputUpdate::from_iter([(
        "principal",
        LiteralValue::Number(-1.0),
    )]));
    assert!(negative.is_err(), "constraint not enforced");

    // The same manifest, shipped as YAML.
    let yaml = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../examples/sheetport/loan-calculator.fio.yaml"
    ))?;
    let from_yaml = Manifest::from_yaml_str(&yaml)?;
    assert_eq!(from_yaml.to_yaml()?, session.manifest().to_yaml()?);
    Ok(())
}
