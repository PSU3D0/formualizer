use formualizer_common::{ExcelError, ExcelErrorExtra, ExcelErrorKind};

#[test]
fn function_panic_payload_extraction() {
    for (payload, expected) in [
        (
            Box::new("slice") as Box<dyn std::any::Any + Send>,
            Some("slice"),
        ),
        (Box::new(String::from("owned")), Some("owned")),
        (Box::new(7_u32), None),
    ] {
        let error = ExcelError::from_function_panic("FN", &*payload);
        assert_eq!(error.kind, ExcelErrorKind::Value);
        assert_eq!(
            error.extra(),
            &ExcelErrorExtra::FunctionPanicked {
                function: "FN".into(),
                message: expected.map(str::to_owned),
            }
        );
    }
}

#[cfg(feature = "serde")]
#[test]
fn panic_and_poison_errors_roundtrip_serde() {
    for extra in [
        ExcelErrorExtra::FunctionPanicked {
            function: "FN".into(),
            message: Some("payload".into()),
        },
        ExcelErrorExtra::FunctionPanicked {
            function: "FN".into(),
            message: None,
        },
        ExcelErrorExtra::EnginePoisoned,
    ] {
        let error = ExcelError::new(ExcelErrorKind::Value).with_extra(extra);
        let json = serde_json::to_string(&error).unwrap();
        assert_eq!(serde_json::from_str::<ExcelError>(&json).unwrap(), error);
    }
}
