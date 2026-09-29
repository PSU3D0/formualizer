use formualizer_parse::parser::{ASTNodeType, Parser, parse};

#[test]
fn ragged_array_literals_are_parse_errors() {
    for formula in ["={1,2;3}", "={1;2,3}", "=SUM({1,2;3})", "={{1,2;3}}"] {
        let error = parse(formula).expect_err(formula);
        assert!(error.message.contains("Array rows must have equal length"));
        assert!(error.position.is_some());
        let mut parser = Parser::new(formula).unwrap();
        assert!(parser.parse().is_err(), "{formula}");
    }
}

#[test]
fn array_syntax_controls() {
    for formula in [
        "={;}",
        "={1;;2}",
        "={1,;2,3}",
        "={1;}",
        "={,1}",
        "={1,2",
        "={1,2;3,4",
    ] {
        assert!(parse(formula).is_err(), "{formula}");
    }
    for formula in ["={1,2;3,4}", "={1;2}", "={1,2}", "={}", "={{1},{2}}"] {
        assert!(
            matches!(parse(formula).unwrap().node_type, ASTNodeType::Array(_)),
            "{formula}"
        );
    }
}
