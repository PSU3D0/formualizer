use formualizer_parse::{ASTNode, ASTNodeType, BinaryOperator, UnaryOperator};

#[test]
fn core_operator_serde_keeps_the_string_wire_shape() {
    for text in [
        "+", "-", "*", "/", "^", "&", "=", "<>", "<", ">", "<=", ">=", ":", ",", " ",
    ] {
        let op = BinaryOperator::try_from(text).unwrap();
        assert_eq!(serde_json::to_value(op).unwrap(), serde_json::json!(text));
        assert_eq!(
            serde_json::from_value::<BinaryOperator>(serde_json::json!(text)).unwrap(),
            op
        );
        let formula = format!("=A1{text}B2");
        let ast = formualizer_parse::parse(&formula).unwrap();
        let wire = serde_json::to_value(&ast).unwrap();
        // A1:B2 is folded into a Reference by the parser, so explicitly
        // construct an operator node to exercise that wire shape too.
        let node = ASTNode::new(
            ASTNodeType::BinaryOp {
                op,
                left: Box::new(ast.clone()),
                right: Box::new(ast.clone()),
            },
            None,
        );
        let node_wire = serde_json::to_value(&node).unwrap();
        assert_eq!(node_wire["node_type"]["BinaryOp"]["op"], text);
        assert_eq!(serde_json::from_value::<ASTNode>(node_wire).unwrap(), node);
        assert_eq!(serde_json::from_value::<ASTNode>(wire).unwrap(), ast);
    }
    for text in ["+", "-", "%", "#", "@"] {
        let op = UnaryOperator::try_from(text).unwrap();
        assert_eq!(serde_json::to_value(op).unwrap(), serde_json::json!(text));
        assert_eq!(
            serde_json::from_value::<UnaryOperator>(serde_json::json!(text)).unwrap(),
            op
        );
        let formula = match op {
            UnaryOperator::Percent | UnaryOperator::Spill => format!("=A1{text}"),
            _ => format!("={text}A1"),
        };
        let ast = formualizer_parse::parse(&formula).unwrap();
        let wire = serde_json::to_value(&ast).unwrap();
        assert_eq!(wire["node_type"]["UnaryOp"]["op"], text);
        assert_eq!(serde_json::from_value::<ASTNode>(wire).unwrap(), ast);
    }
}
