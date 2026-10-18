//! Inputs that used to overflow the stack or panic. Each test runs on a
//! thread with a small stack so a recursion regression shows up in both
//! debug and release builds.

#![cfg(test)]

use formualizer_parse::parser::ReferenceType;
use formualizer_parse::{ASTNodeType, parse};

const SMALL_STACK: usize = 1024 * 1024;
const PREFIXES: usize = 20_000;

fn parsed_reference(formula: &str) -> ReferenceType {
    match parse(formula).expect("parse").node_type {
        ASTNodeType::Reference { reference, .. } => reference,
        other => panic!("expected a reference, got {other:?}"),
    }
}

fn on_small_stack<F: FnOnce() + Send + 'static>(f: F) {
    std::thread::Builder::new()
        .stack_size(SMALL_STACK)
        .spawn(f)
        .expect("spawn test thread")
        .join()
        .expect("test thread panicked");
}

#[test]
fn many_unquoted_workbook_zero_prefixes() {
    on_small_stack(|| {
        let formula = format!("={}Sheet1!A1", "[0]".repeat(PREFIXES));
        assert_eq!(parsed_reference(&formula), parsed_reference("=Sheet1!A1"));
    });
}

#[test]
fn many_quoted_workbook_zero_prefixes() {
    on_small_stack(|| {
        let formula = format!("='{}Sheet1'!A1", "[0]".repeat(PREFIXES));
        assert_eq!(parsed_reference(&formula), parsed_reference("='Sheet1'!A1"));
    });
}

#[test]
fn many_workbook_zero_prefixes_in_reference_type() {
    on_small_stack(|| {
        let unquoted = format!("{}Sheet1!A1", "[0]".repeat(PREFIXES));
        assert_eq!(
            ReferenceType::from_string(&unquoted).expect("unquoted"),
            ReferenceType::from_string("Sheet1!A1").expect("plain")
        );
        let quoted = format!("'{}Sheet 1'!A1", "[0]".repeat(PREFIXES));
        assert_eq!(
            ReferenceType::from_string(&quoted).expect("quoted"),
            ReferenceType::from_string("'Sheet 1'!A1").expect("plain")
        );
        // A workbook-scoped name keeps its `[0]!` stripping.
        let name = format!("{}!Name", "[0]".repeat(PREFIXES));
        assert_eq!(
            ReferenceType::from_string(&name).expect("name"),
            ReferenceType::from_string("Name").expect("plain")
        );
    });
}

#[test]
fn mixed_workbook_zero_prefix_forms() {
    let cases = [
        ("[0]'[0]Sheet 1'!A1", "'Sheet 1'!A1"),
        ("'[0][0]Sheet 1'!A1", "'Sheet 1'!A1"),
        ("[0][0]!Name", "Name"),
        ("[0]!'[0]Sheet 1'!A1", "'Sheet 1'!A1"),
    ];
    for (prefixed, plain) in cases {
        assert_eq!(
            ReferenceType::from_string(prefixed).expect(prefixed),
            ReferenceType::from_string(plain).expect(plain),
            "{prefixed}"
        );
    }
}

#[test]
fn collect_references_full_height_hand_built_range_is_not_expanded() {
    use formualizer_parse::ASTNode;
    use formualizer_parse::parser::CollectPolicy;

    let policy = CollectPolicy {
        expand_small_ranges: true,
        range_expansion_limit: 16,
        include_names: true,
    };
    for reference in [
        ReferenceType::range(None, Some(0), Some(1), Some(u32::MAX), Some(1)),
        ReferenceType::range(None, Some(1), Some(0), Some(1), Some(u32::MAX)),
    ] {
        let ast = ASTNode::new(
            ASTNodeType::Reference {
                original: "x".into(),
                reference: reference.clone(),
            },
            None,
        );
        let refs = ast.collect_references(&policy);
        assert_eq!(refs.as_slice(), &[reference]);
    }
}

#[test]
fn array_with_unknown_separator_subtype_is_an_error() {
    use formualizer_parse::{Parser, TokenStream, TokenSubType, TokenType};
    use std::sync::mpsc;
    use std::time::Duration;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut stream = TokenStream::new("={1,2}").expect("tokenize");
        let sep = stream
            .spans
            .iter_mut()
            .find(|span| span.token_type == TokenType::Sep)
            .expect("separator span");
        sep.subtype = TokenSubType::None;
        let _ = tx.send(Parser::from_token_stream(&stream).parse().is_err());
    });
    let is_err = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("parser did not return within 10 s");
    assert!(is_err);
}

/// `=((Sheet1!A1 + Sheet1!B1) + 1) + 1 ...` with `depth` left-nested levels,
/// built by hand (the parser's `ast_height` limit would reject it). Left
/// nesting keeps a recursive walk from becoming a tail call in release.
fn deep_hand_built_tree(depth: usize) -> formualizer_parse::ASTNode {
    use formualizer_parse::ASTNode;
    let leaf = |col: u32| {
        ASTNode::new(
            ASTNodeType::Reference {
                original: String::new(),
                reference: ReferenceType::cell(Some("Sheet1".into()), 1, col),
            },
            None,
        )
    };
    let mut node = ASTNode::new(
        ASTNodeType::BinaryOp {
            op: formualizer_parse::BinaryOperator::Add,
            left: Box::new(leaf(1)),
            right: Box::new(leaf(2)),
        },
        None,
    );
    for _ in 0..depth {
        node = ASTNode::new(
            ASTNodeType::BinaryOp {
                op: formualizer_parse::BinaryOperator::Add,
                left: Box::new(node),
                right: Box::new(ASTNode::new(
                    ASTNodeType::Literal(formualizer_parse::LiteralValue::Number(1.0)),
                    None,
                )),
            },
            None,
        );
    }
    node
}

#[test]
fn dependency_walks_do_not_recurse_on_deep_hand_built_trees() {
    on_small_stack(|| {
        let mut tree = deep_hand_built_tree(200_000);
        let deps: Vec<String> = tree.get_dependency_strings();
        assert_eq!(deps, ["Sheet1!A1", "Sheet1!B1"]);
        tree.update_sheet_references(Some("Sheet1"), "Renamed");
        assert_eq!(tree.get_dependency_strings(), ["Renamed!A1", "Renamed!B1"]);
        // Dropping the tree recurses (derived Drop); that is outside this
        // test's scope.
        std::mem::forget(tree);
    });
}

#[test]
fn dependency_walks_visit_in_source_order() {
    let mut ast =
        parse("=SUM(A1,B1:C2)+LAMBDA(x,x+D1)(E1,{1,2;3,4})-(-Sheet2!F1)*Sheet2:Sheet3!G1")
            .expect("parse");
    assert_eq!(
        ast.get_dependency_strings(),
        [
            "A1",
            "B1:C2",
            "x",
            "x",
            "D1",
            "E1",
            "Sheet2!F1",
            "Sheet2:Sheet3!G1"
        ]
    );
    ast.update_sheet_references(Some("Sheet2"), "Renamed");
    assert_eq!(
        ast.get_dependency_strings(),
        [
            "A1",
            "B1:C2",
            "x",
            "x",
            "D1",
            "E1",
            "Renamed!F1",
            "Renamed:Sheet3!G1"
        ]
    );
}
