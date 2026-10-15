//! Inputs that used to overflow the stack or panic. Each test runs on a
//! thread with a small stack so a recursion regression shows up in both
//! debug and release builds.

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
