use formualizer_parse::{BinaryOperator, UnaryOperator};

#[test]
fn openformula_semicolon_is_not_an_infix_operator() {
    use formualizer_parse::{FormulaDialect, parse_with_dialect};
    for (formula, position) in [("=A1;B1", 3), ("=(A1;B1)", 4), ("=SUM((A1;B1))", 8)] {
        let error = parse_with_dialect(formula, FormulaDialect::OpenFormula).unwrap_err();
        assert_eq!(error.position, Some(position));
        assert_eq!(
            error.message,
            "';' is an argument separator, not an operator in this position; OpenFormula union uses '~'"
        );
    }
    assert!(parse_with_dialect("=SUM(A1;B1)", FormulaDialect::OpenFormula).is_ok());
    assert!(parse_with_dialect("=A1~B1", FormulaDialect::OpenFormula).is_ok());
}

#[test]
fn union_with_a_constant_operand_is_refused() {
    // Excel 16 refuses `=1,000` when it is entered (Range.Formula and
    // Range.Formula2 both raise) and stores `=A1,B1` and `=(A1,B1)` as
    // written. The other refused rows follow from the union operator joining
    // references; they were not entered in Excel.
    use formualizer_parse::parse;
    for (formula, position) in [
        ("=1,000", 2),
        ("=(1,000)", 3),
        ("=A1,1", 3),
        ("=\"a\",B1", 4),
        ("=TRUE,A1", 5),
    ] {
        let error = parse(formula).unwrap_err();
        assert_eq!(error.position, Some(position), "{formula}");
        assert!(
            error.message.contains("union operator"),
            "{formula}: {error}"
        );
    }
    for formula in [
        "=A1,B1",
        "=(A1,B1)",
        "=SUM((A1,B1),C1)",
        "=SUM(1,000)",
        "=(#REF!,A1)",
        "=INDEX(A1:B2,1,1),A1",
    ] {
        assert!(parse(formula).is_ok(), "{formula}");
    }
}

#[test]
fn hand_built_asts_reject_unknown_operator_strings() {
    // Callers constructing ASTNodeType::{BinaryOp, UnaryOp} must convert
    // strings through these checked constructors; arbitrary strings cannot fit.
    for text in ["unknown", ";", "~", "!"] {
        assert!(BinaryOperator::try_from(text).is_err());
        assert!(UnaryOperator::try_from(text).is_err());
    }
}
use std::hash::{DefaultHasher, Hash, Hasher};

fn hash(value: impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn operators_preserve_spelling_and_string_hashes() {
    for text in [
        "+", "-", "*", "/", "^", "&", "=", "<>", "<", ">", "<=", ">=", ":", ",", " ",
    ] {
        let op = BinaryOperator::try_from(text).unwrap();
        assert_eq!(op.as_str(), text);
        assert_eq!(op.to_string(), text);
        assert_eq!(format!("{op:?}"), format!("{text:?}"));
        assert_eq!(hash(op), hash(text.to_string()));
    }
    for text in ["+", "-", "%", "#", "@"] {
        let op = UnaryOperator::try_from(text).unwrap();
        assert_eq!(op.as_str(), text);
        assert_eq!(op.to_string(), text);
        assert_eq!(format!("{op:?}"), format!("{text:?}"));
        assert_eq!(hash(op), hash(text.to_string()));
    }
    for text in ["", "==", "u", "unsupported", "\t"] {
        assert!(BinaryOperator::try_from(text).is_err());
        assert!(UnaryOperator::try_from(text).is_err());
    }
}
