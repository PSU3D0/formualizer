use formualizer_parse::{BinaryOperator, UnaryOperator};
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
