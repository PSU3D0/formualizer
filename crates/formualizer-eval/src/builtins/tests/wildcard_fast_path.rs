use crate::args::CriteriaPredicate;
use crate::builtins::utils::criteria_match;
use formualizer_common::LiteralValue;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_text_like(pattern: &str) -> CriteriaPredicate {
        CriteriaPredicate::TextLike {
            pattern: pattern.to_string(),
            case_insensitive: true,
        }
    }

    #[test]
    fn test_anchored_start_wildcard() {
        let pred = create_text_like("abc*");

        assert!(criteria_match(&pred, &LiteralValue::Text("abc".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("abcdef".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("ABC123".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("ABCxyz".into())));

        assert!(!criteria_match(&pred, &LiteralValue::Text("xabc".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("ab".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("dabc".into())));
    }

    #[test]
    fn test_anchored_end_wildcard() {
        let pred = create_text_like("*xyz");

        assert!(criteria_match(&pred, &LiteralValue::Text("xyz".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("abcxyz".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("123XYZ".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("testXYZ".into())));

        assert!(!criteria_match(&pred, &LiteralValue::Text("xyzabc".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("xy".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("xyzd".into())));
    }

    #[test]
    fn test_contains_wildcard() {
        let pred = create_text_like("*mid*");

        assert!(criteria_match(&pred, &LiteralValue::Text("mid".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("middle".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("amid".into())));
        assert!(criteria_match(
            &pred,
            &LiteralValue::Text("beginning_MID_end".into())
        ));
        assert!(criteria_match(&pred, &LiteralValue::Text("MID".into())));

        assert!(!criteria_match(&pred, &LiteralValue::Text("md".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("mdi".into())));
    }

    #[test]
    fn test_exact_match_no_wildcard() {
        let pred = create_text_like("exact");

        assert!(criteria_match(&pred, &LiteralValue::Text("exact".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("EXACT".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("ExAcT".into())));

        assert!(!criteria_match(&pred, &LiteralValue::Text("exac".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("exacta".into())));
    }

    #[test]
    fn test_question_mark_fallback() {
        let pred = create_text_like("a?c");

        assert!(criteria_match(&pred, &LiteralValue::Text("abc".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("a1c".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("AXC".into())));

        assert!(!criteria_match(&pred, &LiteralValue::Text("ac".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("abbc".into())));
    }

    #[test]
    fn test_complex_pattern_fallback() {
        let pred = create_text_like("a*b?c*");

        assert!(criteria_match(&pred, &LiteralValue::Text("abxc".into())));
        assert!(criteria_match(
            &pred,
            &LiteralValue::Text("axxxxbxc".into())
        ));
        assert!(criteria_match(&pred, &LiteralValue::Text("abxcyyy".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("ABXCDEF".into())));

        assert!(!criteria_match(&pred, &LiteralValue::Text("abc".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("axc".into())));
    }

    #[test]
    fn test_case_sensitivity() {
        let pred_insensitive = create_text_like("ABC*");
        let pred_sensitive = CriteriaPredicate::TextLike {
            pattern: "ABC*".to_string(),
            case_insensitive: false,
        };

        assert!(criteria_match(
            &pred_insensitive,
            &LiteralValue::Text("abc123".into())
        ));
        assert!(criteria_match(
            &pred_insensitive,
            &LiteralValue::Text("ABC123".into())
        ));

        assert!(!criteria_match(
            &pred_sensitive,
            &LiteralValue::Text("abc123".into())
        ));
        assert!(criteria_match(
            &pred_sensitive,
            &LiteralValue::Text("ABC123".into())
        ));
    }

    #[test]
    fn test_numeric_coercion() {
        let pred = create_text_like("123*");

        assert!(criteria_match(&pred, &LiteralValue::Number(123.0)));
        assert!(criteria_match(&pred, &LiteralValue::Number(123.456)));
        assert!(criteria_match(&pred, &LiteralValue::Int(123)));

        assert!(!criteria_match(&pred, &LiteralValue::Number(12.3)));
        assert!(!criteria_match(&pred, &LiteralValue::Int(12)));
    }

    #[test]
    fn test_empty_values() {
        let pred_empty = create_text_like("*");
        let pred_something = create_text_like("some*");

        assert!(criteria_match(&pred_empty, &LiteralValue::Empty));
        assert!(criteria_match(&pred_empty, &LiteralValue::Text("".into())));

        assert!(!criteria_match(&pred_something, &LiteralValue::Empty));
        assert!(!criteria_match(
            &pred_something,
            &LiteralValue::Text("".into())
        ));
    }
}
