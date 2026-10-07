use crate::args::CriteriaPredicate;
use crate::builtins::utils::criteria_match;
use formualizer_common::LiteralValue;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_like_punctuation_is_literal_in_scalar_criteria() {
        let data = [
            "a%b", "axxxb", "a_b", "aQb", "1_0", "1x0", "a%xyz", "ab", r"a\b", r"a\xyz", "a*b",
            "a?b", "a~b", "A%B",
        ];
        let cases: &[(&str, &[usize])] = &[
            ("a%b", &[0, 13]),
            ("1_0", &[4]),
            ("a_b", &[2]),
            ("<>a%b", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            ("a%*", &[0, 6, 13]),
            ("a_?", &[2]),
            (r"a\b", &[8]),
            (r"a\*", &[8, 9]),
            ("a~*b", &[10]),
            ("a~?b", &[11]),
            ("a~~b", &[12]),
            ("a?b", &[0, 2, 3, 8, 10, 11, 12, 13]),
            ("<>a%*", &[1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12]),
        ];
        for &(criterion, matches) in cases {
            let pred = crate::args::parse_criteria(&LiteralValue::Text(criterion.into())).unwrap();
            for (index, text) in data.iter().enumerate() {
                assert_eq!(
                    criteria_match(&pred, &LiteralValue::Text((*text).into())),
                    matches.contains(&index),
                    "{criterion:?} against {text:?}"
                );
            }
        }
    }

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
    fn wildcard_escapes_token_boundaries_and_bounded_backtracking() {
        for (pattern, text, expected) in [
            ("a~*", "a*", true),
            ("a~*", "a*x", false),
            ("a~?", "a?", true),
            ("a~~", "a~", true),
            ("a~~*", "a~tail", true),
            ("a~~~*", "a~*", true),
            ("~~~~", "~~", true),
            ("~x", "~x", true),
            ("a~", "a~", true),
            ("*~**~?", "xx*yy?", true),
            ("*~**~?", "xx*yy?z", false),
            ("a*b?c*d?", "axabQcdZ", true),
            ("a*b?c*d?", "axabQcd", false),
            (".[+](x)^$", ".[+](x)^$", true),
            (".[+](x)^$", "ax", false),
        ] {
            assert_eq!(
                criteria_match(&create_text_like(pattern), &LiteralValue::Text(text.into())),
                expected,
                "{pattern} / {text}"
            );
        }
        // Former recursive implementations branch exponentially or exhaust the stack.
        let stars = "*".repeat(20_000);
        assert!(criteria_match(
            &create_text_like(&stars),
            &LiteralValue::Text("x".into())
        ));
        let branching = format!("{}b", "*a".repeat(64));
        assert!(!criteria_match(
            &create_text_like(&branching),
            &LiteralValue::Text("a".repeat(128))
        ));
    }

    #[test]
    fn test_wildcards_match_text_only() {
        // Excel: wildcard criteria match text cells only; numbers never match,
        // even when their digits fit the pattern.
        let pred = create_text_like("123*");

        assert!(!criteria_match(&pred, &LiteralValue::Number(123.0)));
        assert!(!criteria_match(&pred, &LiteralValue::Number(123.456)));
        assert!(!criteria_match(&pred, &LiteralValue::Int(123)));
        assert!(!criteria_match(&pred, &LiteralValue::Boolean(true)));
        assert!(criteria_match(&pred, &LiteralValue::Text("123".into())));
        assert!(criteria_match(&pred, &LiteralValue::Text("1234".into())));
        assert!(!criteria_match(&pred, &LiteralValue::Text("12.3".into())));

        let four = create_text_like("????");
        assert!(!criteria_match(&four, &LiteralValue::Number(2025.0)));
        assert!(criteria_match(&four, &LiteralValue::Text("2025".into())));
    }

    #[test]
    fn test_empty_values() {
        let pred_empty = create_text_like("*");
        let pred_something = create_text_like("some*");

        // `*` matches any text, including empty text, but not a blank cell.
        assert!(!criteria_match(&pred_empty, &LiteralValue::Empty));
        assert!(criteria_match(&pred_empty, &LiteralValue::Text("".into())));

        assert!(!criteria_match(&pred_something, &LiteralValue::Empty));
        assert!(!criteria_match(
            &pred_something,
            &LiteralValue::Text("".into())
        ));
    }
}
