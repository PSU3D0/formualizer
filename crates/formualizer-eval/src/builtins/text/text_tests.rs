//! Comprehensive tests for text functions

#[cfg(test)]
mod tests {
    use crate::builtins::text::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    use std::sync::Arc;

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn test_len_edge_cases() {
        let wb = TestWorkbook::new().with_function(Arc::new(LenFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "LEN").unwrap();

        // Empty string
        let empty = lit(LiteralValue::Text("".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&empty, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Int(0)
        );

        // Number converted to text
        let num = lit(LiteralValue::Number(123.45));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&num, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Int(6) // "123.45"
        );

        // Boolean
        let bool_val = lit(LiteralValue::Boolean(true));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&bool_val, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Int(4) // "TRUE"
        );
    }

    #[test]
    fn test_left_right_edge_cases() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(LeftFn))
            .with_function(Arc::new(RightFn));
        let ctx = wb.interpreter();
        let left = ctx.context.get_function("", "LEFT").unwrap();
        let right = ctx.context.get_function("", "RIGHT").unwrap();

        let text = lit(LiteralValue::Text("hello".into()));

        // LEFT with 0 characters
        let zero = lit(LiteralValue::Int(0));
        assert_eq!(
            left.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&zero, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("".into())
        );

        // LEFT with more than string length
        let large = lit(LiteralValue::Int(100));
        assert_eq!(
            left.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&large, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("hello".into())
        );

        // RIGHT with negative should return #VALUE!
        let neg = lit(LiteralValue::Int(-1));
        match right
            .dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&neg, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e.to_string(), "#VALUE!"),
            _ => panic!("Expected #VALUE! error"),
        }
    }

    #[test]
    fn test_mid_boundaries() {
        let wb = TestWorkbook::new().with_function(Arc::new(MidFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "MID").unwrap();

        let text = lit(LiteralValue::Text("abcdef".into()));

        // MID with start < 1 should return #VALUE!
        let start_zero = lit(LiteralValue::Int(0));
        let count = lit(LiteralValue::Int(3));
        match f
            .dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&start_zero, &ctx),
                    ArgumentHandle::new(&count, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e.to_string(), "#VALUE!"),
            _ => panic!("Expected #VALUE! error"),
        }

        // MID with start > length should return empty
        let start_large = lit(LiteralValue::Int(100));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&start_large, &ctx),
                    ArgumentHandle::new(&count, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("".into())
        );

        // MID with large count clips at end
        let start = lit(LiteralValue::Int(4));
        let large_count = lit(LiteralValue::Int(100));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&large_count, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("def".into())
        );
    }

    #[test]
    fn test_find_search_differences() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(FindFn))
            .with_function(Arc::new(SearchFn));
        let ctx = wb.interpreter();
        let find = ctx.context.get_function("", "FIND").unwrap();
        let search = ctx.context.get_function("", "SEARCH").unwrap();

        // FIND is case-sensitive, SEARCH is not
        let needle = lit(LiteralValue::Text("B".into()));
        let haystack = lit(LiteralValue::Text("abc".into()));

        // FIND should not find "B" in "abc"
        match find
            .dispatch(
                &[
                    ArgumentHandle::new(&needle, &ctx),
                    ArgumentHandle::new(&haystack, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e.to_string(), "#VALUE!"),
            _ => panic!("Expected #VALUE! error"),
        }

        // SEARCH should find "B" in "abc" (case-insensitive)
        assert_eq!(
            search
                .dispatch(
                    &[
                        ArgumentHandle::new(&needle, &ctx),
                        ArgumentHandle::new(&haystack, &ctx)
                    ],
                    &ctx.function_context(None)
                )
                .unwrap(),
            LiteralValue::Int(2) // 1-based index
        );
    }

    #[test]
    fn test_substitute_occurrences() {
        let wb = TestWorkbook::new().with_function(Arc::new(SubstituteFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "SUBSTITUTE").unwrap();

        let text = lit(LiteralValue::Text("aaa bbb aaa".into()));
        let old = lit(LiteralValue::Text("aaa".into()));
        let new = lit(LiteralValue::Text("ccc".into()));

        // Replace all occurrences (no instance_num)
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&old, &ctx),
                    ArgumentHandle::new(&new, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("ccc bbb ccc".into())
        );

        // Replace only first occurrence
        let instance = lit(LiteralValue::Int(1));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&old, &ctx),
                    ArgumentHandle::new(&new, &ctx),
                    ArgumentHandle::new(&instance, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("ccc bbb aaa".into())
        );

        // Instance number > occurrences returns original
        let large_instance = lit(LiteralValue::Int(10));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&old, &ctx),
                    ArgumentHandle::new(&new, &ctx),
                    ArgumentHandle::new(&large_instance, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("aaa bbb aaa".into())
        );
    }

    #[test]
    fn test_trim_edge_cases() {
        let wb = TestWorkbook::new().with_function(Arc::new(TrimFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TRIM").unwrap();

        // Multiple spaces between words
        let text = lit(LiteralValue::Text("  hello    world  ".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&text, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("hello world".into())
        );

        // Only spaces
        let spaces = lit(LiteralValue::Text("     ".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&spaces, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("".into())
        );

        // Empty string
        let empty = lit(LiteralValue::Text("".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&empty, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("".into())
        );
    }

    #[test]
    fn test_proper_case() {
        let wb = TestWorkbook::new().with_function(Arc::new(ProperFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "PROPER").unwrap();

        // Basic proper case
        let text = lit(LiteralValue::Text("hello world".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&text, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("Hello World".into())
        );

        // Mixed case input
        let mixed = lit(LiteralValue::Text("hELLo WoRLd".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&mixed, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("Hello World".into())
        );

        // Numbers and punctuation
        let punct = lit(LiteralValue::Text("it's 123-test".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&punct, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("It'S 123-Test".into())
        );
    }

    #[test]
    fn test_exact_comparison() {
        let wb = TestWorkbook::new().with_function(Arc::new(ExactFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "EXACT").unwrap();

        // Case sensitive match
        let a = lit(LiteralValue::Text("Hello".into()));
        let b = lit(LiteralValue::Text("Hello".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Boolean(true)
        );

        // Case sensitive mismatch
        let c = lit(LiteralValue::Text("hello".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&c, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn test_value_parsing() {
        let wb = TestWorkbook::new().with_function(Arc::new(ValueFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "VALUE").unwrap();

        // Scientific notation
        let sci = lit(LiteralValue::Text("1.23E+2".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&sci, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Number(123.0)
        );

        // Leading/trailing spaces
        let spaces = lit(LiteralValue::Text("  42.5  ".into()));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&spaces, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Number(42.5)
        );

        // Invalid text returns #VALUE!
        let invalid = lit(LiteralValue::Text("not a number".into()));
        match f
            .dispatch(
                &[ArgumentHandle::new(&invalid, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e.to_string(), "#VALUE!"),
            _ => panic!("Expected #VALUE! error"),
        }
    }

    #[test]
    fn test_text_formatting() {
        let wb = TestWorkbook::new().with_function(Arc::new(TextFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXT").unwrap();

        // Percent format
        let num = lit(LiteralValue::Number(0.125));
        let fmt = lit(LiteralValue::Text("%".into()));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&num, &ctx),
                    ArgumentHandle::new(&fmt, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("12%".into()) // 0.125 * 100 = 12.5, rounds to 12
        );

        // Two decimal places
        let pi = lit(LiteralValue::Number(std::f64::consts::PI));
        let dec_fmt = lit(LiteralValue::Text("0.00".into()));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&pi, &ctx),
                    ArgumentHandle::new(&dec_fmt, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("3.14".into())
        );
    }

    #[test]
    fn test_textjoin_empty_delimiter() {
        let wb = TestWorkbook::new().with_function(Arc::new(TextJoinFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXTJOIN").unwrap();

        // Empty delimiter
        let delim = lit(LiteralValue::Text("".into()));
        let ignore = lit(LiteralValue::Boolean(true));
        let a = lit(LiteralValue::Text("a".into()));
        let b = lit(LiteralValue::Text("b".into()));
        let c = lit(LiteralValue::Text("c".into()));

        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&delim, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&a, &ctx),
                    ArgumentHandle::new(&b, &ctx),
                    ArgumentHandle::new(&c, &ctx),
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("abc".into())
        );
    }

    #[test]
    fn test_replace_bounds() {
        let wb = TestWorkbook::new().with_function(Arc::new(ReplaceFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "REPLACE").unwrap();

        let text = lit(LiteralValue::Text("hello".into()));

        // Replace at start
        let start = lit(LiteralValue::Int(1));
        let count = lit(LiteralValue::Int(2));
        let new = lit(LiteralValue::Text("HE".into()));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&count, &ctx),
                    ArgumentHandle::new(&new, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("HEllo".into())
        );

        // Replace beyond end
        let start_end = lit(LiteralValue::Int(4));
        let count_large = lit(LiteralValue::Int(10));
        let suffix = lit(LiteralValue::Text("LO!".into()));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&start_end, &ctx),
                    ArgumentHandle::new(&count_large, &ctx),
                    ArgumentHandle::new(&suffix, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Text("helLO!".into())
        );
    }
}
