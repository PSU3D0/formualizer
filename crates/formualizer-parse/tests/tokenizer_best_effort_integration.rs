use formualizer_parse::{TokenStream, Tokenizer};

#[test]
fn integration_best_effort_render_roundtrip_and_coverage() {
    let formulas = [
        "=A1+",
        "=A1+)",
        "=SUM(A1",
        "=\"unterminated",
        "=[A1",
        "=#BAD",
        "=(1}",
        "=A1+{1,2,3}",
    ];

    for formula in formulas {
        let stream = TokenStream::new_best_effort(formula);
        assert_eq!(stream.render_formula(), formula);
        assert_full_span_coverage(formula, &stream.spans);
    }
}

#[test]
fn integration_best_effort_parses_without_strict_tokenizer_regression() {
    assert!(Tokenizer::new("=A1+").is_ok());
    assert!(Tokenizer::new("=A1+)").is_err());

    let stream = TokenStream::new_best_effort("=A1+)");
    assert!(stream.has_errors());
    assert_eq!(stream.invalid_spans().len(), 1);
    assert_eq!(
        (
            stream.invalid_spans()[0].start,
            stream.invalid_spans()[0].end
        ),
        (4, 5)
    );
}

#[test]
fn integration_property_like_best_effort_random_coverage() {
    let alphabet = [
        '=', '(', ')', '{', '}', '[', ']', '!', '#', '+', '-', '*', '/', '^', '&', '<', '>', '=',
        ',', ';', '.', 'A', '1', '2', '3', '4', '5', 'X', 'Y', 'Z', '\'', '"', ' ', '\n',
    ];

    let mut state = 0xDEAD_BEEF_CAFE_u64;
    for _ in 0..128 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        let len = ((state % 32) as usize) + 1;
        let mut formula = String::with_capacity(len);
        let mut cursor = state;
        for _ in 0..len {
            cursor ^= cursor << 5;
            cursor ^= cursor >> 3;
            cursor ^= cursor << 7;
            formula.push(alphabet[(cursor as usize) % alphabet.len()]);
        }

        let stream = TokenStream::new_best_effort(&formula);
        assert_eq!(stream.render_formula(), formula);
        assert_full_span_coverage(&formula, &stream.spans);
    }
}

fn assert_full_span_coverage(formula: &str, spans: &[formualizer_parse::TokenSpan]) {
    let mut covered = vec![false; formula.len()];
    let offset = if formula.starts_with('=') { 1 } else { 0 };

    for span in spans {
        assert!(span.start <= span.end, "invalid span order {:?}", span);
        assert!(span.end <= formula.len(), "span out of bounds {:?}", span);
        for idx in span.start..span.end {
            assert!(!covered[idx], "overlap at {idx} for formula {formula:?}");
            covered[idx] = true;
        }
    }

    if formula.len() > offset {
        assert!(
            covered
                .iter()
                .enumerate()
                .skip(offset)
                .all(|(_, covered)| *covered)
        );
    }
}
