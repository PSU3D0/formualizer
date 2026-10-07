use formualizer_parse::{
    Parser, RecoveryAction, TokenSpan, TokenStream, TokenSubType, TokenType, Tokenizer,
};

#[test]
fn overlapping_or_reordered_spans_cannot_amplify_source_copies() {
    let source = format!("=F(\"{}\")", "x".repeat(64_000));
    let mut stream = TokenStream::new(&source).unwrap();
    let literal = stream.spans[1];
    let close = *stream.spans.last().unwrap();
    stream.spans = vec![
        stream.spans[0],
        literal,
        TokenSpan {
            token_type: TokenType::Sep,
            subtype: TokenSubType::Arg,
            start: 2,
            end: 3,
        },
        literal,
        close,
    ];
    let mut parser = Parser::from_token_stream(&stream);
    for _ in 0..2 {
        assert!(
            parser
                .parse()
                .unwrap_err()
                .message
                .contains("span sequence")
        );
    }
    let owned = Tokenizer::from_token_stream(&stream);
    assert!(
        owned
            .admission_error()
            .unwrap()
            .message
            .contains("span sequence")
    );
    assert!(owned.items.is_empty());

    let mut stream = TokenStream::new("=1+2").unwrap();
    stream.spans.swap(0, 2);
    assert!(Parser::from_token_stream(&stream).parse().is_err());
}

#[test]
fn trusted_lexer_spans_are_ordered_and_disjoint() {
    for source in [
        "=A1:SUM(B1,B2)",
        "=A1:Total",
        "=A1:#REF!",
        "=#REF!#REF!",
        "=SUM((A1,B1)+1)",
        "={1,2;3,4}",
        "=LAMBDA(x,x+1)(A1)",
        "='Jan 24:Mar 24'!B5",
        "=Table1[[#Data],[Amount]]",
        "=A1 B1",
        "=SUM(,A1,,)",
        "=\"é夏\"",
        "=SUM(\"a\"\"b\",1)",
    ] {
        let stream = TokenStream::new(source).unwrap();
        let mut previous_end = 0;
        for span in stream.spans {
            assert!(span.start >= previous_end, "{source}: {span:?}");
            assert!(source.get(span.start..span.end).is_some());
            previous_end = span.end;
        }
    }
}

#[test]
fn owned_best_effort_and_stream_adapters_report_admission_failure() {
    for source in ["x".repeat(65_537), format!("={}1", "1+".repeat(9000))] {
        let owned = Tokenizer::new_best_effort(&source);
        assert!(owned.admission_error().unwrap().message.contains("limit"));
        assert!(owned.items.is_empty());
        assert!(owned.render().is_empty());
    }
    assert!(
        Tokenizer::new_best_effort("=1+2")
            .admission_error()
            .is_none()
    );
    let mut stream = TokenStream::new("=1").unwrap();
    stream.spans = vec![stream.spans[0]; 16_385];
    let owned = Tokenizer::from_token_stream(&stream);
    assert!(
        owned
            .admission_error()
            .unwrap()
            .message
            .contains("token limit")
    );
    assert!(owned.items.is_empty());
}

#[test]
fn refused_recovery_emission_does_not_create_an_orphan_syntax_diagnostic() {
    let source = format!("={})", "1+".repeat(8192));
    let stream = TokenStream::new_best_effort(&source);
    assert!(
        stream
            .diagnostics_ref()
            .iter()
            .any(|d| d.recovery == RecoveryAction::ResourceLimitExceeded)
    );
    for diagnostic in stream
        .diagnostics_ref()
        .iter()
        .filter(|d| d.recovery != RecoveryAction::ResourceLimitExceeded)
    {
        assert!(stream.spans.contains(&diagnostic.span), "{diagnostic:?}");
    }
}
