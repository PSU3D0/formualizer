//! Differential harness for the two public parser implementations in
//! `formualizer-parse`.
//!
//! See issue PSU3D0/formualizer#77 for context. The crate ships two
//! parsers with subtly different behaviours:
//!
//! - the classic token-based `Parser` (`Parser::new(tokens, ...)`,
//!   `Parser::try_from(&str)`); and
//! - the source-span-backed `SpanParser`, exposed via the free
//!   functions `parse`, `parse_with_dialect`,
//!   `parse_with_volatility_classifier`, and `BatchParser`.
//!
//! This file exists so any drift between the two front-ends becomes
//! visible in CI before a unification PR rewrites `Parser` as a thin
//! forwarder. It deliberately:
//!
//! 1. asserts AST-equality on a corpus of formulas where both parsers
//!    already agree (the bulk of the harness — these guard against
//!    future regressions); and
//! 2. pins the *currently-known* divergences as explicit expectations,
//!    so future fixes can flip them to assertions of equality without
//!    having to re-discover the cases.
//!
//! When a follow-up PR unifies the implementations, divergence cases
//! should be migrated into the `agreeing` corpus.
//!
//! See also `docs/parser-divergences.md` (at the workspace root) for the
//! human-readable catalog.
use formualizer_parse::parse;
use formualizer_parse::parser::{ASTNode, ASTNodeType, Parser, ParserError};
use formualizer_parse::tokenizer::Tokenizer;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn classic(formula: &str, include_whitespace: bool) -> Result<ASTNode, ParserError> {
    let tokenizer = Tokenizer::new(formula).map_err(|e| ParserError {
        message: e.to_string(),
        position: Some(e.pos),
    })?;
    let mut parser = Parser::new(tokenizer.items, include_whitespace);
    parser.parse()
}

fn span(formula: &str) -> Result<ASTNode, ParserError> {
    parse(formula)
}

/// Compare two ASTs for structural equality, ignoring `source_token`
/// (which legitimately differs between the parsers because they build
/// `Token` instances from different sources) and `contains_volatile`
/// (set only when a classifier is supplied).
fn ast_eq(a: &ASTNode, b: &ASTNode) -> bool {
    if a.node_type == b.node_type {
        return true;
    }
    match (&a.node_type, &b.node_type) {
        (ASTNodeType::UnaryOp { op: oa, expr: ea }, ASTNodeType::UnaryOp { op: ob, expr: eb }) => {
            oa == ob && ast_eq(ea, eb)
        }
        (
            ASTNodeType::BinaryOp {
                op: oa,
                left: la,
                right: ra,
            },
            ASTNodeType::BinaryOp {
                op: ob,
                left: lb,
                right: rb,
            },
        ) => oa == ob && ast_eq(la, lb) && ast_eq(ra, rb),
        (
            ASTNodeType::Function { name: na, args: aa },
            ASTNodeType::Function { name: nb, args: ab },
        ) => {
            na == nb && aa.len() == ab.len() && aa.iter().zip(ab.iter()).all(|(x, y)| ast_eq(x, y))
        }
        (ASTNodeType::Array(ra), ASTNodeType::Array(rb)) => {
            ra.len() == rb.len()
                && ra.iter().zip(rb.iter()).all(|(rowa, rowb)| {
                    rowa.len() == rowb.len()
                        && rowa.iter().zip(rowb.iter()).all(|(x, y)| ast_eq(x, y))
                })
        }
        _ => false,
    }
}

fn assert_ast_eq(formula: &str, a: &ASTNode, b: &ASTNode, label_a: &str, label_b: &str) {
    if !ast_eq(a, b) {
        panic!(
            "AST divergence for `{formula}`:\n  {label_a}: {:#?}\n  {label_b}: {:#?}",
            a.node_type, b.node_type
        );
    }
}

// ---------------------------------------------------------------------------
// Corpus that BOTH parsers must agree on
// ---------------------------------------------------------------------------

/// Formulas where the classic and span parsers must produce structurally
/// identical ASTs. Adding a formula here that fails will surface a real
/// regression.
const AGREEING_CORPUS: &[&str] = &[
    // Literals
    "=1",
    "=1.5",
    "=-1",
    "=+1",
    "=1e3",
    "=\"hello\"",
    "=\"\"",
    "=TRUE",
    "=FALSE",
    "=#REF!",
    "=#VALUE!",
    "=#DIV/0!",
    "=#NAME?",
    "=#NULL!",
    "=#NUM!",
    "=#N/A",
    "=#GETTING_DATA",
    "=#ref!",        // lowercase error literal (PR #65 region)
    "=source!#ref!", // sheet-qualified lowercase error literal
    // Simple references
    "=A1",
    "=$A$1",
    "=A1:B2",
    "=$A$1:$B$2",
    "=Sheet1!A1",
    "='Some Sheet'!A1:B2",
    "=Table1[Col]",
    "=NamedRange",
    "=A:A",
    "=1:1",
    // Arithmetic / boolean / comparison
    "=1+2",
    "=1-2",
    "=2*3",
    "=6/3",
    "=2^10",
    "=1+2*3",
    "=(1+2)*3",
    "=A1+B1",
    "=A1*-B1",
    "=A1&B1",
    "=A1=B1",
    "=A1<>B1",
    "=A1<=B1",
    "=A1>=B1",
    "=A1<B1",
    "=A1>B1",
    "=50%",
    "=A1%",
    "=-A1",
    "=--A1",
    "=- -A1",
    "=- -1",
    // Functions
    "=SUM(A1,B1)",
    "=SUM(A1:A10)",
    "=SUM(A1, B1, C1)",
    "=IF(A1>0,\"yes\",\"no\")",
    "=IF(A1>0,B1,IF(C1<0,D1,E1))",
    "=AVERAGE(A1:A10)",
    "=COUNTIF(A1:A10,\">0\")",
    "=VLOOKUP(A1,B1:C10,2,FALSE)",
    "=IFS(A1=1,\"a\",A1=2,\"b\",TRUE,\"c\")",
    "=LET(x,1,y,2,x+y)",
    "=LAMBDA(x,x+1)(2)",
    "=SUM()", // no-arg function
    // Whitespace tolerated by both
    "= 1 + 2",
    "=  SUM(A1,B1)  ",
    "=SUM( A1 , B1 )",
    "=( A1 + B1 )",
    "= ( A1 + B1 ) ",
    "=SUM(A1 )", // trailing space inside arg
    "=( A1 )",
    "= A1 + B1",
    // Arrays
    "={1,2,3}",
    "={1,2;3,4}",
    "={\"a\",\"b\";\"c\",\"d\"}",
    "=SUM({1,2,3})",
    // Mixed
    "=SUM(A1:A10)*COUNT(B1:B10)+IF(C1,1,0)",
    "=A1+SUM(B1:B5)",
    "=Sheet1!A1+Sheet2!B2",
    "='My Sheet'!A1+'Other Sheet'!$B$2",
    "=INDEX(A:A,MATCH(B1,C:C,0))",
    "=A1#",  // spilled-range operator
    "=A1:A", // partial range
    // Unary precedence (post-#65)
    "=-A1^2",
    "=-(A1^2)",
    "=- -A1",
    "=---1",
    // Comparison + arithmetic mixing
    "=A1+B1>=C1*D1",
    // Concatenation
    "=\"a\"&\"b\"&\"c\"",
];

#[test]
fn classic_and_span_agree_on_corpus_no_whitespace_tokens() {
    let mut failures: Vec<String> = Vec::new();
    for formula in AGREEING_CORPUS {
        let c = classic(formula, false);
        let s = span(formula);
        match (&c, &s) {
            (Ok(a), Ok(b)) => {
                if !ast_eq(a, b) {
                    failures.push(format!(
                        "{formula}: AST mismatch\n  classic: {:?}\n  span:    {:?}",
                        a.node_type, b.node_type
                    ));
                }
            }
            (Err(ea), Err(eb)) => {
                // Both errored — accept; specific error messages may differ.
                let _ = (ea, eb);
            }
            (Ok(_), Err(e)) => {
                failures.push(format!("{formula}: classic OK but span Err: {e}"));
            }
            (Err(e), Ok(_)) => {
                failures.push(format!("{formula}: span OK but classic Err: {e}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "differential failures (classic vs span, include_whitespace=false):\n{}",
        failures.join("\n")
    );
}

#[test]
fn classic_with_whitespace_tokens_agrees_with_span_on_corpus() {
    // Most of the corpus also agrees when the classic parser is configured
    // to keep whitespace tokens. The cases listed in
    // `divergence::WS_BREAKS_CLASSIC_WITH_WS_TOKENS` are documented holes
    // and excluded here.
    let mut failures: Vec<String> = Vec::new();
    for formula in AGREEING_CORPUS {
        if divergence::WS_BREAKS_CLASSIC_WITH_WS_TOKENS.contains(formula) {
            continue;
        }
        let c = classic(formula, true);
        let s = span(formula);
        match (&c, &s) {
            (Ok(a), Ok(b)) => {
                if !ast_eq(a, b) {
                    failures.push(format!(
                        "{formula}: AST mismatch (include_whitespace=true)\n  classic: {:?}\n  span:    {:?}",
                        a.node_type, b.node_type
                    ));
                }
            }
            (Err(ea), Err(eb)) => {
                let _ = (ea, eb);
            }
            (Ok(_), Err(e)) => {
                failures.push(format!("{formula}: classic(ws) OK but span Err: {e}"));
            }
            (Err(e), Ok(_)) => {
                failures.push(format!(
                    "{formula}: span OK but classic(ws) Err: {e} — \
                     if intentional, add to divergence::WS_BREAKS_CLASSIC_WITH_WS_TOKENS"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "differential failures (classic include_whitespace=true vs span):\n{}",
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Public-API compatibility smoke tests
// ---------------------------------------------------------------------------

#[test]
fn parser_try_from_str_still_works() {
    let mut p = Parser::try_from("=A1+B1").unwrap();
    let ast = p.parse().unwrap();
    assert!(matches!(ast.node_type, ASTNodeType::BinaryOp { .. }));
}

#[test]
fn parser_new_with_whitespace_tokens_still_works() {
    let tokenizer = Tokenizer::new("= 1 + 2").unwrap();
    let mut p = Parser::new(tokenizer.items, true);
    let ast = p.parse().unwrap();
    match ast.node_type {
        ASTNodeType::BinaryOp { op, .. } => assert_eq!(op, "+"),
        other => panic!("expected BinaryOp, got {other:?}"),
    }
}

#[test]
fn batch_parser_still_works() {
    use formualizer_parse::parser::BatchParser;
    let mut bp = BatchParser::builder().build();
    let a = bp.parse("=A1+B1").unwrap();
    let b = bp.parse("=A1+B1").unwrap(); // cached
    assert!(ast_eq(&a, &b));
    let s = parse("=A1+B1").unwrap();
    assert_ast_eq("=A1+B1", &a, &s, "batch", "span");
}

// ---------------------------------------------------------------------------
// Targeted regression guards (from issue body)
// ---------------------------------------------------------------------------

#[test]
fn regression_sum_with_inner_whitespace() {
    // The issue points out that whitespace before `)` in a function arg
    // list is currently handled in SpanParser but not in classic Parser
    // when whitespace tokens are kept. With include_whitespace=false the
    // classic path filters whitespace before parsing, so both agree.
    let formula = "=SUM( A1 , B1 )";
    let c = classic(formula, false).expect("classic no_ws should parse");
    let s = span(formula).expect("span should parse");
    assert_ast_eq(formula, &c, &s, "classic no_ws", "span");
}

#[test]
fn regression_paren_group_with_inner_and_trailing_whitespace() {
    let formula = "=( A1 + B1 ) ";
    let c = classic(formula, false).expect("classic no_ws should parse");
    let s = span(formula).expect("span should parse");
    assert_ast_eq(formula, &c, &s, "classic no_ws", "span");
}

#[test]
fn regression_classic_path_basic_arith() {
    let formula = "=A1 + B1";
    let c = classic(formula, false).expect("classic no_ws should parse");
    let s = span(formula).expect("span should parse");
    assert_ast_eq(formula, &c, &s, "classic no_ws", "span");
}

// ---------------------------------------------------------------------------
// Pinned divergences — these MUST stay consistent with the divergences doc.
// When a future PR fixes one of these, move the formula into the agreeing
// corpus above and delete the pin.
// ---------------------------------------------------------------------------

mod divergence {
    use super::*;

    /// Classic-with-whitespace-tokens fails to skip whitespace before `)`
    /// in a handful of edge productions. These formulas parse fine via
    /// the span parser and via classic with `include_whitespace=false`,
    /// but error when the classic parser is constructed with
    /// `include_whitespace=true`. They are excluded from
    /// `classic_with_whitespace_tokens_agrees_with_span_on_corpus` and
    /// pinned by the divergence tests below.
    pub(super) const WS_BREAKS_CLASSIC_WITH_WS_TOKENS: &[&str] = &[];

    /// Cases where classic with `include_whitespace=true` currently
    /// errors but the span parser succeeds. Each of these has been
    /// observed by hand on this branch; if any starts succeeding, the
    /// corresponding divergence test will fire and the formula should
    /// be moved into the agreeing corpus.
    const CLASSIC_WS_REJECTS_BUT_SPAN_ACCEPTS: &[&str] = &[
        // Empty arg list with interior whitespace.
        "=SUM(  )",
        // Trailing-comma whitespace before `)`.
        "=SUM(A1, )",
        "=SUM(A1, B1, )",
    ];

    #[test]
    fn divergence_classic_with_ws_tokens_rejects_whitespace_before_close_paren() {
        // The issue calls out that classic's `parse_function_arguments`
        // does not call `skip_whitespace` before checking for `)` (or
        // before checking for the next argument after a comma). Pin
        // representative breakages so they are visible in CI.
        let mut still_broken: Vec<&str> = Vec::new();
        let mut now_accepting: Vec<&str> = Vec::new();
        for &formula in CLASSIC_WS_REJECTS_BUT_SPAN_ACCEPTS {
            let span_ast = span(formula).expect("span parses");
            let classic_no_ws = classic(formula, false).expect("classic no_ws parses");
            assert_ast_eq(formula, &classic_no_ws, &span_ast, "classic no_ws", "span");

            match classic(formula, true) {
                Err(_) => still_broken.push(formula),
                Ok(_) => now_accepting.push(formula),
            }
        }
        assert!(
            now_accepting.is_empty(),
            "classic Parser::new(.., include_whitespace=true) now parses {now_accepting:?}; \
             remove from CLASSIC_WS_REJECTS_BUT_SPAN_ACCEPTS and consider moving to the agreeing corpus"
        );
        assert_eq!(
            still_broken.len(),
            CLASSIC_WS_REJECTS_BUT_SPAN_ACCEPTS.len(),
            "divergence pin out of sync"
        );
    }
}
