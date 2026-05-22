//! Public parser entrypoint parity tests.
//!
//! Issue PSU3D0/formualizer#77 consolidated the old token-based parser and the
//! source-span parser into a single canonical `Parser`. These tests make sure the
//! supported public calling patterns continue to produce the same AST shape.

use std::str::FromStr;

use formualizer_parse::parser::{ASTNode, ASTNodeType, BatchParser, Parser};
use formualizer_parse::tokenizer::TokenStream;
use formualizer_parse::{LiteralValue, parse};

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
    assert!(
        ast_eq(a, b),
        "AST divergence for `{formula}`:\n  {label_a}: {:#?}\n  {label_b}: {:#?}",
        a.node_type,
        b.node_type
    );
}

const CORPUS: &[&str] = &[
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
    "=#ref!",
    "=source!#ref!",
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
    "=SUM(A1,B1)",
    "=SUM(A1:A10)",
    "=SUM(A1, B1, C1)",
    "=IF(A1>0,\"yes\",\"no\")",
    "=IF(A1>0,B1,IF(C1<0,D1,E1))",
    "=AVERAGE(A1:A10)",
    "=COUNTIF(A1:A10,\">0\")",
    "=VLOOKUP(A1,B1:C10,2,FALSE)",
    "=VLOOKUP(,A1:C10,2,FALSE)",
    "=IFS(A1=1,\"a\",A1=2,\"b\",TRUE,\"c\")",
    "=LET(x,1,y,2,x+y)",
    "=LAMBDA(x,x+1)(2)",
    "=SUM()",
    "=SUM(  )",
    "=SUM(A1, )",
    "=SUM(A1, B1, )",
    "=FOO(,A1:C3,TRUE,A13)",
    "=FOO(,A1:C3)",
    "=FOO(A1,,B2)",
    "=FOO(,,A1)",
    "=FOO(,)",
    "= 1 + 2",
    "=  SUM(A1,B1)  ",
    "=SUM( A1 , B1 )",
    "=( A1 + B1 )",
    "= ( A1 + B1 ) ",
    "=SUM(A1 )",
    "=( A1 )",
    "= A1 + B1",
    "={1,2,3}",
    "={1,2;3,4}",
    "={\"a\",\"b\";\"c\",\"d\"}",
    "=SUM({1,2,3})",
    "=SUM(A1:A10)*COUNT(B1:B10)+IF(C1,1,0)",
    "=A1+SUM(B1:B5)",
    "=Sheet1!A1+Sheet2!B2",
    "='My Sheet'!A1+'Other Sheet'!$B$2",
    "=INDEX(A:A,MATCH(B1,C:C,0))",
    "=A1#",
    "=A1:A",
    "=-A1^2",
    "=-(A1^2)",
    "=---1",
    "=A1+B1>=C1*D1",
    "=\"a\"&\"b\"&\"c\"",
];

#[test]
fn public_entrypoints_agree_on_corpus() {
    let mut batch = BatchParser::builder().build();

    for formula in CORPUS {
        let direct = parse(formula).unwrap_or_else(|e| panic!("parse failed for {formula}: {e}"));

        let mut parser = Parser::new(formula).unwrap_or_else(|e| panic!("Parser::new failed: {e}"));
        let parser_ast = parser
            .parse()
            .unwrap_or_else(|e| panic!("Parser::parse failed: {e}"));
        assert_ast_eq(formula, &direct, &parser_ast, "parse", "Parser::new");

        let mut parser = Parser::try_from(*formula).expect("Parser::try_from");
        let try_from_parser_ast = parser.parse().expect("Parser::try_from parse");
        assert_ast_eq(
            formula,
            &direct,
            &try_from_parser_ast,
            "parse",
            "Parser::try_from",
        );

        let from_str_ast = ASTNode::from_str(formula).expect("ASTNode::from_str");
        assert_ast_eq(formula, &direct, &from_str_ast, "parse", "FromStr");

        let try_from_ast = ASTNode::try_from(*formula).expect("ASTNode::try_from");
        assert_ast_eq(
            formula,
            &direct,
            &try_from_ast,
            "parse",
            "ASTNode::try_from",
        );

        let stream = TokenStream::new(formula).expect("TokenStream::new");
        let mut stream_parser = Parser::from_token_stream(&stream);
        let stream_ast = stream_parser.parse().expect("TokenStream parser");
        assert_ast_eq(formula, &direct, &stream_ast, "parse", "TokenStream");

        let batch_ast = batch.parse(formula).expect("BatchParser");
        assert_ast_eq(formula, &direct, &batch_ast, "parse", "BatchParser");
    }
}

#[test]
fn parser_builder_supports_dialect_and_volatility_classifier() {
    let ast = Parser::builder()
        .with_volatility_classifier(|name| name.eq_ignore_ascii_case("RAND"))
        .parse("=RAND()+A1")
        .unwrap();
    assert!(ast.contains_volatile());
}

#[test]
fn leading_empty_argument_followed_by_argument_parses() {
    let ast = parse("=VLOOKUP(,A1:C10,2,FALSE)").unwrap();
    match ast.node_type {
        ASTNodeType::Function { name, args } => {
            assert_eq!(name, "VLOOKUP");
            assert_eq!(args.len(), 4);
            assert!(matches!(
                &args[0].node_type,
                ASTNodeType::Literal(LiteralValue::Text(s)) if s.is_empty()
            ));
        }
        other => panic!("expected Function, got {other:?}"),
    }
}

#[test]
fn comma_only_empty_arguments_preserve_arity() {
    let ast = parse("=FOO(,)").unwrap();
    match ast.node_type {
        ASTNodeType::Function { args, .. } => {
            assert_eq!(args.len(), 2);
            assert!(args.iter().all(|arg| matches!(
                &arg.node_type,
                ASTNodeType::Literal(LiteralValue::Text(s)) if s.is_empty()
            )));
        }
        other => panic!("expected Function, got {other:?}"),
    }
}
