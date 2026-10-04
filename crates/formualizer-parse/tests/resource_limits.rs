use formualizer_parse::parser::BatchParser;
use formualizer_parse::{FormulaDialect, Parser, ParserLimits, TokenStream, parse};
fn limits(
    source: usize,
    tokens: usize,
    nodes: usize,
    frames: usize,
    height: usize,
) -> ParserLimits {
    ParserLimits::new(source, tokens, nodes, frames, height).unwrap()
}
#[test]
fn tiny_boundaries() {
    for dialect in [FormulaDialect::Excel, FormulaDialect::OpenFormula] {
        let l = limits(4, 3, 3, 2, 2);
        assert!(
            Parser::builder()
                .dialect(dialect)
                .limits(l)
                .parse("=1+1")
                .is_ok()
        );
        for l in [
            limits(3, 3, 3, 2, 2),
            limits(4, 2, 3, 2, 2),
            limits(4, 3, 2, 2, 2),
            limits(4, 3, 3, 1, 2),
            limits(4, 3, 3, 2, 1),
        ] {
            assert!(
                Parser::builder()
                    .dialect(dialect)
                    .limits(l)
                    .parse("=1+1")
                    .is_err()
            );
        }
    }
    assert!(
        Parser::builder()
            .limits(limits(2, 1, 1, 1, 1))
            .parse("é")
            .is_ok()
    );
    assert!(
        Parser::builder()
            .limits(limits(1, 1, 1, 1, 1))
            .parse("é")
            .is_err()
    );
    assert!(ParserLimits::new(10, 10, 10, 73, 256).is_err());
    assert!(ParserLimits::new(10, 10, 10, 72, 257).is_err());
    for formula in ["=F(,)", "={1,2}", "=F(1,2)"] {
        assert!(
            Parser::builder()
                .limits(limits(100, 100, 3, 72, 2))
                .parse(formula)
                .is_ok()
        );
        assert!(
            Parser::builder()
                .limits(limits(100, 100, 2, 72, 2))
                .parse(formula)
                .is_err()
        );
    }
    assert!(
        Parser::builder()
            .limits(limits(100, 100, 0, 72, 256))
            .parse("=1")
            .is_err()
    );
}
#[test]
fn default_source_token_and_node_boundaries() {
    for bytes in [65535, 65536] {
        assert!(parse("x".repeat(bytes)).is_ok());
    }
    assert!(
        parse("x".repeat(65537))
            .unwrap_err()
            .message
            .contains("source byte")
    );
    // 8,191 leaves plus F are exactly 8,192 nodes and 16,383 tokens.
    let at_nodes = format!("=F({}1)", "1,".repeat(8190));
    assert!(parse(&at_nodes).is_ok());
    let above_nodes = format!("=F({}1)", "1,".repeat(8191));
    let error = Parser::builder()
        .limits(limits(65536, 20000, 8192, 72, 256))
        .parse(&above_nodes)
        .unwrap_err();
    assert!(error.message.contains("AST node limit"));
    assert!(
        parse(above_nodes)
            .unwrap_err()
            .message
            .contains("token limit")
    );
    // Add one retained whitespace span to reach the token ceiling exactly.
    let at_tokens = at_nodes.replacen("=F(", "=F( ", 1);
    assert_eq!(TokenStream::new(&at_tokens).unwrap().len(), 16384);
    assert!(parse(at_tokens).is_ok());
}
#[test]
fn external_spans_are_checked() {
    for (start, end) in [(0, 99), (3, 2), (1, 2)] {
        let mut stream = TokenStream::new("é").unwrap();
        stream.spans[0].start = start;
        stream.spans[0].end = end;
        assert!(
            Parser::from_token_stream(&stream)
                .parse()
                .unwrap_err()
                .message
                .contains("span")
        );
    }
}
#[test]
fn best_effort_resource_diagnostics_and_repeatable_errors() {
    for source in [
        "x".repeat(65537),
        format!("={}1", "1+".repeat(9000)),
        format!("={}", "(".repeat(16385)),
    ] {
        let stream = TokenStream::new_best_effort(&source);
        assert!(!stream.diagnostics_ref().is_empty());
        assert!(
            stream
                .diagnostics_ref()
                .iter()
                .any(|d| d.recovery == formualizer_parse::RecoveryAction::ResourceLimitExceeded)
        );
        let mut parser = Parser::from_token_stream(&stream);
        let first = parser.parse().unwrap_err();
        let second = parser.parse().unwrap_err();
        assert_eq!(first.message, second.message);
        assert!(first.message.contains("limit exceeded"));
    }
}
#[test]
fn cache_does_not_change_outputs_or_limits() {
    let mut batch = BatchParser::builder().cache_capacity(2, 1000).build();
    for formula in ["=1+2", "=A1", "=3", "=1+2", "=A1"] {
        assert_eq!(batch.parse(formula).unwrap(), parse(formula).unwrap());
    }
    let mut batch = BatchParser::builder()
        .cache_capacity(0, 0)
        .limits(limits(4, 3, 3, 2, 2))
        .build();
    assert!(batch.parse("=1+1").is_ok());
    assert!(batch.parse("=1+11").is_err());
}
#[test]
fn node_budgets_are_exact_across_grammar_shapes() {
    use formualizer_parse::ASTNodeType;
    for source in [
        "=1",
        "text",
        "=1+2*3",
        "=F()",
        "=F(,)",
        "=F(1,,3,)",
        "=F()(,)",
        "=SUM((A1,B1)+1)",
        "={1,2;3,4}",
        "=--1%%",
        "=IF(A1>0,IF(A2>0,1,0),0)",
        "=A1:Total",
        "=Table1[@[Amount]]",
    ] {
        let stream = TokenStream::new(source).unwrap();
        let ast = parse(source).unwrap();
        let mut stack = vec![&ast];
        let mut nodes = 0;
        while let Some(node) = stack.pop() {
            nodes += 1;
            match &node.node_type {
                ASTNodeType::UnaryOp { expr, .. } => stack.push(expr),
                ASTNodeType::BinaryOp { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                ASTNodeType::Function { args, .. } => stack.extend(args),
                ASTNodeType::Call { callee, args } => {
                    stack.push(callee);
                    stack.extend(args);
                }
                ASTNodeType::Array(rows) => stack.extend(rows.iter().flatten()),
                _ => {}
            }
        }
        assert!(
            nodes <= stream.len(),
            "{source}: {nodes} nodes / {} tokens",
            stream.len()
        );
        assert!(
            Parser::builder()
                .limits(limits(65536, 16384, nodes, 72, 256))
                .parse(source)
                .is_ok(),
            "{source}"
        );
        assert!(
            Parser::builder()
                .limits(limits(65536, 16384, nodes - 1, 72, 256))
                .parse(source)
                .is_err(),
            "{source}"
        );
    }
}

#[test]
fn resource_stack_subprocess() {
    const CHILD: &str = "FORMUALIZER_RESOURCE_STACK_CHILD";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(|| {
                for source in [
                    format!("={}A1", "A1+".repeat(255)),
                    format!("={}1", "1^".repeat(255)),
                    format!("=1{}", "%".repeat(255)),
                    format!("=F(){}", "()".repeat(255)),
                ] {
                    let ast = parse(&source).unwrap();
                    let cloned = ast.clone();
                    let _ = cloned.calculate_hash();
                    let _ = cloned.get_dependencies();
                    let _ = formualizer_parse::pretty_print(&cloned);
                    drop(cloned);
                    drop(ast);
                    assert!(
                        parse(format!("=({})%", &source[1..]))
                            .unwrap_err()
                            .message
                            .contains("AST height")
                    );
                }
                // Stack ceilings stay effective independently of relaxed work budgets.
                let relaxed = limits(1 << 20, 1 << 20, 1 << 20, 72, 256);
                for source in [
                    format!("={}1{}", "IF(A1>0,".repeat(5000), ",0)".repeat(5000)),
                    format!("={}1{}", "(".repeat(5000), ")".repeat(5000)),
                    format!("={}1", "-".repeat(5000)),
                    format!("={}1{}", "{".repeat(5000), "}".repeat(5000)),
                ] {
                    let error = Parser::builder().limits(relaxed).parse(source).unwrap_err();
                    assert!(error.message.contains("nesting too deep"), "{error}");
                }
                for operator in ["1+", "1^"] {
                    let error = Parser::builder()
                        .limits(relaxed)
                        .parse(format!("={}1", operator.repeat(50_000)))
                        .unwrap_err();
                    assert!(error.message.contains("AST height"), "{error}");
                }
                for chain in ["1^", "1+", "A1 "] {
                    assert!(parse(format!("={}1", chain.repeat(5000))).is_err());
                }
                assert!(parse(format!("=1{}", "%".repeat(5000))).is_err());
                // Error destroys a height-256 partial tree while 71 Pratt frames
                // are still live (rather than only after those frames unwind).
                let source = format!(
                    "={}{}A1+{}",
                    "(".repeat(70),
                    "A1+".repeat(255),
                    ")".repeat(70)
                );
                assert!(parse(source).is_err());
                let source = format!(
                    "={}({}1{})",
                    "A1+".repeat(254),
                    "(".repeat(70),
                    ")".repeat(70)
                );
                assert!(parse(source).is_err());
                // AST-node exhaustion inside active frames with a large flat partial list.
                let source = format!(
                    "=F({}{}1{})",
                    "1,".repeat(8140),
                    "SUM(".repeat(65),
                    ")".repeat(65)
                );
                let error = Parser::builder()
                    .limits(limits(65536, 20000, 8192, 72, 256))
                    .parse(source)
                    .unwrap_err();
                assert!(error.message.contains("AST node"));
            })
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "resource_stack_subprocess", "--nocapture"])
        .env(CHILD, "1")
        .status()
        .unwrap();
    assert!(status.success(), "small-stack subprocess failed: {status}");
}
