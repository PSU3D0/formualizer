//! `Tokenizer` and `TokenStream` must produce the same tokens and errors.
//!
//! The corpus is every formula literal in this crate's tests plus a list of
//! edge cases (`tests/data/tokenizer_corpus.txt`, one formula per line with
//! `\\`, `\n`, `\r` and `\t` escaped). The expected output is recorded in
//! `tests/data/tokenizer_golden.txt`; regenerate it with
//! `UPDATE_TOKENIZER_GOLDEN=1 cargo test -p formualizer-parse --test tokenizer_equivalence`.

#![cfg(test)]

use std::fmt::Write as _;

use formualizer_parse::{FormulaDialect, Token, TokenStream, Tokenizer, TokenizerError};

const CORPUS: &str = include_str!("data/tokenizer_corpus.txt");
const GOLDEN_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/data/tokenizer_golden.txt"
);

fn unescape(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('\\') => out.push('\\'),
            other => panic!("bad escape {other:?} in corpus line {line:?}"),
        }
    }
    out
}

/// Formulas built in code: resource limits around their boundaries.
fn generated() -> Vec<String> {
    let mut out = Vec::new();
    for n in [8190, 8191, 8192, 8193] {
        out.push(format!("={}1", "1+".repeat(n)));
    }
    for n in [16383, 16384, 16385] {
        out.push(format!("={}", "(".repeat(n)));
        out.push(format!("={}1{}", "(".repeat(n), ")".repeat(n)));
        out.push(format!("={{{}}}", "1,".repeat(n)));
    }
    out.push(format!("={}", "1".repeat(65_535)));
    out.push(format!("={}", "1".repeat(65_536)));
    out
}

fn corpus() -> Vec<String> {
    let mut out: Vec<String> = CORPUS.lines().map(unescape).collect();
    out.extend(generated());
    out
}

fn render_tokens(out: &mut String, items: &[Token]) {
    // Long generated formulas: count plus the last three tokens.
    let shown = if items.len() > 64 {
        writeln!(out, "  {} tokens, ending", items.len()).unwrap();
        &items[items.len().saturating_sub(3)..]
    } else {
        items
    };
    for t in shown {
        writeln!(
            out,
            "  {:?} {:?} {:?} {} {}",
            t.value, t.token_type, t.subtype, t.start, t.end
        )
        .unwrap();
    }
}

fn render_result(out: &mut String, result: Result<Vec<Token>, TokenizerError>) {
    match result {
        Ok(items) => render_tokens(out, &items),
        Err(e) => writeln!(out, "  error {:?} at {}", e.message, e.pos).unwrap(),
    }
}

fn header(formula: &str) -> String {
    if formula.len() > 200 {
        let head: String = formula.chars().take(40).collect();
        format!("{head:?}... ({} bytes)", formula.len())
    } else {
        format!("{formula:?}")
    }
}

const DIALECTS: [FormulaDialect; 2] = [FormulaDialect::Excel, FormulaDialect::OpenFormula];

fn tokenizer_output(formula: &str) -> String {
    let mut out = String::new();
    for dialect in DIALECTS {
        writeln!(out, "{} {dialect:?}", header(formula)).unwrap();
        render_result(
            &mut out,
            Tokenizer::new_with_dialect(formula, dialect).map(|t| t.items),
        );
        let best = Tokenizer::new_best_effort_with_dialect(formula, dialect);
        if let Some(e) = best.admission_error() {
            writeln!(out, "  best-effort rejected {:?} at {}", e.message, e.pos).unwrap();
        } else {
            writeln!(out, " best-effort").unwrap();
            render_tokens(&mut out, &best.items);
        }
    }
    out
}

fn stream_output(formula: &str) -> String {
    let mut out = String::new();
    for dialect in DIALECTS {
        writeln!(out, "{} {dialect:?}", header(formula)).unwrap();
        render_result(
            &mut out,
            TokenStream::new_with_dialect(formula, dialect).map(|s| s.to_tokens()),
        );
        let best = TokenStream::new_best_effort_with_dialect(formula, dialect);
        let best = Tokenizer::from_token_stream(&best);
        if let Some(e) = best.admission_error() {
            writeln!(out, "  best-effort rejected {:?} at {}", e.message, e.pos).unwrap();
        } else {
            writeln!(out, " best-effort").unwrap();
            render_tokens(&mut out, &best.items);
        }
    }
    out
}

#[test]
fn tokenizer_matches_token_stream_on_corpus() {
    let mut differences = Vec::new();
    for formula in corpus() {
        let a = tokenizer_output(&formula);
        let b = stream_output(&formula);
        if a != b {
            differences.push(format!("Tokenizer:\n{a}TokenStream:\n{b}"));
        }
    }
    assert!(
        differences.is_empty(),
        "{} formulas differ; first:\n{}",
        differences.len(),
        differences.first().map(String::as_str).unwrap_or_default()
    );
}

#[test]
fn tokenizer_matches_golden_output() {
    let mut actual = String::new();
    for formula in corpus() {
        actual.push_str(&tokenizer_output(&formula));
    }
    if std::env::var_os("UPDATE_TOKENIZER_GOLDEN").is_some() {
        std::fs::write(GOLDEN_PATH, &actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(GOLDEN_PATH).unwrap();
    if actual != expected {
        let line = actual
            .lines()
            .zip(expected.lines())
            .position(|(a, e)| a != e)
            .unwrap_or_else(|| actual.lines().count().min(expected.lines().count()));
        panic!(
            "tokenizer output differs from {GOLDEN_PATH} at line {}:\n actual:   {:?}\n expected: {:?}",
            line + 1,
            actual.lines().nth(line),
            expected.lines().nth(line)
        );
    }
}

/// `#` after a logical literal is not the spill operator in either case
/// (`TRUE` is not a reference), so both spellings are invalid error literals.
#[test]
fn spill_after_logical_literal_is_rejected_in_any_case() {
    for formula in ["=true#", "=TRUE#", "=False#"] {
        let err = Tokenizer::new(formula).err().expect(formula);
        let hash = formula.len() - 1;
        assert_eq!(
            err.message,
            format!("Invalid error code at position {hash}")
        );
        assert_eq!(err.pos, hash, "{formula}");
        assert!(TokenStream::new(formula).is_err(), "{formula}");
    }
}

#[test]
fn get_closer_matches_each_opener() {
    use formualizer_parse::{TokenSubType, TokenType};
    let tokens = Tokenizer::new("=SUM((1),{2})").unwrap().items;
    let closers: Vec<(String, TokenType, TokenSubType)> = tokens
        .iter()
        .filter(|t| t.subtype == TokenSubType::Open)
        .map(|t| {
            let c = t.get_closer().unwrap();
            (c.value, c.token_type, c.subtype)
        })
        .collect();
    assert_eq!(
        closers,
        [
            (")".to_string(), TokenType::Func, TokenSubType::Close),
            (")".to_string(), TokenType::Paren, TokenSubType::Close),
            ("}".to_string(), TokenType::Array, TokenSubType::Close),
        ]
    );
    let not_opener = &tokens[tokens.len() - 1];
    assert!(not_opener.get_closer().is_err());
}
