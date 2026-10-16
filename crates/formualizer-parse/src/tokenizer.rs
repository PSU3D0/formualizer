use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{self, Display};
use std::sync::Arc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::types::FormulaDialect;

const TOKEN_ENDERS: &str = ",;}) +-*/^&=><%@";

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "const fn evaluated at compile time: a u8 index into a 256-entry table and a cursor bounded by the slice length"
)]
const fn build_token_enders() -> [bool; 256] {
    let mut tbl = [false; 256];
    let bytes = TOKEN_ENDERS.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        tbl[bytes[i] as usize] = true;
        i += 1;
    }
    tbl
}
static TOKEN_ENDERS_TABLE: [bool; 256] = build_token_enders();

#[inline(always)]
#[expect(clippy::indexing_slicing, reason = "u8 index into a 256-entry table")]
fn is_token_ender(c: u8) -> bool {
    TOKEN_ENDERS_TABLE[c as usize]
}

// Recognised Excel error literals. The lookup matches an exact-length slice
// using `eq_ignore_ascii_case`, so ordering only matters when one entry is a
// prefix of another (none currently are). We keep entries grouped by era and
// sorted longest-first defensively so future additions don't introduce
// prefix-collision ambiguity.
//
// Modern (Excel 2018+) literals currently recognised here are limited to the
// ones whose `ExcelErrorKind` already exists in `formualizer-common`:
//   - `#SPILL!` (dynamic-array spill blocked)
//   - `#CALC!`  (generic calc-engine error)
// Other modern literals (`#FIELD!`, `#BLOCKED!`, `#CONNECT!`, `#UNKNOWN!`,
// `#EXTERNAL!`, `#BUSY!`, `#PYTHON!`) are intentionally not recognised yet;
// adding them requires a coordinated change in `ExcelErrorKind` and all
// downstream exhaustive matches/bindings/serde/display.
//
// `#GETTING_DATA` is kept for OOXML/legacy compatibility.
static ERROR_CODES: &[&str] = &[
    "#GETTING_DATA",
    "#DIV/0!",
    "#VALUE!",
    "#SPILL!",
    "#NAME?",
    "#NULL!",
    "#CALC!",
    "#NUM!",
    "#REF!",
    "#N/A",
];

/// Represents operator associativity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Associativity {
    Left,
    Right,
}

/// A custom error type for the tokenizer.
#[derive(Debug)]
pub struct TokenizerError {
    pub message: String,
    pub pos: usize,
}

/// Recovering action taken for a malformed span.
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryAction {
    /// Tokenization stopped at a resource budget; source may have been discarded.
    ResourceLimitExceeded,
    /// Unmatched closer was emitted as a recovery span and scanning continued.
    SkippedUnmatchedCloser,
    /// Unterminated string was emitted as one recovery span.
    UnterminatedString,
    /// Unmatched opening '[' was emitted as a recovery span.
    UnmatchedBracket,
    /// Invalid # literal was emitted as a recovery span.
    InvalidErrorLiteral,
    /// Unmatched opener was recorded at end-of-input.
    UnmatchedOpener,
}

/// Token-level diagnostic emitted by best-effort tokenization.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenDiagnostic {
    pub span: TokenSpan,
    pub message: String,
    pub recovery: RecoveryAction,
}

impl TokenDiagnostic {
    fn resource_limit(message: String) -> Self {
        Self::new(
            TokenSpan {
                token_type: TokenType::Literal,
                subtype: TokenSubType::None,
                start: 0,
                end: 0,
            },
            message,
            RecoveryAction::ResourceLimitExceeded,
        )
    }
    fn new(span: TokenSpan, message: String, recovery: RecoveryAction) -> Self {
        Self {
            span,
            message,
            recovery,
        }
    }
}

#[derive(Debug, Clone)]
struct SpanTokenizerError {
    kind: SpanTokenizerErrorKind,
    pos: usize,
    message: String,
    span_start: Option<usize>,
    span_end: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
enum SpanTokenizerErrorKind {
    NoMatchingOpener,
    UnmatchedOpening,
    UnterminatedString,
    UnmatchedBracket,
    MismatchedPair,
    InvalidErrorLiteral,
}

impl From<SpanTokenizerError> for TokenizerError {
    fn from(value: SpanTokenizerError) -> Self {
        TokenizerError {
            message: value.message,
            pos: value.pos,
        }
    }
}

impl fmt::Display for TokenizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TokenizerError: {}", self.message)
    }
}

impl Error for TokenizerError {}

/// The type of a token.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenType {
    Literal,
    Operand,
    Func,
    Array,
    Paren,
    Sep,
    OpPrefix,
    OpInfix,
    OpPostfix,
    Whitespace,
}
impl Display for TokenType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// The subtype of a token.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenSubType {
    None,
    Text,
    Number,
    Logical,
    Error,
    Range,
    Open,
    Close,
    Arg,
    Row,
}
impl Display for TokenSubType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// A token in an Excel formula.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Token {
    pub value: String, // We'll keep this for API compatibility but compute it lazily
    pub token_type: TokenType,
    pub subtype: TokenSubType,
    pub start: usize,
    pub end: usize,
}

impl Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<{} subtype: {:?} value: {}>",
            self.token_type, self.subtype, self.value
        )
    }
}

impl Token {
    pub fn new(value: String, token_type: TokenType, subtype: TokenSubType) -> Self {
        Token {
            value,
            token_type,
            subtype,
            start: 0,
            end: 0,
        }
    }

    pub fn new_with_span(
        value: String,
        token_type: TokenType,
        subtype: TokenSubType,
        start: usize,
        end: usize,
    ) -> Self {
        Token {
            value,
            token_type,
            subtype,
            start,
            end,
        }
    }

    pub fn is_operator(&self) -> bool {
        matches!(
            self.token_type,
            TokenType::OpPrefix | TokenType::OpInfix | TokenType::OpPostfix
        )
    }

    pub fn get_precedence(&self) -> Option<(u8, Associativity)> {
        // For a prefix operator, use the 'u' key.
        let op = if self.token_type == TokenType::OpPrefix {
            "u"
        } else {
            self.value.as_str()
        };

        // Higher number => tighter binding.
        // Excel precedence (high to low, simplified):
        //   reference ops (:
        //   postfix %
        //   prefix unary +/- (binds tighter than ^)
        //   exponent ^ (left-assoc, as in Excel: 2^3^2 = (2^3)^2)
        //   */
        //   +-
        //   &
        //   comparisons
        match op {
            "#" => Some((11, Associativity::Left)),
            ":" => Some((10, Associativity::Left)),
            " " => Some((9, Associativity::Left)),
            "," => Some((8, Associativity::Left)),
            "%" => Some((7, Associativity::Left)),
            "u" => Some((6, Associativity::Right)),
            "^" => Some((5, Associativity::Left)),
            "*" | "/" => Some((4, Associativity::Left)),
            "+" | "-" => Some((3, Associativity::Left)),
            "&" => Some((2, Associativity::Left)),
            "=" | "<" | ">" | "<=" | ">=" | "<>" => Some((1, Associativity::Left)),
            _ => None,
        }
    }

    /// Create an operand token based on the value.
    pub fn make_operand(value: String) -> Self {
        let subtype = if value.starts_with('"') {
            TokenSubType::Text
        } else if value.starts_with('#') {
            TokenSubType::Error
        } else if value.eq_ignore_ascii_case("TRUE") || value.eq_ignore_ascii_case("FALSE") {
            TokenSubType::Logical
        } else if value.parse::<f64>().is_ok() {
            TokenSubType::Number
        } else {
            TokenSubType::Range
        };
        Token::new(value, TokenType::Operand, subtype)
    }

    /// Create an operand token with byte position span.
    pub fn make_operand_with_span(value: String, start: usize, end: usize) -> Self {
        let subtype = if value.starts_with('"') {
            TokenSubType::Text
        } else if value.starts_with('#') {
            TokenSubType::Error
        } else if value.eq_ignore_ascii_case("TRUE") || value.eq_ignore_ascii_case("FALSE") {
            TokenSubType::Logical
        } else if value.parse::<f64>().is_ok() {
            TokenSubType::Number
        } else {
            TokenSubType::Range
        };
        Token::new_with_span(value, TokenType::Operand, subtype, start, end)
    }

    /// Given an opener token, return its corresponding closer token.
    pub fn get_closer(&self) -> Result<Token, TokenizerError> {
        if self.subtype != TokenSubType::Open {
            return Err(TokenizerError {
                message: "Token is not an opener".to_string(),
                pos: 0,
            });
        }
        let (value, token_type) = match self.token_type {
            TokenType::Func => (")", TokenType::Func),
            TokenType::Array => ("}", TokenType::Array),
            _ => (")", TokenType::Paren),
        };
        Ok(Token::new(
            value.to_string(),
            token_type,
            TokenSubType::Close,
        ))
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenSpan {
    pub token_type: TokenType,
    pub subtype: TokenSubType,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenView<'a> {
    pub span: &'a TokenSpan,
    pub value: &'a str,
}

/// Source-backed token stream (span-only).
///
/// This is intended as a high-performance representation for callers that
/// want to avoid allocating a `String` per token. It can materialize owned
/// `Token`s when needed (FFI/debug).
#[derive(Debug, Clone)]
pub struct TokenStream {
    pub(crate) pending_error: Option<String>,
    source: Arc<str>,
    pub spans: Vec<TokenSpan>,
    dialect: FormulaDialect,
    diagnostics: Vec<TokenDiagnostic>,
}

fn is_whitespace_intersection_span(source: &str, span: &TokenSpan) -> bool {
    if span.token_type != TokenType::OpInfix {
        return false;
    }
    let Some(value) = source.get(span.start..span.end) else {
        return false;
    };
    value.as_bytes().contains(&b' ')
        && value
            .as_bytes()
            .iter()
            .all(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
}

impl TokenStream {
    pub fn new(formula: &str) -> Result<Self, TokenizerError> {
        Self::new_with_dialect(formula, FormulaDialect::Excel)
    }

    pub fn new_with_dialect(
        formula: &str,
        dialect: FormulaDialect,
    ) -> Result<Self, TokenizerError> {
        let spans = tokenize_spans_with_dialect(formula, dialect)?;
        let source: Arc<str> = Arc::from(formula);
        Ok(TokenStream {
            pending_error: None,
            source,
            spans,
            dialect,
            diagnostics: Vec::new(),
        })
    }

    pub fn new_best_effort(formula: &str) -> Self {
        Self::new_best_effort_with_dialect(formula, FormulaDialect::Excel)
    }

    pub fn new_best_effort_with_dialect(formula: &str, dialect: FormulaDialect) -> Self {
        if let Err(error) = crate::ParserLimits::default().check_source(formula) {
            return TokenStream {
                source: Arc::from(""),
                spans: Vec::new(),
                dialect,
                diagnostics: vec![TokenDiagnostic::resource_limit(error.message.clone())],
                pending_error: Some(error.message),
            };
        }
        let source: Arc<str> = Arc::from(formula);
        let mut tokenizer = SpanTokenizer::new(source.as_ref(), dialect);
        let spans = tokenizer.parse_best_effort();
        let pending_error = tokenizer.exceeded.then(|| {
            format!(
                "Formula token limit exceeded (max {})",
                tokenizer.token_limit
            )
        });
        let mut diagnostics = tokenizer.diagnostics;
        if let Some(message) = &pending_error {
            diagnostics.push(TokenDiagnostic::resource_limit(message.clone()));
        }
        TokenStream {
            pending_error,
            source,
            spans,
            dialect,
            diagnostics,
        }
    }

    /// Admission for external mutable streams, before any owned materialization.
    pub(crate) fn admission_error(&self, limits: crate::ParserLimits) -> Option<TokenizerError> {
        if let Some(message) = &self.pending_error {
            return Some(TokenizerError {
                message: message.clone(),
                pos: 0,
            });
        }
        if let Err(error) = limits.check_source(&self.source) {
            return Some(error);
        }
        if self.spans.len() > limits.tokens() {
            return Some(TokenizerError {
                message: format!("Formula token limit exceeded (max {})", limits.tokens()),
                pos: 0,
            });
        }
        let mut previous_end = 0;
        for span in &self.spans {
            // Disjoint ordered spans prevent repeating a large source slice
            // thousands of times into otherwise shallow, budget-compliant ASTs.
            if span.start < previous_end || self.source.get(span.start..span.end).is_none() {
                return Some(TokenizerError {
                    message: "Invalid external token span sequence".into(),
                    pos: span.start,
                });
            }
            previous_end = span.end;
        }
        None
    }

    pub fn diagnostics(&self) -> Vec<TokenDiagnostic> {
        self.diagnostics.clone()
    }

    pub fn diagnostics_ref(&self) -> &[TokenDiagnostic] {
        &self.diagnostics
    }

    pub fn has_errors(&self) -> bool {
        !self.diagnostics.is_empty()
    }

    pub fn invalid_spans_iter(&self) -> impl Iterator<Item = &TokenSpan> {
        self.spans.iter().filter(|span| {
            self.diagnostics.iter().any(|diag| {
                diag.span.start == span.start
                    && diag.span.end == span.end
                    && diag.span.token_type == span.token_type
            })
        })
    }

    pub fn invalid_spans(&self) -> Vec<&TokenSpan> {
        self.invalid_spans_iter().collect()
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn dialect(&self) -> FormulaDialect {
        self.dialect
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<TokenView<'_>> {
        let span = self.spans.get(index)?;
        let value = self.source.get(span.start..span.end)?;
        Some(TokenView { span, value })
    }

    pub fn to_tokens(&self) -> Vec<Token> {
        tokens_from_spans(&self.source, &self.spans)
    }

    /// Reconstruct the tokenized payload from spans.
    ///
    /// For formulas that start with '=', this intentionally omits the leading
    /// '=' to preserve historical `TokenStream::render()` behavior.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(self.source.len());
        for span in &self.spans {
            if let Some(s) = self.source.get(span.start..span.end) {
                out.push_str(s);
            }
        }
        out
    }

    /// Reconstruct the full input formula, including a leading '=' when present.
    pub fn render_formula(&self) -> String {
        if self.source.as_bytes().first() == Some(&b'=') {
            format!("={}", self.render())
        } else {
            self.render()
        }
    }
}

/// Owned tokens for `spans` over `source`. A whitespace run that is the
/// intersection operator gets the value `" "`.
fn tokens_from_spans(source: &str, spans: &[TokenSpan]) -> Vec<Token> {
    spans
        .iter()
        .map(|s| {
            let value = if is_whitespace_intersection_span(source, s) {
                " ".to_string()
            } else {
                source.get(s.start..s.end).unwrap_or_default().to_string()
            };
            Token::new_with_span(value, s.token_type, s.subtype, s.start, s.end)
        })
        .collect()
}

pub(crate) fn tokenize_spans_with_dialect(
    formula: &str,
    dialect: FormulaDialect,
) -> Result<Vec<TokenSpan>, TokenizerError> {
    tokenize_spans_with_limits(formula, dialect, crate::ParserLimits::default())
}
pub(crate) fn tokenize_spans_with_limits(
    formula: &str,
    dialect: FormulaDialect,
    limits: crate::ParserLimits,
) -> Result<Vec<TokenSpan>, TokenizerError> {
    limits.check_source(formula)?;
    let mut tokenizer = SpanTokenizer::new_with_limit(formula, dialect, limits.tokens());
    tokenizer.parse()?;
    Ok(tokenizer.spans)
}

fn operand_subtype(value_str: &str) -> TokenSubType {
    if value_str.starts_with('"') {
        TokenSubType::Text
    } else if value_str.starts_with('#') {
        TokenSubType::Error
    } else if value_str.eq_ignore_ascii_case("TRUE") || value_str.eq_ignore_ascii_case("FALSE") {
        TokenSubType::Logical
    } else if value_str.parse::<f64>().is_ok() {
        TokenSubType::Number
    } else {
        TokenSubType::Range
    }
}

/// `$`? letters `$`? digits, with at least one letter and one digit.
fn is_cell_reference_like(value: &str) -> bool {
    let col = value.strip_prefix('$').unwrap_or(value);
    let after_letters = col.trim_start_matches(|c: char| c.is_ascii_alphabetic());
    if after_letters.len() == col.len() {
        return false;
    }
    let row = after_letters.strip_prefix('$').unwrap_or(after_letters);
    !row.is_empty() && row.bytes().all(|b| b.is_ascii_digit())
}

fn reference_value_contains_range_colon(value: &str) -> bool {
    let value_part = value
        .rsplit_once('!')
        .map_or(value, |(_, value_part)| value_part);
    value_part.contains(':')
}

/// Whether a pending operand contains a structured-reference bracket
/// (`Table1[Col]`, `Sheet1!Table1[Col]`). A bracket that only appears in the
/// sheet qualifier is an external workbook (`[1]Sheet1!A1`,
/// `'[Book.xlsx]Sheet 1'!A1`), whose `:` continues the same range reference.
fn value_has_structured_reference_bracket(value: &str) -> bool {
    value
        .rsplit_once('!')
        .map_or(value, |(_, value_part)| value_part)
        .contains('[')
}

/// The byte offset of a range `:` inside an accumulated function-name token,
/// as in `B10:INDEX(` or `Sheet1!B10:OFFSET(`. No function name contains a
/// colon, so the text before it is the left end of a range and the call is
/// the right end: `B10`, `:`, `INDEX(`.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "byte offset from ASCII delimiter search: bang and colon are indices of bytes inside value"
)]
fn range_colon_before_call(value: &str) -> Option<usize> {
    let name_start = value.rfind('!').map_or(0, |bang| bang + 1);
    let colon = name_start + value.get(name_start..)?.rfind(':')?;
    (colon > 0 && colon + 1 < value.len()).then_some(colon)
}

/// Whether `piece` can be one end of an A1 range: a cell (`B5`, `$B$5`), a
/// column (`B`, `$XFD`) or a row (`5`, `$5`) inside the grid.
#[expect(
    clippy::arithmetic_side_effects,
    clippy::string_slice,
    reason = "cursor bounded by slice length; at most 3 letters are accumulated (col <= 18_278), each checked alphabetic before its offset is subtracted; the digit run is sliced at ASCII positions"
)]
fn is_a1_range_end(piece: &str) -> bool {
    let bytes = piece.as_bytes();
    let mut i = 0;
    if bytes.get(i) == Some(&b'$') {
        i += 1;
    }
    let letters_start = i;
    let mut col: u32 = 0;
    while let Some(&b) = bytes.get(i)
        && b.is_ascii_alphabetic()
        && i - letters_start < 3
    {
        col = col * 26 + u32::from(b.to_ascii_uppercase() - b'A' + 1);
        i += 1;
    }
    let has_letters = i > letters_start;
    if has_letters && (col > 16_384 || bytes.get(i).is_some_and(u8::is_ascii_alphabetic)) {
        return false;
    }
    if has_letters && bytes.get(i) == Some(&b'$') {
        i += 1;
    }
    let digits_start = i;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
    }
    let digits = &piece[digits_start..i];
    let row_ok = digits.is_empty()
        || (digits.len() <= 7
            && digits
                .parse::<u32>()
                .is_ok_and(|r| (1..=1_048_576).contains(&r)));
    i == bytes.len() && (has_letters || !digits.is_empty()) && row_ok
}

/// The byte offset of a `:` inside an accumulated operand that is the range
/// operator between two operands rather than part of one A1 range. Excel
/// reads `A1:Finish` as the cell A1, `:`, and the name `Finish`, and
/// `Seed_1:Seed_4` as two names, because neither end can be an A1 range end.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "byte offset from ASCII delimiter search: start and the colon are positions inside value"
)]
fn range_operator_colon(value: &str) -> Option<usize> {
    let start = value.rfind('!').map_or(0, |bang| bang + 1);
    let part = value.get(start..)?;
    if part.contains(['[', '\'', '"', '#']) || part.matches(':').count() != 1 {
        return None;
    }
    let (left, right) = part.split_once(':')?;
    if left.is_empty() || right.is_empty() || (is_a1_range_end(left) && is_a1_range_end(right)) {
        return None;
    }
    Some(start + left.len())
}

/// Whether a `#REF!` error literal starts at `offset`.
fn ref_error_starts_at(formula: &str, offset: usize) -> bool {
    formula
        .as_bytes()
        .get(offset..)
        .and_then(|rest| rest.get(..5))
        .is_some_and(|s| s.eq_ignore_ascii_case(b"#REF!"))
}

fn is_reference_operand_value(value: &str) -> bool {
    operand_subtype(value) == TokenSubType::Range
        && (reference_value_contains_range_colon(value)
            || value.contains('!')
            || value.contains('[')
            || is_cell_reference_like(value))
}

fn next_starts_reference_expression(formula: &str, offset: usize) -> bool {
    let Some(rest) = formula.as_bytes().get(offset..) else {
        return false;
    };
    rest.iter()
        .find(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
        .is_some_and(|&b| matches!(b, b'(' | b'[' | b'\'' | b'$') || b.is_ascii_alphabetic())
}

fn next_reference_has_sheet_qualifier(formula: &str, offset: usize) -> bool {
    let Some(rest) = formula.as_bytes().get(offset..) else {
        return false;
    };
    let mut bytes = rest
        .iter()
        .copied()
        .skip_while(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
        .peekable();

    let mut in_quote = false;
    while let Some(b) = bytes.next() {
        match b {
            b'\'' => {
                if in_quote && bytes.peek() == Some(&b'\'') {
                    bytes.next();
                    continue;
                }
                in_quote = !in_quote;
            }
            b'!' => return true,
            b':' if !in_quote => return false,
            b',' | b';' | b'}' | b')' | b' ' | b'\t' | b'\r' | b'\n' | b'+' | b'-' | b'*'
            | b'/' | b'^' | b'&' | b'=' | b'>' | b'<' | b'%' | b'@'
                if !in_quote =>
            {
                return false;
            }
            _ => {}
        }
    }

    false
}

/// Byte-level formula tokenizer producing [`TokenSpan`]s.
///
/// Invariant: `token_start <= token_end <= offset <= formula.len()` outside
/// the body of a step, and every stored offset is 0, `formula.len()`, the
/// index of an ASCII byte or one past one (non-ASCII bytes only advance
/// `offset` in the accumulate arm), so every offset is a char boundary. The
/// `expect` attributes below cite this invariant.
struct SpanTokenizer<'a> {
    formula: &'a str,
    spans: Vec<TokenSpan>,
    token_stack: Vec<TokenSpan>,
    offset: usize,
    token_start: usize,
    token_end: usize,
    dialect: FormulaDialect,
    diagnostics: Vec<TokenDiagnostic>,
    token_limit: usize,
    exceeded: bool,
}

impl<'a> SpanTokenizer<'a> {
    fn new(formula: &'a str, dialect: FormulaDialect) -> Self {
        Self::new_with_limit(formula, dialect, crate::ParserLimits::default().tokens())
    }
    fn new_with_limit(formula: &'a str, dialect: FormulaDialect, token_limit: usize) -> Self {
        SpanTokenizer {
            formula,
            spans: Vec::with_capacity((formula.len() / 2).min(token_limit)),
            token_stack: Vec::with_capacity(16),
            offset: 0,
            token_start: 0,
            token_end: 0,
            dialect,
            diagnostics: Vec::new(),
            token_limit,
            exceeded: false,
        }
    }

    #[inline]
    fn current_byte(&self) -> Option<u8> {
        self.formula.as_bytes().get(self.offset).copied()
    }

    #[inline]
    fn has_token(&self) -> bool {
        self.token_end > self.token_start
    }

    #[inline]
    fn start_token(&mut self) {
        self.token_start = self.offset;
        self.token_end = self.offset;
    }

    #[inline]
    fn extend_token(&mut self) {
        self.token_end = self.offset;
    }

    /// The accumulated token text.
    #[inline]
    #[expect(
        clippy::string_slice,
        reason = "SpanTokenizer invariant: token_start <= token_end <= formula.len(), and every stored offset is 0, the length, the index of an ASCII byte or one past one, so it is a char boundary"
    )]
    fn token_str(&self) -> &'a str {
        &self.formula[self.token_start..self.token_end]
    }

    /// The source text between two offsets of this tokenizer.
    #[inline]
    #[expect(
        clippy::string_slice,
        reason = "SpanTokenizer invariant: token_start <= token_end <= formula.len(), and every stored offset is 0, the length, the index of an ASCII byte or one past one, so it is a char boundary"
    )]
    fn source_str(&self, start: usize, end: usize) -> &'a str {
        &self.formula[start..end]
    }

    fn push_span(
        &mut self,
        token_type: TokenType,
        subtype: TokenSubType,
        start: usize,
        end: usize,
    ) {
        if self.spans.len() >= self.token_limit {
            self.exceeded = true;
            return;
        }
        self.spans.push(TokenSpan {
            token_type,
            subtype,
            start,
            end,
        });
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "byte offset from ASCII delimiter search: colon is the index of a `:` inside the token, so colon + 1 <= token_end"
    )]
    fn save_token(&mut self) {
        if self.has_token() {
            if let Some(colon) = range_operator_colon(self.token_str()) {
                let colon = self.token_start + colon;
                let left = operand_subtype(self.source_str(self.token_start, colon));
                self.push_span(TokenType::Operand, left, self.token_start, colon);
                self.push_span(TokenType::OpInfix, TokenSubType::None, colon, colon + 1);
                self.token_start = colon + 1;
            }
            let subtype = operand_subtype(self.token_str());
            self.push_span(
                TokenType::Operand,
                subtype,
                self.token_start,
                self.token_end,
            );
        }
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: current_byte() returned Some, so offset < len"
    )]
    fn check_scientific_notation(&mut self) -> bool {
        if let Some(curr_byte) = self.current_byte()
            && (curr_byte == b'+' || curr_byte == b'-')
            && self.has_token()
            && self.is_scientific_notation_base()
            && self
                .formula
                .as_bytes()
                .get(self.offset + 1)
                .is_some_and(|b| b.is_ascii_digit())
        {
            self.offset += 1;
            self.extend_token();
            return true;
        }
        false
    }

    fn is_scientific_notation_base(&self) -> bool {
        let [first, middle @ .., last] = self.token_str().as_bytes() else {
            return false;
        };
        if !(*last == b'E' || *last == b'e') {
            return false;
        }
        if !first.is_ascii_digit() {
            return false;
        }

        let mut dot_seen = false;
        for &ch in middle {
            match ch {
                b'0'..=b'9' => {}
                b'.' if !dot_seen => dot_seen = true,
                _ => return false,
            }
        }
        true
    }

    fn parse(&mut self) -> Result<(), TokenizerError> {
        let result = self.parse_with_recovery(false).map_err(Into::into);
        if self.exceeded {
            return Err(TokenizerError {
                message: format!("Formula token limit exceeded (max {})", self.token_limit),
                pos: self.offset,
            });
        }
        result
    }

    pub(crate) fn parse_best_effort(&mut self) -> Vec<TokenSpan> {
        let _ = self.parse_with_recovery(true);
        self.spans.clone()
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: current_byte() returned Some, so offset < len"
    )]
    fn parse_with_recovery(&mut self, best_effort: bool) -> Result<(), SpanTokenizerError> {
        match self.formula.as_bytes().first() {
            None => return Ok(()),
            Some(b'=') => {}
            Some(_) => {
                self.push_span(
                    TokenType::Literal,
                    TokenSubType::None,
                    0,
                    self.formula.len(),
                );
                return Ok(());
            }
        }

        self.offset = 1;
        self.start_token();

        // The scientific-notation check may advance `offset`; it then
        // `continue`s, so `curr_byte` is always the byte at `offset`.
        while let Some(curr_byte) = self.current_byte() {
            if self.exceeded {
                break;
            }
            if self.check_scientific_notation() {
                continue;
            }

            if is_token_ender(curr_byte) && self.has_token() {
                self.save_token();
                self.start_token();
            }

            let parse_result = match curr_byte {
                b'"' | b'\'' => self.parse_string(curr_byte),
                b'[' => self.parse_brackets(),
                b'#' => {
                    if self.should_emit_hash_postfix() {
                        self.emit_hash_postfix();
                        Ok(())
                    } else {
                        self.parse_error()
                    }
                }
                b' ' | b'\t' | b'\r' | b'\n' => self.parse_whitespace(),
                b':' => {
                    if self.should_emit_colon_infix() {
                        self.emit_infix_operator(self.offset, self.offset + 1);
                        Ok(())
                    } else {
                        if !self.has_token() {
                            self.start_token();
                        }
                        self.offset += 1;
                        self.extend_token();
                        Ok(())
                    }
                }
                b'+' | b'-' | b'*' | b'/' | b'^' | b'&' | b'=' | b'>' | b'<' | b'%' | b'@' => {
                    self.parse_operator(curr_byte)
                }
                b'{' | b'(' => self.parse_opener(curr_byte),
                b')' | b'}' => self.parse_closer(curr_byte),
                b';' | b',' => self.parse_separator(curr_byte),
                _ => {
                    if !self.has_token() {
                        self.start_token();
                    }
                    self.offset += 1;
                    self.extend_token();
                    Ok(())
                }
            };

            if let Err(err) = parse_result {
                if best_effort {
                    self.recover_from_error(err);
                } else {
                    return Err(err);
                }
            }
        }

        // Do not recover/sweep unmatched openers after budget exhaustion.
        // Admission failure is reported by parse() or the best-effort adapter.
        if self.exceeded {
            return Ok(());
        }
        if self.has_token() {
            self.save_token();
        }
        if self.exceeded {
            return Ok(());
        }

        if !self.token_stack.is_empty() {
            if best_effort {
                while let Some(open_token) = self.token_stack.pop() {
                    if let Some(span) = self.spans.iter().find(|span| {
                        span.start == open_token.start
                            && span.end == open_token.end
                            && span.token_type == open_token.token_type
                            && span.subtype == open_token.subtype
                    }) {
                        self.diagnostics.push(TokenDiagnostic::new(
                            *span,
                            "Unmatched opening parenthesis or bracket".to_string(),
                            RecoveryAction::UnmatchedOpener,
                        ));
                    }
                }
            } else {
                return Err(SpanTokenizerError {
                    kind: SpanTokenizerErrorKind::UnmatchedOpening,
                    pos: self.offset,
                    message: "Unmatched opening parenthesis or bracket".to_string(),
                    span_start: None,
                    span_end: None,
                });
            }
        }

        Ok(())
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "error.pos is the offset of the ASCII byte that failed, < len, so pos + 1 <= len"
    )]
    fn recover_from_error(&mut self, error: SpanTokenizerError) {
        if self.spans.len() >= self.token_limit {
            self.exceeded = true;
            return;
        }
        match error.kind {
            SpanTokenizerErrorKind::NoMatchingOpener => {
                let span = TokenSpan {
                    token_type: TokenType::Operand,
                    subtype: TokenSubType::None,
                    start: error.pos,
                    end: error.pos + 1,
                };
                self.push_span(TokenType::Operand, TokenSubType::None, span.start, span.end);
                self.offset = span.end;
                self.start_token();
                self.diagnostics.push(TokenDiagnostic::new(
                    span,
                    format!("No matching opener for closer at position {}", error.pos),
                    RecoveryAction::SkippedUnmatchedCloser,
                ));
            }
            SpanTokenizerErrorKind::UnmatchedOpening => {
                debug_assert!(
                    false,
                    "UnmatchedOpening is handled at end-of-input and should not be routed through recover_from_error"
                );
            }
            SpanTokenizerErrorKind::UnterminatedString => {
                let start = error.span_start.unwrap_or(error.pos);
                let span = TokenSpan {
                    token_type: TokenType::Operand,
                    subtype: TokenSubType::None,
                    start,
                    end: self.formula.len(),
                };
                self.push_span(TokenType::Operand, TokenSubType::None, span.start, span.end);
                self.offset = span.end;
                self.start_token();
                self.diagnostics.push(TokenDiagnostic::new(
                    span,
                    "Reached end of formula while parsing string".to_string(),
                    RecoveryAction::UnterminatedString,
                ));
            }
            SpanTokenizerErrorKind::UnmatchedBracket => {
                let start = error.span_start.unwrap_or(error.pos);
                let end = error.span_end.unwrap_or(self.formula.len());
                let span = TokenSpan {
                    token_type: TokenType::Operand,
                    subtype: TokenSubType::None,
                    start,
                    end,
                };
                self.push_span(TokenType::Operand, TokenSubType::None, span.start, span.end);
                self.offset = span.end;
                self.start_token();
                self.diagnostics.push(TokenDiagnostic::new(
                    span,
                    "Encountered unmatched '['".to_string(),
                    RecoveryAction::UnmatchedBracket,
                ));
            }
            SpanTokenizerErrorKind::MismatchedPair => {
                let span = TokenSpan {
                    token_type: TokenType::Operand,
                    subtype: TokenSubType::None,
                    start: error.pos,
                    end: error.pos + 1,
                };
                self.push_span(TokenType::Operand, TokenSubType::None, span.start, span.end);
                self.offset = span.end;
                self.start_token();
                self.diagnostics.push(TokenDiagnostic::new(
                    span,
                    "Mismatched ( and { pair".to_string(),
                    RecoveryAction::SkippedUnmatchedCloser,
                ));
            }
            SpanTokenizerErrorKind::InvalidErrorLiteral => {
                let start = error.span_start.unwrap_or(error.pos);
                let end = error.span_end.unwrap_or(error.pos + 1);
                let span = TokenSpan {
                    token_type: TokenType::Operand,
                    subtype: TokenSubType::None,
                    start,
                    end,
                };
                self.push_span(TokenType::Operand, TokenSubType::None, span.start, span.end);
                self.offset = span.end;
                self.start_token();
                self.diagnostics.push(TokenDiagnostic::new(
                    span,
                    "Invalid error code".to_string(),
                    RecoveryAction::InvalidErrorLiteral,
                ));
            }
        }
    }

    /// `delim` is the quote byte at `offset`.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset only advances past bytes read with get"
    )]
    fn parse_string(&mut self, delim: u8) -> Result<(), SpanTokenizerError> {
        debug_assert!(delim == b'"' || delim == b'\'');
        debug_assert_eq!(self.current_byte(), Some(delim));

        let is_dollar_ref = delim == b'\'' && self.token_str() == "$";

        // Issue #79: only single-quoted strings (cross-sheet references like
        // `A1:'Other Sheet'!B2`) should glue onto a pending `:`-terminated
        // token. Double-quoted string literals must always flush so the
        // pending `A1:` prefix is not silently discarded.
        let glue_to_token = delim == b'\'' && self.token_str().ends_with(':');

        if !is_dollar_ref && !glue_to_token && self.has_token() {
            self.save_token();
            self.start_token();
        }

        let string_start = if is_dollar_ref {
            self.token_start
        } else {
            self.offset
        };
        self.offset += 1;

        while let Some(b) = self.current_byte() {
            if b == delim {
                self.offset += 1;
                if self.current_byte() == Some(delim) {
                    self.offset += 1;
                } else {
                    if delim == b'"' {
                        let subtype = operand_subtype(self.source_str(string_start, self.offset));
                        self.push_span(TokenType::Operand, subtype, string_start, self.offset);
                        self.start_token();
                    } else {
                        self.token_end = self.offset;
                    }
                    return Ok(());
                }
            } else {
                self.offset += 1;
            }
        }

        Err(SpanTokenizerError {
            kind: SpanTokenizerErrorKind::UnterminatedString,
            pos: self.offset,
            message: "Reached end of formula while parsing string".to_string(),
            span_start: Some(string_start),
            span_end: Some(self.formula.len()),
        })
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset advances by 2 only when offset + 1 < len; open_count counts unclosed `[` and is decremented only while positive"
    )]
    fn parse_brackets(&mut self) -> Result<(), SpanTokenizerError> {
        debug_assert_eq!(self.current_byte(), Some(b'['));

        if !self.has_token() {
            self.start_token();
        }

        let bracket_start = self.offset;
        let mut open_count: usize = 1;
        self.offset += 1;

        while let Some(b) = self.current_byte() {
            match b {
                // OOXML structured-reference escape: a single apostrophe makes
                // the next byte literal (used to embed `[`, `]`, `'`, or `#`
                // inside a column name). Skip the following byte without
                // updating nesting depth.
                b'\'' => {
                    if self.offset + 1 < self.formula.len() {
                        self.offset += 2;
                        continue;
                    }
                    // Trailing apostrophe inside brackets is malformed; fall
                    // through so the loop ends with an UnmatchedBracket error.
                    self.offset += 1;
                    continue;
                }
                b'[' => open_count += 1,
                b']' => {
                    open_count -= 1;
                    if open_count == 0 {
                        self.offset += 1;
                        self.extend_token();
                        return Ok(());
                    }
                }
                _ => {}
            }
            self.offset += 1;
        }

        Err(SpanTokenizerError {
            kind: SpanTokenizerErrorKind::UnmatchedBracket,
            pos: self.offset,
            message: "Encountered unmatched '['".to_string(),
            span_start: Some(bracket_start),
            span_end: Some(self.formula.len()),
        })
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset advances past an error code matched inside the formula; end scans under get"
    )]
    fn parse_error(&mut self) -> Result<(), SpanTokenizerError> {
        // OOXML serializes broken sheet-qualified references as `Sheet1!#REF!`.
        // When an accumulated token ends with `!`, treat the prefix as a sheet
        // qualifier and discard it: the resulting AST is identical to the bare
        // error literal `=#REF!`, preserving the error kind via the matched
        // `ERROR_CODES` entry below.
        let has_sheet_prefix = self.token_str().ends_with('!');
        if has_sheet_prefix {
            if self.token_str().len() <= 1 {
                return Err(SpanTokenizerError {
                    kind: SpanTokenizerErrorKind::InvalidErrorLiteral,
                    pos: self.offset,
                    message: format!(
                        "Empty sheet qualifier before error literal at position {}",
                        self.offset
                    ),
                    span_start: Some(self.token_start),
                    span_end: Some(self.offset),
                });
            }
            // Discard the sheet prefix; the error kind is what matters.
            self.start_token();
        } else if self.has_token() {
            self.save_token();
            self.start_token();
        }

        // A defined name whose sheet was deleted decays to `#REF!#REF!`: a
        // deleted sheet qualifier followed by a deleted address. Like the
        // `Sheet1!#REF!` prefix above, the qualifier is discarded and the
        // operand is the single error literal `#REF!`.
        if ref_error_starts_at(self.formula, self.offset)
            && ref_error_starts_at(self.formula, self.offset + 5)
        {
            self.offset += 5;
        }

        let error_start = self.offset;

        for &err_code in ERROR_CODES {
            let err_bytes = err_code.as_bytes();
            if self
                .formula
                .as_bytes()
                .get(self.offset..)
                .and_then(|rest| rest.get(..err_bytes.len()))
                .is_some_and(|slice| slice.eq_ignore_ascii_case(err_bytes))
            {
                self.push_span(
                    TokenType::Operand,
                    TokenSubType::Error,
                    error_start,
                    self.offset + err_bytes.len(),
                );
                self.offset += err_bytes.len();
                self.start_token();
                return Ok(());
            }
        }

        let mut end = self.offset + 1;
        while let Some(&ch) = self.formula.as_bytes().get(end) {
            if is_token_ender(ch)
                || ch == b' '
                || ch == b'\t'
                || ch == b'\r'
                || ch == b'\n'
                || ch == b'('
                || ch == b'{'
                || ch == b'['
                || ch == b'"'
                || ch == b'\''
            {
                break;
            }
            end += 1;
        }

        Err(SpanTokenizerError {
            kind: SpanTokenizerErrorKind::InvalidErrorLiteral,
            pos: self.offset,
            message: format!("Invalid error code at position {}", self.offset),
            span_start: Some(error_start),
            span_end: Some(end),
        })
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length"
    )]
    fn parse_whitespace(&mut self) -> Result<(), SpanTokenizerError> {
        self.save_token();

        let ws_start = self.offset;
        let mut contains_intersection_space = false;
        while let Some(b) = self.current_byte() {
            match b {
                b' ' => {
                    contains_intersection_space = true;
                    self.offset += 1;
                }
                b'\t' | b'\r' | b'\n' => self.offset += 1,
                _ => break,
            }
        }

        // Excel accepts TAB/CR/LF as lexical whitespace, but only an ASCII
        // space in the run can spell the range-intersection operator.
        let token_type = if contains_intersection_space
            && self.prev_is_reference_producing()
            && next_starts_reference_expression(self.formula, self.offset)
        {
            TokenType::OpInfix
        } else {
            TokenType::Whitespace
        };
        self.push_span(token_type, TokenSubType::None, ws_start, self.offset);
        self.start_token();
        Ok(())
    }

    fn prev_is_reference_producing(&self) -> bool {
        match self.prev_non_whitespace() {
            Some(prev) => match prev.token_type {
                TokenType::OpPostfix => true,
                TokenType::Paren | TokenType::Func | TokenType::Array
                    if prev.subtype == TokenSubType::Close =>
                {
                    true
                }
                TokenType::Operand if prev.subtype == TokenSubType::Range => self
                    .formula
                    .get(prev.start..prev.end)
                    .is_some_and(is_reference_operand_value),
                _ => false,
            },
            None => false,
        }
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset is the index of the `:` being dispatched, < len"
    )]
    fn should_emit_colon_infix(&self) -> bool {
        if self.has_token() {
            let value = self.token_str();
            if value.ends_with('!') {
                return false;
            }
            return reference_value_contains_range_colon(value)
                || value_has_structured_reference_bracket(value)
                // A range end pointing at deleted cells is stored as `#REF!`
                // (`A1:#REF!`); the `:` next to it is still the range operator.
                || ref_error_starts_at(self.formula, self.offset + 1)
                || (value.contains('!')
                    && next_reference_has_sheet_qualifier(self.formula, self.offset + 1));
        }
        self.prev_is_reference_producing()
            || self.prev_non_whitespace().is_some_and(|prev| {
                prev.subtype == TokenSubType::Error && ref_error_starts_at(self.formula, prev.start)
            })
    }

    fn emit_infix_operator(&mut self, start: usize, end: usize) {
        self.save_token();
        self.start_token();
        self.push_span(TokenType::OpInfix, TokenSubType::None, start, end);
        self.offset = end;
        self.start_token();
    }

    fn prev_non_whitespace(&self) -> Option<&TokenSpan> {
        self.spans
            .iter()
            .rev()
            .find(|t| t.token_type != TokenType::Whitespace)
    }

    /// Decide whether a `#` at `self.offset` should be emitted as a spill
    /// postfix operator (`OpPostfix`) instead of routed to the error-literal
    /// parser. The rule mirrors the space-operator gating: `#` is postfix when
    /// it follows a reference-producing token.
    fn should_emit_hash_postfix(&self) -> bool {
        if self.has_token() {
            let value = self.token_str();
            // An accumulated token whose last byte is `!` is a sheet/file
            // qualifier (e.g. `Sheet1!`). Defer to `parse_error` so it can
            // merge `Sheet1!` + `#REF!` into a single qualified-error operand.
            if value.ends_with('!') {
                return false;
            }
            return operand_subtype(value) == TokenSubType::Range;
        }
        match self.prev_non_whitespace() {
            Some(prev) => match prev.token_type {
                TokenType::OpPostfix => true,
                TokenType::Paren | TokenType::Func | TokenType::Array
                    if prev.subtype == TokenSubType::Close =>
                {
                    true
                }
                TokenType::Operand if prev.subtype == TokenSubType::Range => true,
                _ => false,
            },
            None => false,
        }
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset is the index of the `#` being dispatched, < len"
    )]
    fn emit_hash_postfix(&mut self) {
        self.save_token();
        self.start_token();
        self.push_span(
            TokenType::OpPostfix,
            TokenSubType::None,
            self.offset,
            self.offset + 1,
        );
        self.offset += 1;
        self.start_token();
    }

    /// `curr_byte` is the operator byte at `offset`.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset < len while dispatching, and the two-byte operators were read with get"
    )]
    fn parse_operator(&mut self, curr_byte: u8) -> Result<(), SpanTokenizerError> {
        debug_assert_eq!(self.current_byte(), Some(curr_byte));
        self.save_token();

        if let Some(b">=" | b"<=" | b"<>") = self
            .formula
            .as_bytes()
            .get(self.offset..)
            .and_then(|rest| rest.get(..2))
        {
            self.push_span(
                TokenType::OpInfix,
                TokenSubType::None,
                self.offset,
                self.offset + 2,
            );
            self.offset += 2;
            self.start_token();
            return Ok(());
        }

        let token_type = match curr_byte {
            b'@' => TokenType::OpPrefix,
            b'%' => TokenType::OpPostfix,
            b'+' | b'-' => {
                if self.spans.is_empty() {
                    TokenType::OpPrefix
                } else {
                    let prev = self.prev_non_whitespace();
                    if let Some(p) = prev {
                        if p.subtype == TokenSubType::Close
                            || p.token_type == TokenType::OpPostfix
                            || p.token_type == TokenType::Operand
                        {
                            TokenType::OpInfix
                        } else {
                            TokenType::OpPrefix
                        }
                    } else {
                        TokenType::OpPrefix
                    }
                }
            }
            _ => TokenType::OpInfix,
        };

        self.push_span(token_type, TokenSubType::None, self.offset, self.offset + 1);
        self.offset += 1;
        self.start_token();
        Ok(())
    }

    /// `curr_byte` is the `(` or `{` at `offset`.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset < len while dispatching; colon is the index of a `:` inside the token"
    )]
    fn parse_opener(&mut self, curr_byte: u8) -> Result<(), SpanTokenizerError> {
        debug_assert!(curr_byte == b'(' || curr_byte == b'{');
        debug_assert_eq!(self.current_byte(), Some(curr_byte));

        let token = if curr_byte == b'{' {
            self.save_token();
            TokenSpan {
                token_type: TokenType::Array,
                subtype: TokenSubType::Open,
                start: self.offset,
                end: self.offset + 1,
            }
        } else if self.has_token() {
            if let Some(colon) = range_colon_before_call(self.token_str()) {
                let colon = self.token_start + colon;
                let subtype = operand_subtype(self.source_str(self.token_start, colon));
                self.push_span(TokenType::Operand, subtype, self.token_start, colon);
                self.push_span(TokenType::OpInfix, TokenSubType::None, colon, colon + 1);
                self.token_start = colon + 1;
            }
            let token = TokenSpan {
                token_type: TokenType::Func,
                subtype: TokenSubType::Open,
                start: self.token_start,
                end: self.offset + 1,
            };
            self.token_start = self.offset + 1;
            self.token_end = self.offset + 1;
            token
        } else {
            TokenSpan {
                token_type: TokenType::Paren,
                subtype: TokenSubType::Open,
                start: self.offset,
                end: self.offset + 1,
            }
        };

        if self.spans.len() >= self.token_limit {
            self.exceeded = true;
            return Ok(());
        }
        self.spans.push(token);
        self.token_stack.push(token);
        self.offset += 1;
        self.start_token();
        Ok(())
    }

    /// `curr_byte` is the `)` or `}` at `offset`.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset < len while dispatching"
    )]
    fn parse_closer(&mut self, curr_byte: u8) -> Result<(), SpanTokenizerError> {
        debug_assert!(curr_byte == b')' || curr_byte == b'}');
        debug_assert_eq!(self.current_byte(), Some(curr_byte));
        self.save_token();

        if let Some(open_token) = self.token_stack.last().copied() {
            let expected = if open_token.token_type == TokenType::Array {
                b'}'
            } else {
                b')'
            };
            if curr_byte != expected {
                return Err(SpanTokenizerError {
                    kind: SpanTokenizerErrorKind::MismatchedPair,
                    pos: self.offset,
                    message: "Mismatched ( and { pair".to_string(),
                    span_start: Some(self.offset),
                    span_end: Some(self.offset + 1),
                });
            }

            self.token_stack.pop();
            self.push_span(
                open_token.token_type,
                TokenSubType::Close,
                self.offset,
                self.offset + 1,
            );
        } else {
            return Err(SpanTokenizerError {
                kind: SpanTokenizerErrorKind::NoMatchingOpener,
                pos: self.offset,
                message: format!("No matching opener for closer at position {}", self.offset),
                span_start: Some(self.offset),
                span_end: Some(self.offset + 1),
            });
        }

        self.offset += 1;
        self.start_token();
        Ok(())
    }

    /// `curr_byte` is the `,` or `;` at `offset`.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "cursor bounded by slice length: offset < len while dispatching"
    )]
    fn parse_separator(&mut self, curr_byte: u8) -> Result<(), SpanTokenizerError> {
        debug_assert!(curr_byte == b';' || curr_byte == b',');
        debug_assert_eq!(self.current_byte(), Some(curr_byte));
        self.save_token();

        let top_token = self.token_stack.last();
        let in_function_or_array = matches!(
            top_token.map(|t| t.token_type),
            Some(TokenType::Func | TokenType::Array)
        );
        let in_array = matches!(top_token.map(|t| t.token_type), Some(TokenType::Array));

        let (token_type, subtype) = match curr_byte {
            b',' => {
                if in_function_or_array {
                    (TokenType::Sep, TokenSubType::Arg)
                } else {
                    (TokenType::OpInfix, TokenSubType::None)
                }
            }
            b';' => {
                if in_array {
                    (TokenType::Sep, TokenSubType::Row)
                } else if self.dialect == FormulaDialect::OpenFormula && in_function_or_array {
                    (TokenType::Sep, TokenSubType::Arg)
                } else if self.dialect == FormulaDialect::OpenFormula {
                    (TokenType::OpInfix, TokenSubType::None)
                } else {
                    (TokenType::Sep, TokenSubType::Row)
                }
            }
            _ => (TokenType::OpInfix, TokenSubType::None),
        };

        self.push_span(token_type, subtype, self.offset, self.offset + 1);
        self.offset += 1;
        self.start_token();
        Ok(())
    }
}

/// A tokenizer for Excel worksheet formulas.
///
/// An owned form of [`TokenStream`]: the same tokens, each with its text
/// copied into [`Token::value`], in [`Self::items`].
pub struct Tokenizer {
    pub items: Vec<Token>,
    admission_error: Option<TokenizerError>,
    dialect: FormulaDialect,
}

impl Tokenizer {
    /// Create a new tokenizer and immediately parse the formula.
    pub fn new(formula: &str) -> Result<Self, TokenizerError> {
        Self::new_with_dialect(formula, FormulaDialect::Excel)
    }

    /// Recover malformed syntax without returning a Result.
    /// Check [`Self::admission_error`] before using output: resource rejection
    /// produces empty output rather than a silently truncated formula.
    pub fn new_best_effort(formula: &str) -> Self {
        Self::new_best_effort_with_dialect(formula, FormulaDialect::Excel)
    }

    /// Create a new tokenizer with best-effort parsing for the specified dialect.
    pub fn new_best_effort_with_dialect(formula: &str, dialect: FormulaDialect) -> Self {
        let stream = TokenStream::new_best_effort_with_dialect(formula, dialect);
        Self::from_token_stream(&stream)
    }

    /// Create a new tokenizer for the specified formula dialect.
    pub fn new_with_dialect(
        formula: &str,
        dialect: FormulaDialect,
    ) -> Result<Self, TokenizerError> {
        let spans = tokenize_spans_with_dialect(formula, dialect)?;
        Ok(Tokenizer {
            items: tokens_from_spans(formula, &spans),
            admission_error: None,
            dialect,
        })
    }

    pub fn from_token_stream(stream: &TokenStream) -> Self {
        let admission_error = stream.admission_error(crate::ParserLimits::default());
        let items = if admission_error.is_some() {
            Vec::new()
        } else {
            stream.to_tokens()
        };
        Tokenizer {
            items,
            admission_error,
            dialect: stream.dialect,
        }
    }

    /// Why an infallible best-effort/stream constructor could not admit input.
    pub fn admission_error(&self) -> Option<&TokenizerError> {
        self.admission_error.as_ref()
    }

    /// Reconstruct the formula from the parsed tokens.
    pub fn render(&self) -> String {
        match self.items.first() {
            None => String::new(),
            Some(first) if first.token_type == TokenType::Literal => first.value.clone(),
            Some(_) => {
                let mut out = String::from("=");
                for token in &self.items {
                    out.push_str(&token.value);
                }
                out
            }
        }
    }

    /// Return the dialect used when tokenizing this formula.
    pub fn dialect(&self) -> FormulaDialect {
        self.dialect
    }
}

impl TryFrom<&str> for Tokenizer {
    type Error = TokenizerError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Tokenizer::new(value)
    }
}

impl TryFrom<String> for Tokenizer {
    type Error = TokenizerError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Tokenizer::new(&value)
    }
}
