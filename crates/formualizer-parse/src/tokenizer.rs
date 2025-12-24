use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{self, Display};
use std::sync::Arc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::types::FormulaDialect;

const TOKEN_ENDERS: &str = ",;}) +-*/^&=><%";

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
fn is_token_ender(c: u8) -> bool {
    TOKEN_ENDERS_TABLE[c as usize]
}

static ERROR_CODES: &[&str] = &[
    "#NULL!",
    "#DIV/0!",
    "#VALUE!",
    "#REF!",
    "#NAME?",
    "#NUM!",
    "#N/A",
    "#GETTING_DATA",
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

    fn from_slice(
        source: &str,
        token_type: TokenType,
        subtype: TokenSubType,
        start: usize,
        end: usize,
    ) -> Self {
        Token {
            value: source[start..end].to_string(),
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
        //   exponent ^ (right-assoc)
        //   prefix unary +/-(...) (binds looser than ^)
        //   */
        //   +-
        //   &
        //   comparisons
        match op {
            ":" | " " | "," => Some((8, Associativity::Left)),
            "%" => Some((7, Associativity::Left)),
            "^" => Some((6, Associativity::Right)),
            "u" => Some((5, Associativity::Right)),
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
        } else if value == "TRUE" || value == "FALSE" {
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
        } else if value == "TRUE" || value == "FALSE" {
            TokenSubType::Logical
        } else if value.parse::<f64>().is_ok() {
            TokenSubType::Number
        } else {
            TokenSubType::Range
        };
        Token::new_with_span(value, TokenType::Operand, subtype, start, end)
    }

    fn make_operand_from_slice(source: &str, start: usize, end: usize) -> Self {
        let value_str = &source[start..end];
        let subtype = if value_str.starts_with('"') {
            TokenSubType::Text
        } else if value_str.starts_with('#') {
            TokenSubType::Error
        } else if value_str == "TRUE" || value_str == "FALSE" {
            TokenSubType::Logical
        } else if value_str.parse::<f64>().is_ok() {
            TokenSubType::Number
        } else {
            TokenSubType::Range
        };
        Token::from_slice(source, TokenType::Operand, subtype, start, end)
    }

    /// Create a subexpression token.
    ///
    /// `value` must end with one of '{', '}', '(' or ')'. If `func` is true,
    /// the token's type is forced to be Func.
    pub fn make_subexp(value: &str, func: bool) -> Self {
        let last_char = value.chars().last().expect("Empty token value");
        assert!(matches!(last_char, '{' | '}' | '(' | ')'));
        let token_type = if func {
            TokenType::Func
        } else if "{}".contains(last_char) {
            TokenType::Array
        } else if "()".contains(last_char) {
            TokenType::Paren
        } else {
            TokenType::Func
        };
        let subtype = if ")}".contains(last_char) {
            TokenSubType::Close
        } else {
            TokenSubType::Open
        };
        Token::new(value.to_string(), token_type, subtype)
    }

    /// Create a subexpression token with byte position span.
    pub fn make_subexp_with_span(value: &str, func: bool, start: usize, end: usize) -> Self {
        let last_char = value.chars().last().expect("Empty token value");
        assert!(matches!(last_char, '{' | '}' | '(' | ')'));
        let token_type = if func {
            TokenType::Func
        } else if "{}".contains(last_char) {
            TokenType::Array
        } else if "()".contains(last_char) {
            TokenType::Paren
        } else {
            TokenType::Func
        };
        let subtype = if ")}".contains(last_char) {
            TokenSubType::Close
        } else {
            TokenSubType::Open
        };
        Token::new_with_span(value.to_string(), token_type, subtype, start, end)
    }

    fn make_subexp_from_slice(source: &str, func: bool, start: usize, end: usize) -> Self {
        let value_str = &source[start..end];
        let last_char = value_str.chars().last().expect("Empty token value");
        let token_type = if func {
            TokenType::Func
        } else if "{}".contains(last_char) {
            TokenType::Array
        } else if "()".contains(last_char) {
            TokenType::Paren
        } else {
            TokenType::Func
        };
        let subtype = if ")}".contains(last_char) {
            TokenSubType::Close
        } else {
            TokenSubType::Open
        };
        Token::from_slice(source, token_type, subtype, start, end)
    }

    /// Given an opener token, return its corresponding closer token.
    pub fn get_closer(&self) -> Result<Token, TokenizerError> {
        if self.subtype != TokenSubType::Open {
            return Err(TokenizerError {
                message: "Token is not an opener".to_string(),
                pos: 0,
            });
        }
        let closer_value = if self.token_type == TokenType::Array {
            "}"
        } else {
            ")"
        };
        Ok(Token::make_subexp(
            closer_value,
            self.token_type == TokenType::Func,
        ))
    }

    /// Create a separator token.
    pub fn make_separator(value: &str) -> Self {
        assert!(value == "," || value == ";");
        let subtype = if value == "," {
            TokenSubType::Arg
        } else {
            TokenSubType::Row
        };
        Token::new(value.to_string(), TokenType::Sep, subtype)
    }

    /// Create a separator token with byte position span.
    pub fn make_separator_with_span(value: &str, start: usize, end: usize) -> Self {
        assert!(value == "," || value == ";");
        let subtype = if value == "," {
            TokenSubType::Arg
        } else {
            TokenSubType::Row
        };
        Token::new_with_span(value.to_string(), TokenType::Sep, subtype, start, end)
    }
}

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
    source: Arc<str>,
    pub spans: Vec<TokenSpan>,
    dialect: FormulaDialect,
}

impl TokenStream {
    pub fn new(formula: &str) -> Result<Self, TokenizerError> {
        Self::new_with_dialect(formula, FormulaDialect::Excel)
    }

    pub fn new_with_dialect(
        formula: &str,
        dialect: FormulaDialect,
    ) -> Result<Self, TokenizerError> {
        let source: Arc<str> = Arc::from(formula);
        let spans = tokenize_spans_with_dialect(source.as_ref(), dialect)?;
        Ok(TokenStream {
            source,
            spans,
            dialect,
        })
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
        self.spans
            .iter()
            .map(|s| {
                let value = self
                    .source
                    .get(s.start..s.end)
                    .unwrap_or_default()
                    .to_string();
                Token::new_with_span(value, s.token_type, s.subtype, s.start, s.end)
            })
            .collect()
    }

    pub fn render(&self) -> String {
        let mut out = String::with_capacity(self.source.len());
        for span in &self.spans {
            if let Some(s) = self.source.get(span.start..span.end) {
                out.push_str(s);
            }
        }
        out
    }
}

pub(crate) fn tokenize_spans_with_dialect(
    formula: &str,
    dialect: FormulaDialect,
) -> Result<Vec<TokenSpan>, TokenizerError> {
    let mut tokenizer = SpanTokenizer::new(formula, dialect);
    tokenizer.parse()?;
    Ok(tokenizer.spans)
}

fn operand_subtype(value_str: &str) -> TokenSubType {
    if value_str.starts_with('"') {
        TokenSubType::Text
    } else if value_str.starts_with('#') {
        TokenSubType::Error
    } else if value_str == "TRUE" || value_str == "FALSE" {
        TokenSubType::Logical
    } else if value_str.parse::<f64>().is_ok() {
        TokenSubType::Number
    } else {
        TokenSubType::Range
    }
}

struct SpanTokenizer<'a> {
    formula: &'a str,
    spans: Vec<TokenSpan>,
    token_stack: Vec<TokenSpan>,
    offset: usize,
    token_start: usize,
    token_end: usize,
    dialect: FormulaDialect,
}

impl<'a> SpanTokenizer<'a> {
    fn new(formula: &'a str, dialect: FormulaDialect) -> Self {
        SpanTokenizer {
            formula,
            spans: Vec::with_capacity(formula.len() / 2),
            token_stack: Vec::with_capacity(16),
            offset: 0,
            token_start: 0,
            token_end: 0,
            dialect,
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

    fn push_span(
        &mut self,
        token_type: TokenType,
        subtype: TokenSubType,
        start: usize,
        end: usize,
    ) {
        self.spans.push(TokenSpan {
            token_type,
            subtype,
            start,
            end,
        });
    }

    fn save_token(&mut self) {
        if self.has_token() {
            let value_str = &self.formula[self.token_start..self.token_end];
            let subtype = operand_subtype(value_str);
            self.push_span(
                TokenType::Operand,
                subtype,
                self.token_start,
                self.token_end,
            );
        }
    }

    fn check_scientific_notation(&mut self) -> bool {
        if let Some(curr_byte) = self.current_byte()
            && (curr_byte == b'+' || curr_byte == b'-')
            && self.has_token()
            && self.is_scientific_notation_base()
        {
            self.offset += 1;
            self.extend_token();
            return true;
        }
        false
    }

    fn is_scientific_notation_base(&self) -> bool {
        if !self.has_token() {
            return false;
        }

        let token_slice = &self.formula.as_bytes()[self.token_start..self.token_end];
        if token_slice.len() < 2 {
            return false;
        }

        let last = token_slice[token_slice.len() - 1];
        if !(last == b'E' || last == b'e') {
            return false;
        }

        let first = token_slice[0];
        if !first.is_ascii_digit() {
            return false;
        }

        let mut dot_seen = false;
        for &ch in &token_slice[1..token_slice.len() - 1] {
            match ch {
                b'0'..=b'9' => {}
                b'.' if !dot_seen => dot_seen = true,
                _ => return false,
            }
        }
        true
    }

    fn parse(&mut self) -> Result<(), TokenizerError> {
        if self.formula.is_empty() {
            return Ok(());
        }

        if self.formula.as_bytes()[0] != b'=' {
            self.push_span(
                TokenType::Literal,
                TokenSubType::None,
                0,
                self.formula.len(),
            );
            return Ok(());
        }

        self.offset = 1;
        self.start_token();

        while self.offset < self.formula.len() {
            if self.check_scientific_notation() {
                continue;
            }

            let curr_byte = self.formula.as_bytes()[self.offset];

            if is_token_ender(curr_byte) && self.has_token() {
                self.save_token();
                self.start_token();
            }

            match curr_byte {
                b'"' | b'\'' => self.parse_string()?,
                b'[' => self.parse_brackets()?,
                b'#' => self.parse_error()?,
                b' ' | b'\n' => self.parse_whitespace()?,
                b'+' | b'-' | b'*' | b'/' | b'^' | b'&' | b'=' | b'>' | b'<' | b'%' => {
                    self.parse_operator()?
                }
                b'{' | b'(' => self.parse_opener()?,
                b')' | b'}' => self.parse_closer()?,
                b';' | b',' => self.parse_separator()?,
                _ => {
                    if !self.has_token() {
                        self.start_token();
                    }
                    self.offset += 1;
                    self.extend_token();
                }
            }
        }

        if self.has_token() {
            self.save_token();
        }

        if !self.token_stack.is_empty() {
            return Err(TokenizerError {
                message: "Unmatched opening parenthesis or bracket".to_string(),
                pos: self.offset,
            });
        }

        Ok(())
    }

    fn parse_string(&mut self) -> Result<(), TokenizerError> {
        let delim = self.formula.as_bytes()[self.offset];
        assert!(delim == b'"' || delim == b'\'');

        let is_dollar_ref = delim == b'\''
            && self.has_token()
            && self.token_end - self.token_start == 1
            && self.formula.as_bytes()[self.token_start] == b'$';

        if !is_dollar_ref
            && self.has_token()
            && self.token_end > 0
            && self.formula.as_bytes()[self.token_end - 1] != b':'
        {
            self.save_token();
            self.start_token();
        }

        let string_start = if is_dollar_ref {
            self.token_start
        } else {
            self.offset
        };
        self.offset += 1;

        while self.offset < self.formula.len() {
            if self.formula.as_bytes()[self.offset] == delim {
                self.offset += 1;
                if self.offset < self.formula.len() && self.formula.as_bytes()[self.offset] == delim
                {
                    self.offset += 1;
                } else {
                    if delim == b'"' {
                        let value_str = &self.formula[string_start..self.offset];
                        let subtype = operand_subtype(value_str);
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

        Err(TokenizerError {
            message: "Reached end of formula while parsing string".to_string(),
            pos: self.offset,
        })
    }

    fn parse_brackets(&mut self) -> Result<(), TokenizerError> {
        assert_eq!(self.formula.as_bytes()[self.offset], b'[');

        if !self.has_token() {
            self.start_token();
        }

        let mut open_count = 1;
        self.offset += 1;

        while self.offset < self.formula.len() {
            match self.formula.as_bytes()[self.offset] {
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

        Err(TokenizerError {
            message: "Encountered unmatched '['".to_string(),
            pos: self.offset,
        })
    }

    fn parse_error(&mut self) -> Result<(), TokenizerError> {
        if self.has_token()
            && self.token_end > 0
            && self.formula.as_bytes()[self.token_end - 1] != b'!'
        {
            self.save_token();
            self.start_token();
        }

        let error_start = if self.has_token() {
            self.token_start
        } else {
            self.offset
        };

        for &err_code in ERROR_CODES {
            let err_bytes = err_code.as_bytes();
            if self.offset + err_bytes.len() <= self.formula.len() {
                let slice = &self.formula.as_bytes()[self.offset..self.offset + err_bytes.len()];
                if slice == err_bytes {
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
        }

        Err(TokenizerError {
            message: format!("Invalid error code at position {}", self.offset),
            pos: self.offset,
        })
    }

    fn parse_whitespace(&mut self) -> Result<(), TokenizerError> {
        self.save_token();

        let ws_start = self.offset;
        while self.offset < self.formula.len() {
            match self.formula.as_bytes()[self.offset] {
                b' ' | b'\n' => self.offset += 1,
                _ => break,
            }
        }

        self.push_span(
            TokenType::Whitespace,
            TokenSubType::None,
            ws_start,
            self.offset,
        );
        self.start_token();
        Ok(())
    }

    fn prev_non_whitespace(&self) -> Option<&TokenSpan> {
        self.spans
            .iter()
            .rev()
            .find(|t| t.token_type != TokenType::Whitespace)
    }

    fn parse_operator(&mut self) -> Result<(), TokenizerError> {
        self.save_token();

        if self.offset + 1 < self.formula.len() {
            let two_char = &self.formula.as_bytes()[self.offset..self.offset + 2];
            if two_char == b">=" || two_char == b"<=" || two_char == b"<>" {
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
        }

        let curr_byte = self.formula.as_bytes()[self.offset];
        let token_type = match curr_byte {
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

    fn parse_opener(&mut self) -> Result<(), TokenizerError> {
        let curr_byte = self.formula.as_bytes()[self.offset];
        assert!(curr_byte == b'(' || curr_byte == b'{');

        let token = if curr_byte == b'{' {
            self.save_token();
            TokenSpan {
                token_type: TokenType::Array,
                subtype: TokenSubType::Open,
                start: self.offset,
                end: self.offset + 1,
            }
        } else if self.has_token() {
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

        self.spans.push(token);
        self.token_stack.push(token);
        self.offset += 1;
        self.start_token();
        Ok(())
    }

    fn parse_closer(&mut self) -> Result<(), TokenizerError> {
        self.save_token();

        let curr_byte = self.formula.as_bytes()[self.offset];
        assert!(curr_byte == b')' || curr_byte == b'}');

        if let Some(open_token) = self.token_stack.pop() {
            let expected = if open_token.token_type == TokenType::Array {
                b'}'
            } else {
                b')'
            };
            if curr_byte != expected {
                return Err(TokenizerError {
                    message: "Mismatched ( and { pair".to_string(),
                    pos: self.offset,
                });
            }

            self.push_span(
                open_token.token_type,
                TokenSubType::Close,
                self.offset,
                self.offset + 1,
            );
        } else {
            return Err(TokenizerError {
                message: format!("No matching opener for closer at position {}", self.offset),
                pos: self.offset,
            });
        }

        self.offset += 1;
        self.start_token();
        Ok(())
    }

    fn parse_separator(&mut self) -> Result<(), TokenizerError> {
        self.save_token();

        let curr_byte = self.formula.as_bytes()[self.offset];
        assert!(curr_byte == b';' || curr_byte == b',');

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
pub struct Tokenizer {
    formula: String, // The formula string
    pub items: Vec<Token>,
    token_stack: Vec<Token>,
    offset: usize,      // Byte offset in formula
    token_start: usize, // Start of current token
    token_end: usize,   // End of current token
    dialect: FormulaDialect,
}

impl Tokenizer {
    /// Create a new tokenizer and immediately parse the formula.
    pub fn new(formula: &str) -> Result<Self, TokenizerError> {
        Self::new_with_dialect(formula, FormulaDialect::Excel)
    }

    /// Create a new tokenizer for the specified formula dialect.
    pub fn new_with_dialect(
        formula: &str,
        dialect: FormulaDialect,
    ) -> Result<Self, TokenizerError> {
        let mut tokenizer = Tokenizer {
            formula: formula.to_string(),
            items: Vec::with_capacity(formula.len() / 2), // Reasonable estimate
            token_stack: Vec::with_capacity(16),
            offset: 0,
            token_start: 0,
            token_end: 0,
            dialect,
        };
        tokenizer.parse()?;
        Ok(tokenizer)
    }

    pub fn from_token_stream(stream: &TokenStream) -> Self {
        Tokenizer {
            formula: stream.source.to_string(),
            items: stream.to_tokens(),
            token_stack: Vec::with_capacity(16),
            offset: 0,
            token_start: 0,
            token_end: 0,
            dialect: stream.dialect,
        }
    }

    /// Get byte at current offset
    #[inline]
    fn current_byte(&self) -> Option<u8> {
        self.formula.as_bytes().get(self.offset).copied()
    }

    /// Check if we have a token accumulated
    #[inline]
    fn has_token(&self) -> bool {
        self.token_end > self.token_start
    }

    /// Start a new token at current position
    #[inline]
    fn start_token(&mut self) {
        self.token_start = self.offset;
        self.token_end = self.offset;
    }

    /// Extend current token to current position
    #[inline]
    fn extend_token(&mut self) {
        self.token_end = self.offset;
    }

    /// Parse the formula into tokens.
    fn parse(&mut self) -> Result<(), TokenizerError> {
        if self.formula.is_empty() {
            return Ok(());
        }

        // Check for literal formula (doesn't start with '=')
        if self.formula.as_bytes()[0] != b'=' {
            self.items.push(Token::new_with_span(
                self.formula.clone(),
                TokenType::Literal,
                TokenSubType::None,
                0,
                self.formula.len(),
            ));
            return Ok(());
        }

        // Skip the '=' character
        self.offset = 1;
        self.start_token();

        while self.offset < self.formula.len() {
            if self.check_scientific_notation()? {
                continue;
            }

            let curr_byte = self.formula.as_bytes()[self.offset];

            // Check if this ends a token
            if is_token_ender(curr_byte) && self.has_token() {
                self.save_token();
                self.start_token();
            }

            // Dispatch based on the current character
            match curr_byte {
                b'"' | b'\'' => self.parse_string()?,
                b'[' => self.parse_brackets()?,
                b'#' => self.parse_error()?,
                b' ' | b'\n' => self.parse_whitespace()?,
                // operator characters
                b'+' | b'-' | b'*' | b'/' | b'^' | b'&' | b'=' | b'>' | b'<' | b'%' => {
                    self.parse_operator()?
                }
                b'{' | b'(' => self.parse_opener()?,
                b')' | b'}' => self.parse_closer()?,
                b';' | b',' => self.parse_separator()?,
                _ => {
                    // Accumulate into current token
                    if !self.has_token() {
                        self.start_token();
                    }
                    self.offset += 1;
                    self.extend_token();
                }
            }
        }

        // Save any remaining token
        if self.has_token() {
            self.save_token();
        }

        // Check for unmatched opening parentheses/brackets
        if !self.token_stack.is_empty() {
            return Err(TokenizerError {
                message: "Unmatched opening parenthesis or bracket".to_string(),
                pos: self.offset,
            });
        }

        Ok(())
    }

    /// If the current token looks like a number in scientific notation,
    /// consume the '+' or '-' as part of the number.
    fn check_scientific_notation(&mut self) -> Result<bool, TokenizerError> {
        if let Some(curr_byte) = self.current_byte()
            && (curr_byte == b'+' || curr_byte == b'-')
            && self.has_token()
            && self.is_scientific_notation_base()
        {
            self.offset += 1;
            self.extend_token();
            return Ok(true);
        }
        Ok(false)
    }

    /// Helper: Determine if the current accumulated token is the base of a
    /// scientific notation number (e.g., "1.23E" or "9e").
    fn is_scientific_notation_base(&self) -> bool {
        if !self.has_token() {
            return false;
        }

        let token_slice = &self.formula.as_bytes()[self.token_start..self.token_end];
        if token_slice.len() < 2 {
            return false;
        }

        let last = token_slice[token_slice.len() - 1];
        if !(last == b'E' || last == b'e') {
            return false;
        }

        let first = token_slice[0];
        if !first.is_ascii_digit() {
            return false;
        }

        let mut dot_seen = false;
        // Check middle characters
        for &ch in &token_slice[1..token_slice.len() - 1] {
            match ch {
                b'0'..=b'9' => {}
                b'.' if !dot_seen => dot_seen = true,
                _ => return false,
            }
        }
        true
    }

    /// If there is an accumulated token, convert it to an operand token and add it to the list.
    fn save_token(&mut self) {
        if self.has_token() {
            let token =
                Token::make_operand_from_slice(&self.formula, self.token_start, self.token_end);
            self.items.push(token);
        }
    }

    /// Parse a string (or link) literal.
    fn parse_string(&mut self) -> Result<(), TokenizerError> {
        let delim = self.formula.as_bytes()[self.offset];
        assert!(delim == b'"' || delim == b'\'');

        // Check for dollar reference special case
        let is_dollar_ref = delim == b'\''
            && self.has_token()
            && self.token_end - self.token_start == 1
            && self.formula.as_bytes()[self.token_start] == b'$';

        if !is_dollar_ref && self.has_token() {
            // Check if last char is ':'
            if self.token_end > 0 && self.formula.as_bytes()[self.token_end - 1] != b':' {
                self.save_token();
                self.start_token();
            }
        }

        let string_start = if is_dollar_ref {
            self.token_start
        } else {
            self.offset
        };
        self.offset += 1; // Skip opening delimiter

        while self.offset < self.formula.len() {
            if self.formula.as_bytes()[self.offset] == delim {
                self.offset += 1;
                // Check for escaped quote
                if self.offset < self.formula.len() && self.formula.as_bytes()[self.offset] == delim
                {
                    self.offset += 1; // Skip escaped quote
                } else {
                    // End of string
                    if delim == b'"' {
                        let token = Token::make_operand_from_slice(
                            &self.formula,
                            string_start,
                            self.offset,
                        );
                        self.items.push(token);
                        self.start_token();
                    } else {
                        // Single-quoted string becomes part of current token
                        self.token_end = self.offset;
                    }
                    return Ok(());
                }
            } else {
                self.offset += 1;
            }
        }

        Err(TokenizerError {
            message: "Reached end of formula while parsing string".to_string(),
            pos: self.offset,
        })
    }

    /// Parse the text between matching square brackets.
    fn parse_brackets(&mut self) -> Result<(), TokenizerError> {
        assert_eq!(self.formula.as_bytes()[self.offset], b'[');

        if !self.has_token() {
            self.start_token();
        }

        let mut open_count = 1;
        self.offset += 1;

        while self.offset < self.formula.len() {
            match self.formula.as_bytes()[self.offset] {
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

        Err(TokenizerError {
            message: "Encountered unmatched '['".to_string(),
            pos: self.offset,
        })
    }

    /// Parse an error literal that starts with '#'.
    fn parse_error(&mut self) -> Result<(), TokenizerError> {
        // Check if we have a partial token ending with '!'
        if self.has_token()
            && self.token_end > 0
            && self.formula.as_bytes()[self.token_end - 1] != b'!'
        {
            self.save_token();
            self.start_token();
        }

        let error_start = if self.has_token() {
            self.token_start
        } else {
            self.offset
        };

        // Try to match error codes
        for &err_code in ERROR_CODES {
            let err_bytes = err_code.as_bytes();
            if self.offset + err_bytes.len() <= self.formula.len() {
                let slice = &self.formula.as_bytes()[self.offset..self.offset + err_bytes.len()];
                if slice == err_bytes {
                    let token = Token::make_operand_from_slice(
                        &self.formula,
                        error_start,
                        self.offset + err_bytes.len(),
                    );
                    self.items.push(token);
                    self.offset += err_bytes.len();
                    self.start_token();
                    return Ok(());
                }
            }
        }

        Err(TokenizerError {
            message: format!("Invalid error code at position {}", self.offset),
            pos: self.offset,
        })
    }

    /// Parse a sequence of whitespace characters.
    fn parse_whitespace(&mut self) -> Result<(), TokenizerError> {
        self.save_token();

        let ws_start = self.offset;
        while self.offset < self.formula.len() {
            match self.formula.as_bytes()[self.offset] {
                b' ' | b'\n' => self.offset += 1,
                _ => break,
            }
        }

        self.items.push(Token::from_slice(
            &self.formula,
            TokenType::Whitespace,
            TokenSubType::None,
            ws_start,
            self.offset,
        ));
        self.start_token();
        Ok(())
    }

    /// Parse an operator token.
    fn parse_operator(&mut self) -> Result<(), TokenizerError> {
        self.save_token();

        // Check for two-character operators
        if self.offset + 1 < self.formula.len() {
            let two_char = &self.formula.as_bytes()[self.offset..self.offset + 2];
            if two_char == b">=" || two_char == b"<=" || two_char == b"<>" {
                self.items.push(Token::from_slice(
                    &self.formula,
                    TokenType::OpInfix,
                    TokenSubType::None,
                    self.offset,
                    self.offset + 2,
                ));
                self.offset += 2;
                self.start_token();
                return Ok(());
            }
        }

        let curr_byte = self.formula.as_bytes()[self.offset];
        let token_type = match curr_byte {
            b'%' => TokenType::OpPostfix,
            b'+' | b'-' => {
                // Determine if prefix or infix
                if self.items.is_empty() {
                    TokenType::OpPrefix
                } else {
                    let prev = self
                        .items
                        .iter()
                        .rev()
                        .find(|t| t.token_type != TokenType::Whitespace);
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

        self.items.push(Token::from_slice(
            &self.formula,
            token_type,
            TokenSubType::None,
            self.offset,
            self.offset + 1,
        ));
        self.offset += 1;
        self.start_token();
        Ok(())
    }

    /// Parse an opener token – either '(' or '{'.
    fn parse_opener(&mut self) -> Result<(), TokenizerError> {
        let curr_byte = self.formula.as_bytes()[self.offset];
        assert!(curr_byte == b'(' || curr_byte == b'{');

        let token = if curr_byte == b'{' {
            self.save_token();
            Token::make_subexp_from_slice(&self.formula, false, self.offset, self.offset + 1)
        } else if self.has_token() {
            // Function call
            let token = Token::make_subexp_from_slice(
                &self.formula,
                true,
                self.token_start,
                self.offset + 1,
            );
            self.token_start = self.offset + 1;
            self.token_end = self.offset + 1;
            token
        } else {
            Token::make_subexp_from_slice(&self.formula, false, self.offset, self.offset + 1)
        };

        self.items.push(token.clone());
        self.token_stack.push(token);
        self.offset += 1;
        self.start_token();
        Ok(())
    }

    /// Parse a closer token – either ')' or '}'.
    fn parse_closer(&mut self) -> Result<(), TokenizerError> {
        self.save_token();

        let curr_byte = self.formula.as_bytes()[self.offset];
        assert!(curr_byte == b')' || curr_byte == b'}');

        if let Some(open_token) = self.token_stack.pop() {
            let closer = open_token.get_closer()?;
            if (curr_byte == b'}' && closer.value != "}")
                || (curr_byte == b')' && closer.value != ")")
            {
                return Err(TokenizerError {
                    message: "Mismatched ( and { pair".to_string(),
                    pos: self.offset,
                });
            }

            self.items.push(Token::from_slice(
                &self.formula,
                closer.token_type,
                TokenSubType::Close,
                self.offset,
                self.offset + 1,
            ));
        } else {
            return Err(TokenizerError {
                message: format!("No matching opener for closer at position {}", self.offset),
                pos: self.offset,
            });
        }

        self.offset += 1;
        self.start_token();
        Ok(())
    }

    /// Parse a separator token – either ',' or ';'.
    fn parse_separator(&mut self) -> Result<(), TokenizerError> {
        self.save_token();

        let curr_byte = self.formula.as_bytes()[self.offset];
        assert!(curr_byte == b';' || curr_byte == b',');

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
                    // Array row separator for both dialects
                    (TokenType::Sep, TokenSubType::Row)
                } else if self.dialect == FormulaDialect::OpenFormula && in_function_or_array {
                    // OpenFormula uses ';' for argument separators inside functions
                    (TokenType::Sep, TokenSubType::Arg)
                } else if self.dialect == FormulaDialect::OpenFormula {
                    (TokenType::OpInfix, TokenSubType::None)
                } else {
                    (TokenType::Sep, TokenSubType::Row)
                }
            }
            _ => (TokenType::OpInfix, TokenSubType::None),
        };

        self.items.push(Token::from_slice(
            &self.formula,
            token_type,
            subtype,
            self.offset,
            self.offset + 1,
        ));

        self.offset += 1;
        self.start_token();
        Ok(())
    }

    /// Reconstruct the formula from the parsed tokens.
    pub fn render(&self) -> String {
        if self.items.is_empty() {
            "".to_string()
        } else if self.items[0].token_type == TokenType::Literal {
            self.items[0].value.clone()
        } else {
            let concatenated: String = self.items.iter().map(|t| t.value.clone()).collect();
            format!("={concatenated}")
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
