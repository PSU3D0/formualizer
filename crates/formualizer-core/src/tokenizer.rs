use std::error::Error;
use std::fmt::{self, Display};

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
fn is_token_ender(c: char) -> bool {
    c.is_ascii() && TOKEN_ENDERS_TABLE[c as usize]
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
#[derive(Debug, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        write!(f, "{:?}", self)
    }
}

/// The subtype of a token.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        write!(f, "{:?}", self)
    }
}

/// A token in an Excel formula.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Token {
    pub value: String,
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

        match op {
            ":" | " " | "," => Some((8, Associativity::Left)),
            "u" => Some((7, Associativity::Right)),
            "%" => Some((6, Associativity::Left)),
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

    /// Create a subexpression token.
    ///
    /// `value` must end with one of '{', '}', '(' or ')'. If `func` is true,
    /// the token’s type is forced to be Func.
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

/// A tokenizer for Excel worksheet formulas.
pub struct Tokenizer {
    formula: Vec<char>,  // The formula as a vector of characters.
    formula_str: String, // Original formula string for byte position tracking.
    pub items: Vec<Token>,
    token_stack: Vec<Token>,
    offset: usize,
    byte_pos: usize,         // Current byte position in the original string.
    token: Vec<char>,        // Accumulator for the current token.
    token_start_byte: usize, // Byte position where current token started.
}

impl Tokenizer {
    /// Create a new tokenizer and immediately parse the formula.
    pub fn new(formula: &str) -> Result<Self, TokenizerError> {
        let mut tokenizer = Tokenizer {
            formula: formula.chars().collect(),
            formula_str: formula.to_string(),
            items: Vec::with_capacity(formula.len() * 6), // Very safe estimate of 6 characters per token
            token_stack: Vec::with_capacity(64),
            offset: 0,
            byte_pos: 0,
            token: Vec::with_capacity(64),
            token_start_byte: 0,
        };
        tokenizer.parse()?;
        Ok(tokenizer)
    }

    /// Advance byte position by the given number of characters.
    fn advance_byte_pos(&mut self, char_count: usize) {
        for i in 0..char_count {
            if self.offset + i < self.formula.len() {
                self.byte_pos += self.formula[self.offset + i].len_utf8();
            }
        }
    }

    /// Start tracking a new token at the current byte position.
    fn start_token(&mut self) {
        self.token_start_byte = self.byte_pos;
    }

    /// Get current token's start and end byte positions.
    fn get_token_span(&self) -> (usize, usize) {
        (self.token_start_byte, self.byte_pos)
    }

    /// Parse the formula into tokens.
    fn parse(&mut self) -> Result<(), TokenizerError> {
        if self.offset != 0 {
            return Ok(());
        }
        if self.formula.is_empty() {
            return Ok(());
        } else if self.formula[0] == '=' {
            self.offset += 1;
            self.byte_pos += 1; // Skip the '=' character
        } else {
            let formula_str: String = self.formula.iter().collect();
            self.items.push(Token::new_with_span(
                formula_str,
                TokenType::Literal,
                TokenSubType::None,
                0,
                self.formula_str.len(),
            ));
            return Ok(());
        }

        self.start_token(); // Begin tracking for the first real token

        while self.offset < self.formula.len() {
            if self.check_scientific_notation()? {
                continue;
            }
            let curr_char = self.formula[self.offset];
            if is_token_ender(curr_char) {
                self.save_token();
                self.start_token(); // Start new token
            }
            // Dispatch based on the current character.
            let consumed = match curr_char {
                '"' | '\'' => self.parse_string()?,
                '[' => self.parse_brackets()?,
                '#' => self.parse_error()?,
                ' ' | '\n' => self.parse_whitespace()?,
                // operator characters
                '+' | '-' | '*' | '/' | '^' | '&' | '=' | '>' | '<' | '%' => {
                    self.parse_operator()?
                }
                '{' | '(' => self.parse_opener()?,
                ')' | '}' => self.parse_closer()?,
                ';' | ',' => self.parse_separator()?,
                _ => {
                    if self.token.is_empty() {
                        self.start_token();
                    }
                    self.token.push(curr_char);
                    1
                }
            };
            self.advance_byte_pos(consumed);
            self.offset += consumed;
        }
        self.save_token();
        Ok(())
    }

    /// If the current token looks like a number in scientific notation,
    /// consume the '+' or '-' as part of the number.
    fn check_scientific_notation(&mut self) -> Result<bool, TokenizerError> {
        let curr_char = self.formula[self.offset];
        if (curr_char == '+' || curr_char == '-')
            && !self.token.is_empty()
            && self.is_scientific_notation_base()
        {
            self.token.push(curr_char);
            self.advance_byte_pos(1);
            self.offset += 1;
            return Ok(true);
        }
        Ok(false)
    }

    /// Helper: Determine if the current accumulated token is the base of a
    /// scientific notation number (e.g., "1.23E" or "9e").
    fn is_scientific_notation_base(&self) -> bool {
        let token = &self.token;
        if token.len() < 2 {
            return false;
        }
        let last = token[token.len() - 1];
        if !(last == 'E' || last == 'e') {
            return false;
        }
        let first = token[0];
        if !('1'..='9').contains(&first) {
            return false;
        }
        let mut dot_seen = false;
        // Iterate over everything except the first and last characters.
        for &ch in &token[1..token.len() - 1] {
            match ch {
                '0'..='9' => {}
                '.' if !dot_seen => dot_seen = true,
                _ => return false,
            }
        }
        true
    }

    /// Ensure that there is no unconsumed token (or that it ends in an allowed character).
    fn assert_empty_token(&self, can_follow: Option<&[char]>) -> Result<(), TokenizerError> {
        if !self.token.is_empty() {
            if let Some(allowed) = can_follow {
                let last_char = *self.token.last().unwrap();
                if !allowed.contains(&last_char) {
                    return Err(TokenizerError {
                        message: format!(
                            "Unexpected character at position {} in '{}'",
                            self.offset,
                            self.formula.iter().collect::<String>()
                        ),
                        pos: self.byte_pos,
                    });
                }
            } else {
                return Err(TokenizerError {
                    message: format!(
                        "Unexpected character at position {} in '{}'",
                        self.offset,
                        self.formula.iter().collect::<String>()
                    ),
                    pos: self.byte_pos,
                });
            }
        }
        Ok(())
    }

    /// If there is an accumulated token, convert it to an operand token and add it to the list.
    fn save_token(&mut self) {
        if !self.token.is_empty() {
            let token_str: String = self.token.iter().collect();
            let (start, end) = self.get_token_span();
            self.items
                .push(Token::make_operand_with_span(token_str, start, end));
            self.token.clear();
        }
    }

    /// Parse a string (or link) literal.
    fn parse_string(&mut self) -> Result<usize, TokenizerError> {
        let delim = self.formula[self.offset];
        assert!(delim == '"' || delim == '\'');

        // If we're parsing a single-quoted string and the token is just a $,
        // don't save it as a separate token - it's part of the cell reference
        let is_dollar_ref =
            delim == '\'' && !self.token.is_empty() && self.token.iter().collect::<String>() == "$";

        if !is_dollar_ref {
            self.assert_empty_token(Some(&[':']))?;
            self.save_token();
            self.start_token(); // Start tracking for string token
        }

        // Manual parsing of quoted strings
        let mut i = self.offset;
        let formula_len = self.formula.len();

        // Collect the characters of the string, including the delimiters
        let mut string_chars = Vec::with_capacity(64);
        string_chars.push(delim);

        i += 1; // Skip the opening delimiter

        let mut found_end = false;

        while i < formula_len {
            let ch = self.formula[i];
            string_chars.push(ch);
            i += 1;

            if ch == delim {
                // Check if this is an escaped quote or the end
                if i < formula_len && self.formula[i] == delim {
                    // This is an escaped quote (doubled quotes) - include it
                    string_chars.push(self.formula[i]);
                    i += 1;
                } else {
                    // This is the end of the string
                    found_end = true;
                    break;
                }
            }
        }

        // Check if we found the closing delimiter
        if found_end {
            let matched_str: String = string_chars.iter().collect();
            let matched_len = string_chars.len();

            if delim == '"' {
                let start_byte = self.byte_pos;
                let end_byte = start_byte + matched_str.len();
                self.items.push(Token::make_operand_with_span(
                    matched_str,
                    start_byte,
                    end_byte,
                ));
            } else {
                // For a single-quote delimited string (sheet name reference)
                if is_dollar_ref {
                    // Combine the $ with the matched string
                    let dollar_str = format!("{}{}", "$", matched_str);
                    self.token.clear();
                    self.token.extend(dollar_str.chars());
                } else {
                    // Regular behavior
                    self.token.extend(matched_str.chars());
                }
            }

            Ok(matched_len)
        } else {
            // Error handling remains the same
            let subtype = if delim == '"' { "string" } else { "link" };
            Err(TokenizerError {
                message: format!(
                    "Reached end of formula while parsing {} in '{}'",
                    subtype,
                    self.formula.iter().collect::<String>()
                ),
                pos: self.byte_pos,
            })
        }
    }

    /// Parse the text between matching square brackets.
    fn parse_brackets(&mut self) -> Result<usize, TokenizerError> {
        assert_eq!(self.formula[self.offset], '[');
        let start = self.offset;
        let mut open_count = 0;
        for i in self.offset..self.formula.len() {
            if self.formula[i] == '[' {
                open_count += 1;
            } else if self.formula[i] == ']' {
                open_count -= 1;
            }
            if open_count == 0 {
                let outer_right = i - start + 1;
                let s: String = self.formula[self.offset..self.offset + outer_right]
                    .iter()
                    .collect();
                self.token.extend(s.chars());
                return Ok(outer_right);
            }
        }
        Err(TokenizerError {
            message: format!(
                "Encountered unmatched '[' in '{}'",
                self.formula.iter().collect::<String>()
            ),
            pos: self.byte_pos,
        })
    }

    /// Parse an error literal that starts with '#'.
    fn parse_error(&mut self) -> Result<usize, TokenizerError> {
        self.assert_empty_token(Some(&['!']))?;
        assert_eq!(self.formula[self.offset], '#');
        let remaining: String = self.formula[self.offset..].iter().collect();
        for &err in ERROR_CODES.iter() {
            if remaining.starts_with(err) {
                let token_str: String = self.token.iter().collect();
                let combined = format!("{}{}", token_str, err);
                let (start, _) = if !self.token.is_empty() {
                    self.get_token_span()
                } else {
                    (self.byte_pos, self.byte_pos)
                };
                let end = self.byte_pos + err.len();
                self.items
                    .push(Token::make_operand_with_span(combined, start, end));
                self.token.clear();
                return Ok(err.len());
            }
        }
        Err(TokenizerError {
            message: format!(
                "Invalid error code at position {} in '{}'",
                self.offset,
                self.formula.iter().collect::<String>()
            ),
            pos: self.byte_pos,
        })
    }

    /// Parse a sequence of whitespace characters.
    fn parse_whitespace(&mut self) -> Result<usize, TokenizerError> {
        let start_byte = self.byte_pos;
        let start = self.offset;
        let mut i = start;
        while i < self.formula.len() {
            let ch = self.formula[i];
            if ch == ' ' || ch == '\n' {
                i += 1;
            } else {
                break;
            }
        }
        let matched_len = i - start;
        let matched_str: String = self.formula[start..i].iter().collect();
        let end_byte = start_byte + matched_str.len(); // ASCII whitespace is 1 byte each
        self.items.push(Token::new_with_span(
            matched_str,
            TokenType::Whitespace,
            TokenSubType::None,
            start_byte,
            end_byte,
        ));
        Ok(matched_len)
    }

    /// Parse an operator token.
    fn parse_operator(&mut self) -> Result<usize, TokenizerError> {
        self.save_token();
        self.start_token(); // Start new token for operator

        if self.offset + 1 < self.formula.len() {
            let op2: String = self.formula[self.offset..self.offset + 2].iter().collect();
            if op2 == ">=" || op2 == "<=" || op2 == "<>" {
                let end_byte = self.byte_pos + 2; // Two-character operators are ASCII
                self.items.push(Token::new_with_span(
                    op2,
                    TokenType::OpInfix,
                    TokenSubType::None,
                    self.byte_pos,
                    end_byte,
                ));
                return Ok(2);
            }
        }
        let curr_char = self.formula[self.offset];
        assert!(matches!(
            curr_char,
            '%' | '*' | '/' | '^' | '&' | '=' | '>' | '<' | '+' | '-'
        ));
        let end_byte = self.byte_pos + 1; // Single character operators are ASCII
        let token = if curr_char == '%' {
            Token::new_with_span(
                "%".to_string(),
                TokenType::OpPostfix,
                TokenSubType::None,
                self.byte_pos,
                end_byte,
            )
        } else if "* /^&=><".contains(curr_char) {
            Token::new_with_span(
                curr_char.to_string(),
                TokenType::OpInfix,
                TokenSubType::None,
                self.byte_pos,
                end_byte,
            )
        } else if curr_char == '+' || curr_char == '-' {
            if self.items.is_empty() {
                Token::new_with_span(
                    curr_char.to_string(),
                    TokenType::OpPrefix,
                    TokenSubType::None,
                    self.byte_pos,
                    end_byte,
                )
            } else {
                let prev = self
                    .items
                    .iter()
                    .rev()
                    .find(|t| t.token_type != TokenType::Whitespace);
                let is_infix = prev.is_some_and(|p| {
                    p.subtype == TokenSubType::Close
                        || p.token_type == TokenType::OpPostfix
                        || p.token_type == TokenType::Operand
                });
                if is_infix {
                    Token::new_with_span(
                        curr_char.to_string(),
                        TokenType::OpInfix,
                        TokenSubType::None,
                        self.byte_pos,
                        end_byte,
                    )
                } else {
                    Token::new_with_span(
                        curr_char.to_string(),
                        TokenType::OpPrefix,
                        TokenSubType::None,
                        self.byte_pos,
                        end_byte,
                    )
                }
            }
        } else {
            Token::new_with_span(
                curr_char.to_string(),
                TokenType::OpInfix,
                TokenSubType::None,
                self.byte_pos,
                end_byte,
            )
        };
        self.items.push(token);
        Ok(1)
    }

    /// Parse an opener token – either '(' or '{'.
    fn parse_opener(&mut self) -> Result<usize, TokenizerError> {
        let curr_char = self.formula[self.offset];
        assert!(curr_char == '(' || curr_char == '{');

        let token = if curr_char == '{' {
            self.assert_empty_token(None)?;
            self.start_token();
            let end_byte = self.byte_pos + 1;
            Token::make_subexp_with_span("{", false, self.byte_pos, end_byte)
        } else if !self.token.is_empty() {
            let token_value: String = self.token.iter().collect::<String>() + "(";
            let (start, _) = self.get_token_span();
            let end_byte = self.byte_pos + 1;
            self.token.clear();
            Token::make_subexp_with_span(&token_value, true, start, end_byte)
        } else {
            self.start_token();
            let end_byte = self.byte_pos + 1;
            Token::make_subexp_with_span("(", false, self.byte_pos, end_byte)
        };
        self.items.push(token.clone());
        self.token_stack.push(token);
        Ok(1)
    }

    /// Parse a closer token – either ')' or '}'.
    fn parse_closer(&mut self) -> Result<usize, TokenizerError> {
        let curr_char = self.formula[self.offset];
        assert!(curr_char == ')' || curr_char == '}');
        if let Some(open_token) = self.token_stack.pop() {
            let mut closer = open_token.get_closer()?;
            if closer.value.chars().next().unwrap() != curr_char {
                return Err(TokenizerError {
                    message: format!(
                        "Mismatched ( and {{ pair in '{}'",
                        self.formula.iter().collect::<String>()
                    ),
                    pos: self.byte_pos,
                });
            }
            // Set correct byte positions for the closer
            closer.start = self.byte_pos;
            closer.end = self.byte_pos + 1;
            self.items.push(closer);
            Ok(1)
        } else {
            Err(TokenizerError {
                message: format!(
                    "No matching opener for closer at position {} in '{}'",
                    self.offset,
                    self.formula.iter().collect::<String>()
                ),
                pos: self.byte_pos,
            })
        }
    }

    /// Parse a separator token – either ',' or ';'.
    fn parse_separator(&mut self) -> Result<usize, TokenizerError> {
        let curr_char = self.formula[self.offset];
        assert!(curr_char == ';' || curr_char == ',');
        let start_byte = self.byte_pos;
        let end_byte = self.byte_pos + 1;

        let token = if curr_char == ';' {
            Token::make_separator_with_span(";", start_byte, end_byte)
        } else if let Some(top) = self.token_stack.last() {
            if top.token_type == TokenType::Paren {
                Token::new_with_span(
                    ",".to_string(),
                    TokenType::OpInfix,
                    TokenSubType::None,
                    start_byte,
                    end_byte,
                )
            } else if top.token_type == TokenType::Func || top.token_type == TokenType::Array {
                Token::make_separator_with_span(",", start_byte, end_byte)
            } else {
                Token::new_with_span(
                    ",".to_string(),
                    TokenType::OpInfix,
                    TokenSubType::None,
                    start_byte,
                    end_byte,
                )
            }
        } else {
            Token::new_with_span(
                ",".to_string(),
                TokenType::OpInfix,
                TokenSubType::None,
                start_byte,
                end_byte,
            )
        };
        self.items.push(token);
        Ok(1)
    }

    /// Reconstruct the formula from the parsed tokens.
    pub fn render(&self) -> String {
        if self.items.is_empty() {
            "".to_string()
        } else if self.items[0].token_type == TokenType::Literal {
            self.items[0].value.clone()
        } else {
            let concatenated: String = self.items.iter().map(|t| t.value.clone()).collect();
            format!("={}", concatenated)
        }
    }
}
