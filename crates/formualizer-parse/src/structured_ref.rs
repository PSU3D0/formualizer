//! Recursive-descent parser for Excel structured (table) references.
//!
//! Implements the bracket-content grammar described in MS-XLSX
//! \u00a718.17.6.2 and used throughout `Table[Column]` style references:
//!
//! ```text
//! specifier       := "[" content "]"
//! content         := "" | special | column | column_range | combination
//! special         := "#All" | "#Headers" | "#Data" | "#Totals" | "#This Row" | "@"
//! column          := "[" name "]"   ; outer bracket optional in simple form
//! column_range    := column ":" column
//! combination     := item ("," item)*
//! item            := "[" special "]" | column | column_range
//! ```
//!
//! Naming is case-insensitive for specials. Column names support an
//! OOXML-style per-character escape `'X` that lets `[`, `]`, `'`, and `#`
//! appear inside a column identifier.

use crate::parser::{SpecialItem, TableSpecifier};
use crate::types::ParsingError;

/// Top-level entry point: parse a complete bracketed specifier and reject
/// trailing garbage.
pub(crate) fn parse_full_specifier(input: &str) -> Result<Option<TableSpecifier>, ParsingError> {
    if input.is_empty() {
        return Ok(None);
    }
    let mut p = SpecifierParser::new(input);
    let spec = p.parse_specifier()?;
    if p.pos != p.src.len() {
        return Err(ParsingError::InvalidReference(format!(
            "Trailing content after structured reference at offset {}: {:?}",
            p.pos,
            &p.src[p.pos..]
        )));
    }
    Ok(Some(spec))
}

struct SpecifierParser<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SpecifierParser<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            bytes: src.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self, expected: u8) -> Result<(), ParsingError> {
        match self.peek() {
            Some(b) if b == expected => {
                self.pos += 1;
                Ok(())
            }
            Some(b) => Err(ParsingError::InvalidReference(format!(
                "Expected {:?} at offset {}, found {:?}",
                expected as char, self.pos, b as char
            ))),
            None => Err(ParsingError::InvalidReference(format!(
                "Expected {:?} at offset {}, found end of input",
                expected as char, self.pos
            ))),
        }
    }

    /// Parse the outermost `[ content ]` block.
    fn parse_specifier(&mut self) -> Result<TableSpecifier, ParsingError> {
        self.bump(b'[')?;
        let inner_start = self.pos;
        // Find the matching `]` that terminates this specifier (respecting
        // nesting and `'`-escapes). The substring [inner_start..close) is
        // the "content".
        let close = self.find_matching_close(inner_start - 1)?;
        let content = &self.src[inner_start..close];
        // Advance past the closing bracket so callers see what comes next.
        self.pos = close + 1;
        parse_content(content)
    }

    /// Given the position of an opening `[`, return the byte index of the
    /// matching `]`.
    fn find_matching_close(&self, open_pos: usize) -> Result<usize, ParsingError> {
        debug_assert_eq!(self.bytes[open_pos], b'[');
        let mut depth: u32 = 1;
        let mut i = open_pos + 1;
        while i < self.bytes.len() {
            match self.bytes[i] {
                b'\'' => {
                    // Per-character escape; skip the next byte unconditionally.
                    if i + 1 < self.bytes.len() {
                        i += 2;
                        continue;
                    } else {
                        return Err(ParsingError::InvalidReference(
                            "Trailing '\\'' inside structured reference".to_string(),
                        ));
                    }
                }
                b'[' => depth += 1,
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(i);
                    }
                }
                _ => {}
            }
            i += 1;
        }
        Err(ParsingError::InvalidReference(format!(
            "Unbalanced '[' at offset {open_pos} in structured reference"
        )))
    }
}

/// Parse the content between the outermost brackets of a specifier.
fn parse_content(content: &str) -> Result<TableSpecifier, ParsingError> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        // `Table1[]` is canonically the whole table.
        return Ok(TableSpecifier::All);
    }

    // Decide between three top-level shapes based on a structural scan that
    // respects `'`-escapes and `[]` nesting:
    //   * a single bracketed item or a comma-separated combination of items
    //     (any `[` at depth 0)
    //   * a column-range using bare names: `name : name`
    //   * a special starting with `#` or `@`
    //   * a single bare column name
    // `@`-prefixed shorthands cannot be confused with combinations because
    // the `@` always precedes either nothing (`@`) or a single column (a
    // bracketed `[Col]` or bare `Col`). Handle this form first so the
    // ordinary combination parser doesn't choke on the leading `@`.
    if let Some(rest) = trimmed.strip_prefix('@') {
        return parse_at_shorthand(rest.trim());
    }

    let scan = scan_top_level(trimmed)?;

    if scan.has_top_level_bracket {
        return parse_combination_or_item(trimmed);
    }

    if scan.has_top_level_comma {
        return Err(ParsingError::InvalidReference(format!(
            "Unexpected ',' in structured reference content: {trimmed:?}"
        )));
    }

    if let Some(colon_idx) = scan.top_level_colon {
        let start = trimmed[..colon_idx].trim();
        let end = trimmed[colon_idx + 1..].trim();
        return build_column_range(start, end);
    }

    if trimmed.starts_with('#') {
        return parse_special_token(trimmed).map(TableSpecifier::SpecialItem);
    }

    Ok(TableSpecifier::Column(unescape_name(trimmed)))
}

struct TopLevelScan {
    has_top_level_bracket: bool,
    has_top_level_comma: bool,
    top_level_colon: Option<usize>,
}

/// Walk `s` once, ignoring escaped bytes and bracket-enclosed regions, and
/// report top-level structural punctuation.
fn scan_top_level(s: &str) -> Result<TopLevelScan, ParsingError> {
    let bytes = s.as_bytes();
    let mut depth: u32 = 0;
    let mut top_bracket = false;
    let mut top_comma = false;
    let mut top_colon: Option<usize> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                if i + 1 < bytes.len() {
                    i += 2;
                    continue;
                } else {
                    return Err(ParsingError::InvalidReference(
                        "Dangling '\\'' escape in structured reference".to_string(),
                    ));
                }
            }
            b'[' => {
                if depth == 0 {
                    top_bracket = true;
                }
                depth += 1;
            }
            b']' => {
                if depth == 0 {
                    return Err(ParsingError::InvalidReference(
                        "Stray ']' in structured reference content".to_string(),
                    ));
                }
                depth -= 1;
            }
            b',' if depth == 0 => top_comma = true,
            b':' if depth == 0 && top_colon.is_none() => top_colon = Some(i),
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return Err(ParsingError::InvalidReference(
            "Unbalanced '[' in structured reference content".to_string(),
        ));
    }
    Ok(TopLevelScan {
        has_top_level_bracket: top_bracket,
        has_top_level_comma: top_comma,
        top_level_colon: top_colon,
    })
}

/// Parse content that contains at least one bracketed segment: either a
/// single bracketed item, a `[col]:[col]` range, or a combination.
fn parse_combination_or_item(content: &str) -> Result<TableSpecifier, ParsingError> {
    let mut items: Vec<TableSpecifier> = Vec::new();
    let mut p = ItemParser::new(content);
    p.skip_ws();
    if p.eof() {
        return Ok(TableSpecifier::All);
    }
    loop {
        let item = p.parse_item()?;
        items.push(item);
        p.skip_ws();
        if p.eof() {
            break;
        }
        if p.peek() == Some(b',') {
            p.advance(1);
            p.skip_ws();
            continue;
        }
        return Err(ParsingError::InvalidReference(format!(
            "Expected ',' between structured-reference items at offset {} of {:?}",
            p.pos, content
        )));
    }

    if items.len() == 1 {
        Ok(items.pop().unwrap())
    } else {
        Ok(TableSpecifier::Combination(
            items.into_iter().map(Box::new).collect(),
        ))
    }
}

struct ItemParser<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ItemParser<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            bytes: src.as_bytes(),
            pos: 0,
        }
    }
    fn eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }
    fn advance(&mut self, n: usize) {
        self.pos += n;
    }
    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            if b == b' ' || b == b'\t' || b == b'\n' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// Parse a single item: a bracketed item (`[...]`), or - in a combination -
    /// a bare column or column range. Bracketed items themselves may be
    /// either a special, a column name, or part of a `[col]:[col]` range.
    fn parse_item(&mut self) -> Result<TableSpecifier, ParsingError> {
        self.skip_ws();
        if self.peek() != Some(b'[') {
            return Err(ParsingError::InvalidReference(format!(
                "Expected '[' to start structured-reference item at offset {} of {:?}",
                self.pos, self.src
            )));
        }
        let first = self.parse_bracketed_token()?;
        // Look-ahead for `:` (column range continuation).
        let save = self.pos;
        self.skip_ws();
        if self.peek() == Some(b':') {
            self.advance(1);
            self.skip_ws();
            if self.peek() != Some(b'[') {
                return Err(ParsingError::InvalidReference(format!(
                    "Expected '[' after ':' in column range at offset {} of {:?}",
                    self.pos, self.src
                )));
            }
            let second = self.parse_bracketed_token()?;
            return build_column_range_from_tokens(first, second);
        }
        // No `:` follow-on; rewind whitespace skip.
        self.pos = save;
        bracketed_token_to_specifier(first)
    }

    fn parse_bracketed_token(&mut self) -> Result<BracketedToken, ParsingError> {
        debug_assert_eq!(self.peek(), Some(b'['));
        let open = self.pos;
        self.pos += 1;
        let inner_start = self.pos;
        let mut depth: u32 = 1;
        while !self.eof() {
            match self.bytes[self.pos] {
                b'\'' => {
                    if self.pos + 1 < self.bytes.len() {
                        self.pos += 2;
                        continue;
                    } else {
                        return Err(ParsingError::InvalidReference(format!(
                            "Trailing '\\'' inside item at offset {}",
                            self.pos
                        )));
                    }
                }
                b'[' => {
                    depth += 1;
                    self.pos += 1;
                }
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        let inner = &self.src[inner_start..self.pos];
                        self.pos += 1;
                        return Ok(BracketedToken {
                            inner: inner.to_string(),
                        });
                    }
                    self.pos += 1;
                }
                _ => self.pos += 1,
            }
        }
        Err(ParsingError::InvalidReference(format!(
            "Unbalanced '[' starting at offset {open} in {:?}",
            self.src
        )))
    }
}

#[derive(Debug, Clone)]
struct BracketedToken {
    inner: String,
}

fn bracketed_token_to_specifier(tok: BracketedToken) -> Result<TableSpecifier, ParsingError> {
    let trimmed = tok.inner.trim();
    if trimmed.is_empty() {
        return Err(ParsingError::InvalidReference(
            "Empty '[]' inside structured-reference combination".to_string(),
        ));
    }
    if trimmed.starts_with('#') {
        return parse_special_token(trimmed).map(TableSpecifier::SpecialItem);
    }
    if let Some(rest) = trimmed.strip_prefix('@') {
        let rest = rest.trim();
        if rest.is_empty() {
            // Bare `[@]` inside a combination is the ThisRow special item; the
            // combination form distinguishes itself from `Table1[@]` (which
            // alone resolves to `Row(Current)`).
            return Ok(TableSpecifier::SpecialItem(SpecialItem::ThisRow));
        }
        // `[@Column]` inside a combination/item still resolves to ThisRow + Column.
        return parse_at_shorthand(rest);
    }
    Ok(TableSpecifier::Column(unescape_name(trimmed)))
}

fn build_column_range_from_tokens(
    a: BracketedToken,
    b: BracketedToken,
) -> Result<TableSpecifier, ParsingError> {
    let lhs = a.inner.trim();
    let rhs = b.inner.trim();
    if lhs.is_empty() || rhs.is_empty() {
        return Err(ParsingError::InvalidReference(
            "Empty column name in structured-reference range".to_string(),
        ));
    }
    if lhs.starts_with('#') || lhs.starts_with('@') || rhs.starts_with('#') || rhs.starts_with('@')
    {
        return Err(ParsingError::InvalidReference(format!(
            "Special items cannot appear in column range: [{lhs}]:[{rhs}]"
        )));
    }
    Ok(TableSpecifier::ColumnRange(
        unescape_name(lhs),
        unescape_name(rhs),
    ))
}

fn build_column_range(lhs: &str, rhs: &str) -> Result<TableSpecifier, ParsingError> {
    if lhs.is_empty() || rhs.is_empty() {
        return Err(ParsingError::InvalidReference(
            "Empty column name in structured-reference range".to_string(),
        ));
    }
    Ok(TableSpecifier::ColumnRange(
        unescape_name(lhs),
        unescape_name(rhs),
    ))
}

/// Parse a `#`-prefixed special item, case-insensitively. Returns the bare
/// `SpecialItem` enum (`@` is handled by the caller, not here).
fn parse_special_token(token: &str) -> Result<SpecialItem, ParsingError> {
    debug_assert!(token.starts_with('#'));
    // Normalise internal whitespace runs to a single ASCII space so
    // `#This  Row` and `#This\tRow` still match.
    let normalized: String = token
        .chars()
        .map(|c| if c == '\t' { ' ' } else { c })
        .collect();
    let mut compact = String::with_capacity(normalized.len());
    let mut prev_space = false;
    for c in normalized.chars() {
        if c == ' ' {
            if !prev_space {
                compact.push(' ');
            }
            prev_space = true;
        } else {
            compact.push(c);
            prev_space = false;
        }
    }
    match compact.to_ascii_lowercase().as_str() {
        "#all" => Ok(SpecialItem::All),
        "#headers" => Ok(SpecialItem::Headers),
        "#data" => Ok(SpecialItem::Data),
        "#totals" => Ok(SpecialItem::Totals),
        "#this row" => Ok(SpecialItem::ThisRow),
        _ => Err(ParsingError::InvalidReference(format!(
            "Unknown special item: {token}"
        ))),
    }
}

/// Parse `@...` shorthand: `@`, `@Col`, `@[Col Name]` ...
fn parse_at_shorthand(rest: &str) -> Result<TableSpecifier, ParsingError> {
    if rest.is_empty() {
        return Ok(TableSpecifier::Row(
            crate::parser::TableRowSpecifier::Current,
        ));
    }
    // Strip an optional surrounding `[ ... ]` to support `@[Col Name]`.
    let trimmed = rest.trim();
    let column = if let Some(stripped) = strip_outer_brackets(trimmed) {
        stripped
    } else {
        trimmed.to_string()
    };
    if column.is_empty() {
        return Err(ParsingError::InvalidReference(
            "Empty column after '@' in structured reference".to_string(),
        ));
    }
    Ok(TableSpecifier::Combination(vec![
        Box::new(TableSpecifier::SpecialItem(SpecialItem::ThisRow)),
        Box::new(TableSpecifier::Column(unescape_name(&column))),
    ]))
}

fn strip_outer_brackets(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'[' || bytes[bytes.len() - 1] != b']' {
        return None;
    }
    // Validate balanced (with `'`-escape) and that the matching close is the
    // final byte; if the first `[` matches an interior `]` then this isn't
    // a single outer-bracketed token.
    let mut depth: u32 = 1;
    let mut i = 1;
    while i < bytes.len() - 1 {
        match bytes[i] {
            b'\'' => {
                i += 2;
                continue;
            }
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    return None;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 1 {
        return None;
    }
    Some(s[1..s.len() - 1].to_string())
}

/// Apply the OOXML `'X` per-character escape: a single apostrophe makes the
/// next character literal. A trailing apostrophe is left as-is (the caller
/// will already have caught structural truncation in earlier phases).
fn unescape_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(c);
        }
    }
    // Trim outer ASCII whitespace; column names rarely have leading/trailing
    // spaces and Excel itself strips them on round-trip.
    out.trim().to_string()
}
