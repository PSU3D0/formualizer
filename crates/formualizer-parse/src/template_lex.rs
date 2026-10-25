//! Conservative lexical recognition of relocatable formula templates.
//!
//! [`lex_template`] retains the conservative SDK 4.0 subset: local canonical
//! A1 cells and finite ranges, an allowlist of pure functions, and no names,
//! qualifiers, whitespace or `@` outside string literals.
//!
//! [`lex_template_for_certified_ast`] additionally recognises invariant names,
//! sheet qualifiers, whitespace, `@` and general functions. It provides only a
//! lexical envelope, **not** purity, non-volatility or AST certification. Callers
//! must certify every parsed leaf against its slots and reject structural
//! rewrites or node/reference kinds their family comparator cannot instantiate
//! before trusting a match. The engine's source-family ingress does this.
//!
//! Both functions use the parser's tokenizer and canonical coordinate spelling
//! (uppercase letters, no leading zeros). External references, whole rows or
//! columns, structured references, arrays, error literals, `#`, unions and
//! intersections remain excluded. [`TemplateLexeme::match_relocated`] compares
//! fixed bytes and relocated reference slots without parsing.

use crate::tokenizer::{TokenSubType, TokenType, tokenize_spans_with_limits};
use crate::types::FormulaDialect;

const MAX_ROW: u32 = 1_048_576;
const MAX_COL: u32 = 16_384;

// Preserve the published SDK 4.0 recognizer's exact function inventory.
const FUNCTIONS: &[&str] = &[
    "ABS",
    "AND",
    "AVERAGE",
    "AVERAGEIF",
    "AVERAGEIFS",
    "CHOOSE",
    "CONCATENATE",
    "COUNT",
    "COUNTA",
    "COUNTBLANK",
    "COUNTIF",
    "COUNTIFS",
    "DATE",
    "DAY",
    "EXACT",
    "EXP",
    "FIND",
    "HLOOKUP",
    "IF",
    "IFERROR",
    "INT",
    "ISBLANK",
    "ISERR",
    "ISERROR",
    "ISNA",
    "ISNUMBER",
    "ISTEXT",
    "LEFT",
    "LEN",
    "LN",
    "LOWER",
    "MATCH",
    "MAX",
    "MID",
    "MIN",
    "MOD",
    "MONTH",
    "NOT",
    "OR",
    "POWER",
    "PRODUCT",
    "PROPER",
    "RIGHT",
    "ROUND",
    "ROUNDDOWN",
    "ROUNDUP",
    "SEARCH",
    "SIGN",
    "SQRT",
    "SUBSTITUTE",
    "SUM",
    "SUMIF",
    "SUMIFS",
    "SUMPRODUCT",
    "TRIM",
    "UPPER",
    "VALUE",
    "VLOOKUP",
    "YEAR",
];

/// One endpoint's A1 coordinates: 1-based coordinates and `$` flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct A1Point {
    pub row: u32,
    pub col: u32,
    pub row_abs: bool,
    pub col_abs: bool,
}

/// A reference slot's referent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotRef {
    Cell(A1Point),
    Range(A1Point, A1Point),
}

/// A coordinate slot of the template: its byte span and referent.
/// Any sheet qualifier is outside the span and remains fixed text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefSlot {
    pub start: u32,
    pub end: u32,
    pub reference: SlotRef,
}

/// A template's reference slots, in source order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateLexeme {
    slots: Box<[RefSlot]>,
    /// Byte length of the text the lexeme was built from.
    template_len: usize,
}

/// The verdict of [`TemplateLexeme::match_relocated`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelocatedMatch {
    /// The candidate is the template relocated by the offset.
    Match,
    /// The candidate's bytes differ from the relocated template.
    Mismatch,
    /// Some relocated reference leaves the grid, or a range would have its
    /// ends out of order; no candidate is accepted.
    OffGrid,
}

impl TemplateLexeme {
    pub fn slots(&self) -> &[RefSlot] {
        &self.slots
    }

    /// Whether `candidate` is exactly `template` (the text this lexeme was
    /// built from) with every reference relocated by `(dr, dc)`: relative
    /// axes move, absolute axes stay, every other byte is identical.
    ///
    /// A `template` of a different byte length than the build text is a
    /// [`RelocatedMatch::Mismatch`]. A different text of the same length is
    /// outside this contract: the result then compares `candidate` against
    /// that text's bytes around the build text's slots, without panicking.
    pub fn match_relocated(
        &self,
        template: &str,
        candidate: &str,
        dr: i64,
        dc: i64,
    ) -> RelocatedMatch {
        if template.len() != self.template_len {
            return RelocatedMatch::Mismatch;
        }
        let template = template.as_bytes();
        let mut rest = candidate.as_bytes();
        let mut buf = [0u8; 32];
        let mut fixed = 0usize;
        for slot in self.slots.iter() {
            let (start, end) = (slot.start as usize, slot.end as usize);
            let Some(len) = render_relocated(slot.reference, dr, dc, &mut buf) else {
                return RelocatedMatch::OffGrid;
            };
            let Some(lead) = template.get(fixed..start) else {
                return RelocatedMatch::Mismatch;
            };
            let Some(after_lead) = rest.strip_prefix(lead) else {
                return RelocatedMatch::Mismatch;
            };
            #[expect(
                clippy::indexing_slicing,
                reason = "render_relocated writes at most 25 bytes (`$XFD$1048576:$XFD$1048576`) into the 32-byte buffer"
            )]
            let rendered = &buf[..len];
            let Some(after_slot) = after_lead.strip_prefix(rendered) else {
                return RelocatedMatch::Mismatch;
            };
            rest = after_slot;
            fixed = end;
        }
        if template.get(fixed..) == Some(rest) {
            RelocatedMatch::Match
        } else {
            RelocatedMatch::Mismatch
        }
    }
}

/// Recognise `formula` (with its leading `=`) as a relocatable template.
/// Returns `None` for anything outside the original SDK 4.0 conservative subset
/// described in the module documentation. Broader recognition is opt-in through
/// [`lex_template_for_certified_ast`].
pub fn lex_template(formula: &str) -> Option<TemplateLexeme> {
    lex_template_impl::<false>(formula)
}

/// Recognise a broader lexical envelope for an AST-certified source family.
///
/// Invariant names, sheet qualifiers, whitespace, `@` and arbitrary functions
/// are accepted. `Some` is **not** proof of purity or non-volatility: for example,
/// `NOW()` and `INDIRECT(...)` can be recognised. Before trusting relocation,
/// callers must certify the parsed tree and every reference leaf against these
/// slots, including the safety rules of their source-family implementation.
/// This function neither parses nor certifies that tree itself.
pub fn lex_template_for_certified_ast(formula: &str) -> Option<TemplateLexeme> {
    lex_template_impl::<true>(formula)
}

fn lex_template_impl<const CERTIFIED_AST: bool>(formula: &str) -> Option<TemplateLexeme> {
    let body = formula.strip_prefix('=')?;
    if body.is_empty() || formula.len() > u32::MAX as usize {
        return None;
    }
    // Cheap byte screen before tokenizing: exclude structured/external
    // references, arrays, error/spill syntax and array row separators.
    let mut in_string = false;
    for &b in body.as_bytes() {
        if b == b'"' {
            in_string = !in_string;
        } else if !in_string
            && (matches!(b, b'[' | b']' | b'{' | b'}' | b'#' | b';')
                || (!CERTIFIED_AST
                    && matches!(b, b' ' | b'\t' | b'\r' | b'\n' | b'!' | b'\'' | b'@')))
        {
            return None;
        }
    }
    let spans = tokenize_spans_with_limits(
        formula,
        FormulaDialect::Excel,
        crate::ParserLimits::default(),
    )
    .ok()?;
    let mut slots = Vec::new();
    // Open brackets: true for a function call, false for a parenthesis.
    let mut open: Vec<bool> = Vec::new();
    for span in &spans {
        let text = formula.get(span.start..span.end)?;
        match (span.token_type, span.subtype) {
            (TokenType::Operand, TokenSubType::Number) => {
                if !text.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
                    return None;
                }
            }
            (TokenType::Operand, TokenSubType::Text | TokenSubType::Logical) => {}
            (TokenType::Operand, TokenSubType::Range) => {
                if !CERTIFIED_AST {
                    slots.push(RefSlot {
                        start: span.start as u32,
                        end: span.end as u32,
                        reference: canonical_reference(text)?,
                    });
                    continue;
                }
                // A qualifier is invariant source text; only its coordinate
                // suffix relocates. Other range operands may be invariant
                // defined names. The engine must certify every parsed leaf
                // against these slots before trusting this lexical envelope.
                let (prefix_len, coordinates) =
                    text.rsplit_once('!').map_or((0, text), |(_, suffix)| {
                        (text.len().saturating_sub(suffix.len()), suffix)
                    });
                if let Some(reference) = canonical_reference(coordinates) {
                    if text.get(..prefix_len)?.contains(':') {
                        return None;
                    }
                    slots.push(RefSlot {
                        start: span.start.checked_add(prefix_len)? as u32,
                        end: span.end as u32,
                        reference,
                    });
                } else if !matches!(
                    crate::parser::ReferenceType::from_string(text).ok()?,
                    crate::parser::ReferenceType::NamedRange(_)
                ) {
                    return None;
                }
            }
            (TokenType::Func, TokenSubType::Open) => {
                let name = text.strip_suffix('(')?;
                if name.trim().is_empty()
                    || (!CERTIFIED_AST && !FUNCTIONS.iter().any(|f| f.eq_ignore_ascii_case(name)))
                {
                    return None;
                }
                open.push(true);
            }
            (TokenType::Paren, TokenSubType::Open) => open.push(false),
            (TokenType::Func, TokenSubType::Close) => {
                if open.pop() != Some(true) {
                    return None;
                }
            }
            (TokenType::Paren, TokenSubType::Close) => {
                if open.pop() != Some(false) {
                    return None;
                }
            }
            (TokenType::Sep, TokenSubType::Arg) => {
                if open.last() != Some(&true) {
                    return None;
                }
            }
            (TokenType::OpPrefix, _) => {
                if !(matches!(text, "+" | "-") || (CERTIFIED_AST && text == "@")) {
                    return None;
                }
            }
            (TokenType::OpInfix, _) => {
                if !matches!(
                    text,
                    "+" | "-" | "*" | "/" | "^" | "&" | "=" | "<>" | "<" | ">" | "<=" | ">="
                ) {
                    return None;
                }
            }
            (TokenType::OpPostfix, _) => {
                if text != "%" {
                    return None;
                }
            }
            (TokenType::Whitespace, _) if CERTIFIED_AST => {}
            _ => return None,
        }
    }
    if !open.is_empty() {
        return None;
    }
    Some(TemplateLexeme {
        slots: slots.into_boxed_slice(),
        template_len: formula.len(),
    })
}

/// A canonical local A1 cell (`B7`, `$B7`, `B$7`, `$B$7`) or finite range
/// of two such cells with its ends in order.
fn canonical_reference(text: &str) -> Option<SlotRef> {
    match text.split_once(':') {
        None => canonical_point(text).map(SlotRef::Cell),
        Some((a, b)) => {
            let (a, b) = (canonical_point(a)?, canonical_point(b)?);
            (a.row <= b.row && a.col <= b.col).then_some(SlotRef::Range(a, b))
        }
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "cursor bounded by slice length; at most 3 letters and 7 digits are accumulated (col <= 18_278, row <= 9_999_999), each checked uppercase or digit before its offset is subtracted"
)]
fn canonical_point(text: &str) -> Option<A1Point> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let col_abs = bytes.first() == Some(&b'$');
    if col_abs {
        i += 1;
    }
    let letters = i;
    let mut col: u32 = 0;
    while let Some(&b) = bytes.get(i)
        && b.is_ascii_uppercase()
    {
        if i - letters == 3 {
            return None;
        }
        col = col * 26 + u32::from(b - b'A' + 1);
        i += 1;
    }
    if i == letters || col > MAX_COL {
        return None;
    }
    let row_abs = bytes.get(i) == Some(&b'$');
    if row_abs {
        i += 1;
    }
    let digits = i;
    if !bytes.get(i).is_some_and(|b| (b'1'..=b'9').contains(b)) {
        return None;
    }
    let mut row: u32 = 0;
    while let Some(&b) = bytes.get(i)
        && b.is_ascii_digit()
    {
        if i - digits == 7 {
            return None;
        }
        row = row * 10 + u32::from(b - b'0');
        i += 1;
    }
    (i == bytes.len() && row <= MAX_ROW).then_some(A1Point {
        row,
        col,
        row_abs,
        col_abs,
    })
}

fn relocate_axis(value: u32, abs: bool, delta: i64, max: u32) -> Option<u32> {
    if abs {
        return Some(value);
    }
    let moved = i64::from(value).checked_add(delta)?;
    (1..=i64::from(max))
        .contains(&moved)
        .then_some(moved as u32)
}

fn relocate_point(p: A1Point, dr: i64, dc: i64) -> Option<A1Point> {
    Some(A1Point {
        row: relocate_axis(p.row, p.row_abs, dr, MAX_ROW)?,
        col: relocate_axis(p.col, p.col_abs, dc, MAX_COL)?,
        ..p
    })
}

/// Write the canonical spelling of `reference` relocated by `(dr, dc)`;
/// `None` when it leaves the grid or a range's ends cross.
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "slots are built only by lex_template with 1 <= col <= 16_384 and 1 <= row <= 1_048_576, and relocate_point keeps that range: at most 3 letters, 7 digits and 25 bytes are written into the 32-byte buffer"
)]
fn render_relocated(reference: SlotRef, dr: i64, dc: i64, buf: &mut [u8; 32]) -> Option<usize> {
    match reference {
        SlotRef::Cell(p) => Some(render_point(relocate_point(p, dr, dc)?, buf, 0)),
        SlotRef::Range(a, b) => {
            let (a, b) = (relocate_point(a, dr, dc)?, relocate_point(b, dr, dc)?);
            if a.row > b.row || a.col > b.col {
                return None;
            }
            let mut at = render_point(a, buf, 0);
            buf[at] = b':';
            at += 1;
            Some(render_point(b, buf, at))
        }
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "slots are built only by lex_template with 1 <= col <= 16_384 and 1 <= row <= 1_048_576, and relocate_point keeps that range: at most 3 letters, 7 digits and 25 bytes are written into the 32-byte buffer"
)]
fn render_point(p: A1Point, buf: &mut [u8; 32], mut at: usize) -> usize {
    if p.col_abs {
        buf[at] = b'$';
        at += 1;
    }
    let mut letters = [0u8; 3];
    let mut n = 0;
    let mut col = p.col;
    while col > 0 {
        let rem = (col - 1) % 26;
        letters[n] = b'A' + rem as u8;
        n += 1;
        col = (col - 1) / 26;
    }
    for i in (0..n).rev() {
        buf[at] = letters[i];
        at += 1;
    }
    if p.row_abs {
        buf[at] = b'$';
        at += 1;
    }
    let mut digits = [0u8; 7];
    let mut d = 0;
    let mut row = p.row;
    while row > 0 {
        digits[d] = b'0' + (row % 10) as u8;
        d += 1;
        row /= 10;
    }
    for i in (0..d).rev() {
        buf[at] = digits[i];
        at += 1;
    }
    at
}

#[cfg(test)]
#[expect(
    clippy::string_slice,
    reason = "tests slice formulas at slot offsets produced by lex_template"
)]
mod tests {
    use super::*;
    use crate::parser::{ASTNode, ASTNodeType, ReferenceType, parse};

    fn slots(formula: &str) -> Option<Vec<String>> {
        lex_template_for_certified_ast(formula).map(|lex| {
            lex.slots()
                .iter()
                .map(|s| formula[s.start as usize..s.end as usize].to_string())
                .collect()
        })
    }

    fn render(reference: SlotRef, dr: i64, dc: i64) -> Option<String> {
        let mut buf = [0u8; 32];
        render_relocated(reference, dr, dc, &mut buf)
            .map(|n| String::from_utf8(buf[..n].to_vec()).unwrap())
    }

    #[test]
    fn finds_local_a1_slots_in_source_order() {
        assert_eq!(
            slots("=SUM(A1:B9)*$C$2+D$4-$E5%"),
            Some(vec![
                "A1:B9".into(),
                "$C$2".into(),
                "D$4".into(),
                "$E5".into()
            ])
        );
        assert_eq!(
            slots("=IF(ISNUMBER(FIND(\"Phy\",N11))=TRUE,\"Physical\",\"Financial\")"),
            Some(vec!["N11".into()])
        );
        assert_eq!(
            slots("=B11&\" \"&C11"),
            Some(vec!["B11".into(), "C11".into()])
        );
        assert_eq!(slots("=1+2"), Some(vec![]));
    }

    #[test]
    fn rejects_excluded_syntax() {
        for formula in [
            "A1",
            "=",
            "=A1 B1",
            "=SUM((A1,B1))",
            "=Sheet1:Sheet3!A1",
            "=[1]Sheet1!A1",
            "=Table1[Col]",
            "=[@Col]",
            "=A1#",
            "={1,2;3,4}",
            "=SUM({1,2})",
            "=#REF!",
            "=A:A",
            "=$1:3",
            "=A1:B",
            "=a1",
            "=A01",
            "=B2:A1",
            "=1E10",
            "=1e+10*A1",
            "=A1:INDEX(B1:B3,1)",
            "=(A1",
        ] {
            assert_eq!(lex_template(formula), None, "{formula}");
        }
    }

    #[test]
    fn legacy_api_keeps_the_published_conservative_subset() {
        for formula in [
            "=A1 + B1",
            "=Sheet1!A1",
            "='Sheet 1'!A1",
            "=@A1",
            "=A0",
            "=XFE1",
            "=A1048577",
            "=TAXRATE",
            "=MyName+1",
            "=R1C1",
            "=RC",
            "=_A1",
            "=A1_name",
            "=name.with.dots",
            "=LOG10(A1)",
            "=ROW()",
            "=COLUMN(A1)",
            "=INDIRECT(\"A1\")",
            "=OFFSET(A1,1,1)",
            "=NOW()",
            "=_xlfn.IFS(A1,1)",
            "=MYFUNC(A1)",
            "=TAX2024(1)",
        ] {
            assert_eq!(lex_template(formula), None, "{formula}");
        }
        let formula = "=SUM($A1:B$2)";
        let template = lex_template(formula).unwrap();
        assert_eq!(template.slots().len(), 1);
        assert_eq!(
            template.match_relocated(formula, "=SUM($A2:C$2)", 1, 1),
            RelocatedMatch::Match,
        );
        // Exclusions inside a string literal were never exclusions of the formula.
        assert!(lex_template("=IF(A1,\"NOW() ! @ # [ ]\",\"\")").is_some());
    }

    #[test]
    fn broader_recognition_is_explicit_and_not_a_purity_certificate() {
        for formula in [
            "=NOW()",
            "=INDIRECT(\"A1\")",
            "=@A1",
            "=Sheet1!A1",
            "='Sheet 1'!A1",
            "=A1 + B1",
            "=Rate+1",
            "=MYFUNC(A1)",
        ] {
            assert!(lex_template(formula).is_none(), "{formula}");
            assert!(
                lex_template_for_certified_ast(formula).is_some(),
                "{formula}"
            );
        }
        for formula in ["=A1+B1", "=SUM($A1:B$2)", "=IF(A1,\"x\",FALSE)"] {
            assert_eq!(
                lex_template(formula),
                lex_template_for_certified_ast(formula)
            );
        }
    }

    #[test]
    fn qualified_slots_leave_names_whitespace_and_functions_byte_exact() {
        let root = "=SPDVOL( Sheet1!A1, 'Sheet 1'!$B2, Rate, @(C1:C10))";
        let follower = "=SPDVOL( Sheet1!A2, 'Sheet 1'!$B3, Rate, @(C2:C11))";
        assert_eq!(
            slots(root),
            Some(vec!["A1".into(), "$B2".into(), "C1:C10".into()])
        );
        let lexical = lex_template_for_certified_ast(root).unwrap();
        assert_eq!(
            lexical.match_relocated(root, follower, 1, 0),
            RelocatedMatch::Match
        );
        for changed in [
            follower.replace("Rate", "RATE"),
            follower.replace(" Sheet1", "Sheet1"),
            follower.replace("SPDVOL", "OTHER"),
            follower.replace("Sheet1!", "Sheet2!"),
        ] {
            assert_eq!(
                lexical.match_relocated(root, &changed, 1, 0),
                RelocatedMatch::Mismatch
            );
        }
    }

    #[test]
    fn tax2024_and_function_like_names_follow_the_tokenizer() {
        // TAX2024 is an in-grid cell (column TAX), not a name.
        assert_eq!(slots("=TAX2024*2"), Some(vec!["TAX2024".into()]));
        // XFD is the last column.
        assert_eq!(slots("=XFD1048576"), Some(vec!["XFD1048576".into()]));
        // A function-shaped atom is a function call, never a reference.
        assert_eq!(slots("=TAX2024(1)"), Some(vec![]));
    }

    #[test]
    fn strings_with_reference_like_text_are_fixed_bytes() {
        let t = "=IF(A1=\"B2\",\"C3\"\"D4\",A1)";
        let lex = lex_template(t).unwrap();
        assert_eq!(lex.slots().len(), 2);
        assert_eq!(
            lex.match_relocated(t, "=IF(A2=\"B2\",\"C3\"\"D4\",A2)", 1, 0),
            RelocatedMatch::Match
        );
        assert_eq!(
            lex.match_relocated(t, "=IF(A2=\"B3\",\"C3\"\"D4\",A2)", 1, 0),
            RelocatedMatch::Mismatch
        );
    }

    #[test]
    fn relocation_moves_relative_axes_only() {
        let t = "=A1+$A1+A$1+$A$1+SUM(B2:C3)";
        let lex = lex_template(t).unwrap();
        assert_eq!(
            lex.match_relocated(t, "=C4+$A4+C$1+$A$1+SUM(D5:E6)", 3, 2),
            RelocatedMatch::Match
        );
        assert_eq!(
            lex.match_relocated(t, "=C4+$A4+C$1+$A$1+SUM(D5:E6)", 3, 1),
            RelocatedMatch::Mismatch
        );
        assert_eq!(lex.match_relocated(t, t, 0, 0), RelocatedMatch::Match);
        // Trailing or missing bytes.
        assert_eq!(
            lex.match_relocated(t, "=A1+$A1+A$1+$A$1+SUM(B2:C3)+1", 0, 0),
            RelocatedMatch::Mismatch
        );
        assert_eq!(
            lex.match_relocated(t, "=A1+$A1+A$1+$A$1+SUM(B2:C3", 0, 0),
            RelocatedMatch::Mismatch
        );
    }

    #[test]
    fn match_relocated_with_unrelated_template_is_mismatch() {
        let lex = lex_template("=A1+B1").unwrap();
        assert_eq!(
            lex.match_relocated("=1", "=A2+B2", 1, 0),
            RelocatedMatch::Mismatch
        );
        assert_eq!(lex.match_relocated("", "", 0, 0), RelocatedMatch::Mismatch);
        // A longer template text is not the one the lexeme was built from.
        assert_eq!(
            lex.match_relocated("=A1+B1+1", "=A2+B2+1", 1, 0),
            RelocatedMatch::Mismatch
        );
    }

    #[test]
    fn off_grid_never_clamps_or_wraps() {
        let t = "=A1+1";
        let lex = lex_template(t).unwrap();
        assert_eq!(
            lex.match_relocated(t, "=A1+1", -1, 0),
            RelocatedMatch::OffGrid
        );
        assert_eq!(
            lex.match_relocated(t, "=A1+1", 0, -1),
            RelocatedMatch::OffGrid
        );
        let t = "=XFD1048576+1";
        let lex = lex_template(t).unwrap();
        assert_eq!(
            lex.match_relocated(t, "=XFD1048576+1", 1, 0),
            RelocatedMatch::OffGrid
        );
        assert_eq!(
            lex.match_relocated(t, "=XFD1048576+1", 0, 1),
            RelocatedMatch::OffGrid
        );
        assert_eq!(
            lex.match_relocated(t, "=A1+1", -1_048_575, -16_383),
            RelocatedMatch::Match
        );
        assert_eq!(
            lex.match_relocated(t, "=A1+1", i64::MIN, i64::MAX),
            RelocatedMatch::OffGrid
        );
        // Absolute axes never leave the grid.
        let t = "=$A$1+1";
        let lex = lex_template(t).unwrap();
        assert_eq!(lex.match_relocated(t, t, -5, -5), RelocatedMatch::Match);
        // Mixed range ends that would cross.
        let t = "=SUM(A1:A$3)";
        let lex = lex_template(t).unwrap();
        assert_eq!(
            lex.match_relocated(t, "=SUM(A3:A$3)", 2, 0),
            RelocatedMatch::Match
        );
        assert_eq!(
            lex.match_relocated(t, "=SUM(A4:A$3)", 3, 0),
            RelocatedMatch::OffGrid
        );
    }

    #[test]
    fn rendering_is_the_parsers_normal_form() {
        let points = [
            (1, 1),
            (1, 26),
            (1, 27),
            (9, 52),
            (10, 702),
            (99, 703),
            (1_048_576, 16_384),
            (123_456, 18_278 - 1894),
        ];
        for (row, col) in points {
            for (row_abs, col_abs) in [(false, false), (true, false), (false, true), (true, true)] {
                let p = A1Point {
                    row,
                    col,
                    row_abs,
                    col_abs,
                };
                let text = render(SlotRef::Cell(p), 0, 0).unwrap();
                assert_eq!(canonical_point(&text), Some(p), "{text}");
                let parsed = ReferenceType::from_string(&text).unwrap();
                assert_eq!(parsed.normalise(), text);
                assert_eq!(
                    parsed,
                    ReferenceType::Cell {
                        sheet: None,
                        row,
                        col,
                        row_abs,
                        col_abs,
                    }
                );
            }
        }
    }

    /// Every column spelling a relocated reference can take parses as that
    /// cell, never as a name, function, R1C1 reference or literal (for
    /// example `RC1`, `LOG10`, `TAX2024`).
    #[test]
    fn every_relocated_column_spelling_parses_as_a_cell() {
        for col in 1..=MAX_COL {
            for (row, row_abs, col_abs) in [
                (1, false, false),
                (10, true, false),
                (1_048_576, false, true),
            ] {
                let p = A1Point {
                    row,
                    col,
                    row_abs,
                    col_abs,
                };
                let text = render(SlotRef::Cell(p), 0, 0).unwrap();
                let formula = format!("={text}+1");
                let lex = lex_template(&formula).unwrap_or_else(|| panic!("{formula}"));
                assert_eq!(lex.slots()[0].reference, SlotRef::Cell(p));
                let mut refs = Vec::new();
                reference_leaves(&parse(&formula).unwrap(), &mut refs);
                assert_eq!(refs.len(), 1, "{formula}");
                assert_eq!(
                    refs[0].1,
                    ReferenceType::Cell {
                        sheet: None,
                        row,
                        col,
                        row_abs,
                        col_abs,
                    },
                    "{formula}"
                );
            }
        }
    }

    fn reference_leaves(ast: &ASTNode, out: &mut Vec<(String, ReferenceType)>) {
        match &ast.node_type {
            ASTNodeType::Reference {
                original,
                reference,
            } => out.push((original.clone(), reference.clone())),
            ASTNodeType::UnaryOp { expr, .. } => reference_leaves(expr, out),
            ASTNodeType::BinaryOp { left, right, .. } => {
                reference_leaves(left, out);
                reference_leaves(right, out);
            }
            ASTNodeType::Function { args, .. } => {
                for a in args {
                    reference_leaves(a, out);
                }
            }
            _ => {}
        }
    }

    /// Property check of the soundness argument on a deterministic sample:
    /// a matched candidate parses to the template's tree with each reference
    /// relocated and spelled canonically.
    #[test]
    fn matched_candidates_parse_to_the_relocated_template() {
        let templates = [
            "=A1+B2*C3",
            "=SUM($A$1:A1)/COUNT(B$2:$C9)",
            "=IF(LEFT(N11,2)=\"US\",\"US\",\"\")",
            "=E11*SUM(P11:Q11)-1.5%",
            "=-A1^2&\"x\"",
            "=VLOOKUP(A2,$H$1:$K$500,3,FALSE)",
            "=TAX2024+Z9",
        ];
        let offsets = [(0, 0), (1, 0), (0, 1), (17, 3), (999, 25), (4096, 700)];
        for t in templates {
            let lex = lex_template(t).unwrap_or_else(|| panic!("{t}"));
            let base = parse(t).unwrap();
            let mut base_refs = Vec::new();
            reference_leaves(&base, &mut base_refs);
            assert_eq!(base_refs.len(), lex.slots().len(), "{t}");
            for (dr, dc) in offsets {
                if lex
                    .slots()
                    .iter()
                    .any(|s| render(s.reference, dr, dc).is_none())
                {
                    assert_ne!(lex.match_relocated(t, t, dr, dc), RelocatedMatch::Match);
                    continue;
                }
                let mut text = String::new();
                let mut fixed = 0;
                for s in lex.slots() {
                    text.push_str(&t[fixed..s.start as usize]);
                    text.push_str(&render(s.reference, dr, dc).unwrap());
                    fixed = s.end as usize;
                }
                text.push_str(&t[fixed..]);
                assert_eq!(lex.match_relocated(t, &text, dr, dc), RelocatedMatch::Match);
                let parsed = parse(&text).unwrap();
                let mut refs = Vec::new();
                reference_leaves(&parsed, &mut refs);
                assert_eq!(refs.len(), base_refs.len());
                for ((original, reference), s) in refs.iter().zip(lex.slots()) {
                    assert_eq!(*original, render(s.reference, dr, dc).unwrap());
                    assert_eq!(reference.normalise(), *original);
                }
            }
        }
    }
}
