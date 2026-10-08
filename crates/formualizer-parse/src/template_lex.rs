//! Conservative lexical recognition of relocatable formula templates.
//!
//! A formula in the supported subset is fixed bytes plus local A1 cell and
//! finite range references. [`lex_template`] finds those reference slots
//! with the parser's own tokenizer, and [`TemplateLexeme::match_relocated`]
//! decides, without parsing, whether another formula's text is exactly the
//! template with every reference relocated by a row/column offset. A load
//! path uses this to recognise copies of an already parsed formula; the
//! engine certifies the template's parsed tree against its slots before it
//! trusts a match.
//!
//! The subset is deliberately narrow: no whitespace, names, sheet or
//! external qualifiers, whole rows/columns, structured references, arrays,
//! error literals, `@`, `#`, unions or intersections, and only an allowlist
//! of pure functions. Reference spelling must be canonical (uppercase
//! letters, no leading zeros), so a relocated reference has exactly one
//! spelling. Anything else is not a template; its copies are parsed.

use crate::tokenizer::{TokenSubType, TokenType, tokenize_spans_with_limits};
use crate::types::FormulaDialect;

const MAX_ROW: u32 = 1_048_576;
const MAX_COL: u32 = 16_384;

/// Functions a template may call: pure, non-volatile, not reference
/// returning and not dependent on the calling cell.
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

/// One endpoint of a local A1 reference: 1-based coordinates and `$` flags.
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

/// A reference token of the template: its byte span and referent.
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
    pub fn match_relocated(
        &self,
        template: &str,
        candidate: &str,
        dr: i64,
        dc: i64,
    ) -> RelocatedMatch {
        let template = template.as_bytes();
        let candidate = candidate.as_bytes();
        let mut buf = [0u8; 32];
        let mut fixed = 0usize;
        let mut at = 0usize;
        for slot in self.slots.iter() {
            let (start, end) = (slot.start as usize, slot.end as usize);
            let Some(len) = render_relocated(slot.reference, dr, dc, &mut buf) else {
                return RelocatedMatch::OffGrid;
            };
            let lead = &template[fixed..start];
            if candidate.get(at..at + lead.len()) != Some(lead) {
                return RelocatedMatch::Mismatch;
            }
            at += lead.len();
            if candidate.get(at..at + len) != Some(&buf[..len]) {
                return RelocatedMatch::Mismatch;
            }
            at += len;
            fixed = end;
        }
        if candidate.get(at..) == Some(&template[fixed..]) {
            RelocatedMatch::Match
        } else {
            RelocatedMatch::Mismatch
        }
    }
}

/// Recognise `formula` (with its leading `=`) as a relocatable template.
/// Returns `None` for anything outside the conservative subset described in
/// the module documentation.
pub fn lex_template(formula: &str) -> Option<TemplateLexeme> {
    let body = formula.strip_prefix('=')?;
    if body.is_empty() || formula.len() > u32::MAX as usize {
        return None;
    }
    // Cheap byte screen before tokenizing: characters that only occur in
    // excluded syntax (outside string literals they mean whitespace,
    // qualifiers, structured references, arrays, `#` or `@`).
    let mut in_string = false;
    for &b in body.as_bytes() {
        if b == b'"' {
            in_string = !in_string;
        } else if !in_string
            && matches!(
                b,
                b' ' | b'\t'
                    | b'\r'
                    | b'\n'
                    | b'!'
                    | b'\''
                    | b'['
                    | b']'
                    | b'{'
                    | b'}'
                    | b'#'
                    | b'@'
                    | b';'
            )
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
        let text = &formula[span.start..span.end];
        match (span.token_type, span.subtype) {
            (TokenType::Operand, TokenSubType::Number) => {
                if !text.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
                    return None;
                }
            }
            (TokenType::Operand, TokenSubType::Text | TokenSubType::Logical) => {}
            (TokenType::Operand, TokenSubType::Range) => {
                slots.push(RefSlot {
                    start: span.start as u32,
                    end: span.end as u32,
                    reference: canonical_reference(text)?,
                });
            }
            (TokenType::Func, TokenSubType::Open) => {
                let name = text.strip_suffix('(')?;
                if !FUNCTIONS.iter().any(|f| f.eq_ignore_ascii_case(name)) {
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
                if !matches!(text, "+" | "-") {
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
            _ => return None,
        }
    }
    if !open.is_empty() {
        return None;
    }
    Some(TemplateLexeme {
        slots: slots.into_boxed_slice(),
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

fn canonical_point(text: &str) -> Option<A1Point> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let col_abs = bytes.first() == Some(&b'$');
    if col_abs {
        i += 1;
    }
    let letters = i;
    let mut col: u32 = 0;
    while i < bytes.len() && bytes[i].is_ascii_uppercase() {
        if i - letters == 3 {
            return None;
        }
        col = col * 26 + u32::from(bytes[i] - b'A' + 1);
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
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        if i - digits == 7 {
            return None;
        }
        row = row * 10 + u32::from(bytes[i] - b'0');
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
mod tests {
    use super::*;
    use crate::parser::{ASTNode, ASTNodeType, ReferenceType, parse};

    fn slots(formula: &str) -> Option<Vec<String>> {
        lex_template(formula).map(|lex| {
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
            "=A1 + B1",
            "=A1 B1",
            "=SUM((A1,B1))",
            "=Sheet1!A1",
            "='Sheet 1'!A1",
            "=Sheet1:Sheet3!A1",
            "=[1]Sheet1!A1",
            "=Table1[Col]",
            "=[@Col]",
            "=A1#",
            "=@A1",
            "={1,2;3,4}",
            "=SUM({1,2})",
            "=#REF!",
            "=A:A",
            "=$1:3",
            "=A1:B",
            "=a1",
            "=A01",
            "=A0",
            "=XFE1",
            "=A1048577",
            "=B2:A1",
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
            "=1E10",
            "=1e+10*A1",
            "=A1:INDEX(B1:B3,1)",
            "=(A1",
        ] {
            assert_eq!(lex_template(formula), None, "{formula}");
        }
    }

    #[test]
    fn tax2024_and_function_like_names_follow_the_tokenizer() {
        // TAX2024 is an in-grid cell (column TAX), not a name.
        assert_eq!(slots("=TAX2024*2"), Some(vec!["TAX2024".into()]));
        // XFD is the last column.
        assert_eq!(slots("=XFD1048576"), Some(vec!["XFD1048576".into()]));
        // A function-shaped atom is a function call, never a reference.
        assert_eq!(lex_template("=TAX2024(1)"), None);
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
