//! Shared Excel legacy token-class lowering for imported formulas.
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use formualizer_parse::tokenizer::{TokenStream, TokenSubType, TokenType};
use std::collections::HashMap;
pub(crate) mod arrays;
mod classes;

/// Excel token class of a function's return value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
    R,
    V,
    A,
}
/// Class of one token after conversion (`None`: operators and literals).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tok {
    None,
    Ref,
    Val,
    Arr,
}
/// Parameter class conversion (`Rpo`: operator operands repeat the parent's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Conv {
    Org,
    Val,
    Arr,
    Rpt,
    Rpx,
    Rpo,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassConv {
    Org,
    Val,
    Arr,
}
/// One parameter: its conversion and whether it requires a value type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Param {
    conv: Conv,
    value: bool,
}
const fn param(conv: Conv, value: bool) -> Param {
    Param { conv, value }
}
pub(crate) const RO: Param = param(Conv::Org, false);
pub(crate) const RA: Param = param(Conv::Arr, false);
pub(crate) const RR: Param = param(Conv::Rpt, false);
pub(crate) const RX: Param = param(Conv::Rpx, false);
pub(crate) const VO: Param = param(Conv::Org, true);
pub(crate) const VV: Param = param(Conv::Val, true);
pub(crate) const VA: Param = param(Conv::Arr, true);
pub(crate) const VR: Param = param(Conv::Rpt, true);
pub(crate) const VX: Param = param(Conv::Rpx, true);
const OPERAND: Param = param(Conv::Rpo, true);
const REF_OPERAND: Param = param(Conv::Rpo, false);

struct Function {
    ret: Class,
    params: &'static [Param],
    pairs: bool,
}
/// The uppercase function name without its `_xlfn.` / `_xlws.` prefixes.
fn base_name(name: &str) -> String {
    let mut upper = name.to_ascii_uppercase();
    for prefix in ["_XLFN.", "_XLWS."] {
        if let Some(rest) = upper.strip_prefix(prefix) {
            upper = rest.to_string();
        }
    }
    upper
}
fn function(name: &str) -> Option<(String, Function)> {
    let upper = base_name(name);
    lookup(&upper).map(|f| (upper, f))
}
/// The legacy classes of a function, by its normalised name (see [`base_name`]).
fn lookup(upper: &str) -> Option<Function> {
    let i = classes::FUNCTIONS
        .binary_search_by(|(n, ..)| (*n).cmp(upper))
        .ok()?;
    let (_, ret, params, pairs) = classes::FUNCTIONS[i];
    Some(Function { ret, params, pairs })
}
impl Function {
    fn param(&self, i: usize) -> Param {
        let n = self.params.len();
        match n {
            0 => VR,
            _ if i < n => self.params[i],
            _ if self.pairs && n >= 2 => self.params[n - 2 + (i - n) % 2],
            _ => self.params[n - 1],
        }
    }
}
/// Functions whose (value-class) result follows a reference or array
/// operand: when that operand can be multi-cell, so can the result.
const PASS_THROUGH: &[&str] = &["IF", "CHOOSE", "IFERROR", "IFNA", "ROW", "COLUMN"];
/// Functions that may return a multi-cell reference whatever their operands.
const ALWAYS_MULTI: &[&str] = &["INDEX", "OFFSET", "INDIRECT"];
/// Database functions: their `RR` criteria parameter is a genuine range.
const DATABASE: &[&str] = &[
    "DAVERAGE", "DCOUNT", "DCOUNTA", "DGET", "DMAX", "DMIN", "DPRODUCT", "DSTDEV", "DSTDEVP",
    "DSUM", "DVAR", "DVARP",
];
/// Functions whose reference parameters denote one cell in legacy Excel.
const SCALAR_REFERENCE: &[&str] = &["N", "T", "CELL", "ISFORMULA", "FORMULATEXT", "PHONETIC"];

/// Why a formula was left with dynamic-array evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Skip(pub &'static str);

/// Outcome of lowering one formula.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Lowering {
    /// No value position can hold more than one cell.
    Unchanged,
    /// The formula with explicit `@(...)` at every legacy intersection.
    Rewritten(String),
    /// A shape this rule does not claim; evaluation stays dynamic.
    Skipped(Skip),
}

struct Walk<'a> {
    stream: &'a TokenStream,
    /// Matching closer for each opener span index.
    closer: HashMap<usize, usize>,
    /// Span index by start offset (non-whitespace spans).
    by_start: HashMap<usize, usize>,
    wraps: Vec<(usize, usize)>,
}

/// Rewrite `formula` (stored text, with or without the leading `=`).
#[cfg(test)]
pub(crate) fn lower(formula: &str) -> Lowering {
    Lowerer::default().lower(formula)
}

type WrapSpans = Vec<(usize, usize)>;

/// Cached lowering outcome for a formula shape key.
#[derive(Debug, Clone)]
enum CachedLowering {
    Unchanged,
    Skipped(&'static str),
    /// Wrap spans stored as `(start_key_offset, end_key_offset)` in `shape_key` coordinates.
    Rewritten(WrapSpans),
}

#[derive(Debug, Clone, Copy)]
struct Placeholder {
    orig_start: usize,
    orig_end: usize,
    key_pos: usize,
}

/// Lowers formulas, remembering token shapes and rewrite patterns.
///
/// The classification depends only on the formula text with single-cell
/// addresses and A1 cell ranges abstracted: which cells a reference names
/// never changes a position's class or whether it can hold more than one cell
/// (apart from 1x1 vs multi-cell ranges, which use distinct markers).
/// Formulas filled down a sheet (`INDEX($B$2:$B$5000,MATCH(A2,...))`,
/// `SUM(B2:M2)`) share one shape, so only the first is tokenized and parsed.
#[derive(Default)]
pub(crate) struct Lowerer {
    cache: HashMap<String, CachedLowering>,
    key_buf: String,
    placeholders: Vec<Placeholder>,
}
impl Lowerer {
    /// Imported ordinary formulas have legacy value positions even without a calc chain.
    pub(crate) fn evaluation_text<'a>(&mut self, formula: &'a str) -> std::borrow::Cow<'a, str> {
        if may_intersect(formula)
            && let Lowering::Rewritten(text) = self.lower(formula)
        {
            return std::borrow::Cow::Owned(text);
        }
        std::borrow::Cow::Borrowed(formula)
    }

    pub fn lower(&mut self, formula: &str) -> Lowering {
        match self.lower_inner(formula) {
            Ok(Some(text)) => Lowering::Rewritten(text),
            Ok(None) => Lowering::Unchanged,
            Err(skip) => Lowering::Skipped(skip),
        }
    }

    fn lower_inner(&mut self, formula: &str) -> Result<Option<String>, Skip> {
        // Control bytes 1..=3 are reserved as shape-key markers; bypass cache if present.
        let cacheable = !formula.as_bytes().iter().any(|&b| matches!(b, 1..=3));
        if cacheable {
            fill_shape_key(formula, &mut self.key_buf, &mut self.placeholders);
            if let Some(cached) = self.cache.get(self.key_buf.as_str()) {
                return match cached {
                    CachedLowering::Unchanged => Ok(None),
                    CachedLowering::Skipped(reason) => Err(Skip(reason)),
                    CachedLowering::Rewritten(key_wraps) => {
                        let orig_wraps: Vec<(usize, usize)> = key_wraps
                            .iter()
                            .map(|&(ks, ke)| {
                                (
                                    key_to_orig(ks, &self.placeholders),
                                    key_to_orig(ke, &self.placeholders),
                                )
                            })
                            .collect();
                        apply_wraps(formula, 0, &orig_wraps).map(Some)
                    }
                };
            }
        }
        let (source, offset) = if formula.starts_with('=') {
            (std::borrow::Cow::Borrowed(formula), 0)
        } else {
            (std::borrow::Cow::Owned(format!("={formula}")), 1)
        };
        let stream = TokenStream::new(&source).map_err(|_| Skip("tokenizer"))?;
        let result = lower_stream(&source, offset, &stream);
        if cacheable {
            match &result {
                Ok(None) => {
                    self.cache
                        .insert(self.key_buf.clone(), CachedLowering::Unchanged);
                }
                Err(Skip(reason)) => {
                    self.cache
                        .insert(self.key_buf.clone(), CachedLowering::Skipped(reason));
                }
                Ok(Some((_, wraps))) => {
                    let key_wraps: Option<Vec<(usize, usize)>> = wraps
                        .iter()
                        .map(|&(s, e)| {
                            Some((
                                orig_to_key(s - offset, &self.placeholders)?,
                                orig_to_key(e - offset, &self.placeholders)?,
                            ))
                        })
                        .collect();
                    if let Some(key_wraps) = key_wraps {
                        self.cache
                            .insert(self.key_buf.clone(), CachedLowering::Rewritten(key_wraps));
                    }
                }
            }
        }
        result.map(|opt| opt.map(|(text, _)| text))
    }
}

/// The formula text with single-cell addresses (`\u{1}`), multi-cell A1 ranges
/// (`\u{2}`), and single-cell A1 ranges (`\u{3}`) replaced by canonical markers.
/// Function names, sheet names, whole-row/column ranges, 3-D references and text
/// inside string literals or quoted sheet names are kept verbatim.
#[cfg(test)]
fn shape_key(formula: &str) -> String {
    let mut key = String::new();
    let mut placeholders = Vec::new();
    fill_shape_key(formula, &mut key, &mut placeholders);
    key
}

fn fill_shape_key(formula: &str, key: &mut String, placeholders: &mut Vec<Placeholder>) {
    key.clear();
    placeholders.clear();
    let bytes = formula.as_bytes();
    key.reserve(formula.len());
    let (mut i, mut at) = (0, 0);
    let (mut in_string, mut in_sheet) = (false, false);
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'"' && !in_sheet {
            in_string = !in_string;
        } else if b == b'\'' && !in_string {
            in_sheet = !in_sheet;
        }
        let word_start = !in_string
            && !in_sheet
            && (b.is_ascii_alphabetic() || b == b'$')
            && (i == 0
                || !(bytes[i - 1].is_ascii_alphanumeric()
                    || matches!(bytes[i - 1], b'_' | b'.' | b'$')));
        if !word_start {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'.' | b'$'))
        {
            i += 1;
        }
        let next = bytes.get(i).copied();
        let prev = start.checked_sub(1).map(|p| bytes[p]);
        if prev == Some(b':') {
            continue;
        }
        if next == Some(b':') {
            // Check for an A1:B2 range whose endpoints are both valid cell addresses.
            let start2 = i + 1;
            let mut j = start2;
            while j < bytes.len()
                && (bytes[j].is_ascii_alphanumeric() || matches!(bytes[j], b'_' | b'.' | b'$'))
            {
                j += 1;
            }
            let after2 = bytes.get(j).copied();
            if j > start2
                && !matches!(after2, Some(b'(' | b':' | b'!' | b'['))
                && let (Some(c1), Some(c2)) = (
                    parse_single_cell(&formula[start..i]),
                    parse_single_cell(&formula[start2..j]),
                )
            {
                key.push_str(&formula[at..start]);
                let key_pos = key.len();
                key.push(if c1 == c2 { '\u{3}' } else { '\u{2}' });
                placeholders.push(Placeholder {
                    orig_start: start,
                    orig_end: j,
                    key_pos,
                });
                at = j;
                i = j;
                continue;
            }
            continue;
        }
        if !matches!(next, Some(b'(' | b'!' | b'['))
            && parse_single_cell(&formula[start..i]).is_some()
        {
            key.push_str(&formula[at..start]);
            let key_pos = key.len();
            key.push('\u{1}');
            placeholders.push(Placeholder {
                orig_start: start,
                orig_end: i,
                key_pos,
            });
            at = i;
        }
    }
    key.push_str(&formula[at..]);
}

fn orig_to_key(pos: usize, placeholders: &[Placeholder]) -> Option<usize> {
    let mut delta: isize = 0;
    for ph in placeholders {
        if pos < ph.orig_start {
            break;
        }
        if pos == ph.orig_start {
            return Some(ph.key_pos);
        }
        if pos == ph.orig_end {
            return Some(ph.key_pos + 1);
        }
        if pos < ph.orig_end {
            return None;
        }
        delta += (ph.orig_end - ph.orig_start) as isize - 1;
    }
    usize::try_from(pos as isize - delta).ok()
}

fn key_to_orig(kpos: usize, placeholders: &[Placeholder]) -> usize {
    let mut delta: isize = 0;
    for ph in placeholders {
        if kpos < ph.key_pos {
            break;
        }
        if kpos == ph.key_pos {
            return ph.orig_start;
        }
        if kpos == ph.key_pos + 1 {
            return ph.orig_end;
        }
        delta += (ph.orig_end - ph.orig_start) as isize - 1;
    }
    (kpos as isize + delta) as usize
}

/// `A1` or `$B$7` within the sheet bounds.
fn parse_single_cell(text: &str) -> Option<(u32, u32)> {
    if !is_cell_address(text) {
        return None;
    }
    let (row, col, _, _) = formualizer_common::coord::parse_a1_1based(text).ok()?;
    (row <= 1_048_576 && col <= 16_384).then_some((row, col))
}

fn apply_wraps(source: &str, offset: usize, wraps: &[(usize, usize)]) -> Result<String, Skip> {
    // Insert `@(` and `)`. Opens of outer nodes precede inner ones at one
    // offset, closes of inner nodes precede outer ones.
    let mut inserts: Vec<(usize, u8, usize, &str)> = Vec::with_capacity(wraps.len() * 2);
    for &(s, e) in wraps {
        let len = e - s;
        inserts.push((s, 1, usize::MAX - len, "@("));
        inserts.push((e, 0, len, ")"));
    }
    inserts.sort();
    let mut out = String::with_capacity(source.len() + inserts.len() * 2);
    let mut at = offset;
    for (pos, _, _, text) in inserts {
        if pos < at {
            return Err(Skip("overlapping wraps"));
        }
        out.push_str(&source[at..pos]);
        out.push_str(text);
        at = pos;
    }
    out.push_str(&source[at..]);
    Ok(out)
}

fn lower_stream(
    source: &str,
    offset: usize,
    stream: &TokenStream,
) -> Result<Option<(String, WrapSpans)>, Skip> {
    let ast = formualizer_parse::parser::Parser::from_token_stream(stream)
        .parse()
        .map_err(|_| Skip("parser"))?;
    let mut closer = HashMap::new();
    let mut by_start = HashMap::new();
    let mut open: Vec<usize> = Vec::new();
    for (i, span) in stream.spans.iter().enumerate() {
        if span.token_type == TokenType::Whitespace {
            continue;
        }
        by_start.entry(span.start).or_insert(i);
        if matches!(
            span.token_type,
            TokenType::Func | TokenType::Paren | TokenType::Array
        ) {
            match span.subtype {
                TokenSubType::Open => open.push(i),
                TokenSubType::Close => {
                    let o = open.pop().ok_or(Skip("unbalanced"))?;
                    closer.insert(o, i);
                }
                _ => {}
            }
        }
    }
    let mut walk = Walk {
        stream,
        closer,
        by_start,
        wraps: Vec::new(),
    };
    walk.visit(
        &ast,
        param(Conv::Val, true),
        Conv::Val,
        ClassConv::Val,
        false,
    )?;
    if walk.wraps.is_empty() {
        return Ok(None);
    }
    let out = apply_wraps(source, offset, &walk.wraps)?;
    let check = if offset == 0 {
        std::borrow::Cow::Borrowed(out.as_str())
    } else {
        std::borrow::Cow::Owned(format!("={out}"))
    };
    formualizer_parse::parser::parse(&check).map_err(|_| Skip("rewrite does not parse"))?;
    Ok(Some((out, walk.wraps)))
}

impl Walk<'_> {
    /// Port of the BIFF token-class propagation (LibreOffice
    /// `XclExpFmlaCompImpl::RecalcTokenClass`). Returns whether the node's
    /// value can still be multi-cell after its own wrapping.
    fn visit(
        &mut self,
        node: &ASTNode,
        info: Param,
        prev_conv: Conv,
        prev_class_conv: ClassConv,
        was_ref: bool,
    ) -> Result<bool, Skip> {
        enum Kind<'n> {
            Leaf,
            Reference { multi: bool },
            ArrayLiteral { multi: bool },
            Operator(Vec<(&'n ASTNode, Param)>),
            Function(String, Function, &'n [ASTNode]),
        }
        let kind = match &node.node_type {
            ASTNodeType::Literal(_) | ASTNodeType::Omitted => Kind::Leaf,
            ASTNodeType::Reference { reference, .. } => Kind::Reference {
                multi: reference_multi(reference)?,
            },
            ASTNodeType::Array(rows) => {
                if rows
                    .iter()
                    .flatten()
                    .any(|n| !matches!(n.node_type, ASTNodeType::Literal(_)))
                {
                    return Err(Skip("array literal with expressions"));
                }
                Kind::ArrayLiteral {
                    multi: rows.iter().map(Vec::len).sum::<usize>() > 1,
                }
            }
            ASTNodeType::UnaryOp { op, expr } => match op.as_str() {
                // An explicit `@` already reduces to one value.
                "@" => Kind::Leaf,
                "#" => Kind::Reference { multi: true },
                "+" | "-" | "%" => Kind::Operator(vec![(expr.as_ref(), OPERAND)]),
                _ => return Err(Skip("unary operator")),
            },
            ASTNodeType::BinaryOp { op, left, right } => match op.as_str() {
                ":" | "," | " " => {
                    // Reference operators: the result is a reference.
                    for child in [left, right] {
                        self.visit(child, REF_OPERAND, Conv::Org, ClassConv::Org, true)?;
                    }
                    Kind::Reference { multi: true }
                }
                "+" | "-" | "*" | "/" | "^" | "&" | "=" | "<>" | "<" | ">" | "<=" | ">=" => {
                    Kind::Operator(vec![(left.as_ref(), OPERAND), (right.as_ref(), OPERAND)])
                }
                _ => return Err(Skip("binary operator")),
            },
            ASTNodeType::Function { name, args } => {
                let upper = base_name(name);
                if upper == "SINGLE" {
                    // An explicit intersection (`@`, stored as `SINGLE`) already
                    // reduces to one value.
                    Kind::Leaf
                } else {
                    let f = lookup(&upper).ok_or(Skip("function without legacy classes"))?;
                    Kind::Function(upper, f, args)
                }
            }
            ASTNodeType::Call { .. } => return Err(Skip("call expression")),
        };
        let mut tok = match &kind {
            Kind::Leaf | Kind::Operator(_) => Tok::None,
            Kind::Reference { .. } => Tok::Ref,
            Kind::ArrayLiteral { .. } => Tok::Arr,
            Kind::Function(_, f, _) => match f.ret {
                Class::R => Tok::Ref,
                Class::V => Tok::Val,
                Class::A => Tok::Arr,
            },
        };
        // REF tokens in value-type parameters behave like VAL tokens.
        if info.value && tok == Tok::Ref {
            tok = Tok::Val;
        }
        let conv = if info.conv == Conv::Rpo {
            prev_conv
        } else {
            info.conv
        };
        let class_conv = match conv {
            Conv::Org => ClassConv::Org,
            Conv::Val => ClassConv::Val,
            Conv::Arr => ClassConv::Arr,
            Conv::Rpt => match prev_conv {
                Conv::Org | Conv::Val | Conv::Arr => {
                    if was_ref {
                        ClassConv::Val
                    } else {
                        prev_class_conv
                    }
                }
                Conv::Rpt => prev_class_conv,
                Conv::Rpx => {
                    if was_ref {
                        prev_class_conv
                    } else {
                        ClassConv::Org
                    }
                }
                Conv::Rpo => ClassConv::Org,
            },
            Conv::Rpx => {
                if tok == Tok::Ref || prev_class_conv == ClassConv::Arr {
                    prev_class_conv
                } else {
                    ClassConv::Org
                }
            }
            Conv::Rpo => ClassConv::Org,
        };
        match class_conv {
            ClassConv::Org => {}
            ClassConv::Val => {
                if tok == Tok::Arr {
                    tok = Tok::Val;
                }
            }
            ClassConv::Arr => {
                if tok == Tok::Val {
                    tok = Tok::Arr;
                }
            }
        }
        let is_ref = tok == Tok::Ref;
        let multi = match kind {
            Kind::Leaf => false,
            Kind::Reference { multi } | Kind::ArrayLiteral { multi } => multi,
            Kind::Operator(operands) => {
                let mut any = false;
                for (child, p) in operands {
                    any |= self.visit(child, p, conv, class_conv, is_ref)?;
                }
                any
            }
            Kind::Function(upper, f, args) => {
                let mut any = false;
                for (i, child) in args.iter().enumerate() {
                    let p = f.param(i);
                    let child_multi = self.visit(child, p, conv, class_conv, is_ref)?;
                    if child_multi {
                        let scalar_reference = (p == RR && !DATABASE.contains(&upper.as_str()))
                            || (p == RO && SCALAR_REFERENCE.contains(&upper.as_str()));
                        if scalar_reference {
                            return Err(Skip("range in a scalar reference parameter"));
                        }
                    }
                    any |= child_multi;
                }
                f.ret == Class::A
                    || ALWAYS_MULTI.contains(&upper.as_str())
                    || (any && PASS_THROUGH.contains(&upper.as_str()))
            }
        };
        if tok == Tok::Val && multi {
            let extent = self.extent(node).ok_or(Skip("no source extent"))?;
            self.wraps.push(extent);
            return Ok(false);
        }
        Ok(multi)
    }

    fn span_index(&self, start: usize) -> Option<usize> {
        self.by_start.get(&start).copied()
    }

    /// Source extent of a node, including parentheses that enclose exactly it.
    fn extent(&self, node: &ASTNode) -> Option<(usize, usize)> {
        let (s, e) = self.raw_extent(node)?;
        Some(self.absorb(s, e))
    }

    fn raw_extent(&self, node: &ASTNode) -> Option<(usize, usize)> {
        let token = node.source_token.as_ref();
        match &node.node_type {
            ASTNodeType::Literal(_) | ASTNodeType::Reference { .. } => {
                let t = token?;
                Some((t.start, t.end))
            }
            ASTNodeType::Function { .. } => {
                let t = token?;
                let open = self.span_index(t.start)?;
                let close = *self.closer.get(&open)?;
                Some((t.start, self.stream.spans[close].end))
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                let t = token?;
                let (s, e) = self.extent(expr)?;
                Some(if t.start < s {
                    (t.start, e)
                } else {
                    (s, t.end)
                })
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                Some((self.extent(left)?.0, self.extent(right)?.1))
            }
            ASTNodeType::Array(rows) => {
                let first = rows.first()?.first()?;
                let (s, _) = self.raw_extent(first)?;
                let open = self
                    .stream
                    .spans
                    .iter()
                    .enumerate()
                    .filter(|(_, sp)| {
                        sp.token_type == TokenType::Array
                            && sp.subtype == TokenSubType::Open
                            && sp.end <= s
                    })
                    .map(|(i, _)| i)
                    .next_back()?;
                let close = *self.closer.get(&open)?;
                Some((self.stream.spans[open].start, self.stream.spans[close].end))
            }
            ASTNodeType::Omitted | ASTNodeType::Call { .. } => None,
        }
    }

    /// Widen `(s, e)` over grouping parentheses that enclose exactly it.
    fn absorb(&self, mut s: usize, mut e: usize) -> (usize, usize) {
        let spans = &self.stream.spans;
        loop {
            let before = spans
                .iter()
                .enumerate()
                .filter(|(_, sp)| sp.token_type != TokenType::Whitespace && sp.end <= s)
                .map(|(i, _)| i)
                .next_back();
            let after = spans
                .iter()
                .position(|sp| sp.token_type != TokenType::Whitespace && sp.start >= e);
            match (before, after) {
                (Some(b), Some(a))
                    if spans[b].token_type == TokenType::Paren
                        && spans[b].subtype == TokenSubType::Open
                        && self.closer.get(&b) == Some(&a) =>
                {
                    s = spans[b].start;
                    e = spans[a].end;
                }
                _ => return (s, e),
            }
        }
    }
}

/// Whether a reference can denote more than one cell. 3-D, whole-row or
/// whole-column external and structured references are not claimed.
fn reference_multi(reference: &ReferenceType) -> Result<bool, Skip> {
    use formualizer_parse::parser::ExternalRefKind;
    match reference {
        ReferenceType::Cell { .. } => Ok(false),
        ReferenceType::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            ..
        } => Ok(!(start_row.is_some()
            && start_row == end_row
            && start_col.is_some()
            && start_col == end_col)),
        ReferenceType::NamedRange(_) => Ok(true),
        ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => Err(Skip("3-D reference")),
        ReferenceType::External(external) => match external.kind {
            ExternalRefKind::Cell { .. } => Ok(false),
            ExternalRefKind::Range {
                start_row: Some(r1),
                start_col: Some(c1),
                end_row: Some(r2),
                end_col: Some(c2),
                ..
            } => Ok(r1 != r2 || c1 != c2),
            ExternalRefKind::Range { .. } => Err(Skip("unbounded external reference")),
        },
        ReferenceType::Table(_) => Err(Skip("structured reference")),
    }
}

/// Cheap prefilter: a formula can only gain an intersection through a range
/// (`:`), an array constant, a name, or a reference/array-returning function.
pub(crate) fn may_intersect(formula: &str) -> bool {
    if formula.contains(':') || formula.contains('{') {
        return true;
    }
    // Any identifier that is not a function call or a cell address may be a
    // defined name; any of these functions may return a multi-cell value.
    let bytes = formula.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    let mut in_sheet = false;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'"' && !in_sheet {
            in_string = !in_string;
            i += 1;
            continue;
        }
        if b == b'\'' && !in_string {
            in_sheet = !in_sheet;
            i += 1;
            continue;
        }
        if in_string || in_sheet || !(b.is_ascii_alphabetic() || b == b'_') {
            i += 1;
            continue;
        }
        if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'.') {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'.' | b'$'))
        {
            i += 1;
        }
        let word = &formula[start..i];
        let called = bytes.get(i) == Some(&b'(');
        let sheet_prefix = bytes.get(i) == Some(&b'!');
        if called {
            let upper = word.to_ascii_uppercase();
            let upper = upper
                .trim_start_matches("_XLFN.")
                .trim_start_matches("_XLWS.");
            if ALWAYS_MULTI.contains(&upper)
                || function(upper).is_some_and(|(_, f)| f.ret == Class::A)
            {
                return true;
            }
            continue;
        }
        if sheet_prefix || is_cell_address(word) {
            continue;
        }
        let upper = word.to_ascii_uppercase();
        if upper == "TRUE" || upper == "FALSE" {
            continue;
        }
        return true;
    }
    false
}

fn is_cell_address(word: &str) -> bool {
    let w = word.trim_start_matches('$');
    let letters = w.bytes().take_while(u8::is_ascii_alphabetic).count();
    let rest = w[letters..].trim_start_matches('$');
    (1..=3).contains(&letters) && !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests;
