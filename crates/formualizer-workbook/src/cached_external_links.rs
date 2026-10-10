//! External workbook links (`xl/externalLinks/externalLinkN.xml`).
//!
//! Excel keeps the values it last read from each linked workbook in the link
//! part (`externalBook/sheetDataSet`) and, when links are not refreshed,
//! calculates with them. Recalculation does the same and never refreshes:
//! every external reference that a formula or a used defined name reads is
//! served from that cache as an engine source named by its reference text.
//! Anything that cannot be mapped exactly to cached cells is refused. Link
//! parts, their relationships and content types are never edited.
//!
//! A cell inside a listed, cached sheet that the cache omits is blank: Excel
//! stores only non-empty cells. On a sheet whose last refresh failed
//! (`refreshError`), Excel reads an omitted cell as `#REF!`; a range there
//! that contains an omitted cell is refused.
use crate::IoError;
#[cfg(feature = "xlsx-recalc")]
use crate::cache_recalculate::{SheetPlan, XlsxRecalculateOptions, package};
use crate::xlsx_cache_options::{CacheOptions, checkpoint, unsupported};
use crate::xlsx_xml as xml;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ExternalRefKind, ReferenceType};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
mod ordinary;
pub use ordinary::CachedExternalLinkValues;
pub(crate) use ordinary::{has_links, load};

/// Workbook relationship type of an external link part.
pub(crate) const LINK_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLink";
/// Content type of an external link part.
#[cfg(feature = "xlsx-recalc")]
pub(crate) const LINK_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml";

/// Bound on the summed area of distinct external ranges, as a multiple of
/// the workbook cell limit: the engine materializes each one per run.
const EXTERNAL_AREA_FACTOR: usize = 4;
/// Functions whose result depends on where an argument reference lives, not
/// only on its values: over a cached external reference they cannot be
/// evaluated exactly.
const REFERENCE_FUNCTIONS: &[&str] = &[
    "OFFSET",
    "ROW",
    "COLUMN",
    "CELL",
    "ISREF",
    "AREAS",
    "ISFORMULA",
    "FORMULATEXT",
    "SHEET",
    "SUBTOTAL",
    "AGGREGATE",
];

/// The cached values of every external link, in `[n]` order.
pub(crate) struct Links {
    links: Vec<Link>,
}
#[cfg_attr(not(feature = "xlsx-recalc"), allow(dead_code))]
enum Link {
    Book(Book),
    /// A DDE or OLE link: refused.
    Other(&'static str),
}
struct Book {
    sheets: Vec<String>,
    /// `None` when the link has no `sheetDataSet`.
    cache: Option<HashMap<usize, SheetCache>>,
}
#[derive(Default)]
struct SheetCache {
    refresh_error: bool,
    cells: HashMap<(u32, u32), RawCell>,
}
#[derive(Default)]
struct RawCell {
    kind: Option<String>,
    value: Option<String>,
    metadata: bool,
}

/// Values served to the engine, keyed by external reference text.
#[derive(Debug, Default)]
pub(crate) struct ExternalValues {
    scalars: HashMap<String, LiteralValue>,
    ranges: HashMap<String, Arc<CachedGrid>>,
}
impl ExternalValues {
    pub(crate) fn scalar(&self, name: &str) -> Option<&LiteralValue> {
        self.scalars.get(name)
    }
    pub(crate) fn range(&self, name: &str) -> Option<&Arc<CachedGrid>> {
        self.ranges.get(name)
    }
    pub(crate) fn scalar_names(&self) -> impl Iterator<Item = &str> {
        self.scalars.keys().map(String::as_str)
    }
    pub(crate) fn range_names(&self) -> impl Iterator<Item = &str> {
        self.ranges.keys().map(String::as_str)
    }
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.scalars.contains_key(name) || self.ranges.contains_key(name)
    }
}

/// The cached cells of one external range, relative to its top-left
/// corner; every other cell of the rectangle is blank.
#[derive(Debug)]
pub(crate) struct CachedGrid {
    rows: usize,
    cols: usize,
    cells: HashMap<(usize, usize), LiteralValue>,
}
impl formualizer_eval::traits::Range for CachedGrid {
    fn get(&self, row: usize, col: usize) -> Result<LiteralValue, ExcelError> {
        Ok(self
            .cells
            .get(&(row, col))
            .cloned()
            .unwrap_or(LiteralValue::Empty))
    }
    fn dimensions(&self) -> (usize, usize) {
        (self.rows, self.cols)
    }
    fn materialise(&self) -> std::borrow::Cow<'_, [Vec<LiteralValue>]> {
        let mut rows = vec![vec![LiteralValue::Empty; self.cols]; self.rows];
        for (&(r, c), v) in &self.cells {
            rows[r][c] = v.clone();
        }
        std::borrow::Cow::Owned(rows)
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
#[derive(Debug, Clone)]
struct SharedGrid(Arc<CachedGrid>);
impl formualizer_eval::traits::Range for SharedGrid {
    fn get(&self, row: usize, col: usize) -> Result<LiteralValue, ExcelError> {
        self.0.get(row, col)
    }
    fn dimensions(&self) -> (usize, usize) {
        self.0.dimensions()
    }
    fn materialise(&self) -> std::borrow::Cow<'_, [Vec<LiteralValue>]> {
        self.0.materialise()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
/// A cached external range served as an engine source table: its data body
/// is the referenced rectangle, without headers.
#[derive(Debug, Clone)]
pub(crate) struct CachedRange(pub(crate) Arc<CachedGrid>);
impl formualizer_eval::traits::Table for CachedRange {
    fn get_cell(&self, _row: usize, _column: &str) -> Result<LiteralValue, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::Ref))
    }
    fn get_column(
        &self,
        _column: &str,
    ) -> Result<Box<dyn formualizer_eval::traits::Range>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::Ref))
    }
    fn data_height(&self) -> usize {
        self.0.rows
    }
    fn data_body(&self) -> Option<Box<dyn formualizer_eval::traits::Range>> {
        Some(Box::new(SharedGrid(self.0.clone())))
    }
    fn clone_box(&self) -> Box<dyn formualizer_eval::traits::Table> {
        Box::new(self.clone())
    }
}

/// The external sources a recalculation defines.
#[cfg(feature = "xlsx-recalc")]
pub(crate) struct Plan {
    pub values: ExternalValues,
    /// Distinct links whose cached values a formula or used name reads.
    pub links_used: usize,
    /// The first read (`Sheet1!A1: [1]Data!B2`), for a refusal under
    /// [`ExternalLinkPolicy::Refuse`](crate::cache_recalculate::ExternalLinkPolicy::Refuse).
    pub first_read: Option<String>,
    /// Lowercased defined names that read external links but that no
    /// formula or name references: left out of the calculation.
    pub skipped_names: Vec<String>,
    /// Lowercased used defined names whose definition is exactly one
    /// multi-cell external range.
    pub range_names: HashSet<String>,
}

fn malformed(part: &str) -> IoError {
    unsupported("malformed external link part", part)
}

/// Read every link part, in workbook `externalReference` order.
#[cfg(feature = "xlsx-recalc")]
pub(crate) fn parse(
    archive: &mut package::Archive<'_>,
    parts: &[String],
    options: &XlsxRecalculateOptions,
) -> Result<Links, IoError> {
    let mut links = Vec::with_capacity(parts.len());
    let mut cells = 0usize;
    for part in parts {
        checkpoint(&options.cancel)?;
        let data = package::read_part(archive, part, options.limits.max_worksheet_bytes)?;
        links.push(parse_part(&data, part, &options.into(), &mut cells)?);
    }
    Ok(Links { links })
}

fn parse_part(
    data: &[u8],
    part: &str,
    options: &CacheOptions,
    counted: &mut usize,
) -> Result<Link, IoError> {
    let mut kind: Option<&'static str> = None;
    let mut sheets = Vec::new();
    let mut cache: Option<HashMap<usize, SheetCache>> = None;
    let mut sheet: Option<usize> = None;
    let mut cell: Option<((u32, u32), RawCell)> = None;
    let mut in_value = false;
    const BOOK: &[&str] = &["externalLink", "externalBook"];
    let at = |path: &[xml::Element<'_>], tail: &[&str]| {
        path.len() == BOOK.len() + tail.len()
            && xml::path_is(&path[..2], xml::MAIN, BOOK)
            && path[2..]
                .iter()
                .zip(tail)
                .all(|(e, n)| e.local == *n && e.ns.is_main())
    };
    xml::walk(data, options, |path, node| {
        match &node.kind {
            xml::Kind::Open { .. } => {
                if path.len() == 1 && !xml::path_is(path, xml::MAIN, &["externalLink"]) {
                    return Err(unsupported("external link XML root/namespace", part));
                }
                if path.len() == 2 && path[1].ns.is_main() {
                    let child = match path[1].local {
                        "externalBook" => "externalBook",
                        "ddeLink" => "DDE",
                        "oleLink" => "OLE",
                        _ => return Ok(()),
                    };
                    if kind.replace(child).is_some() {
                        return Err(malformed(part));
                    }
                }
                if at(path, &["sheetNames", "sheetName"]) {
                    sheets.push(node.required("val")?.to_owned());
                } else if at(path, &["sheetDataSet"]) {
                    if cache.replace(HashMap::new()).is_some() {
                        return Err(malformed(part));
                    }
                } else if at(path, &["sheetDataSet", "sheetData"]) {
                    let id = node
                        .required("sheetId")?
                        .parse::<usize>()
                        .map_err(|_| malformed(part))?;
                    let refresh_error = match node.value("refreshError") {
                        None | Some("0" | "false") => false,
                        Some("1" | "true") => true,
                        _ => return Err(malformed(part)),
                    };
                    let sheets = cache.as_mut().ok_or_else(|| malformed(part))?;
                    if sheets
                        .insert(
                            id,
                            SheetCache {
                                refresh_error,
                                cells: HashMap::new(),
                            },
                        )
                        .is_some()
                    {
                        return Err(malformed(part));
                    }
                    sheet = Some(id);
                } else if at(path, &["sheetDataSet", "sheetData", "row", "cell"]) {
                    let r = node.required("r")?;
                    let coord = crate::xlsx_xml::plain_coord(r).ok_or_else(|| malformed(part))?;
                    *counted += 1;
                    if *counted > options.limits.max_cells {
                        return Err(unsupported("external link cache cell limit", part));
                    }
                    cell = Some((
                        coord,
                        RawCell {
                            kind: node.value("t").map(str::to_owned),
                            value: None,
                            metadata: node.value("vm").is_some(),
                        },
                    ));
                } else if at(path, &["sheetDataSet", "sheetData", "row", "cell", "v"]) {
                    let (_, raw) = cell.as_mut().ok_or_else(|| malformed(part))?;
                    raw.value.get_or_insert_with(String::new);
                    in_value = true;
                }
            }
            xml::Kind::Text(text) => {
                if in_value && let Some((_, raw)) = cell.as_mut() {
                    raw.value.get_or_insert_with(String::new).push_str(text);
                }
            }
            xml::Kind::Close => {
                if at(path, &["sheetDataSet", "sheetData", "row", "cell", "v"]) {
                    in_value = false;
                } else if at(path, &["sheetDataSet", "sheetData", "row", "cell"]) {
                    let (coord, raw) = cell.take().ok_or_else(|| malformed(part))?;
                    let id = sheet.ok_or_else(|| malformed(part))?;
                    let cells = &mut cache
                        .as_mut()
                        .and_then(|c| c.get_mut(&id))
                        .ok_or_else(|| malformed(part))?
                        .cells;
                    if cells.insert(coord, raw).is_some() {
                        return Err(malformed(part));
                    }
                } else if at(path, &["sheetDataSet", "sheetData"]) {
                    sheet = None;
                }
            }
        }
        Ok(())
    })?;
    match kind {
        Some("externalBook") => {
            if cache
                .as_ref()
                .is_some_and(|c| c.keys().any(|id| *id >= sheets.len()))
            {
                return Err(malformed(part));
            }
            Ok(Link::Book(Book { sheets, cache }))
        }
        Some(other) => Ok(Link::Other(other)),
        None => Err(malformed(part)),
    }
}

/// One served external reference.
enum Served {
    Scalar(LiteralValue),
    Range(Arc<CachedGrid>),
}

/// Typed value of one cached cell, with the typing rules of ordinary cached
/// values: numbers, `str` text, booleans and the classic error codes.
fn cached_value(raw: &RawCell, context: &str) -> Result<LiteralValue, IoError> {
    let refuse = || unsupported("unsupported external cached value", context);
    if raw.metadata {
        return Err(refuse());
    }
    let text = raw.value.as_deref().ok_or_else(refuse)?;
    Ok(match raw.kind.as_deref() {
        None | Some("n") => {
            let n = text.trim().parse::<f64>().map_err(|_| refuse())?;
            if !n.is_finite() {
                return Err(refuse());
            }
            LiteralValue::Number(n)
        }
        Some("str") => {
            // `_xHHHH_` has reader-specific escape semantics; see the writer.
            if text.as_bytes().windows(7).any(|w| {
                w[0] == b'_'
                    && w[1] == b'x'
                    && w[6] == b'_'
                    && w[2..6].iter().all(u8::is_ascii_hexdigit)
            }) {
                return Err(refuse());
            }
            LiteralValue::Text(text.to_owned())
        }
        Some("b") => match text {
            "1" => LiteralValue::Boolean(true),
            "0" => LiteralValue::Boolean(false),
            _ => return Err(refuse()),
        },
        Some("e") => {
            let kind = match text {
                "#NULL!" => ExcelErrorKind::Null,
                "#REF!" => ExcelErrorKind::Ref,
                "#NAME?" => ExcelErrorKind::Name,
                "#VALUE!" => ExcelErrorKind::Value,
                "#DIV/0!" => ExcelErrorKind::Div,
                "#N/A" => ExcelErrorKind::Na,
                "#NUM!" => ExcelErrorKind::Num,
                _ => return Err(refuse()),
            };
            LiteralValue::Error(ExcelError::new(kind))
        }
        _ => return Err(refuse()),
    })
}

impl Links {
    /// Refuse DDE/OLE links, which have no workbook cache.
    #[cfg(feature = "xlsx-recalc")]
    pub(crate) fn check_kinds(&self) -> Result<(), IoError> {
        for (i, link) in self.links.iter().enumerate() {
            if let Link::Other(kind) = link {
                return Err(unsupported(
                    format!("{kind} external link"),
                    format!("external link [{}]", i + 1),
                ));
            }
        }
        Ok(())
    }

    /// The link index and cached sheet an external reference reads.
    fn sheet(
        &self,
        ext: &formualizer_parse::parser::ExternalReference,
        context: &str,
    ) -> Result<(usize, &SheetCache), IoError> {
        let token = ext.book.token();
        let index = token
            .strip_prefix('[')
            .and_then(|t| t.strip_suffix(']'))
            .filter(|t| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|t| t.parse::<usize>().ok())
            .filter(|i| *i >= 1)
            .ok_or_else(|| unsupported("external reference without a link index", context))?;
        let Some(Link::Book(book)) = self.links.get(index - 1) else {
            return Err(unsupported(
                "external reference to an undeclared link",
                context,
            ));
        };
        let cache = book
            .cache
            .as_ref()
            .ok_or_else(|| unsupported("external link without cached values", context))?;
        let mut matches = book
            .sheets
            .iter()
            .enumerate()
            .filter(|(_, s)| s.to_lowercase() == ext.sheet.to_lowercase());
        let id = match (matches.next(), matches.next()) {
            (Some((id, _)), None) => id,
            _ => {
                return Err(unsupported(
                    "external sheet not listed in the link cache",
                    context,
                ));
            }
        };
        let sheet = cache
            .get(&id)
            .ok_or_else(|| unsupported("external sheet without cached values", context))?;
        Ok((index, sheet))
    }

    /// The value of an external reference, exactly from the cache.
    fn serve(
        &self,
        ext: &formualizer_parse::parser::ExternalReference,
        context: &str,
        area: &mut usize,
        options: &CacheOptions,
    ) -> Result<(usize, Served), IoError> {
        let (index, sheet) = self.sheet(ext, context)?;
        let value = |row: u32, col: u32| -> Result<LiteralValue, IoError> {
            match sheet.cells.get(&(row, col)) {
                Some(raw) => cached_value(raw, context),
                None if sheet.refresh_error => {
                    Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref)))
                }
                None => Ok(LiteralValue::Empty),
            }
        };
        match ext.kind {
            ExternalRefKind::Cell { row, col, .. } => Ok((index, Served::Scalar(value(row, col)?))),
            ExternalRefKind::Range {
                start_row: Some(r1),
                start_col: Some(c1),
                end_row: Some(r2),
                end_col: Some(c2),
                ..
            } => {
                let (r1, r2) = (r1.min(r2), r1.max(r2));
                let (c1, c2) = (c1.min(c2), c1.max(c2));
                let (rows, cols) = ((r2 - r1 + 1) as usize, (c2 - c1 + 1) as usize);
                // The engine materializes each distinct range once per run.
                *area = area.saturating_add(rows * cols);
                if *area
                    > options
                        .limits
                        .max_cells
                        .saturating_mul(EXTERNAL_AREA_FACTOR)
                {
                    return Err(unsupported("external range cell limit", context));
                }
                let mut cells = HashMap::new();
                for (i, (&(row, col), raw)) in sheet.cells.iter().enumerate() {
                    if i % 4096 == 0 {
                        checkpoint(&options.cancel)?;
                    }
                    if (r1..=r2).contains(&row) && (c1..=c2).contains(&col) {
                        let at = ((row - r1) as usize, (col - c1) as usize);
                        cells.insert(at, cached_value(raw, context)?);
                    }
                }
                if sheet.refresh_error && cells.len() != rows * cols {
                    return Err(unsupported(
                        "external range over uncached cells of a sheet whose link refresh failed",
                        context,
                    ));
                }
                Ok((
                    index,
                    Served::Range(Arc::new(CachedGrid { rows, cols, cells })),
                ))
            }
            ExternalRefKind::Range { .. } => Err(unsupported(
                "whole-row or whole-column external reference",
                context,
            )),
        }
    }
}

/// Lowercased name of a defined-name reference (`Sheet1!Name` reads `name`).
fn name_key(name: &str) -> String {
    name.rsplit('!').next().unwrap_or(name).to_ascii_lowercase()
}
/// `[1]!Name` / `[1]Sheet!Name`: a name defined in the linked workbook.
fn is_external_name(name: &str) -> bool {
    name.split_once('!')
        .is_some_and(|(qualifier, _)| qualifier.trim_start_matches('\'').starts_with('['))
}

fn parse_formula(text: &str) -> Option<ASTNode> {
    formualizer_parse::parser::parse(format!(
        "={}",
        text.trim().strip_prefix('=').unwrap_or(text.trim())
    ))
    .ok()
}

/// What one formula or name reads, collected from its AST.
#[derive(Default)]
struct Reads<'a> {
    externals: Vec<&'a formualizer_parse::parser::ExternalReference>,
    names: Vec<String>,
}
fn collect<'a>(ast: &'a ASTNode, reads: &mut Reads<'a>) {
    match &ast.node_type {
        ASTNodeType::Reference { reference, .. } => match reference {
            ReferenceType::External(ext) => reads.externals.push(ext),
            ReferenceType::NamedRange(name) => reads.names.push(name.clone()),
            _ => {}
        },
        ASTNodeType::UnaryOp { expr, .. } => collect(expr, reads),
        ASTNodeType::BinaryOp { left, right, .. } => {
            collect(left, reads);
            collect(right, reads);
        }
        ASTNodeType::Function { args, .. } => args.iter().for_each(|a| collect(a, reads)),
        ASTNodeType::Call { callee, args } => {
            collect(callee, reads);
            args.iter().for_each(|a| collect(a, reads));
        }
        ASTNodeType::Array(rows) => rows.iter().flatten().for_each(|a| collect(a, reads)),
        ASTNodeType::Literal(_) | ASTNodeType::Omitted => {}
    }
}

/// Whether `ast` can produce a reference into a linked workbook: an external
/// reference, a name that reads one, or `INDIRECT`.
fn reaches_external(ast: &ASTNode, external_names: &HashSet<String>) -> bool {
    match &ast.node_type {
        ASTNodeType::Reference { reference, .. } => match reference {
            ReferenceType::External(_) => true,
            ReferenceType::NamedRange(name) => {
                is_external_name(name) || external_names.contains(&name_key(name))
            }
            _ => false,
        },
        ASTNodeType::UnaryOp { expr, .. } => reaches_external(expr, external_names),
        ASTNodeType::BinaryOp { left, right, .. } => {
            reaches_external(left, external_names) || reaches_external(right, external_names)
        }
        ASTNodeType::Function { name, args } => {
            name.eq_ignore_ascii_case("INDIRECT")
                || args.iter().any(|a| reaches_external(a, external_names))
        }
        ASTNodeType::Call { callee, args } => {
            reaches_external(callee, external_names)
                || args.iter().any(|a| reaches_external(a, external_names))
        }
        ASTNodeType::Array(rows) => rows
            .iter()
            .flatten()
            .any(|a| reaches_external(a, external_names)),
        ASTNodeType::Literal(_) | ASTNodeType::Omitted => false,
    }
}

/// Refuse constructs whose result depends on where an external reference
/// lives rather than on its cached values, and `INDIRECT` text that could
/// name another workbook.
fn check_shape(
    ast: &ASTNode,
    external_names: &HashSet<String>,
    context: &str,
) -> Result<(), IoError> {
    match &ast.node_type {
        ASTNodeType::Reference { reference, .. } => {
            if let ReferenceType::NamedRange(name) = reference
                && is_external_name(name)
            {
                return Err(unsupported(
                    "defined name of a linked workbook",
                    format!("{context}: {name}"),
                ));
            }
            Ok(())
        }
        ASTNodeType::UnaryOp { expr, .. } => check_shape(expr, external_names, context),
        ASTNodeType::BinaryOp { op, left, right } => {
            if matches!(op.as_str(), ":" | "," | " ")
                && (reaches_external(left, external_names)
                    || reaches_external(right, external_names))
            {
                return Err(unsupported(
                    "reference operator over an external reference",
                    context,
                ));
            }
            check_shape(left, external_names, context)?;
            check_shape(right, external_names, context)
        }
        ASTNodeType::Function { name, args } => {
            let upper = name.to_ascii_uppercase();
            let upper = upper.strip_prefix("_XLFN.").unwrap_or(&upper);
            if upper == "INDIRECT" {
                let literal = matches!(
                    args.first().map(|a| &a.node_type),
                    Some(ASTNodeType::Literal(LiteralValue::Text(t))) if !t.contains('[')
                );
                if !literal {
                    return Err(unsupported(
                        "INDIRECT that may reach a linked workbook",
                        context,
                    ));
                }
            }
            if REFERENCE_FUNCTIONS.contains(&upper)
                && args.iter().any(|a| reaches_external(a, external_names))
            {
                return Err(unsupported(
                    format!("{upper} over an external reference"),
                    context,
                ));
            }
            if matches!(upper, "SUMIF" | "AVERAGEIF")
                && let Some(sum) = args.get(2)
                && reaches_external(sum, external_names)
                && reference_dims(&args[0]) != reference_dims(sum)
            {
                // The sum range is resized to the criteria range's shape and
                // could read cells outside the cached reference.
                return Err(unsupported(
                    format!("{upper} resizing an external sum range"),
                    context,
                ));
            }
            args.iter()
                .try_for_each(|a| check_shape(a, external_names, context))
        }
        ASTNodeType::Call { callee, args } => {
            check_shape(callee, external_names, context)?;
            args.iter()
                .try_for_each(|a| check_shape(a, external_names, context))
        }
        ASTNodeType::Array(rows) => rows
            .iter()
            .flatten()
            .try_for_each(|a| check_shape(a, external_names, context)),
        ASTNodeType::Literal(_) | ASTNodeType::Omitted => Ok(()),
    }
}
/// A multi-cell external range reference.
fn is_external_range(ast: &ASTNode) -> bool {
    matches!(
        &ast.node_type,
        ASTNodeType::Reference { reference: ReferenceType::External(ext), .. }
            if !matches!(ext.kind, ExternalRefKind::Cell { .. })
                && reference_dims(ast) != Some((1, 1))
    )
}
/// Whether a name's value can be an array built from an external range:
/// the range in an operand or as a result of `IF`/`CHOOSE`.
fn array_valued(ast: &ASTNode) -> bool {
    match &ast.node_type {
        ASTNodeType::Reference { .. } => is_external_range(ast),
        ASTNodeType::UnaryOp { expr, .. } => array_valued(expr),
        ASTNodeType::BinaryOp { left, right, .. } => array_valued(left) || array_valued(right),
        ASTNodeType::Function { name, args } => {
            matches!(name.to_ascii_uppercase().as_str(), "IF" | "CHOOSE")
                && args.iter().skip(1).any(array_valued)
        }
        _ => false,
    }
}
/// `(rows, columns)` of a bounded cell or range reference.
fn reference_dims(ast: &ASTNode) -> Option<(u32, u32)> {
    let ASTNodeType::Reference { reference, .. } = &ast.node_type else {
        return None;
    };
    let span = |a: Option<u32>, b: Option<u32>| Some(a?.abs_diff(b?) + 1);
    match reference {
        ReferenceType::Cell { .. } => Some((1, 1)),
        ReferenceType::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            ..
        } => Some((span(*start_row, *end_row)?, span(*start_col, *end_col)?)),
        ReferenceType::External(ext) => match ext.kind {
            ExternalRefKind::Cell { .. } => Some((1, 1)),
            ExternalRefKind::Range {
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => Some((span(start_row, end_row)?, span(start_col, end_col)?)),
        },
        _ => None,
    }
}

/// One source formula as the ingestion replays it: its text, with shared
/// followers expanded from their anchor exactly as Calamine does.
#[cfg(feature = "xlsx-recalc")]
fn formula_texts<'p>(
    plan: &'p SheetPlan,
) -> Result<
    Vec<(
        &'p crate::cache_recalculate::sheet::Cell,
        std::borrow::Cow<'p, str>,
    )>,
    IoError,
> {
    let mut anchors = HashMap::new();
    for cell in &plan.cells {
        if let Some(si) = cell.shared_id
            && !cell.formula_text.trim().is_empty()
        {
            anchors.insert(si, cell);
        }
    }
    let mut out = Vec::with_capacity(plan.cells.len());
    for cell in &plan.cells {
        if !cell.formula_text.trim().is_empty() {
            out.push((cell, std::borrow::Cow::Borrowed(cell.formula_text.as_str())));
            continue;
        }
        let Some(anchor) = cell.shared_id.and_then(|si| anchors.get(&si)) else {
            continue;
        };
        if !anchor.formula_text.contains('[') {
            out.push((
                cell,
                std::borrow::Cow::Borrowed(anchor.formula_text.as_str()),
            ));
            continue;
        }
        let expanded = calamine::expand_shared_formula(
            &anchor.formula_text,
            (anchor.row - 1, anchor.col - 1),
            (cell.row - 1, cell.col - 1),
        )
        .map_err(|_| unsupported("shared formula expansion", cell.address.clone()))?;
        out.push((cell, std::borrow::Cow::Owned(expanded)));
    }
    Ok(out)
}

/// Every external reference that formulas and used defined names read,
/// served from the link caches, or the first refusal.
#[cfg(feature = "xlsx-recalc")]
pub(crate) fn plan(
    links: &Links,
    sheets: &[package::Sheet],
    plans: &[SheetPlan],
    names: &[(String, String, Option<usize>)],
    options: &XlsxRecalculateOptions,
) -> Result<Plan, IoError> {
    // Defined names: which read external links (directly or through other
    // names), and which other names each references.
    let parsed_names: Vec<Option<ASTNode>> = names
        .iter()
        .map(|(_, text, _)| parse_formula(text))
        .collect();
    let mut name_refs: Vec<Vec<String>> = Vec::with_capacity(names.len());
    let mut external_names: HashSet<String> = HashSet::new();
    for ((name, text, _), ast) in names.iter().zip(&parsed_names) {
        let mut reads = Reads::default();
        if let Some(ast) = ast {
            collect(ast, &mut reads);
        }
        if !reads.externals.is_empty()
            || reads.names.iter().any(|n| is_external_name(n))
            || (ast.is_none() && text.contains('['))
        {
            external_names.insert(name.to_ascii_lowercase());
        }
        name_refs.push(reads.names.iter().map(|n| name_key(n)).collect());
    }
    // Through other names.
    loop {
        let before = external_names.len();
        for ((name, _, _), refs) in names.iter().zip(&name_refs) {
            if refs.iter().any(|r| external_names.contains(r)) {
                external_names.insert(name.to_ascii_lowercase());
            }
        }
        if external_names.len() == before {
            break;
        }
    }
    // A name is used when any formula or any defined name references it
    // (the calculation-name validator's rule).
    let mut used: HashSet<String> = name_refs.iter().flatten().cloned().collect();

    let mut values = ExternalValues::default();
    let mut used_links = BTreeSet::new();
    let mut first_read = None;
    let mut area = 0usize;
    let mut serve = |ext: &formualizer_parse::parser::ExternalReference,
                     context: &str,
                     values: &mut ExternalValues|
     -> Result<(), IoError> {
        if values.contains(&ext.raw) {
            return Ok(());
        }
        let context = format!("{context}: {}", ext.raw);
        let (index, served) = links.serve(ext, &context, &mut area, &options.into())?;
        used_links.insert(index);
        first_read.get_or_insert(context);
        match served {
            Served::Scalar(v) => {
                values.scalars.insert(ext.raw.clone(), v);
            }
            Served::Range(v) => {
                values.ranges.insert(ext.raw.clone(), v);
            }
        }
        Ok(())
    };

    let needles: Vec<&String> = external_names.iter().collect();
    for (sheet, plan) in sheets.iter().zip(plans) {
        for (cell, text) in formula_texts(plan)? {
            checkpoint(&options.cancel)?;
            let lower = text.to_ascii_lowercase();
            let names_hit = needles.iter().any(|n| lower.contains(n.as_str()));
            if !lower.contains('[') && !lower.contains("indirect") && !names_hit {
                continue;
            }
            let context = format!("{}!{}", sheet.name, cell.address);
            let Some(ast) = parse_formula(&text) else {
                // An unparseable formula is refused by ingestion.
                continue;
            };
            check_shape(&ast, &external_names, &context)?;
            let mut reads = Reads::default();
            collect(&ast, &mut reads);
            for name in &reads.names {
                used.insert(name_key(name));
            }
            for ext in reads.externals {
                serve(ext, &context, &mut values)?;
            }
        }
    }
    // Names: serve the externals of used names; leave unused ones out.
    let mut skipped_names = Vec::new();
    let mut range_names = HashSet::new();
    for ((name, _, scope), ast) in names.iter().zip(&parsed_names) {
        let key = name.to_ascii_lowercase();
        if !external_names.contains(&key) {
            continue;
        }
        if !used.contains(&key) {
            skipped_names.push(key);
            continue;
        }
        let context = match scope {
            Some(i) => format!(
                "defined name {}!{name}",
                sheets.get(*i).map_or("?", |s| s.name.as_str())
            ),
            None => format!("defined name {name}"),
        };
        let ast = ast
            .as_ref()
            .ok_or_else(|| unsupported("unsupported or cyclic calculation name", name.clone()))?;
        check_shape(ast, &external_names, &context)?;
        // A calculation name evaluates to one value: an external range is
        // admitted as the whole definition (read as a reference) or inside
        // a function argument, not as an array result.
        if is_external_range(ast) {
            range_names.insert(key);
        } else if array_valued(ast) {
            return Err(unsupported(
                "defined name computing an array from an external range",
                context,
            ));
        }
        let mut reads = Reads::default();
        collect(ast, &mut reads);
        for ext in reads.externals {
            serve(ext, &context, &mut values)?;
        }
    }
    Ok(Plan {
        values,
        links_used: used_links.len(),
        first_read,
        skipped_names,
        range_names,
    })
}
