use crate::structured_ref;
use crate::tokenizer::{Associativity, Token, TokenSubType, TokenType, Tokenizer, TokenizerError};
use crate::types::{FormulaDialect, ParsingError};
use crate::{ExcelError, LiteralValue};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::hasher::FormulaHasher;
use formualizer_common::coord::{
    col_index_from_letters_1based, col_letters_from_1based, parse_a1_1based,
};
use formualizer_common::{
    AxisBound, RelativeCoord, SheetCellRef, SheetLocator, SheetRangeRef, SheetRef,
};
use once_cell::sync::Lazy;
use smallvec::SmallVec;
use std::error::Error;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

type VolatilityFn = dyn Fn(&str) -> bool + Send + Sync + 'static;
type VolatilityClassifierBox = Box<VolatilityFn>;
type VolatilityClassifierArc = Arc<VolatilityFn>;

/// A custom error type for the parser.
#[derive(Debug)]
pub struct ParserError {
    pub message: String,
    pub position: Option<usize>,
}

impl Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(pos) = self.position {
            write!(f, "ParserError at position {}: {}", pos, self.message)
        } else {
            write!(f, "ParserError: {}", self.message)
        }
    }
}

impl Error for ParserError {}

// Column lookup table for common columns (A-ZZ = 702 columns)
static COLUMN_LOOKUP: Lazy<Vec<String>> = Lazy::new(|| {
    let mut cols = Vec::with_capacity(702);
    // Single letters A-Z
    for c in b'A'..=b'Z' {
        cols.push(String::from(c as char));
    }
    // Double letters AA-ZZ
    for c1 in b'A'..=b'Z' {
        for c2 in b'A'..=b'Z' {
            cols.push(format!("{}{}", c1 as char, c2 as char));
        }
    }
    cols
});

/// A structured table reference specifier for accessing specific parts of a table
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TableSpecifier {
    /// The entire table
    All,
    /// The data area of the table (no headers or totals)
    Data,
    /// The headers row
    Headers,
    /// The totals row
    Totals,
    /// A specific row
    Row(TableRowSpecifier),
    /// A specific column
    Column(String),
    /// A range of columns
    ColumnRange(String, String),
    /// Special items like #Headers, #Data, #Totals, etc.
    SpecialItem(SpecialItem),
    /// A combination of specifiers, for complex references
    Combination(Vec<Box<TableSpecifier>>),
}

/// Specifies which row(s) to use in a table reference
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TableRowSpecifier {
    /// The current row (context dependent)
    Current,
    /// All rows
    All,
    /// Data rows only
    Data,
    /// Headers row
    Headers,
    /// Totals row
    Totals,
    /// Specific row by index (1-based)
    Index(u32),
}

/// Special items in structured references
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SpecialItem {
    /// The #Headers item
    Headers,
    /// The #Data item
    Data,
    /// The #Totals item
    Totals,
    /// The #All item (the whole table)
    All,
    /// The @ item (current row)
    ThisRow,
}

/// A reference to a table including specifiers
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct TableReference {
    /// The name of the table
    pub name: String,
    /// Optional specifier for which part of the table to use
    pub specifier: Option<TableSpecifier>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ExternalBookRef {
    Token(String),
}

impl ExternalBookRef {
    pub fn token(&self) -> &str {
        match self {
            ExternalBookRef::Token(s) => s,
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalRefKind {
    Cell {
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    },
    Range {
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    },
}

impl ExternalRefKind {
    pub fn cell(row: u32, col: u32) -> Self {
        Self::Cell {
            row,
            col,
            row_abs: false,
            col_abs: false,
        }
    }

    pub fn cell_with_abs(row: u32, col: u32, row_abs: bool, col_abs: bool) -> Self {
        Self::Cell {
            row,
            col,
            row_abs,
            col_abs,
        }
    }

    pub fn range(
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    ) -> Self {
        Self::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs: false,
            start_col_abs: false,
            end_row_abs: false,
            end_col_abs: false,
        }
    }

    // Constructor-style helper mirroring the enum fields.
    // Keeping the signature explicit makes callers easier to read.
    #[allow(clippy::too_many_arguments)]
    pub fn range_with_abs(
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    ) -> Self {
        Self::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ExternalReference {
    pub raw: String,
    pub book: ExternalBookRef,
    pub sheet: String,
    pub kind: ExternalRefKind,
}

/// A reference to something outside the cell.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ReferenceType {
    Cell {
        sheet: Option<String>,
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    },
    Range {
        sheet: Option<String>,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    },
    /// 3D cell reference (`Sheet1:Sheet3!A1`).
    ///
    /// Excel evaluates aggregating functions across each sheet between
    /// `sheet_first` and `sheet_last` (inclusive) at the same cell address.
    Cell3D {
        sheet_first: String,
        sheet_last: String,
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    },
    /// 3D range reference (`Sheet1:Sheet3!A1:B2`).
    Range3D {
        sheet_first: String,
        sheet_last: String,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    },
    External(ExternalReference),
    Table(TableReference),
    NamedRange(String),
}

impl Display for TableSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableSpecifier::All => write!(f, "#All"),
            TableSpecifier::Data => write!(f, "#Data"),
            TableSpecifier::Headers => write!(f, "#Headers"),
            TableSpecifier::Totals => write!(f, "#Totals"),
            TableSpecifier::Row(row) => write!(f, "{row}"),
            TableSpecifier::Column(column) => write!(f, "{column}"),
            TableSpecifier::ColumnRange(start, end) => write!(f, "{start}:{end}"),
            TableSpecifier::SpecialItem(item) => write!(f, "{item}"),
            TableSpecifier::Combination(specs) => {
                // Emit nested bracketed parts so the surrounding Table formatter prints
                // canonical structured refs like Table[[#Headers],[Column1]:[Column2]].
                // ColumnRange children must split their bracket boundary across
                // both endpoints (`[A]:[B]`) rather than wrapping the whole
                // range in one bracket pair.
                let mut first = true;
                for spec in specs {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;
                    match spec.as_ref() {
                        TableSpecifier::ColumnRange(start, end) => {
                            write!(f, "[{start}]:[{end}]")?;
                        }
                        other => write!(f, "[{other}]")?,
                    }
                }
                Ok(())
            }
        }
    }
}

impl Display for TableRowSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableRowSpecifier::Current => write!(f, "@"),
            TableRowSpecifier::All => write!(f, "#All"),
            TableRowSpecifier::Data => write!(f, "#Data"),
            TableRowSpecifier::Headers => write!(f, "#Headers"),
            TableRowSpecifier::Totals => write!(f, "#Totals"),
            TableRowSpecifier::Index(idx) => write!(f, "{idx}"),
        }
    }
}

impl Display for SpecialItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpecialItem::Headers => write!(f, "#Headers"),
            SpecialItem::Data => write!(f, "#Data"),
            SpecialItem::Totals => write!(f, "#Totals"),
            SpecialItem::All => write!(f, "#All"),
            SpecialItem::ThisRow => write!(f, "@"),
        }
    }
}

/// Check if a sheet name needs to be quoted in Excel formulas
fn sheet_name_needs_quoting(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    let bytes = name.as_bytes();

    // Check if starts with a digit
    if bytes[0].is_ascii_digit() {
        return true;
    }

    // Check for any special characters that require quoting
    // This includes: space, !, ", #, $, %, &, ', (, ), *, +, comma, -, ., /, :, ;, <, =, >, ?, @, [, \, ], ^, `, {, |, }, ~
    for &byte in bytes {
        match byte {
            b' ' | b'!' | b'"' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+'
            | b',' | b'-' | b'.' | b'/' | b':' | b';' | b'<' | b'=' | b'>' | b'?' | b'@' | b'['
            | b'\\' | b']' | b'^' | b'`' | b'{' | b'|' | b'}' | b'~' => return true,
            _ => {}
        }
    }

    // Check for Excel reserved words (case-insensitive)
    let upper = name.to_uppercase();
    matches!(
        upper.as_str(),
        "TRUE" | "FALSE" | "NULL" | "REF" | "DIV" | "NAME" | "NUM" | "VALUE" | "N/A"
    )
}

#[derive(Debug, Clone)]
struct OpenFormulaRefPart {
    sheet: Option<String>,
    coord: String,
}

type AxisPartWithAbs = Option<(u32, bool)>;
type RangePartWithAbs = (AxisPartWithAbs, AxisPartWithAbs);

/// Result of extracting the sheet portion of a reference string.
#[derive(Debug, Clone)]
enum SheetSpec {
    /// No sheet segment was present (e.g. plain `A1`).
    None,
    /// Standard single-sheet reference (`Sheet1!A1`, `'Sheet 1'!A1`).
    Single(String),
    /// Excel 3D sheet range (`Sheet1:Sheet3!A1`, `'Sheet 1':'Sheet 3'!A1`).
    Range { first: String, last: String },
}

impl ReferenceType {
    /// Build a cell reference with relative anchors.
    pub fn cell(sheet: Option<String>, row: u32, col: u32) -> Self {
        Self::Cell {
            sheet,
            row,
            col,
            row_abs: false,
            col_abs: false,
        }
    }

    /// Build a cell reference with explicit anchors.
    pub fn cell_with_abs(
        sheet: Option<String>,
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    ) -> Self {
        Self::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        }
    }

    /// Build a range reference with relative anchors.
    pub fn range(
        sheet: Option<String>,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    ) -> Self {
        Self::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs: false,
            start_col_abs: false,
            end_row_abs: false,
            end_col_abs: false,
        }
    }

    /// Build a range reference with explicit anchors.
    // Constructor-style helper mirroring the enum fields.
    // Keeping the signature explicit makes callers easier to read.
    #[allow(clippy::too_many_arguments)]
    pub fn range_with_abs(
        sheet: Option<String>,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    ) -> Self {
        Self::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        }
    }

    /// Create a reference from a string. Can be A1, A:A, A1:B2, Table1[Column], etc.
    pub fn from_string(reference: &str) -> Result<Self, ParsingError> {
        Self::parse_excel_reference(reference)
    }

    /// Create a reference from a string using the specified formula dialect.
    pub fn from_string_with_dialect(
        reference: &str,
        dialect: FormulaDialect,
    ) -> Result<Self, ParsingError> {
        match dialect {
            FormulaDialect::Excel => Self::parse_excel_reference(reference),
            FormulaDialect::OpenFormula => Self::parse_openformula_reference(reference)
                .or_else(|_| Self::parse_excel_reference(reference)),
        }
    }

    /// Parse a grid reference into a shared SheetRef, preserving $ anchors.
    ///
    /// Only cell and range references are supported. Table and named ranges return an error.
    pub fn parse_sheet_ref(reference: &str) -> Result<SheetRef<'static>, ParsingError> {
        Self::parse_sheet_ref_with_dialect(reference, FormulaDialect::Excel)
    }

    /// Parse a grid reference into a shared SheetRef using the specified dialect.
    pub fn parse_sheet_ref_with_dialect(
        reference: &str,
        dialect: FormulaDialect,
    ) -> Result<SheetRef<'static>, ParsingError> {
        match dialect {
            FormulaDialect::Excel => Self::parse_excel_sheet_ref(reference),
            FormulaDialect::OpenFormula => Self::parse_openformula_sheet_ref(reference)
                .or_else(|_| Self::parse_excel_sheet_ref(reference)),
        }
    }

    /// Lossy conversion from parsed ReferenceType into SheetRef.
    /// External, table, and named ranges are discarded; anchors are preserved.
    pub fn to_sheet_ref_lossy(&self) -> Option<SheetRef<'_>> {
        match self {
            ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => {
                let row0 = row.checked_sub(1)?;
                let col0 = col.checked_sub(1)?;
                let sheet_loc = match sheet.as_deref() {
                    Some(name) => SheetLocator::from_name(name),
                    None => SheetLocator::Current,
                };
                let coord = RelativeCoord::new(row0, col0, *row_abs, *col_abs);
                Some(SheetRef::Cell(SheetCellRef::new(sheet_loc, coord)))
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                let sheet_loc = match sheet.as_deref() {
                    Some(name) => SheetLocator::from_name(name),
                    None => SheetLocator::Current,
                };
                let sr = start_row
                    .and_then(|v| v.checked_sub(1).map(|i| AxisBound::new(i, *start_row_abs)));
                if start_row.is_some() && sr.is_none() {
                    return None;
                }
                let sc = start_col
                    .and_then(|v| v.checked_sub(1).map(|i| AxisBound::new(i, *start_col_abs)));
                if start_col.is_some() && sc.is_none() {
                    return None;
                }
                let er =
                    end_row.and_then(|v| v.checked_sub(1).map(|i| AxisBound::new(i, *end_row_abs)));
                if end_row.is_some() && er.is_none() {
                    return None;
                }
                let ec =
                    end_col.and_then(|v| v.checked_sub(1).map(|i| AxisBound::new(i, *end_col_abs)));
                if end_col.is_some() && ec.is_none() {
                    return None;
                }
                let range = SheetRangeRef::from_parts(sheet_loc, sr, sc, er, ec).ok()?;
                Some(SheetRef::Range(range))
            }
            _ => None,
        }
    }

    fn parse_excel_sheet_ref(reference: &str) -> Result<SheetRef<'static>, ParsingError> {
        let (spec, ref_part) = Self::extract_sheet_spec(reference);
        if matches!(spec, SheetSpec::Range { .. }) {
            return Err(ParsingError::InvalidReference(
                "3D references are not supported for SheetRef".to_string(),
            ));
        }
        let sheet = match spec {
            SheetSpec::None => None,
            SheetSpec::Single(name) => Some(name),
            SheetSpec::Range { .. } => unreachable!(),
        };

        if ref_part.contains('[') {
            return Err(ParsingError::InvalidReference(
                "Table references are not supported for SheetRef".to_string(),
            ));
        }

        let sheet_loc: SheetLocator<'static> = match sheet {
            Some(name) => SheetLocator::from_name(name),
            None => SheetLocator::Current,
        };

        if ref_part.contains(':') {
            let mut parts = ref_part.splitn(2, ':');
            let start = parts.next().unwrap();
            let end = parts.next().ok_or_else(|| {
                ParsingError::InvalidReference(format!("Invalid range: {ref_part}"))
            })?;

            let (start_col, start_row) = Self::parse_range_part_with_abs(start)?;
            let (end_col, end_row) = Self::parse_range_part_with_abs(end)?;

            let start_col = Self::axis_bound_from_1based(start_col)?;
            let start_row = Self::axis_bound_from_1based(start_row)?;
            let end_col = Self::axis_bound_from_1based(end_col)?;
            let end_row = Self::axis_bound_from_1based(end_row)?;

            let range =
                SheetRangeRef::from_parts(sheet_loc, start_row, start_col, end_row, end_col)
                    .map_err(|err| ParsingError::InvalidReference(err.to_string()))?;
            Ok(SheetRef::Range(range))
        } else {
            let (row, col, row_abs, col_abs) = parse_a1_1based(&ref_part)
                .map_err(|err| ParsingError::InvalidReference(err.to_string()))?;
            let coord = RelativeCoord::new(row - 1, col - 1, row_abs, col_abs);
            Ok(SheetRef::Cell(SheetCellRef::new(sheet_loc, coord)))
        }
    }

    fn parse_openformula_sheet_ref(reference: &str) -> Result<SheetRef<'static>, ParsingError> {
        Self::parse_excel_sheet_ref(reference)
    }

    fn axis_bound_from_1based(
        bound: Option<(u32, bool)>,
    ) -> Result<Option<AxisBound>, ParsingError> {
        match bound {
            Some((index, abs)) => AxisBound::from_excel_1based(index, abs)
                .map(Some)
                .map_err(|err| ParsingError::InvalidReference(err.to_string())),
            None => Ok(None),
        }
    }

    fn parse_range_part_with_abs(part: &str) -> Result<RangePartWithAbs, ParsingError> {
        if let Ok((row, col, row_abs, col_abs)) = parse_a1_1based(part) {
            return Ok((Some((col, col_abs)), Some((row, row_abs))));
        }

        let bytes = part.as_bytes();
        let len = bytes.len();
        let mut i = 0usize;

        let mut col_abs = false;
        let mut row_abs = false;

        if i < len && bytes[i] == b'$' {
            col_abs = true;
            i += 1;
        }

        let col_start = i;
        while i < len && bytes[i].is_ascii_alphabetic() {
            i += 1;
        }

        if i > col_start {
            let col_str = &part[col_start..i];
            let col1 = Self::column_to_number(col_str)?;

            if i == len {
                return Ok((Some((col1, col_abs)), None));
            }

            if i < len && bytes[i] == b'$' {
                row_abs = true;
                i += 1;
            }

            if i >= len {
                return Err(ParsingError::InvalidReference(format!(
                    "Invalid range part: {part}"
                )));
            }

            let row_start = i;
            while i < len && bytes[i].is_ascii_digit() {
                i += 1;
            }

            if row_start == i || i != len {
                return Err(ParsingError::InvalidReference(format!(
                    "Invalid range part: {part}"
                )));
            }

            let row_str = &part[row_start..i];
            let row1 = row_str
                .parse::<u32>()
                .map_err(|_| ParsingError::InvalidReference(format!("Invalid row: {row_str}")))?;
            if row1 == 0 {
                return Err(ParsingError::InvalidReference(format!(
                    "Invalid range part: {part}"
                )));
            }

            return Ok((Some((col1, col_abs)), Some((row1, row_abs))));
        }

        i = 0;
        if i < len && bytes[i] == b'$' {
            row_abs = true;
            i += 1;
        }

        let row_start = i;
        while i < len && bytes[i].is_ascii_digit() {
            i += 1;
        }

        if row_start == i || i != len {
            return Err(ParsingError::InvalidReference(format!(
                "Invalid range part: {part}"
            )));
        }

        let row_str = &part[row_start..i];
        let row1 = row_str
            .parse::<u32>()
            .map_err(|_| ParsingError::InvalidReference(format!("Invalid row: {row_str}")))?;
        if row1 == 0 {
            return Err(ParsingError::InvalidReference(format!(
                "Invalid range part: {part}"
            )));
        }

        Ok((None, Some((row1, row_abs))))
    }

    fn parse_3d_reference(first: &str, last: &str, ref_part: &str) -> Result<Self, ParsingError> {
        if first.is_empty() || last.is_empty() {
            return Err(ParsingError::InvalidReference(format!(
                "3D reference requires two sheet names: {first}:{last}!{ref_part}"
            )));
        }
        if ref_part.is_empty() {
            return Err(ParsingError::InvalidReference(format!(
                "3D reference {first}:{last}! is missing a cell or range"
            )));
        }
        // 3D refs cannot point at structured table tokens.
        if ref_part.contains('[') {
            return Err(ParsingError::InvalidReference(format!(
                "3D reference {first}:{last}!{ref_part} cannot target a table"
            )));
        }

        if ref_part.contains(':') {
            let mut parts = ref_part.splitn(2, ':');
            let start = parts.next().unwrap();
            let end = parts.next().ok_or_else(|| {
                ParsingError::InvalidReference(format!("Invalid range: {ref_part}"))
            })?;
            let (start_col, start_row) = Self::parse_range_part_with_abs(start)?;
            let (end_col, end_row) = Self::parse_range_part_with_abs(end)?;

            let split = |bound: Option<(u32, bool)>| match bound {
                Some((index, abs)) => (Some(index), abs),
                None => (None, false),
            };
            let (start_col, start_col_abs) = split(start_col);
            let (start_row, start_row_abs) = split(start_row);
            let (end_col, end_col_abs) = split(end_col);
            let (end_row, end_row_abs) = split(end_row);

            Ok(ReferenceType::Range3D {
                sheet_first: first.to_string(),
                sheet_last: last.to_string(),
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            })
        } else {
            let (col, row, col_abs, row_abs) =
                Self::parse_cell_reference(ref_part).map_err(|_| {
                    ParsingError::InvalidReference(format!(
                        "Invalid 3D reference target: {ref_part}"
                    ))
                })?;
            Ok(ReferenceType::Cell3D {
                sheet_first: first.to_string(),
                sheet_last: last.to_string(),
                row,
                col,
                row_abs,
                col_abs,
            })
        }
    }

    fn parse_excel_reference(reference: &str) -> Result<Self, ParsingError> {
        // Excel structured reference shorthands that appear as a single bracketed token.
        //
        // We use these forms to avoid ambiguity with cell refs / named ranges:
        // - `[TableName]` resolves to the table's data body (equivalent to `TableName[#Data]`).
        // - `[@Column]` / `[@[Column Name]]` is a "This Row" selector; it requires table-aware
        //   context during resolution and will be rewritten by the evaluator/graph builder.
        if reference.starts_with('[') && reference.ends_with(']') && !reference.contains('!') {
            return Self::parse_bracketed_structured_reference(reference);
        }

        // Extract sheet specification (none / single / 3D range) if present.
        let (sheet_spec, ref_part) = Self::extract_sheet_spec(reference);

        // 3D references (`Sheet1:Sheet3!A1` / `Sheet1:Sheet3!A1:B2`) take a
        // dedicated path because they cannot reuse the 2D Cell/Range carriers.
        if let SheetSpec::Range { first, last, .. } = &sheet_spec {
            return Self::parse_3d_reference(first, last, &ref_part);
        }

        let sheet = match sheet_spec {
            SheetSpec::None => None,
            SheetSpec::Single(name) => Some(name),
            // Already handled above.
            SheetSpec::Range { .. } => unreachable!(),
        };

        // Table references live in the ref_part (e.g., "Table1[Column]").
        // Sheet names can contain '[' for external workbook refs (e.g., "[1]Sheet1!A1").
        if ref_part.contains('[') {
            return Self::parse_table_reference(&ref_part);
        }

        let external_sheet = sheet.as_deref().and_then(|s| {
            // Excel external workbook refs embed a "[...]" token inside the sheet segment.
            // Use the last '[' to allow paths/URIs that may contain earlier brackets, then
            // take the first ']' after it to avoid being confused by ']' in the sheet name.
            let lb = s.rfind('[')?;
            let rb_rel = s[lb..].find(']')?;
            let rb = lb + rb_rel;
            if lb >= rb {
                return None;
            }

            let token = &s[..=rb];
            let sheet_name = &s[rb + 1..];
            if sheet_name.is_empty() {
                None
            } else {
                Some((token, sheet_name))
            }
        });

        if ref_part.contains(':') {
            // Range reference
            let mut parts = ref_part.splitn(2, ':');
            let start = parts.next().unwrap();
            let end = parts.next().ok_or_else(|| {
                ParsingError::InvalidReference(format!("Invalid range: {ref_part}"))
            })?;
            let (start_col, start_row) = Self::parse_range_part_with_abs(start)?;
            let (end_col, end_row) = Self::parse_range_part_with_abs(end)?;

            let split = |bound: Option<(u32, bool)>| match bound {
                Some((index, abs)) => (Some(index), abs),
                None => (None, false),
            };
            let (start_col, start_col_abs) = split(start_col);
            let (start_row, start_row_abs) = split(start_row);
            let (end_col, end_col_abs) = split(end_col);
            let (end_row, end_row_abs) = split(end_row);

            if let Some((book_token, sheet_name)) = external_sheet {
                Ok(ReferenceType::External(ExternalReference {
                    raw: reference.to_string(),
                    book: ExternalBookRef::Token(book_token.to_string()),
                    sheet: sheet_name.to_string(),
                    kind: ExternalRefKind::Range {
                        start_row,
                        start_col,
                        end_row,
                        end_col,
                        start_row_abs,
                        start_col_abs,
                        end_row_abs,
                        end_col_abs,
                    },
                }))
            } else {
                Ok(ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                })
            }
        } else {
            // Try to parse as a single cell reference
            match Self::parse_cell_reference(&ref_part) {
                Ok((col, row, col_abs, row_abs)) => {
                    if let Some((book_token, sheet_name)) = external_sheet {
                        Ok(ReferenceType::External(ExternalReference {
                            raw: reference.to_string(),
                            book: ExternalBookRef::Token(book_token.to_string()),
                            sheet: sheet_name.to_string(),
                            kind: ExternalRefKind::Cell {
                                row,
                                col,
                                row_abs,
                                col_abs,
                            },
                        }))
                    } else {
                        Ok(ReferenceType::Cell {
                            sheet,
                            row,
                            col,
                            row_abs,
                            col_abs,
                        })
                    }
                }
                Err(_) => {
                    // Treat it as a named range
                    Ok(ReferenceType::NamedRange(reference.to_string()))
                }
            }
        }
    }

    /// Parse a cell reference like "A1" into (column, row) using byte-based parsing.
    fn parse_cell_reference(reference: &str) -> Result<(u32, u32, bool, bool), ParsingError> {
        parse_a1_1based(reference)
            .map(|(row, col, row_abs, col_abs)| (col, row, col_abs, row_abs))
            .map_err(|_| {
                ParsingError::InvalidReference(format!("Invalid cell reference: {reference}"))
            })
    }

    /// Convert a column letter (e.g., "A", "BC") to a column number (1-based) using byte operations.
    pub(crate) fn column_to_number(column: &str) -> Result<u32, ParsingError> {
        col_index_from_letters_1based(column)
            .map_err(|_| ParsingError::InvalidReference(format!("Invalid column: {column}")))
    }

    /// Convert a column number to a column letter using lookup table for common values.
    pub(crate) fn number_to_column(num: u32) -> String {
        if num == 0 {
            return String::new();
        }
        // Use lookup table for common columns (1-702 covers A-ZZ)
        if num > 0 && num <= 702 {
            return COLUMN_LOOKUP[(num - 1) as usize].clone();
        }

        col_letters_from_1based(num).unwrap_or_default()
    }

    fn format_col(col: u32, abs: bool) -> String {
        if abs {
            format!("${}", Self::number_to_column(col))
        } else {
            Self::number_to_column(col)
        }
    }

    fn format_row(row: u32, abs: bool) -> String {
        if abs {
            format!("${row}")
        } else {
            row.to_string()
        }
    }
}

impl Display for ReferenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ReferenceType::Cell {
                    sheet,
                    row,
                    col,
                    row_abs,
                    col_abs,
                } => {
                    let col_str = Self::format_col(*col, *col_abs);
                    let row_str = Self::format_row(*row, *row_abs);

                    if let Some(sheet_name) = sheet {
                        if sheet_name_needs_quoting(sheet_name) {
                            // Escape any single quotes in the sheet name by doubling them
                            let escaped_name = sheet_name.replace('\'', "''");
                            format!("'{escaped_name}'!{col_str}{row_str}")
                        } else {
                            format!("{sheet_name}!{col_str}{row_str}")
                        }
                    } else {
                        format!("{col_str}{row_str}")
                    }
                }
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                } => {
                    // Format start reference
                    let start_ref = match (start_col, start_row) {
                        (Some(col), Some(row)) => format!(
                            "{}{}",
                            Self::format_col(*col, *start_col_abs),
                            Self::format_row(*row, *start_row_abs)
                        ),
                        (Some(col), None) => Self::format_col(*col, *start_col_abs),
                        (None, Some(row)) => Self::format_row(*row, *start_row_abs),
                        (None, None) => "".to_string(), // Should not happen in normal usage
                    };

                    // Format end reference
                    let end_ref = match (end_col, end_row) {
                        (Some(col), Some(row)) => format!(
                            "{}{}",
                            Self::format_col(*col, *end_col_abs),
                            Self::format_row(*row, *end_row_abs)
                        ),
                        (Some(col), None) => Self::format_col(*col, *end_col_abs),
                        (None, Some(row)) => Self::format_row(*row, *end_row_abs),
                        (None, None) => "".to_string(), // Should not happen in normal usage
                    };

                    let range_part = format!("{start_ref}:{end_ref}");

                    if let Some(sheet_name) = sheet {
                        if sheet_name_needs_quoting(sheet_name) {
                            // Escape any single quotes in the sheet name by doubling them
                            let escaped_name = sheet_name.replace('\'', "''");
                            format!("'{escaped_name}'!{range_part}")
                        } else {
                            format!("{sheet_name}!{range_part}")
                        }
                    } else {
                        range_part
                    }
                }
                ReferenceType::Cell3D {
                    sheet_first,
                    sheet_last,
                    row,
                    col,
                    row_abs,
                    col_abs,
                } => {
                    let col_str = Self::format_col(*col, *col_abs);
                    let row_str = Self::format_row(*row, *row_abs);
                    let prefix = format_3d_sheet_prefix(sheet_first, sheet_last);
                    format!("{prefix}!{col_str}{row_str}")
                }
                ReferenceType::Range3D {
                    sheet_first,
                    sheet_last,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                } => {
                    let start_ref = match (start_col, start_row) {
                        (Some(col), Some(row)) => format!(
                            "{}{}",
                            Self::format_col(*col, *start_col_abs),
                            Self::format_row(*row, *start_row_abs)
                        ),
                        (Some(col), None) => Self::format_col(*col, *start_col_abs),
                        (None, Some(row)) => Self::format_row(*row, *start_row_abs),
                        (None, None) => "".to_string(),
                    };
                    let end_ref = match (end_col, end_row) {
                        (Some(col), Some(row)) => format!(
                            "{}{}",
                            Self::format_col(*col, *end_col_abs),
                            Self::format_row(*row, *end_row_abs)
                        ),
                        (Some(col), None) => Self::format_col(*col, *end_col_abs),
                        (None, Some(row)) => Self::format_row(*row, *end_row_abs),
                        (None, None) => "".to_string(),
                    };
                    let range_part = format!("{start_ref}:{end_ref}");
                    let prefix = format_3d_sheet_prefix(sheet_first, sheet_last);
                    format!("{prefix}!{range_part}")
                }
                ReferenceType::External(ext) => ext.raw.clone(),
                ReferenceType::Table(table_ref) => {
                    if let Some(specifier) = &table_ref.specifier {
                        // For table references, we need to handle column specifiers specially
                        // to remove leading/trailing whitespace
                        match specifier {
                            TableSpecifier::Column(column) => {
                                format!("{}[{}]", table_ref.name, column.trim())
                            }
                            TableSpecifier::ColumnRange(start, end) => {
                                format!("{}[{}:{}]", table_ref.name, start.trim(), end.trim())
                            }
                            _ => {
                                // For other specifiers, use the standard formatting
                                format!("{}[{}]", table_ref.name, specifier)
                            }
                        }
                    } else {
                        table_ref.name.clone()
                    }
                }
                ReferenceType::NamedRange(name) => name.clone(),
            }
        )
    }
}

/// Render the `Sheet1:SheetN` portion of a 3D reference. Either side may
/// require quoting independently; quoting one side does not force the other
/// to be quoted, matching Excel's behaviour.
fn format_3d_sheet_prefix(first: &str, last: &str) -> String {
    let format_one = |name: &str| -> String {
        if sheet_name_needs_quoting(name) {
            let escaped = name.replace('\'', "''");
            format!("'{escaped}'")
        } else {
            name.to_string()
        }
    };
    format!("{}:{}", format_one(first), format_one(last))
}

impl TryFrom<&str> for ReferenceType {
    type Error = ParsingError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ReferenceType::from_string(value)
    }
}

impl FromStr for ReferenceType {
    type Err = ParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ReferenceType::from_string(s)
    }
}

impl ReferenceType {
    /// Normalise the reference string (convert to canonical form)
    pub fn normalise(&self) -> String {
        format!("{self}")
    }

    /// Read one sheet-name segment starting at `start`. Returns the parsed
    /// (unescaped) name, the byte offset directly after the closing quote
    /// (when quoted) or the last alphanumeric byte (when bare), and a flag
    /// indicating whether the segment was quoted.
    fn read_sheet_segment(reference: &str, start: usize) -> Option<(String, usize, bool)> {
        let bytes = reference.as_bytes();
        if start >= bytes.len() {
            return None;
        }

        if bytes[start] == b'\'' {
            // Quoted segment. Excel doubles a literal `'` inside the name.
            let mut i = start + 1;
            let body_start = i;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    let raw = &reference[body_start..i];
                    let name = raw.replace("''", "'");
                    return Some((name, i + 1, true));
                }
                i += 1;
            }
            None
        } else {
            // Bare segment. Sheet names cannot contain ':', '!', '\'', or any
            // ASCII-whitespace/operator characters in unquoted form.
            let mut i = start;
            while i < bytes.len() {
                let b = bytes[i];
                match b {
                    b':' | b'!' | b'\'' | b' ' | b'\t' | b'\n' | b'\r' => break,
                    _ => i += 1,
                }
            }
            if i == start {
                None
            } else {
                Some((reference[start..i].to_string(), i, false))
            }
        }
    }

    /// Extract sheet specification (none, single sheet, or 3D sheet range)
    /// from a reference string.
    fn extract_sheet_spec(reference: &str) -> (SheetSpec, String) {
        let Some((first_name, after_first, first_quoted)) = Self::read_sheet_segment(reference, 0)
        else {
            // No sheet segment recognised – fall back to looking for a bare
            // `!` separator (e.g. external book tokens such as `[1]Sheet!A1`).
            return Self::extract_sheet_spec_fallback(reference);
        };
        let _ = first_quoted;

        let bytes = reference.as_bytes();

        // 3D form: Name1:Name2!...
        if after_first < bytes.len() && bytes[after_first] == b':' {
            let second_start = after_first + 1;
            if let Some((second_name, after_second, _)) =
                Self::read_sheet_segment(reference, second_start)
                && after_second < bytes.len()
                && bytes[after_second] == b'!'
            {
                let ref_part = reference[after_second + 1..].to_string();
                return (
                    SheetSpec::Range {
                        first: first_name,
                        last: second_name,
                    },
                    ref_part,
                );
            }

            // The reference looks like the start of a 3D ref but the second
            // segment is malformed (e.g. `Sheet1:!A1`). Surface the broken
            // form as a 3D range with an empty `last` so the parser layer
            // can report a precise error rather than silently treating it as
            // a sheet name containing `:`.
            if second_start < bytes.len() {
                if let Some(bang) = reference[second_start..].find('!') {
                    let ref_part = reference[second_start + bang + 1..].to_string();
                    return (
                        SheetSpec::Range {
                            first: first_name,
                            last: String::new(),
                        },
                        ref_part,
                    );
                }
            }
        }

        // Single-sheet form: Name!...
        if after_first < bytes.len() && bytes[after_first] == b'!' {
            let ref_part = reference[after_first + 1..].to_string();
            return (SheetSpec::Single(first_name), ref_part);
        }

        // The leading segment did not terminate in `!`; treat the whole input
        // as if no sheet were present and fall through to the legacy logic.
        Self::extract_sheet_spec_fallback(reference)
    }

    fn extract_sheet_spec_fallback(reference: &str) -> (SheetSpec, String) {
        let bytes = reference.as_bytes();
        // Handle unquoted sheet names containing characters our segment
        // reader rejects (such as bracketed external workbook tokens, e.g.
        // `[1]Sheet1!A1`). The original implementation scanned for the first
        // `!` after byte 0; preserve that behaviour for compatibility.
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'!' && i > 0 {
                let sheet = reference[..i].to_string();
                let ref_part = reference[i + 1..].to_string();
                return (SheetSpec::Single(sheet), ref_part);
            }
            i += 1;
        }

        (SheetSpec::None, reference.to_string())
    }

    /// Parse a table reference like "Table1[Column1]" or more complex ones
    /// like "Table1[[#All],[Column1]:[Column2]]".
    ///
    /// The specifier syntax is parsed by a real recursive-descent parser
    /// (`structured_ref::SpecifierParser`) following MS-XLSX §18.17.6.2.
    fn parse_table_reference(reference: &str) -> Result<Self, ParsingError> {
        let bracket_pos = reference.find('[').ok_or_else(|| {
            ParsingError::InvalidReference(format!("Missing '[' in table reference: {reference}"))
        })?;
        let table_name = reference[..bracket_pos].trim();
        if table_name.is_empty() {
            return Err(ParsingError::InvalidReference(reference.to_string()));
        }

        let specifier_str = &reference[bracket_pos..];
        let specifier = structured_ref::parse_full_specifier(specifier_str)?;

        Ok(ReferenceType::Table(TableReference {
            name: table_name.to_string(),
            specifier,
        }))
    }

    /// Handle the `[...]` shorthand that appears without a table name. The
    /// resolver/evaluator binds the implicit table from cell context.
    ///
    /// `[TableName]` is the data-body shorthand and is materialised as
    /// `Table { name = "TableName", specifier = #Data }`; everything else
    /// produces an unnamed `Table` carrying the parsed specifier verbatim.
    fn parse_bracketed_structured_reference(reference: &str) -> Result<Self, ParsingError> {
        debug_assert!(reference.starts_with('[') && reference.ends_with(']'));
        let specifier = structured_ref::parse_full_specifier(reference)?;

        match specifier {
            Some(TableSpecifier::Column(name)) => Ok(ReferenceType::Table(TableReference {
                name,
                specifier: Some(TableSpecifier::SpecialItem(SpecialItem::Data)),
            })),
            other => Ok(ReferenceType::Table(TableReference {
                name: String::new(),
                specifier: other,
            })),
        }
    }

    fn parse_openformula_reference(reference: &str) -> Result<Self, ParsingError> {
        if reference.starts_with('[') && reference.ends_with(']') {
            let inner = &reference[1..reference.len() - 1];
            if inner.is_empty() {
                return Err(ParsingError::InvalidReference(
                    "Empty OpenFormula reference".to_string(),
                ));
            }

            let mut parts = inner.splitn(2, ':');
            let start_part_str = parts.next().unwrap();
            let end_part_str = parts.next();

            let start_part = Self::parse_openformula_part(start_part_str)?;
            let end_part = if let Some(part) = end_part_str {
                Some(Self::parse_openformula_part(part)?)
            } else {
                None
            };

            let sheet = match (&start_part.sheet, &end_part) {
                (Some(sheet), Some(end)) => {
                    if let Some(end_sheet) = &end.sheet {
                        if end_sheet != sheet {
                            return Err(ParsingError::InvalidReference(format!(
                                "Mismatched sheets in reference: {sheet} vs {end_sheet}"
                            )));
                        }
                    }
                    Some(sheet.clone())
                }
                (Some(sheet), None) => Some(sheet.clone()),
                (None, Some(end)) => end.sheet.clone(),
                (None, None) => None,
            };

            let mut excel_like = String::new();
            if let Some(sheet_name) = sheet {
                if sheet_name_needs_quoting(&sheet_name) {
                    let escaped = sheet_name.replace('\'', "''");
                    excel_like.push('\'');
                    excel_like.push_str(&escaped);
                    excel_like.push('\'');
                } else {
                    excel_like.push_str(&sheet_name);
                }
                excel_like.push('!');
            }

            excel_like.push_str(&start_part.coord);
            if let Some(end) = end_part {
                excel_like.push(':');
                excel_like.push_str(&end.coord);
            }

            return Self::parse_excel_reference(&excel_like);
        }

        Err(ParsingError::InvalidReference(format!(
            "Unsupported OpenFormula reference: {reference}"
        )))
    }

    fn parse_openformula_part(part: &str) -> Result<OpenFormulaRefPart, ParsingError> {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            return Err(ParsingError::InvalidReference(
                "Empty component in OpenFormula reference".to_string(),
            ));
        }

        if trimmed == "." {
            return Err(ParsingError::InvalidReference(
                "Incomplete OpenFormula reference component".to_string(),
            ));
        }

        if trimmed.starts_with('[') {
            // Nested brackets are not expected here
            return Err(ParsingError::InvalidReference(format!(
                "Unexpected '[' in OpenFormula reference component: {trimmed}"
            )));
        }

        let (sheet, coord_slice) = if let Some(stripped) = trimmed.strip_prefix('.') {
            (None, stripped.trim())
        } else if let Some(dot_idx) = Self::find_openformula_sheet_separator(trimmed) {
            let sheet_part = trimmed[..dot_idx].trim();
            let coord_part = trimmed[dot_idx + 1..].trim();
            if coord_part.is_empty() {
                return Err(ParsingError::InvalidReference(format!(
                    "Missing coordinate in OpenFormula reference component: {trimmed}"
                )));
            }
            let sheet_name = Self::normalise_openformula_sheet(sheet_part)?;
            (Some(sheet_name), coord_part)
        } else {
            (None, trimmed)
        };

        let coord = coord_slice.trim_start_matches('.').trim().to_string();

        if coord.is_empty() {
            return Err(ParsingError::InvalidReference(format!(
                "Missing coordinate in OpenFormula reference component: {trimmed}"
            )));
        }

        Ok(OpenFormulaRefPart { sheet, coord })
    }

    fn normalise_openformula_sheet(sheet: &str) -> Result<String, ParsingError> {
        let without_abs = sheet.trim().trim_start_matches('$');

        if without_abs.starts_with('\'') {
            if without_abs.len() < 2 || !without_abs.ends_with('\'') {
                return Err(ParsingError::InvalidReference(format!(
                    "Unterminated sheet name in OpenFormula reference: {sheet}"
                )));
            }
            let inner = &without_abs[1..without_abs.len() - 1];
            Ok(inner.replace("''", "'"))
        } else {
            Ok(without_abs.to_string())
        }
    }

    fn find_openformula_sheet_separator(part: &str) -> Option<usize> {
        let bytes = part.as_bytes();
        let mut i = 0;
        let mut in_quotes = false;

        while i < bytes.len() {
            match bytes[i] {
                b'\'' => {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    in_quotes = !in_quotes;
                    i += 1;
                }
                b'.' if !in_quotes => return Some(i),
                _ => i += 1,
            }
        }

        None
    }

    // The structured-reference grammar lives in the `structured_ref`
    // submodule below; legacy `parse_special_item` /
    // `parse_complex_table_specifier` helpers were removed when the real
    // recursive-descent parser landed for issue #73.

    /// Get the Excel-style string representation of this reference
    pub fn to_excel_string(&self) -> String {
        match self {
            ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => {
                let col_str = Self::format_col(*col, *col_abs);
                let row_str = Self::format_row(*row, *row_abs);
                if let Some(s) = sheet {
                    if sheet_name_needs_quoting(s) {
                        let escaped_name = s.replace('\'', "''");
                        format!("'{}'!{}{}", escaped_name, col_str, row_str)
                    } else {
                        format!("{}!{}{}", s, col_str, row_str)
                    }
                } else {
                    format!("{}{}", col_str, row_str)
                }
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                // Format start reference
                let start_ref = match (start_col, start_row) {
                    (Some(col), Some(row)) => format!(
                        "{}{}",
                        Self::format_col(*col, *start_col_abs),
                        Self::format_row(*row, *start_row_abs)
                    ),
                    (Some(col), None) => Self::format_col(*col, *start_col_abs),
                    (None, Some(row)) => Self::format_row(*row, *start_row_abs),
                    (None, None) => "".to_string(), // Should not happen in normal usage
                };

                // Format end reference
                let end_ref = match (end_col, end_row) {
                    (Some(col), Some(row)) => format!(
                        "{}{}",
                        Self::format_col(*col, *end_col_abs),
                        Self::format_row(*row, *end_row_abs)
                    ),
                    (Some(col), None) => Self::format_col(*col, *end_col_abs),
                    (None, Some(row)) => Self::format_row(*row, *end_row_abs),
                    (None, None) => "".to_string(), // Should not happen in normal usage
                };

                let range_part = format!("{start_ref}:{end_ref}");

                if let Some(s) = sheet {
                    if sheet_name_needs_quoting(s) {
                        let escaped_name = s.replace('\'', "''");
                        format!("'{escaped_name}'!{range_part}")
                    } else {
                        format!("{s}!{range_part}")
                    }
                } else {
                    range_part
                }
            }
            ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => format!("{self}"),
            ReferenceType::External(ext) => ext.raw.clone(),
            ReferenceType::Table(table_ref) => {
                if let Some(specifier) = &table_ref.specifier {
                    format!("{}[{}]", table_ref.name, specifier)
                } else {
                    table_ref.name.clone()
                }
            }
            ReferenceType::NamedRange(name) => name.clone(),
        }
    }
}

/// The different types of AST nodes.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ASTNodeType {
    Literal(LiteralValue),
    Reference {
        original: String, // Original reference string (preserved for display/debugging)
        reference: ReferenceType, // Parsed reference
    },
    UnaryOp {
        op: String,
        expr: Box<ASTNode>,
    },
    BinaryOp {
        op: String,
        left: Box<ASTNode>,
        right: Box<ASTNode>,
    },
    Function {
        name: String,
        args: Vec<ASTNode>, // Most functions have <= 4 args
    },
    /// Generic call where the callee is itself an expression that produces
    /// a callable value (e.g. LAMBDA immediate-invocation `LAMBDA(x, x+1)(5)`).
    Call {
        callee: Box<ASTNode>,
        args: Vec<ASTNode>,
    },
    Array(Vec<Vec<ASTNode>>), // Most arrays are small
}

impl Display for ASTNodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ASTNodeType::Literal(value) => write!(f, "Literal({value})"),
            ASTNodeType::Reference { reference, .. } => write!(f, "Reference({reference:?})"),
            ASTNodeType::UnaryOp { op, expr } => write!(f, "UnaryOp({op}, {expr})"),
            ASTNodeType::BinaryOp { op, left, right } => {
                write!(f, "BinaryOp({op}, {left}, {right})")
            }
            ASTNodeType::Function { name, args } => write!(f, "Function({name}, {args:?})"),
            ASTNodeType::Call { callee, args } => write!(f, "Call({callee}, {args:?})"),
            ASTNodeType::Array(rows) => write!(f, "Array({rows:?})"),
        }
    }
}

/// An AST node represents a parsed formula element
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct ASTNode {
    pub node_type: ASTNodeType,
    pub source_token: Option<Token>,
    /// True if this AST contains any volatile function calls.
    ///
    /// This is set by the parser when a volatility classifier is provided.
    /// For ASTs constructed manually (e.g., in tests), this defaults to false.
    pub contains_volatile: bool,
}

impl ASTNode {
    pub fn new(node_type: ASTNodeType, source_token: Option<Token>) -> Self {
        ASTNode {
            node_type,
            source_token,
            contains_volatile: false,
        }
    }

    /// Create an ASTNode while explicitly setting contains_volatile.
    pub fn new_with_volatile(
        node_type: ASTNodeType,
        source_token: Option<Token>,
        contains_volatile: bool,
    ) -> Self {
        ASTNode {
            node_type,
            source_token,
            contains_volatile,
        }
    }

    /// Whether this AST contains any volatile functions.
    pub fn contains_volatile(&self) -> bool {
        self.contains_volatile
    }

    pub fn fingerprint(&self) -> u64 {
        self.calculate_hash()
    }

    /// Calculate a hash for this ASTNode
    pub fn calculate_hash(&self) -> u64 {
        let mut hasher = FormulaHasher::new();
        self.hash_node(&mut hasher);
        hasher.finish()
    }

    fn hash_node(&self, hasher: &mut FormulaHasher) {
        match &self.node_type {
            ASTNodeType::Literal(value) => {
                hasher.write(&[1]); // Discriminant for Literal
                value.hash(hasher);
            }
            ASTNodeType::Reference { reference, .. } => {
                hasher.write(&[2]); // Discriminant for Reference
                reference.hash(hasher);
            }
            ASTNodeType::UnaryOp { op, expr } => {
                hasher.write(&[3]); // Discriminant for UnaryOp
                hasher.write(op.as_bytes());
                expr.hash_node(hasher);
            }
            ASTNodeType::BinaryOp { op, left, right } => {
                hasher.write(&[4]); // Discriminant for BinaryOp
                hasher.write(op.as_bytes());
                left.hash_node(hasher);
                right.hash_node(hasher);
            }
            ASTNodeType::Function { name, args } => {
                hasher.write(&[5]); // Discriminant for Function
                // Use lowercase function name to be case-insensitive
                let name_lower = name.to_lowercase();
                hasher.write(name_lower.as_bytes());
                hasher.write_usize(args.len());
                for arg in args {
                    arg.hash_node(hasher);
                }
            }
            ASTNodeType::Call { callee, args } => {
                hasher.write(&[7]); // Discriminant for Call
                callee.hash_node(hasher);
                hasher.write_usize(args.len());
                for arg in args {
                    arg.hash_node(hasher);
                }
            }
            ASTNodeType::Array(rows) => {
                hasher.write(&[6]); // Discriminant for Array
                hasher.write_usize(rows.len());
                for row in rows {
                    hasher.write_usize(row.len());
                    for item in row {
                        item.hash_node(hasher);
                    }
                }
            }
        }
    }

    pub fn get_dependencies(&self) -> Vec<&ReferenceType> {
        let mut dependencies = Vec::new();
        self.collect_dependencies(&mut dependencies);
        dependencies
    }

    pub fn get_dependency_strings(&self) -> Vec<String> {
        self.get_dependencies()
            .into_iter()
            .map(|dep| format!("{dep}"))
            .collect()
    }

    fn collect_dependencies<'a>(&'a self, dependencies: &mut Vec<&'a ReferenceType>) {
        match &self.node_type {
            ASTNodeType::Reference { reference, .. } => {
                dependencies.push(reference);
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                expr.collect_dependencies(dependencies);
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                left.collect_dependencies(dependencies);
                right.collect_dependencies(dependencies);
            }
            ASTNodeType::Function { args, .. } => {
                for arg in args {
                    arg.collect_dependencies(dependencies);
                }
            }
            ASTNodeType::Call { callee, args } => {
                callee.collect_dependencies(dependencies);
                for arg in args {
                    arg.collect_dependencies(dependencies);
                }
            }
            ASTNodeType::Array(rows) => {
                for row in rows {
                    for item in row {
                        item.collect_dependencies(dependencies);
                    }
                }
            }
            _ => {}
        }
    }

    /// Lightweight borrowed view of a reference encountered during AST traversal.
    /// This mirrors ReferenceType variants but borrows sheet/name strings to avoid allocation.
    pub fn refs(&self) -> RefIter<'_> {
        RefIter {
            stack: smallvec::smallvec![self],
        }
    }

    /// Visit all references in this AST without allocating intermediates.
    pub fn visit_refs<V: FnMut(RefView<'_>)>(&self, mut visitor: V) {
        let mut stack: Vec<&ASTNode> = Vec::with_capacity(8);
        stack.push(self);
        while let Some(node) = stack.pop() {
            match &node.node_type {
                ASTNodeType::Reference { reference, .. } => visitor(RefView::from(reference)),
                ASTNodeType::UnaryOp { expr, .. } => stack.push(expr),
                ASTNodeType::BinaryOp { left, right, .. } => {
                    // Push right first so left is visited first (stable-ish order)
                    stack.push(right);
                    stack.push(left);
                }
                ASTNodeType::Function { args, .. } => {
                    for a in args.iter().rev() {
                        stack.push(a);
                    }
                }
                ASTNodeType::Call { callee, args } => {
                    for a in args.iter().rev() {
                        stack.push(a);
                    }
                    stack.push(callee);
                }
                ASTNodeType::Array(rows) => {
                    for r in rows.iter().rev() {
                        for item in r.iter().rev() {
                            stack.push(item);
                        }
                    }
                }
                ASTNodeType::Literal(_) => {}
            }
        }
    }

    /// Convenience: collect references into a small, inline vector based on a policy.
    pub fn collect_references(&self, policy: &CollectPolicy) -> SmallVec<[ReferenceType; 4]> {
        let mut out: SmallVec<[ReferenceType; 4]> = SmallVec::new();
        self.visit_refs(|rv| match rv {
            RefView::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => out.push(ReferenceType::Cell {
                sheet: sheet.map(|s| s.to_string()),
                row,
                col,
                row_abs,
                col_abs,
            }),
            RefView::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                // Optionally expand very small finite ranges into individual cells
                if policy.expand_small_ranges {
                    if let (Some(sr), Some(sc), Some(er), Some(ec)) =
                        (start_row, start_col, end_row, end_col)
                    {
                        let rows = er.saturating_sub(sr) + 1;
                        let cols = ec.saturating_sub(sc) + 1;
                        let area = rows.saturating_mul(cols);
                        if area as usize <= policy.range_expansion_limit {
                            let row_abs = start_row_abs && end_row_abs;
                            let col_abs = start_col_abs && end_col_abs;
                            for r in sr..=er {
                                for c in sc..=ec {
                                    out.push(ReferenceType::Cell {
                                        sheet: sheet.map(|s| s.to_string()),
                                        row: r,
                                        col: c,
                                        row_abs,
                                        col_abs,
                                    });
                                }
                            }
                            return; // handled
                        }
                    }
                }
                out.push(ReferenceType::Range {
                    sheet: sheet.map(|s| s.to_string()),
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                });
            }
            RefView::Cell3D {
                sheet_first,
                sheet_last,
                row,
                col,
                row_abs,
                col_abs,
            } => out.push(ReferenceType::Cell3D {
                sheet_first: sheet_first.to_string(),
                sheet_last: sheet_last.to_string(),
                row,
                col,
                row_abs,
                col_abs,
            }),
            RefView::Range3D {
                sheet_first,
                sheet_last,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => out.push(ReferenceType::Range3D {
                sheet_first: sheet_first.to_string(),
                sheet_last: sheet_last.to_string(),
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            }),
            RefView::External {
                raw,
                book,
                sheet,
                kind,
            } => out.push(ReferenceType::External(ExternalReference {
                raw: raw.to_string(),
                book: ExternalBookRef::Token(book.to_string()),
                sheet: sheet.to_string(),
                kind,
            })),
            RefView::Table { name, specifier } => out.push(ReferenceType::Table(TableReference {
                name: name.to_string(),
                specifier: specifier.cloned(),
            })),
            RefView::NamedRange { name } => {
                if policy.include_names {
                    out.push(ReferenceType::NamedRange(name.to_string()));
                }
            }
        });
        out
    }
    /// Recursively updates sheet references within the AST.
    ///
    /// If `target_name` is provided, only references matching that sheet name are updated.
    /// This is used for "healing" specific broken references (Tombstone rescue).
    /// If `target_name` is None, it acts as a global rename (standard sheet rename).
    pub fn update_sheet_references(&mut self, target_name: Option<&str>, new_name: &str) {
        match &mut self.node_type {
            ASTNodeType::Reference {
                reference: ReferenceType::Cell { sheet, .. } | ReferenceType::Range { sheet, .. },
                ..
            } => {
                if let Some(current_sheet) = sheet
                    && (target_name.is_none() || target_name == Some(current_sheet.as_str()))
                {
                    *sheet = Some(new_name.to_string());
                }
            }
            ASTNodeType::Reference {
                reference:
                    ReferenceType::Cell3D {
                        sheet_first,
                        sheet_last,
                        ..
                    }
                    | ReferenceType::Range3D {
                        sheet_first,
                        sheet_last,
                        ..
                    },
                ..
            } => {
                if target_name.is_none() || target_name == Some(sheet_first.as_str()) {
                    *sheet_first = new_name.to_string();
                }
                if target_name.is_none() || target_name == Some(sheet_last.as_str()) {
                    *sheet_last = new_name.to_string();
                }
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                expr.update_sheet_references(target_name, new_name);
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                left.update_sheet_references(target_name, new_name);
                right.update_sheet_references(target_name, new_name);
            }
            ASTNodeType::Function { args, .. } => {
                for arg in args {
                    arg.update_sheet_references(target_name, new_name);
                }
            }
            ASTNodeType::Call { callee, args } => {
                callee.update_sheet_references(target_name, new_name);
                for arg in args {
                    arg.update_sheet_references(target_name, new_name);
                }
            }
            ASTNodeType::Array(rows) => {
                for row in rows {
                    for cell in row {
                        cell.update_sheet_references(target_name, new_name);
                    }
                }
            }
            _ => {}
        }
    }
}

/// A borrowing view over a ReferenceType. Avoids cloning sheet/names while walking.
#[derive(Clone, Copy, Debug)]
pub enum RefView<'a> {
    Cell {
        sheet: Option<&'a str>,
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    },
    Range {
        sheet: Option<&'a str>,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    },
    /// 3D cell view (`Sheet1:Sheet3!A1`).
    Cell3D {
        sheet_first: &'a str,
        sheet_last: &'a str,
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    },
    /// 3D range view (`Sheet1:Sheet3!A1:B2`).
    Range3D {
        sheet_first: &'a str,
        sheet_last: &'a str,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    },
    External {
        raw: &'a str,
        book: &'a str,
        sheet: &'a str,
        kind: ExternalRefKind,
    },
    Table {
        name: &'a str,
        specifier: Option<&'a TableSpecifier>,
    },
    NamedRange {
        name: &'a str,
    },
}

impl<'a> From<&'a ReferenceType> for RefView<'a> {
    fn from(r: &'a ReferenceType) -> Self {
        match r {
            ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            } => RefView::Cell {
                sheet: sheet.as_deref(),
                row: *row,
                col: *col,
                row_abs: *row_abs,
                col_abs: *col_abs,
            },
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => RefView::Range {
                sheet: sheet.as_deref(),
                start_row: *start_row,
                start_col: *start_col,
                end_row: *end_row,
                end_col: *end_col,
                start_row_abs: *start_row_abs,
                start_col_abs: *start_col_abs,
                end_row_abs: *end_row_abs,
                end_col_abs: *end_col_abs,
            },
            ReferenceType::Cell3D {
                sheet_first,
                sheet_last,
                row,
                col,
                row_abs,
                col_abs,
            } => RefView::Cell3D {
                sheet_first: sheet_first.as_str(),
                sheet_last: sheet_last.as_str(),
                row: *row,
                col: *col,
                row_abs: *row_abs,
                col_abs: *col_abs,
            },
            ReferenceType::Range3D {
                sheet_first,
                sheet_last,
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => RefView::Range3D {
                sheet_first: sheet_first.as_str(),
                sheet_last: sheet_last.as_str(),
                start_row: *start_row,
                start_col: *start_col,
                end_row: *end_row,
                end_col: *end_col,
                start_row_abs: *start_row_abs,
                start_col_abs: *start_col_abs,
                end_row_abs: *end_row_abs,
                end_col_abs: *end_col_abs,
            },
            ReferenceType::External(ext) => RefView::External {
                raw: ext.raw.as_str(),
                book: ext.book.token(),
                sheet: ext.sheet.as_str(),
                kind: ext.kind,
            },
            ReferenceType::Table(tr) => RefView::Table {
                name: tr.name.as_str(),
                specifier: tr.specifier.as_ref(),
            },
            ReferenceType::NamedRange(name) => RefView::NamedRange { name },
        }
    }
}

/// Iterator over RefView for an AST, implemented via an explicit stack to avoid recursion allocation.
pub struct RefIter<'a> {
    stack: smallvec::SmallVec<[&'a ASTNode; 8]>,
}

impl<'a> Iterator for RefIter<'a> {
    type Item = RefView<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.stack.pop() {
            match &node.node_type {
                ASTNodeType::Reference { reference, .. } => return Some(RefView::from(reference)),
                ASTNodeType::UnaryOp { expr, .. } => self.stack.push(expr),
                ASTNodeType::BinaryOp { left, right, .. } => {
                    self.stack.push(right);
                    self.stack.push(left);
                }
                ASTNodeType::Function { args, .. } => {
                    for a in args.iter().rev() {
                        self.stack.push(a);
                    }
                }
                ASTNodeType::Call { callee, args } => {
                    for a in args.iter().rev() {
                        self.stack.push(a);
                    }
                    self.stack.push(callee);
                }
                ASTNodeType::Array(rows) => {
                    for r in rows.iter().rev() {
                        for item in r.iter().rev() {
                            self.stack.push(item);
                        }
                    }
                }
                ASTNodeType::Literal(_) => {}
            }
        }
        None
    }
}

/// Policy controlling how references are collected.
#[derive(Debug, Clone)]
pub struct CollectPolicy {
    pub expand_small_ranges: bool,
    pub range_expansion_limit: usize,
    pub include_names: bool,
}

impl Default for CollectPolicy {
    fn default() -> Self {
        Self {
            expand_small_ranges: false,
            range_expansion_limit: 0,
            include_names: true,
        }
    }
}

impl Display for ASTNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.node_type)
    }
}

impl std::hash::Hash for ASTNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let hash = self.calculate_hash();
        state.write_u64(hash);
    }
}

/// A parser for converting tokens into an AST.
pub struct Parser {
    tokens: Arc<[Token]>,
    position: usize,
    /// Optional classifier to determine whether a function name is volatile.
    volatility_classifier: Option<VolatilityClassifierBox>,
    dialect: FormulaDialect,
    /// When > 0, treat a top-level `OpInfix(",")` as a terminator (call-arg
    /// separator) instead of the union/list operator. Used by `parse_call_arguments`.
    in_call_args_depth: usize,
}

impl TryFrom<&str> for Parser {
    type Error = TokenizerError;

    fn try_from(formula: &str) -> Result<Self, Self::Error> {
        let tokens = Tokenizer::new(formula)?.items;
        Ok(Self::new(tokens, false))
    }
}

impl TryFrom<String> for Parser {
    type Error = TokenizerError;

    fn try_from(formula: String) -> Result<Self, Self::Error> {
        Self::try_from(formula.as_str())
    }
}

impl Parser {
    pub fn new(tokens: Vec<Token>, include_whitespace: bool) -> Self {
        Self::new_with_dialect(tokens, include_whitespace, FormulaDialect::Excel)
    }

    pub fn new_with_dialect(
        mut tokens: Vec<Token>,
        include_whitespace: bool,
        dialect: FormulaDialect,
    ) -> Self {
        if !include_whitespace {
            tokens.retain(|t| t.token_type != TokenType::Whitespace);
        }

        Parser {
            tokens: Arc::from(tokens.into_boxed_slice()),
            position: 0,
            volatility_classifier: None,
            dialect,
            in_call_args_depth: 0,
        }
    }

    pub fn try_from_formula(formula: &str) -> Result<Self, TokenizerError> {
        let tokens = Tokenizer::new(formula)?.items;
        Ok(Self::new(tokens, false))
    }

    /// Provide a function-volatility classifier for this parser.
    /// If set, the parser will annotate ASTs with a contains_volatile bit.
    pub fn with_volatility_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.volatility_classifier = Some(Box::new(f));
        self
    }

    /// Convenience constructor to set a classifier alongside other options.
    pub fn new_with_classifier<F>(tokens: Vec<Token>, include_whitespace: bool, f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Self::new(tokens, include_whitespace).with_volatility_classifier(f)
    }

    pub fn new_with_classifier_and_dialect<F>(
        tokens: Vec<Token>,
        include_whitespace: bool,
        dialect: FormulaDialect,
        f: F,
    ) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Self::new_with_dialect(tokens, include_whitespace, dialect).with_volatility_classifier(f)
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Whitespace
        {
            self.position += 1;
        }
    }

    /// Parse the tokens into an AST.
    pub fn parse(&mut self) -> Result<ASTNode, ParserError> {
        if self.tokens.is_empty() {
            return Err(ParserError {
                message: "No tokens to parse".to_string(),
                position: None,
            });
        }

        self.skip_whitespace();
        if self.position >= self.tokens.len() {
            return Err(ParserError {
                message: "No tokens to parse".to_string(),
                position: None,
            });
        }

        // Check for literal formula (doesn't start with '=')
        if self.tokens[self.position].token_type == TokenType::Literal {
            let token = self.tokens[self.position].clone();
            self.position += 1;
            self.skip_whitespace();
            if self.position < self.tokens.len() {
                return Err(ParserError {
                    message: format!(
                        "Unexpected token at position {}: {:?}",
                        self.position, self.tokens[self.position]
                    ),
                    position: Some(self.position),
                });
            }
            return Ok(ASTNode::new(
                ASTNodeType::Literal(LiteralValue::Text(token.value.clone())),
                Some(token),
            ));
        }

        let ast = self.parse_expression()?;
        self.skip_whitespace();
        if self.position < self.tokens.len() {
            return Err(ParserError {
                message: format!(
                    "Unexpected token at position {}: {:?}",
                    self.position, self.tokens[self.position]
                ),
                position: Some(self.position),
            });
        }
        Ok(ast)
    }

    fn parse_expression(&mut self) -> Result<ASTNode, ParserError> {
        self.parse_bp(0)
    }

    // Pratt-style precedence parser. `min_precedence` is the minimum binding power
    // an operator must have to be consumed at this level.
    fn parse_bp(&mut self, min_precedence: u8) -> Result<ASTNode, ParserError> {
        let mut left = self.parse_prefix()?;

        loop {
            self.skip_whitespace();
            if self.position >= self.tokens.len() {
                break;
            }

            // Postfix call: a `(` directly following a closed expression denotes
            // immediate invocation of a callable result (e.g. LAMBDA IIFE).
            if self.tokens[self.position].token_type == TokenType::Paren
                && self.tokens[self.position].subtype == TokenSubType::Open
            {
                self.position += 1;
                let args = self.parse_call_arguments()?;
                let call_volatile =
                    left.contains_volatile || args.iter().any(|a| a.contains_volatile);
                left = ASTNode::new_with_volatile(
                    ASTNodeType::Call {
                        callee: Box::new(left),
                        args,
                    },
                    None,
                    call_volatile,
                );
                continue;
            }

            // Postfix operators (e.g. percent).
            if self.tokens[self.position].token_type == TokenType::OpPostfix {
                let (precedence, _) = self.tokens[self.position]
                    .get_precedence()
                    .unwrap_or((0, Associativity::Left));
                if precedence < min_precedence {
                    break;
                }

                let op_token = self.tokens[self.position].clone();
                self.position += 1;
                let contains_volatile = left.contains_volatile;
                left = ASTNode::new_with_volatile(
                    ASTNodeType::UnaryOp {
                        op: op_token.value.clone(),
                        expr: Box::new(left),
                    },
                    Some(op_token),
                    contains_volatile,
                );
                continue;
            }

            let token = &self.tokens[self.position];
            if token.token_type != TokenType::OpInfix {
                break;
            }

            // Inside a postfix call's argument list, treat top-level `,` as
            // an argument separator, not as the union operator.
            if self.in_call_args_depth > 0 && token.value == "," {
                break;
            }

            let (precedence, associativity) =
                token.get_precedence().unwrap_or((0, Associativity::Left));
            if precedence < min_precedence {
                break;
            }

            let op_token = self.tokens[self.position].clone();
            self.position += 1;

            let next_min_precedence = if associativity == Associativity::Left {
                precedence + 1
            } else {
                precedence
            };

            let right = self.parse_bp(next_min_precedence)?;
            let contains_volatile = left.contains_volatile || right.contains_volatile;
            left = ASTNode::new_with_volatile(
                ASTNodeType::BinaryOp {
                    op: op_token.value.clone(),
                    left: Box::new(left),
                    right: Box::new(right),
                },
                Some(op_token),
                contains_volatile,
            );
        }

        Ok(left)
    }

    fn parse_prefix(&mut self) -> Result<ASTNode, ParserError> {
        self.skip_whitespace();
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::OpPrefix
        {
            let op_token = self.tokens[self.position].clone();
            self.position += 1;

            // Prefix unary binds tighter than exponent (Excel semantics),
            // so parse the RHS with min_precedence equal to unary's precedence.
            let (precedence, _) = op_token
                .get_precedence()
                .unwrap_or((0, Associativity::Right));

            let expr = self.parse_bp(precedence)?;
            let contains_volatile = expr.contains_volatile;
            return Ok(ASTNode::new_with_volatile(
                ASTNodeType::UnaryOp {
                    op: op_token.value.clone(),
                    expr: Box::new(expr),
                },
                Some(op_token),
                contains_volatile,
            ));
        }

        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<ASTNode, ParserError> {
        self.skip_whitespace();
        if self.position >= self.tokens.len() {
            return Err(ParserError {
                message: "Unexpected end of tokens".to_string(),
                position: Some(self.position),
            });
        }

        let token = &self.tokens[self.position];
        match token.token_type {
            TokenType::Operand => {
                let operand_token = self.tokens[self.position].clone();
                self.position += 1;
                self.parse_operand(operand_token)
            }
            TokenType::Func => {
                let func_token = self.tokens[self.position].clone();
                self.position += 1;
                self.parse_function(func_token)
            }
            TokenType::Paren if token.subtype == TokenSubType::Open => {
                self.position += 1;
                let expr = self.parse_expression()?;
                if self.position >= self.tokens.len()
                    || self.tokens[self.position].token_type != TokenType::Paren
                    || self.tokens[self.position].subtype != TokenSubType::Close
                {
                    return Err(ParserError {
                        message: "Expected closing parenthesis".to_string(),
                        position: Some(self.position),
                    });
                }
                self.position += 1;
                Ok(expr)
            }
            TokenType::Array if token.subtype == TokenSubType::Open => {
                self.position += 1;
                self.parse_array()
            }
            _ => Err(ParserError {
                message: format!("Unexpected token: {token:?}"),
                position: Some(self.position),
            }),
        }
    }

    fn parse_operand(&mut self, token: Token) -> Result<ASTNode, ParserError> {
        match token.subtype {
            TokenSubType::Number => {
                let value = token.value.parse::<f64>().map_err(|_| ParserError {
                    message: format!("Invalid number: {}", token.value),
                    position: Some(self.position),
                })?;
                Ok(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Number(value)),
                    Some(token),
                ))
            }
            TokenSubType::Text => {
                // Strip surrounding quotes from text literals
                let mut text = token.value.clone();
                if text.starts_with('"') && text.ends_with('"') && text.len() >= 2 {
                    text = text[1..text.len() - 1].to_string();
                    // Handle escaped quotes
                    text = text.replace("\"\"", "\"");
                }
                Ok(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Text(text)),
                    Some(token),
                ))
            }
            TokenSubType::Logical => {
                let value = token.value.eq_ignore_ascii_case("TRUE");
                Ok(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Boolean(value)),
                    Some(token),
                ))
            }
            TokenSubType::Error => {
                let error = ExcelError::from_error_string(&token.value);
                Ok(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Error(error)),
                    Some(token),
                ))
            }
            TokenSubType::Range => {
                let reference = ReferenceType::from_string_with_dialect(&token.value, self.dialect)
                    .map_err(|e| ParserError {
                        message: format!("Invalid reference '{}': {}", token.value, e),
                        position: Some(self.position),
                    })?;
                Ok(ASTNode::new(
                    ASTNodeType::Reference {
                        original: token.value.clone(),
                        reference,
                    },
                    Some(token),
                ))
            }
            _ => Err(ParserError {
                message: format!("Unexpected operand subtype: {:?}", token.subtype),
                position: Some(self.position),
            }),
        }
    }

    fn parse_function(&mut self, func_token: Token) -> Result<ASTNode, ParserError> {
        let name = func_token.value[..func_token.value.len() - 1].to_string();
        let args = self.parse_function_arguments()?;
        // Determine volatility for this function
        let this_is_volatile = self
            .volatility_classifier
            .as_ref()
            .map(|f| f(name.as_str()))
            .unwrap_or(false);
        let args_volatile = args.iter().any(|a| a.contains_volatile);

        Ok(ASTNode::new_with_volatile(
            ASTNodeType::Function { name, args },
            Some(func_token),
            this_is_volatile || args_volatile,
        ))
    }

    /// Parse arguments for a postfix call (immediate invocation), where the
    /// opening `(` is a `Paren:Open` and the matching `)` is a `Paren:Close`.
    /// Caller has already consumed the opening paren.
    ///
    /// Inside this region the tokenizer emits a top-level `,` as `OpInfix`
    /// (Excel's union operator). For call arguments we want it to behave as a
    /// separator, so we bump `in_call_args_depth` while parsing each argument.
    fn parse_call_arguments(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut args: Vec<ASTNode> = Vec::new();

        self.skip_whitespace();
        // Empty argument list: `f()`
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Paren
            && self.tokens[self.position].subtype == TokenSubType::Close
        {
            self.position += 1;
            return Ok(args);
        }

        self.in_call_args_depth += 1;
        let result = (|| -> Result<Vec<ASTNode>, ParserError> {
            args.push(self.parse_expression()?);
            loop {
                self.skip_whitespace();
                if self.position >= self.tokens.len() {
                    return Err(ParserError {
                        message: "Unterminated call argument list".to_string(),
                        position: Some(self.position),
                    });
                }
                let token = &self.tokens[self.position];
                let is_separator = (token.token_type == TokenType::Sep
                    && token.subtype == TokenSubType::Arg)
                    || (token.token_type == TokenType::OpInfix && token.value == ",");
                if is_separator {
                    self.position += 1;
                    args.push(self.parse_expression()?);
                } else if token.token_type == TokenType::Paren
                    && token.subtype == TokenSubType::Close
                {
                    self.position += 1;
                    return Ok(std::mem::take(&mut args));
                } else {
                    return Err(ParserError {
                        message: format!("Expected ',' or ')' in call arguments, got {token:?}"),
                        position: Some(self.position),
                    });
                }
            }
        })();
        self.in_call_args_depth -= 1;
        result
    }

    /// Parse function arguments.
    fn parse_function_arguments(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut args = Vec::new();

        // Check for closing parenthesis (empty arguments)
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Func
            && self.tokens[self.position].subtype == TokenSubType::Close
        {
            self.position += 1;
            return Ok(args);
        }

        // Handle optional arguments (consecutive separators)
        // Check if we start with a separator (empty first argument)
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Sep
            && self.tokens[self.position].subtype == TokenSubType::Arg
        {
            // Empty first argument - represented as empty text literal for compatibility
            args.push(ASTNode::new(
                ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                None,
            ));
            self.position += 1;
        } else {
            // Parse first argument
            args.push(self.parse_expression()?);
        }

        // Parse remaining arguments
        while self.position < self.tokens.len() {
            let token = &self.tokens[self.position];

            if token.token_type == TokenType::Sep && token.subtype == TokenSubType::Arg {
                self.position += 1;
                // Check for consecutive separators (empty argument)
                if self.position < self.tokens.len() {
                    let next_token = &self.tokens[self.position];
                    if next_token.token_type == TokenType::Sep
                        && next_token.subtype == TokenSubType::Arg
                    {
                        // Empty argument - represented as empty text literal for compatibility
                        args.push(ASTNode::new(
                            ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                            None,
                        ));
                    } else if next_token.token_type == TokenType::Func
                        && next_token.subtype == TokenSubType::Close
                    {
                        // Empty last argument
                        args.push(ASTNode::new(
                            ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                            None,
                        ));
                        self.position += 1;
                        break;
                    } else {
                        args.push(self.parse_expression()?);
                    }
                } else {
                    // Trailing separator at end of formula
                    args.push(ASTNode::new(
                        ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                        None,
                    ));
                }
            } else if token.token_type == TokenType::Func && token.subtype == TokenSubType::Close {
                self.position += 1;
                break;
            } else {
                return Err(ParserError {
                    message: format!("Expected ',' or ')' in function arguments, got {token:?}"),
                    position: Some(self.position),
                });
            }
        }

        Ok(args)
    }

    fn parse_array(&mut self) -> Result<ASTNode, ParserError> {
        let mut rows = Vec::new();
        let mut current_row = Vec::new();

        // Check for empty array
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Array
            && self.tokens[self.position].subtype == TokenSubType::Close
        {
            self.position += 1;
            return Ok(ASTNode::new(ASTNodeType::Array(rows), None));
        }

        // Parse first element
        current_row.push(self.parse_expression()?);

        while self.position < self.tokens.len() {
            let token = &self.tokens[self.position];

            if token.token_type == TokenType::Sep {
                if token.subtype == TokenSubType::Arg {
                    // Column separator
                    self.position += 1;
                    current_row.push(self.parse_expression()?);
                } else if token.subtype == TokenSubType::Row {
                    // Row separator
                    self.position += 1;
                    rows.push(current_row);
                    current_row = vec![self.parse_expression()?];
                }
            } else if token.token_type == TokenType::Array && token.subtype == TokenSubType::Close {
                self.position += 1;
                rows.push(current_row);
                break;
            } else {
                return Err(ParserError {
                    message: format!("Unexpected token in array: {token:?}"),
                    position: Some(self.position),
                });
            }
        }

        // Array volatility is the OR of element volatility
        let contains_volatile = rows
            .iter()
            .flat_map(|r| r.iter())
            .any(|n| n.contains_volatile);
        Ok(ASTNode::new_with_volatile(
            ASTNodeType::Array(rows),
            None,
            contains_volatile,
        ))
    }
}

impl From<TokenizerError> for ParserError {
    fn from(err: TokenizerError) -> Self {
        ParserError {
            message: err.message,
            position: Some(err.pos),
        }
    }
}

struct SpanParser<'a> {
    source: &'a str,
    tokens: &'a [crate::tokenizer::TokenSpan],
    position: usize,
    volatility_classifier: Option<VolatilityClassifierBox>,
    dialect: FormulaDialect,
    /// See `Parser::in_call_args_depth`.
    in_call_args_depth: usize,
}

impl<'a> SpanParser<'a> {
    fn new(
        source: &'a str,
        tokens: &'a [crate::tokenizer::TokenSpan],
        dialect: FormulaDialect,
    ) -> Self {
        SpanParser {
            source,
            tokens,
            position: 0,
            volatility_classifier: None,
            dialect,
            in_call_args_depth: 0,
        }
    }

    fn with_volatility_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.volatility_classifier = Some(Box::new(f));
        self
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Whitespace
        {
            self.position += 1;
        }
    }

    fn span_value(&self, span: &crate::tokenizer::TokenSpan) -> &str {
        &self.source[span.start..span.end]
    }

    fn span_to_token(&self, span: &crate::tokenizer::TokenSpan) -> Token {
        Token::new_with_span(
            self.span_value(span).to_string(),
            span.token_type,
            span.subtype,
            span.start,
            span.end,
        )
    }

    fn span_precedence(&self, span: &crate::tokenizer::TokenSpan) -> Option<(u8, Associativity)> {
        if !matches!(
            span.token_type,
            TokenType::OpPrefix | TokenType::OpInfix | TokenType::OpPostfix
        ) {
            return None;
        }

        let op = if span.token_type == TokenType::OpPrefix {
            "u"
        } else {
            self.span_value(span)
        };

        match op {
            "#" => Some((9, Associativity::Left)),
            ":" | " " | "," => Some((8, Associativity::Left)),
            "%" => Some((7, Associativity::Left)),
            "u" => Some((6, Associativity::Right)),
            "^" => Some((5, Associativity::Right)),
            "*" | "/" => Some((4, Associativity::Left)),
            "+" | "-" => Some((3, Associativity::Left)),
            "&" => Some((2, Associativity::Left)),
            "=" | "<" | ">" | "<=" | ">=" | "<>" => Some((1, Associativity::Left)),
            _ => None,
        }
    }

    fn parse(&mut self) -> Result<ASTNode, ParserError> {
        if self.tokens.is_empty() {
            return Err(ParserError {
                message: "No tokens to parse".to_string(),
                position: None,
            });
        }

        self.skip_whitespace();
        if self.position >= self.tokens.len() {
            return Err(ParserError {
                message: "No tokens to parse".to_string(),
                position: None,
            });
        }

        if self.tokens[self.position].token_type == TokenType::Literal {
            let span = self.tokens[self.position];
            self.position += 1;
            self.skip_whitespace();
            if self.position < self.tokens.len() {
                return Err(ParserError {
                    message: format!(
                        "Unexpected token at position {}: {:?}",
                        self.position, self.tokens[self.position]
                    ),
                    position: Some(self.position),
                });
            }

            let token = self.span_to_token(&span);
            return Ok(ASTNode::new(
                ASTNodeType::Literal(LiteralValue::Text(token.value.clone())),
                Some(token),
            ));
        }

        let ast = self.parse_expression()?;
        self.skip_whitespace();
        if self.position < self.tokens.len() {
            return Err(ParserError {
                message: format!(
                    "Unexpected token at position {}: {:?}",
                    self.position, self.tokens[self.position]
                ),
                position: Some(self.position),
            });
        }
        Ok(ast)
    }

    fn parse_expression(&mut self) -> Result<ASTNode, ParserError> {
        self.parse_bp(0)
    }

    fn parse_bp(&mut self, min_precedence: u8) -> Result<ASTNode, ParserError> {
        let mut left = self.parse_prefix()?;

        loop {
            self.skip_whitespace();
            if self.position >= self.tokens.len() {
                break;
            }

            // Postfix call: a `(` directly following a closed expression denotes
            // immediate invocation of a callable result (e.g. LAMBDA IIFE).
            if self.tokens[self.position].token_type == TokenType::Paren
                && self.tokens[self.position].subtype == TokenSubType::Open
            {
                self.position += 1;
                let args = self.parse_call_arguments()?;
                let call_volatile =
                    left.contains_volatile || args.iter().any(|a| a.contains_volatile);
                left = ASTNode::new_with_volatile(
                    ASTNodeType::Call {
                        callee: Box::new(left),
                        args,
                    },
                    None,
                    call_volatile,
                );
                continue;
            }

            if self.tokens[self.position].token_type == TokenType::OpPostfix {
                let (precedence, _) = self
                    .span_precedence(&self.tokens[self.position])
                    .unwrap_or((0, Associativity::Left));
                if precedence < min_precedence {
                    break;
                }

                let op_span = self.tokens[self.position];
                self.position += 1;
                let op_token = self.span_to_token(&op_span);
                let contains_volatile = left.contains_volatile;
                left = ASTNode::new_with_volatile(
                    ASTNodeType::UnaryOp {
                        op: op_token.value.clone(),
                        expr: Box::new(left),
                    },
                    Some(op_token),
                    contains_volatile,
                );
                continue;
            }

            let token = &self.tokens[self.position];
            if token.token_type != TokenType::OpInfix {
                break;
            }

            // Inside a postfix call's argument list, treat top-level `,` as
            // an argument separator, not as the union operator.
            if self.in_call_args_depth > 0 && self.span_value(token) == "," {
                break;
            }

            let (precedence, associativity) = self
                .span_precedence(token)
                .unwrap_or((0, Associativity::Left));
            if precedence < min_precedence {
                break;
            }

            let op_span = self.tokens[self.position];
            self.position += 1;

            let next_min_precedence = if associativity == Associativity::Left {
                precedence + 1
            } else {
                precedence
            };

            let right = self.parse_bp(next_min_precedence)?;
            let op_token = self.span_to_token(&op_span);
            let contains_volatile = left.contains_volatile || right.contains_volatile;
            left = ASTNode::new_with_volatile(
                ASTNodeType::BinaryOp {
                    op: op_token.value.clone(),
                    left: Box::new(left),
                    right: Box::new(right),
                },
                Some(op_token),
                contains_volatile,
            );
        }

        Ok(left)
    }

    fn parse_prefix(&mut self) -> Result<ASTNode, ParserError> {
        self.skip_whitespace();
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::OpPrefix
        {
            let op_span = self.tokens[self.position];
            self.position += 1;

            let (precedence, _) = self
                .span_precedence(&op_span)
                .unwrap_or((0, Associativity::Right));

            let expr = self.parse_bp(precedence)?;
            let op_token = self.span_to_token(&op_span);
            let contains_volatile = expr.contains_volatile;
            return Ok(ASTNode::new_with_volatile(
                ASTNodeType::UnaryOp {
                    op: op_token.value.clone(),
                    expr: Box::new(expr),
                },
                Some(op_token),
                contains_volatile,
            ));
        }

        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<ASTNode, ParserError> {
        self.skip_whitespace();
        if self.position >= self.tokens.len() {
            return Err(ParserError {
                message: "Unexpected end of tokens".to_string(),
                position: Some(self.position),
            });
        }

        let token = &self.tokens[self.position];
        match token.token_type {
            TokenType::Operand => {
                let span = self.tokens[self.position];
                self.position += 1;
                self.parse_operand(span)
            }
            TokenType::Func => {
                let span = self.tokens[self.position];
                self.position += 1;
                self.parse_function(span)
            }
            TokenType::Paren if token.subtype == TokenSubType::Open => {
                self.position += 1;
                let expr = self.parse_expression()?;
                self.skip_whitespace();
                if self.position >= self.tokens.len()
                    || self.tokens[self.position].token_type != TokenType::Paren
                    || self.tokens[self.position].subtype != TokenSubType::Close
                {
                    return Err(ParserError {
                        message: "Expected closing parenthesis".to_string(),
                        position: Some(self.position),
                    });
                }
                self.position += 1;
                Ok(expr)
            }
            TokenType::Array if token.subtype == TokenSubType::Open => {
                self.position += 1;
                self.parse_array()
            }
            _ => Err(ParserError {
                message: format!("Unexpected token: {token:?}"),
                position: Some(self.position),
            }),
        }
    }

    fn parse_operand(&mut self, span: crate::tokenizer::TokenSpan) -> Result<ASTNode, ParserError> {
        let value = self.span_value(&span);
        let token = self.span_to_token(&span);

        match span.subtype {
            TokenSubType::Number => {
                let value = value.parse::<f64>().map_err(|_| ParserError {
                    message: format!("Invalid number: {value}"),
                    position: Some(self.position),
                })?;
                Ok(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Number(value)),
                    Some(token),
                ))
            }
            TokenSubType::Text => {
                let mut text = value.to_string();
                if text.starts_with('"') && text.ends_with('"') && text.len() >= 2 {
                    text = text[1..text.len() - 1].to_string();
                    text = text.replace("\"\"", "\"");
                }
                Ok(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Text(text)),
                    Some(token),
                ))
            }
            TokenSubType::Logical => {
                let v = value.eq_ignore_ascii_case("TRUE");
                Ok(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Boolean(v)),
                    Some(token),
                ))
            }
            TokenSubType::Error => {
                let error = ExcelError::from_error_string(value);
                Ok(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Error(error)),
                    Some(token),
                ))
            }
            TokenSubType::Range => {
                let reference = ReferenceType::from_string_with_dialect(value, self.dialect)
                    .map_err(|e| ParserError {
                        message: format!("Invalid reference '{value}': {e}"),
                        position: Some(self.position),
                    })?;
                Ok(ASTNode::new(
                    ASTNodeType::Reference {
                        original: value.to_string(),
                        reference,
                    },
                    Some(token),
                ))
            }
            _ => Err(ParserError {
                message: format!("Unexpected operand subtype: {:?}", span.subtype),
                position: Some(self.position),
            }),
        }
    }

    fn parse_function(
        &mut self,
        func_span: crate::tokenizer::TokenSpan,
    ) -> Result<ASTNode, ParserError> {
        let func_value = self.span_value(&func_span);
        if func_value.is_empty() {
            return Err(ParserError {
                message: "Invalid function token".to_string(),
                position: Some(self.position),
            });
        }
        let name = func_value[..func_value.len() - 1].to_string();
        let args = self.parse_function_arguments()?;

        let this_is_volatile = self
            .volatility_classifier
            .as_ref()
            .map(|f| f(name.as_str()))
            .unwrap_or(false);
        let args_volatile = args.iter().any(|a| a.contains_volatile);

        let func_token = self.span_to_token(&func_span);
        Ok(ASTNode::new_with_volatile(
            ASTNodeType::Function { name, args },
            Some(func_token),
            this_is_volatile || args_volatile,
        ))
    }

    /// Parse arguments for a postfix call (immediate invocation), where the
    /// opening `(` is a `Paren:Open` and the matching `)` is a `Paren:Close`.
    /// Caller has already consumed the opening paren. See the classic parser
    /// version for details on how top-level `,` is handled.
    fn parse_call_arguments(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut args: Vec<ASTNode> = Vec::new();

        self.skip_whitespace();
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Paren
            && self.tokens[self.position].subtype == TokenSubType::Close
        {
            self.position += 1;
            return Ok(args);
        }

        self.in_call_args_depth += 1;
        let result = (|| -> Result<Vec<ASTNode>, ParserError> {
            args.push(self.parse_expression()?);
            loop {
                self.skip_whitespace();
                if self.position >= self.tokens.len() {
                    return Err(ParserError {
                        message: "Unterminated call argument list".to_string(),
                        position: Some(self.position),
                    });
                }
                let token = &self.tokens[self.position];
                let is_separator = (token.token_type == TokenType::Sep
                    && token.subtype == TokenSubType::Arg)
                    || (token.token_type == TokenType::OpInfix && self.span_value(token) == ",");
                if is_separator {
                    self.position += 1;
                    args.push(self.parse_expression()?);
                } else if token.token_type == TokenType::Paren
                    && token.subtype == TokenSubType::Close
                {
                    self.position += 1;
                    return Ok(std::mem::take(&mut args));
                } else {
                    return Err(ParserError {
                        message: format!("Expected ',' or ')' in call arguments, got {token:?}"),
                        position: Some(self.position),
                    });
                }
            }
        })();
        self.in_call_args_depth -= 1;
        result
    }

    fn parse_function_arguments(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut args = Vec::new();

        self.skip_whitespace();
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Func
            && self.tokens[self.position].subtype == TokenSubType::Close
        {
            self.position += 1;
            return Ok(args);
        }

        self.skip_whitespace();
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Sep
            && self.tokens[self.position].subtype == TokenSubType::Arg
        {
            args.push(ASTNode::new(
                ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                None,
            ));
            self.position += 1;
        } else {
            args.push(self.parse_expression()?);
        }

        while self.position < self.tokens.len() {
            self.skip_whitespace();
            if self.position >= self.tokens.len() {
                break;
            }

            let token = &self.tokens[self.position];
            if token.token_type == TokenType::Sep && token.subtype == TokenSubType::Arg {
                self.position += 1;
                self.skip_whitespace();
                if self.position < self.tokens.len() {
                    let next_token = &self.tokens[self.position];
                    if next_token.token_type == TokenType::Sep
                        && next_token.subtype == TokenSubType::Arg
                    {
                        args.push(ASTNode::new(
                            ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                            None,
                        ));
                    } else if next_token.token_type == TokenType::Func
                        && next_token.subtype == TokenSubType::Close
                    {
                        args.push(ASTNode::new(
                            ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                            None,
                        ));
                        self.position += 1;
                        break;
                    } else {
                        args.push(self.parse_expression()?);
                    }
                } else {
                    args.push(ASTNode::new(
                        ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                        None,
                    ));
                }
            } else if token.token_type == TokenType::Func && token.subtype == TokenSubType::Close {
                self.position += 1;
                break;
            } else {
                return Err(ParserError {
                    message: format!("Expected ',' or ')' in function arguments, got {token:?}"),
                    position: Some(self.position),
                });
            }
        }

        Ok(args)
    }

    fn parse_array(&mut self) -> Result<ASTNode, ParserError> {
        let mut rows = Vec::new();
        let mut current_row = Vec::new();

        self.skip_whitespace();
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::Array
            && self.tokens[self.position].subtype == TokenSubType::Close
        {
            self.position += 1;
            return Ok(ASTNode::new(ASTNodeType::Array(rows), None));
        }

        current_row.push(self.parse_expression()?);

        while self.position < self.tokens.len() {
            self.skip_whitespace();
            if self.position >= self.tokens.len() {
                break;
            }
            let token = &self.tokens[self.position];

            if token.token_type == TokenType::Sep {
                if token.subtype == TokenSubType::Arg {
                    self.position += 1;
                    current_row.push(self.parse_expression()?);
                } else if token.subtype == TokenSubType::Row {
                    self.position += 1;
                    rows.push(current_row);
                    current_row = vec![self.parse_expression()?];
                }
            } else if token.token_type == TokenType::Array && token.subtype == TokenSubType::Close {
                self.position += 1;
                rows.push(current_row);
                break;
            } else {
                return Err(ParserError {
                    message: format!("Unexpected token in array: {token:?}"),
                    position: Some(self.position),
                });
            }
        }

        let contains_volatile = rows
            .iter()
            .flat_map(|r| r.iter())
            .any(|n| n.contains_volatile);

        Ok(ASTNode::new_with_volatile(
            ASTNodeType::Array(rows),
            None,
            contains_volatile,
        ))
    }
}

/// Normalise a reference string to its canonical form
pub fn normalise_reference(reference: &str) -> Result<String, ParsingError> {
    let ref_type = ReferenceType::from_string(reference)?;
    Ok(ref_type.to_string())
}

pub fn parse<T: AsRef<str>>(formula: T) -> Result<ASTNode, ParserError> {
    parse_with_dialect(formula, FormulaDialect::Excel)
}

pub fn parse_with_dialect<T: AsRef<str>>(
    formula: T,
    dialect: FormulaDialect,
) -> Result<ASTNode, ParserError> {
    let spans = crate::tokenizer::tokenize_spans_with_dialect(formula.as_ref(), dialect)?;
    let mut parser = SpanParser::new(formula.as_ref(), &spans, dialect);
    parser.parse()
}

/// Parse a single formula and annotate volatility using the provided classifier.
/// This is a convenience wrapper around `Parser::new_with_classifier`.
pub fn parse_with_volatility_classifier<T, F>(
    formula: T,
    classifier: F,
) -> Result<ASTNode, ParserError>
where
    T: AsRef<str>,
    F: Fn(&str) -> bool + Send + Sync + 'static,
{
    parse_with_dialect_and_volatility_classifier(formula, FormulaDialect::Excel, classifier)
}

pub fn parse_with_dialect_and_volatility_classifier<T, F>(
    formula: T,
    dialect: FormulaDialect,
    classifier: F,
) -> Result<ASTNode, ParserError>
where
    T: AsRef<str>,
    F: Fn(&str) -> bool + Send + Sync + 'static,
{
    let spans = crate::tokenizer::tokenize_spans_with_dialect(formula.as_ref(), dialect)?;
    let mut parser =
        SpanParser::new(formula.as_ref(), &spans, dialect).with_volatility_classifier(classifier);
    parser.parse()
}

/// Efficient batch parser with an internal token cache and optional volatility classifier.
///
/// The cache is keyed by the original formula string; repeated formulas across a batch
/// (very common in spreadsheets) will avoid re-tokenization and whitespace filtering.
pub struct BatchParser {
    include_whitespace: bool,
    volatility_classifier: Option<VolatilityClassifierArc>,
    token_cache: std::collections::HashMap<String, Arc<[crate::tokenizer::TokenSpan]>>, // cached tokens
    dialect: FormulaDialect,
}

impl BatchParser {
    pub fn builder() -> BatchParserBuilder {
        BatchParserBuilder::default()
    }

    /// Parse a formula using the internal cache and configured classifier.
    pub fn parse(&mut self, formula: &str) -> Result<ASTNode, ParserError> {
        let spans = if let Some(tokens) = self.token_cache.get(formula) {
            Arc::clone(tokens)
        } else {
            let mut spans = crate::tokenizer::tokenize_spans_with_dialect(formula, self.dialect)?;
            if !self.include_whitespace {
                spans.retain(|t| t.token_type != TokenType::Whitespace);
            }

            let spans: Arc<[crate::tokenizer::TokenSpan]> = Arc::from(spans.into_boxed_slice());
            self.token_cache
                .insert(formula.to_string(), Arc::clone(&spans));
            spans
        };

        let mut parser = SpanParser::new(formula, spans.as_ref(), self.dialect);
        if let Some(classifier) = self.volatility_classifier.clone() {
            parser = parser.with_volatility_classifier(move |name| classifier(name));
        }
        parser.parse()
    }
}

#[derive(Default)]
pub struct BatchParserBuilder {
    include_whitespace: bool,
    volatility_classifier: Option<VolatilityClassifierArc>,
    dialect: FormulaDialect,
}

impl BatchParserBuilder {
    pub fn include_whitespace(mut self, include: bool) -> Self {
        self.include_whitespace = include;
        self
    }

    pub fn with_volatility_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.volatility_classifier = Some(Arc::new(f));
        self
    }

    pub fn dialect(mut self, dialect: FormulaDialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn build(self) -> BatchParser {
        BatchParser {
            include_whitespace: self.include_whitespace,
            volatility_classifier: self.volatility_classifier,
            token_cache: std::collections::HashMap::new(),
            dialect: self.dialect,
        }
    }
}
