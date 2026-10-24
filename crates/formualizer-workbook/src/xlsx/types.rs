// SPDX-License-Identifier: MIT
// Portions adapted from Calamine 0.36.0, copyright 2016-2026 Johann Tuffe.
// See loader/LICENSE.calamine. XLSX-only value errors and shared-formula expansion.
use quick_xml_cells as quick_xml;
use std::str::FromStr;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellErrorType {
    /// Division by 0 error
    Div0,
    /// Unavailable value error
    NA,
    /// Invalid name error
    Name,
    /// Null value error
    Null,
    /// Number error
    Num,
    /// Invalid cell reference error
    Ref,
    /// Value error
    Value,
}
impl std::fmt::Display for CellErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match *self {
            CellErrorType::Div0 => write!(f, "#DIV/0!"),
            CellErrorType::NA => write!(f, "#N/A"),
            CellErrorType::Name => write!(f, "#NAME?"),
            CellErrorType::Null => write!(f, "#NULL!"),
            CellErrorType::Num => write!(f, "#NUM!"),
            CellErrorType::Ref => write!(f, "#REF!"),
            CellErrorType::Value => write!(f, "#VALUE!"),
        }
    }
}
#[derive(Debug)]
pub enum XlsxError {
    /// A wrapper for a variety of [`std::io::Error`] errors such as file
    /// permissions when reading an XLSX file. This can be caused by a
    /// non-existent file or parent directory or, commonly on Windows, if the
    /// file is already open in Excel.
    Io(std::io::Error),

    /// A wrapper for a variety of [`zip::result::ZipError`] errors from
    /// [`zip::ZipWriter`]. These relate to errors arising from reading the XLSX
    /// file zip container.
    Zip(zip::result::ZipError),

    /// A wrapper for a variety of [`quick_xml::Error`] XML parsing errors, but
    /// most commonly for missing data in the target file.
    Xml(quick_xml::Error),

    /// A wrapper for a variety of [`quick_xml::events::attributes::AttrError`]
    /// errors related to attributes in XML elements.
    XmlAttr(quick_xml::events::attributes::AttrError),

    /// A wrapper for a variety of [`std::string::ParseError`] errors when
    /// parsing strings.
    Parse(std::string::ParseError),

    /// A wrapper for a variety of [`std::num::ParseFloatError`] errors when
    /// parsing floats.
    ParseFloat(std::num::ParseFloatError),

    /// A wrapper for a variety of [`std::num::ParseIntError`] errors when
    /// parsing integers.
    ParseInt(std::num::ParseIntError),

    /// Unexpected end of XML file, usually when an end tag is missing.
    XmlEof(&'static str),

    /// Unexpected node in XML.
    UnexpectedNode(&'static str),

    /// XML file not found in XLSX container.
    FileNotFound(String),

    /// Relationship file not found in XLSX container.
    RelationshipNotFound,

    /// Non alphanumeric character found when parsing `A1` style range string.
    Alphanumeric(u8),

    /// Error when parsing the column name in a `A1` style range string.
    NumericColumn(u8),

    /// Missing column name when parsing an `A1` style range string.
    RangeWithoutColumnComponent,

    /// Missing row number when parsing an `A1` style range string.
    RangeWithoutRowComponent,

    /// Column number exceeds maximum allowed columns.
    ColumnNumberOverflow,

    /// Row number exceeds maximum allowed rows.
    RowNumberOverflow,

    /// Error when parsing dimensions of a worksheet.
    DimensionCount(usize),

    /// Unknown cell type (`t`) attribute.
    CellTAttribute(String),

    /// Unexpected XML element or attribute error.
    Unexpected(&'static str),

    /// Unrecognized worksheet type or state.
    Unrecognized {
        /// The data type.
        typ: &'static str,

        /// The value found.
        val: String,
    },

    /// Unrecognized cell error type.
    CellError(String),

    /// Workbook is password protected.
    Password,

    /// Specified worksheet was not found.
    WorksheetNotFound(String),

    /// Specified worksheet Table was not found.
    TableNotFound(String),

    /// The specified sheet is not a worksheet.
    NotAWorksheet(String),

    /// A wrapper for a variety of [`quick_xml::encoding::EncodingError`]
    /// encoding errors.
    Encoding(quick_xml::encoding::EncodingError),

    /// Specified Pivot Table was not found on worksheet.
    PivotTableNotFound(String),
}

impl std::fmt::Display for XlsxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XlsxError::Io(e) => write!(f, "I/O error: {e}"),
            XlsxError::Zip(e) => write!(f, "Zip error: {e}"),
            XlsxError::Xml(e) => write!(f, "Xml error: {e}"),
            XlsxError::XmlAttr(e) => write!(f, "Xml attribute error: {e}"),
            XlsxError::Parse(e) => write!(f, "Parse string error: {e}"),
            XlsxError::ParseInt(e) => write!(f, "Parse integer error: {e}"),
            XlsxError::ParseFloat(e) => write!(f, "Parse float error: {e}"),

            XlsxError::XmlEof(e) => write!(f, "Unexpected end of xml, expecting '</{e}>'"),
            XlsxError::UnexpectedNode(e) => write!(f, "Expecting '{e}' node"),
            XlsxError::FileNotFound(e) => write!(f, "File not found '{e}'"),
            XlsxError::RelationshipNotFound => write!(f, "Relationship not found"),
            XlsxError::Alphanumeric(e) => {
                write!(f, "Expecting alphanumeric character, got {e:X}")
            }
            XlsxError::NumericColumn(e) => write!(
                f,
                "Numeric character is not allowed for column name, got {e}",
            ),
            XlsxError::DimensionCount(e) => {
                write!(f, "Range dimension must be lower than 2. Got {e}")
            }
            XlsxError::CellTAttribute(e) => write!(f, "Unknown cell 't' attribute: {e:?}"),
            XlsxError::RangeWithoutColumnComponent => {
                write!(f, "Range is missing the expected column component.")
            }
            XlsxError::RangeWithoutRowComponent => {
                write!(f, "Range is missing the expected row component.")
            }
            XlsxError::ColumnNumberOverflow => write!(f, "column number overflow"),
            XlsxError::RowNumberOverflow => write!(f, "row number overflow"),
            XlsxError::Unexpected(e) => write!(f, "{e}"),
            XlsxError::Unrecognized { typ, val } => write!(f, "Unrecognized {typ}: {val}"),
            XlsxError::CellError(e) => write!(f, "Unsupported cell error value '{e}'"),
            XlsxError::WorksheetNotFound(n) => write!(f, "Worksheet '{n}' not found"),
            XlsxError::Password => write!(f, "Workbook is password protected"),
            XlsxError::TableNotFound(n) => write!(f, "Table '{n}' not found"),
            XlsxError::NotAWorksheet(typ) => write!(f, "Expecting a worksheet, got {typ}"),
            XlsxError::Encoding(e) => write!(f, "XML encoding error: {e}"),
            XlsxError::PivotTableNotFound(pt) => {
                write!(f, "Pivot Table '{pt}' was not found on worksheet")
            }
        }
    }
}

impl std::error::Error for XlsxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            XlsxError::Io(e) => Some(e),
            XlsxError::Zip(e) => Some(e),
            XlsxError::Xml(e) => Some(e),
            XlsxError::Parse(e) => Some(e),
            XlsxError::ParseInt(e) => Some(e),
            XlsxError::ParseFloat(e) => Some(e),
            XlsxError::Encoding(e) => Some(e),
            _ => None,
        }
    }
}

impl FromStr for CellErrorType {
    type Err = XlsxError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "#DIV/0!" => Ok(CellErrorType::Div0),
            "#N/A" => Ok(CellErrorType::NA),
            "#NAME?" => Ok(CellErrorType::Name),
            "#NULL!" => Ok(CellErrorType::Null),
            "#NUM!" => Ok(CellErrorType::Num),
            "#REF!" => Ok(CellErrorType::Ref),
            "#VALUE!" => Ok(CellErrorType::Value),
            _ => Err(XlsxError::CellError(s.into())),
        }
    }
}
impl From<std::io::Error> for XlsxError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<zip::result::ZipError> for XlsxError {
    fn from(e: zip::result::ZipError) -> Self {
        Self::Zip(e)
    }
}
impl From<quick_xml::Error> for XlsxError {
    fn from(e: quick_xml::Error) -> Self {
        Self::Xml(e)
    }
}
impl From<std::num::ParseFloatError> for XlsxError {
    fn from(e: std::num::ParseFloatError) -> Self {
        Self::ParseFloat(e)
    }
}
impl From<std::num::ParseIntError> for XlsxError {
    fn from(e: std::num::ParseIntError) -> Self {
        Self::ParseInt(e)
    }
}
impl From<quick_xml::encoding::EncodingError> for XlsxError {
    fn from(e: quick_xml::encoding::EncodingError) -> Self {
        Self::Encoding(e)
    }
}
impl From<quick_xml::events::attributes::AttrError> for XlsxError {
    fn from(e: quick_xml::events::attributes::AttrError) -> Self {
        Self::XmlAttr(e)
    }
}

/// Error produced by the native XLSX value reader.
#[derive(Debug, thiserror::Error)]
pub enum XlsxReadError {
    #[error("Xlsx error: {0}")]
    Xlsx(#[from] XlsxError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
const MAX_ROWS: u32 = 1_048_576;
const MAX_COLUMNS: u32 = 16_384;
#[derive(Debug, Copy, Clone)]
enum Reference {
    Cell {
        row: u32,
        col: u32,
        absolute_row: bool,
        absolute_col: bool,
    },
    Row {
        row: u32,
        absolute: bool,
    },
    Column {
        col: u32,
        absolute: bool,
    },
}

impl Reference {
    // Create a cell reference with validation.
    fn cell(row: u32, col: u32, absolute_row: bool, absolute_col: bool) -> Result<Self, XlsxError> {
        let reference = Reference::Cell {
            row,
            col,
            absolute_row,
            absolute_col,
        };
        reference.validate()?;
        Ok(reference)
    }

    // Create a column reference with validation.
    fn column(col: u32, absolute: bool) -> Result<Self, XlsxError> {
        let reference = Reference::Column { col, absolute };
        reference.validate()?;
        Ok(reference)
    }

    // Create a row reference with validation.
    fn row(row: u32, absolute: bool) -> Result<Self, XlsxError> {
        let reference = Reference::Row { row, absolute };
        reference.validate()?;
        Ok(reference)
    }

    // Parse a reference (e.g., "A1", "$A1", "A$1", "$A$1", "E", "$E", "5", "$5").
    fn parse(name: &[u8]) -> Result<Self, XlsxError> {
        let mut iter = name.iter().peekable();
        let mut col: u32 = 0;
        let mut row: u32 = 0;
        let mut absolute_col = false;
        let mut absolute_row = false;

        while let Some(&c) = iter.next() {
            match (c, iter.peek()) {
                (b'$', Some(b'A'..=b'Z' | b'a'..=b'z')) => {
                    if row > 0 || col > 0 {
                        return Err(XlsxError::Alphanumeric(c));
                    }
                    absolute_col = true;
                }
                (b'$', Some(b'0'..=b'9')) => {
                    if row > 0 {
                        return Err(XlsxError::Alphanumeric(c));
                    }
                    absolute_row = true;
                }
                (b'$', _) => return Err(XlsxError::Alphanumeric(c)),
                (c @ (b'A'..=b'Z' | b'a'..=b'z'), _) => {
                    if row > 0 {
                        return Err(XlsxError::Alphanumeric(c));
                    }
                    col = col
                        .wrapping_mul(26)
                        .wrapping_add((c.to_ascii_uppercase() - b'A') as u32 + 1);
                }
                (c @ b'0'..=b'9', _) => {
                    row = row.wrapping_mul(10).wrapping_add((c - b'0') as u32);
                }
                _ => return Err(XlsxError::Alphanumeric(c)),
            }
        }

        match (col.checked_sub(1), row.checked_sub(1)) {
            (Some(col), Some(row)) => Reference::cell(row, col, absolute_row, absolute_col),
            (Some(col), None) => Reference::column(col, absolute_col),
            (None, Some(row)) => Reference::row(row, absolute_row),
            (None, None) => Err(XlsxError::Unexpected("Empty reference")),
        }
    }

    // Apply offset to create a new reference with validation.
    fn offset(self, offset: (i64, i64)) -> Result<Self, XlsxError> {
        let result = match self {
            Reference::Cell {
                row,
                col,
                absolute_row,
                absolute_col,
            } => {
                let new_col = if absolute_col {
                    col
                } else {
                    (col as i64 + offset.1) as u32
                };
                let new_row = if absolute_row {
                    row
                } else {
                    (row as i64 + offset.0) as u32
                };

                Reference::Cell {
                    row: new_row,
                    col: new_col,
                    absolute_row,
                    absolute_col,
                }
            }
            Reference::Column { col, absolute } => {
                let new_col = if absolute {
                    col
                } else {
                    (col as i64 + offset.1) as u32
                };

                Reference::Column {
                    col: new_col,
                    absolute,
                }
            }
            Reference::Row { row, absolute } => {
                let new_row = if absolute {
                    row
                } else {
                    (row as i64 + offset.0) as u32
                };

                Reference::Row {
                    row: new_row,
                    absolute,
                }
            }
        };

        result.validate()?;
        Ok(result)
    }

    // Validate that row/column values are in bounds.
    fn validate(&self) -> Result<(), XlsxError> {
        match self {
            Reference::Cell { row, col, .. } => {
                if *col >= MAX_COLUMNS {
                    return Err(XlsxError::ColumnNumberOverflow);
                }
                if *row >= MAX_ROWS {
                    return Err(XlsxError::RowNumberOverflow);
                }
                Ok(())
            }
            Reference::Column { col, .. } => {
                if *col >= MAX_COLUMNS {
                    return Err(XlsxError::ColumnNumberOverflow);
                }
                Ok(())
            }
            Reference::Row { row, .. } => {
                if *row >= MAX_ROWS {
                    return Err(XlsxError::RowNumberOverflow);
                }
                Ok(())
            }
        }
    }

    // Format a reference to bytes.
    fn format(&self, buf: &mut Vec<u8>) -> Result<(), XlsxError> {
        match self {
            Reference::Cell {
                row,
                col,
                absolute_row,
                absolute_col,
            } => {
                if *absolute_col {
                    buf.push(b'$');
                }
                column_number_to_name(*col, buf)?;
                if *absolute_row {
                    buf.push(b'$');
                }
                buf.extend((row + 1).to_string().into_bytes());
                Ok(())
            }
            Reference::Column { col, absolute } => {
                if *absolute {
                    buf.push(b'$');
                }
                column_number_to_name(*col, buf)
            }
            Reference::Row { row, absolute } => {
                if *absolute {
                    buf.push(b'$');
                }
                buf.extend((row + 1).to_string().into_bytes());
                Ok(())
            }
        }
    }
}

// Advance a reference by the offset (e.g., "A1", "E:F", "5:6", "A1:B5").
fn offset_range(range: &[u8], offset: (i64, i64), buf: &mut Vec<u8>) -> Result<(), XlsxError> {
    let colon_pos = range.iter().position(|&b| b == b':');

    match colon_pos {
        None => {
            let reference = Reference::parse(range)?;
            if !matches!(reference, Reference::Cell { .. }) {
                return Err(XlsxError::Unexpected("Single reference type must be cell"));
            }
            let offset_ref = reference.offset(offset)?;
            offset_ref.format(buf)
        }
        Some(idx) => {
            let start = &range[..idx];
            let end = &range[idx + 1..];

            let start_ref = Reference::parse(start)?;
            let end_ref = Reference::parse(end)?;

            if std::mem::discriminant(&start_ref) != std::mem::discriminant(&end_ref) {
                return Err(XlsxError::Unexpected("Range type mismatch"));
            }

            let start_offset = start_ref.offset(offset)?;
            let end_offset = end_ref.offset(offset)?;

            start_offset.format(buf)?;
            buf.push(b':');
            end_offset.format(buf)
        }
    }
}

/// Expand a shared-formula template from its starting cell to another cell.
///
/// `start` and `end` are zero-based `(row, column)` positions. Relative
/// references in `formula` are shifted by `end - start`; absolute references
/// remain fixed. This is the same expansion Calamine uses for XLSX shared
/// formulas.
///
/// This helper is intentionally workbook-state agnostic. XLSX shared-formula
/// indices are worksheet-local, so callers are responsible for pairing the
/// right template and starting cell with each derived cell.
pub fn expand_shared_formula(
    formula: &str,
    start: (u32, u32),
    end: (u32, u32),
) -> Result<String, XlsxError> {
    let mut out = String::with_capacity(formula.len());
    expand_shared_formula_into(formula, start, end, &mut out)?;
    Ok(out)
}

/// Expand a shared-formula template into a caller-provided buffer.
///
/// This is the allocation-reuse variant of [`expand_shared_formula`]. `out` is
/// cleared before writing.
pub fn expand_shared_formula_into(
    formula: &str,
    start: (u32, u32),
    end: (u32, u32),
    out: &mut String,
) -> Result<(), XlsxError> {
    let offset = (end.0 as i64 - start.0 as i64, end.1 as i64 - start.1 as i64);
    expand_shared_formula_with_offset_into(formula, offset, out)
}

// Apply a row/column offset to each valid cell or range reference in a formula.
// Function-like tokens (immediately followed by `(`), quoted strings, structured
// references, and references that would move out of XLSX bounds are preserved.
fn expand_shared_formula_with_offset_into(
    formula: &str,
    offset: (i64, i64),
    out: &mut String,
) -> Result<(), XlsxError> {
    let bytes = formula.as_bytes();
    let mut res = std::mem::take(out).into_bytes();
    res.clear();
    let mut token_buf = Vec::with_capacity(32);
    let mut in_quote = false;

    let mut token_start = 0;
    let mut token_end = 0;

    for (i, &c) in bytes.iter().enumerate() {
        if !in_quote
            && (c.is_ascii_alphanumeric()
                || c == b'\\'
                || c == b'.'
                || c == b'_'
                || c == b'$'
                || c == b':')
        {
            token_end = i + 1;
        } else {
            if token_start < token_end {
                let next_is_paren = c == b'(';
                token_buf.clear();
                if !next_is_paren
                    && offset_range(&bytes[token_start..token_end], offset, &mut token_buf).is_ok()
                {
                    res.extend(&token_buf);
                } else {
                    res.extend(&bytes[token_start..token_end]);
                }
            }
            res.push(c);
            token_start = i + 1;
            token_end = i + 1;

            if c == b'"' {
                in_quote = !in_quote;
            }
        }
    }

    if token_start < token_end {
        token_buf.clear();
        if offset_range(&bytes[token_start..token_end], offset, &mut token_buf).is_ok() {
            res.extend(&token_buf);
        } else {
            res.extend(&bytes[token_start..token_end]);
        }
    }

    match String::from_utf8(res) {
        Ok(translated) => {
            *out = translated;
            Ok(())
        }
        Err(_) => Err(XlsxError::Unexpected("fail to convert cell name")),
    }
}

pub(crate) fn column_number_to_name(num: u32, buf: &mut Vec<u8>) -> Result<(), XlsxError> {
    if num >= MAX_COLUMNS {
        return Err(XlsxError::ColumnNumberOverflow);
    }
    let start = buf.len();
    let mut num = num + 1;
    while num > 0 {
        let integer = ((num - 1) % 26 + 65) as u8;
        buf.push(integer);
        num = (num - 1) / 26;
    }
    buf[start..].reverse();
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_formula_translation_preserves_literals_absolutes_and_bounds() {
        assert_eq!(
            expand_shared_formula("A1+$B1+C$1+$D$1", (0, 0), (2, 3)).unwrap(),
            "D3+$B3+F$1+$D$1"
        );
        assert_eq!(
            expand_shared_formula("CONCATENATE(A1, \"A1\", Table1[Column1])", (0, 0), (1, 0))
                .unwrap(),
            "CONCATENATE(A2, \"A1\", Table1[Column1])"
        );
        assert_eq!(
            expand_shared_formula("SUM(A1:XFE1)", (0, 0), (1, 1)).unwrap(),
            "SUM(A1:XFE1)"
        );
    }
    #[test]
    fn known_error_tokens_and_invalid_tokens_preserve_messages() {
        for token in [
            "#DIV/0!", "#N/A", "#NAME?", "#NULL!", "#NUM!", "#REF!", "#VALUE!",
        ] {
            assert_eq!(token.parse::<CellErrorType>().unwrap().to_string(), token);
        }
        assert_eq!(
            "#bogus".parse::<CellErrorType>().unwrap_err().to_string(),
            "Unsupported cell error value '#bogus'"
        );
    }
}
