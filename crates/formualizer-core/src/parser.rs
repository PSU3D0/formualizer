use crate::ParsingError;
use crate::tokenizer::{Associativity, Token, TokenSubType, TokenType, TokenizerError};
use crate::{ExcelError, LiteralValue};

use crate::hasher::FormulaHasher;
use once_cell::sync::Lazy;
use regex::Regex;
use std::error::Error;
use std::fmt::{self, Display};
use std::hash::Hasher;

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

static CELL_REF_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^\$?([A-Za-z]+)\$?(\d+)$").unwrap());
static COLUMN_REF_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^\$?([A-Za-z]+)$").unwrap());
static ROW_REF_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^\$?(\d+)$").unwrap());
static SHEET_REF_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^'?([^!']+)'?!(.+)$").unwrap());

/// A structured table reference specifier for accessing specific parts of a table
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
    Combination(Vec<TableSpecifier>),
}

/// Specifies which row(s) to use in a table reference
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
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct TableReference {
    /// The name of the table
    pub name: String,
    /// Optional specifier for which part of the table to use
    pub specifier: Option<TableSpecifier>,
}

/// A reference to something outside the cell.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ReferenceType {
    Cell {
        sheet: Option<String>,
        row: u32,
        col: u32,
    },
    Range {
        sheet: Option<String>,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    },
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
            TableSpecifier::Row(row_spec) => write!(f, "{row_spec}"),
            TableSpecifier::Column(column) => write!(f, "{column}"),
            TableSpecifier::ColumnRange(start, end) => write!(f, "{start}:{end}"),
            TableSpecifier::SpecialItem(item) => write!(f, "{item}"),
            TableSpecifier::Combination(specs) => {
                write!(f, "[")?;
                for (i, spec) in specs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "[{spec}]")?;
                }
                write!(f, "]")
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

impl Display for TableReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(specifier) = &self.specifier {
            write!(f, "{}[{}]", self.name, specifier)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

impl Display for ReferenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReferenceType::Cell { sheet, row, col } => write!(
                f,
                "Cell({}:{}:{})",
                sheet.clone().unwrap_or_default(),
                row,
                col
            ),
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                let start_row_str = start_row.map_or("*".to_string(), |r| r.to_string());
                let start_col_str = start_col.map_or("*".to_string(), |c| c.to_string());
                let end_row_str = end_row.map_or("*".to_string(), |r| r.to_string());
                let end_col_str = end_col.map_or("*".to_string(), |c| c.to_string());

                write!(
                    f,
                    "Range({}:{}:{}:{} sheet={})",
                    start_row_str,
                    start_col_str,
                    end_row_str,
                    end_col_str,
                    sheet.clone().unwrap_or_default()
                )
            }
            ReferenceType::Table(table_ref) => write!(f, "Table({table_ref})"),
            ReferenceType::NamedRange(named_range) => write!(f, "NamedRange({named_range})"),
        }
    }
}

/// A helper function to normalize a reference string by parsing it and then
/// returning the canonical form.
pub fn normalise_reference(input: &str) -> Result<String, ParsingError> {
    let reference = ReferenceType::parse(input)?;
    Ok(reference.normalise())
}

impl ReferenceType {
    pub fn contains(&self, sheet_name: &str, row: u32, col: u32) -> bool {
        match self {
            ReferenceType::Cell {
                sheet,
                row: r,
                col: c,
            } => {
                let s = sheet.as_deref().unwrap_or_default();
                (s.is_empty() || s == sheet_name) && *r == row && *c == col
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                let s = sheet.as_deref().unwrap_or_default();
                if !s.is_empty() && s != sheet_name {
                    return false;
                }

                let r_start = start_row.unwrap_or(1);
                let c_start = start_col.unwrap_or(1);
                let r_end = end_row.unwrap_or(u32::MAX);
                let c_end = end_col.unwrap_or(u32::MAX);

                row >= r_start && row <= r_end && col >= c_start && col <= c_end
            }
            _ => false, // Named ranges and tables would need resolution first
        }
    }
    /// Parse a reference string into a ReferenceType.
    pub fn parse(reference: &str) -> Result<Self, ParsingError> {
        // Check if this is a table reference
        if reference.contains('[') && reference.contains(']') {
            return Self::parse_table_reference(reference);
        }

        // Extract sheet name if present
        let (sheet, ref_without_sheet) = Self::extract_sheet_name(reference);

        // Check if this is a range reference (contains a colon)
        if ref_without_sheet.contains(':') {
            return Self::parse_range_reference(&ref_without_sheet, sheet);
        }

        // Try to parse as a cell reference
        if let Ok((col, row)) = Self::parse_cell_reference(&ref_without_sheet) {
            return Ok(ReferenceType::Cell { sheet, row, col });
        }

        // If we can't parse it as a cell, assume it's a named range
        Ok(ReferenceType::NamedRange(reference.to_string()))
    }

    /// Return a canonical A1 / structured ref string. Uses uppercase
    /// column letters, no dollar signs, sheet names quoted only when
    /// containing specials, and preserves range/table semantics.
    pub fn normalise(&self) -> String {
        match self {
            ReferenceType::Cell { sheet, row, col } => {
                let col_str = Self::number_to_column(*col); // Already returns uppercase
                let row_str = row.to_string();

                if let Some(sheet_name) = sheet {
                    // Only quote sheet name if it contains spaces or special characters
                    if sheet_name.contains(' ')
                        || sheet_name.contains('!')
                        || sheet_name.contains('\'')
                        || sheet_name.contains('\"')
                    {
                        format!("'{sheet_name}'!{col_str}{row_str}")
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
            } => {
                // Format start reference
                let start_ref = match (start_col, start_row) {
                    (Some(col), Some(row)) => format!("{}{}", Self::number_to_column(*col), row),
                    (Some(col), None) => Self::number_to_column(*col),
                    (None, Some(row)) => row.to_string(),
                    (None, None) => "".to_string(), // Should not happen in normal usage
                };

                // Format end reference
                let end_ref = match (end_col, end_row) {
                    (Some(col), Some(row)) => format!("{}{}", Self::number_to_column(*col), row),
                    (Some(col), None) => Self::number_to_column(*col),
                    (None, Some(row)) => row.to_string(),
                    (None, None) => "".to_string(), // Should not happen in normal usage
                };

                let range_part = format!("{start_ref}:{end_ref}");

                if let Some(sheet_name) = sheet {
                    // Only quote sheet name if it contains spaces or special characters
                    if sheet_name.contains(' ')
                        || sheet_name.contains('!')
                        || sheet_name.contains('\'')
                        || sheet_name.contains('\"')
                    {
                        format!("'{sheet_name}'!{range_part}")
                    } else {
                        format!("{sheet_name}!{range_part}")
                    }
                } else {
                    range_part
                }
            }
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
    }

    /// Extract a sheet name from a reference.
    fn extract_sheet_name(reference: &str) -> (Option<String>, String) {
        if let Some(captures) = SHEET_REF_REGEX.captures(reference) {
            let sheet = captures.get(1).unwrap().as_str();
            let reference = captures.get(2).unwrap().as_str();
            (Some(sheet.to_string()), reference.to_string())
        } else {
            (None, reference.to_string())
        }
    }

    /// Parse a table reference like "Table1[Column1]" or more complex ones like "Table1[[#All],[Column1]:[Column2]]".
    fn parse_table_reference(reference: &str) -> Result<Self, ParsingError> {
        // Check if there's at least a table name followed by a bracket
        let parts: Vec<&str> = reference.split('[').collect();
        if parts.len() < 2 {
            return Err(ParsingError::InvalidReference(reference.to_string()));
        }

        // We need to identify the table name, which is the part before the first '['
        let table_name = parts[0].trim();
        if table_name.is_empty() {
            return Err(ParsingError::InvalidReference(reference.to_string()));
        }

        // Extract the specifier content by finding the closing bracket
        let specifier_str = &reference[table_name.len()..];

        // Parse the table specifier
        let specifier = Self::parse_table_specifier(specifier_str)?;

        Ok(ReferenceType::Table(TableReference {
            name: table_name.to_string(),
            specifier,
        }))
    }

    /// Parse a table specifier like "[Column1]" or "[[#All],[Column1]:[Column2]]"
    fn parse_table_specifier(specifier_str: &str) -> Result<Option<TableSpecifier>, ParsingError> {
        if specifier_str.is_empty() || !specifier_str.starts_with('[') {
            return Ok(None);
        }

        // Find balanced closing bracket
        let mut depth = 0;
        let mut end_pos = 0;

        for (i, c) in specifier_str.chars().enumerate() {
            if c == '[' {
                depth += 1;
            } else if c == ']' {
                depth -= 1;
                if depth == 0 {
                    end_pos = i;
                    break;
                }
            }
        }

        if depth != 0 || end_pos == 0 {
            return Err(ParsingError::InvalidReference(format!(
                "Unbalanced brackets in table specifier: {specifier_str}"
            )));
        }

        // Extract content between outermost brackets
        let content = &specifier_str[1..end_pos];

        // Handle different types of specifiers
        if content.is_empty() {
            // Empty brackets means the whole table
            return Ok(Some(TableSpecifier::All));
        }

        // Handle special items
        if content.starts_with("#") {
            return Self::parse_special_item(content);
        }

        // Handle column references
        if !content.contains('[') && !content.contains('#') {
            if content.contains(':') {
                // Handle column range
                let parts: Vec<&str> = content.split(':').collect();
                if parts.len() == 2 {
                    return Ok(Some(TableSpecifier::ColumnRange(
                        parts[0].trim().to_string(),
                        parts[1].trim().to_string(),
                    )));
                }
            } else {
                // Single column
                return Ok(Some(TableSpecifier::Column(content.trim().to_string())));
            }
        }

        // Handle complex structured references with nested brackets
        if content.contains('[') {
            return Self::parse_complex_table_specifier(content);
        }

        // If we can't determine the type, just use the raw specifier
        Ok(Some(TableSpecifier::Column(content.trim().to_string())))
    }

    /// Parse a special item specifier like "#Headers", "#Data", etc.
    fn parse_special_item(content: &str) -> Result<Option<TableSpecifier>, ParsingError> {
        match content {
            "#All" => Ok(Some(TableSpecifier::SpecialItem(SpecialItem::All))),
            "#Headers" => Ok(Some(TableSpecifier::SpecialItem(SpecialItem::Headers))),
            "#Data" => Ok(Some(TableSpecifier::SpecialItem(SpecialItem::Data))),
            "#Totals" => Ok(Some(TableSpecifier::SpecialItem(SpecialItem::Totals))),
            "@" => Ok(Some(TableSpecifier::Row(TableRowSpecifier::Current))),
            _ => Err(ParsingError::InvalidReference(format!(
                "Unknown special item: {content}"
            ))),
        }
    }

    /// Parse complex table specifiers with nested brackets
    fn parse_complex_table_specifier(
        content: &str,
    ) -> Result<Option<TableSpecifier>, ParsingError> {
        // This is a more complex case like [[#Headers],[Column1]:[Column2]]
        // For now, we'll just store the raw specifier and enhance this in the future

        // Try to identify common patterns
        if content.contains("[#Headers]")
            || content.contains("[#All]")
            || content.contains("[#Data]")
            || content.contains("[#Totals]")
        {
            // This is likely a combination of special items and columns
            // For now, return as a raw specifier
            return Ok(Some(TableSpecifier::Column(content.to_string())));
        }

        // Default fallback
        Ok(Some(TableSpecifier::Column(content.to_string())))
    }

    /// Parse a range reference like "A1:B2", "A:A", "1:1", "A1:A", etc.
    fn parse_range_reference(reference: &str, sheet: Option<String>) -> Result<Self, ParsingError> {
        let parts: Vec<&str> = reference.split(':').collect();
        if parts.len() != 2 {
            return Err(ParsingError::InvalidReference(reference.to_string()));
        }

        let start_part = parts[0];
        let end_part = parts[1];

        // Parse start reference
        let (start_col, start_row) = Self::parse_range_part(start_part)?;

        // Parse end reference
        let (end_col, end_row) = Self::parse_range_part(end_part)?;

        Ok(ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        })
    }

    /// Parse a part of a range reference (either start or end).
    /// Returns (column, row) where either can be None for infinite ranges.
    fn parse_range_part(part: &str) -> Result<(Option<u32>, Option<u32>), ParsingError> {
        // Try to parse as a normal cell reference (A1, B2, etc.)
        if let Ok((col, row)) = Self::parse_cell_reference(part) {
            return Ok((Some(col), Some(row)));
        }

        // Try to parse as a column-only reference (A, B, etc.)
        if let Some(captures) = COLUMN_REF_REGEX.captures(part) {
            let col_str = captures.get(1).unwrap().as_str();
            let col = Self::column_to_number(col_str)?;
            return Ok((Some(col), None));
        }

        // Try to parse as a row-only reference (1, 2, etc.)
        if let Some(captures) = ROW_REF_REGEX.captures(part) {
            let row_str = captures.get(1).unwrap().as_str();
            let row = row_str
                .parse::<u32>()
                .map_err(|_| ParsingError::InvalidReference(format!("Invalid row: {row_str}")))?;
            return Ok((None, Some(row)));
        }

        // If we can't parse it as any known format, return an error
        Err(ParsingError::InvalidReference(format!(
            "Invalid range part: {part}"
        )))
    }

    /// Parse a cell reference like "A1" into (column, row).
    fn parse_cell_reference(reference: &str) -> Result<(u32, u32), ParsingError> {
        if let Some(captures) = CELL_REF_REGEX.captures(reference) {
            let col_str = captures.get(1).unwrap().as_str();
            let row_str = captures.get(2).unwrap().as_str();

            let col = Self::column_to_number(col_str)?;
            let row = row_str
                .parse::<u32>()
                .map_err(|_| ParsingError::InvalidReference(format!("Invalid row: {row_str}")))?;

            Ok((col, row))
        } else {
            Err(ParsingError::InvalidReference(format!(
                "Invalid cell reference: {reference}"
            )))
        }
    }

    /// Convert a column letter (e.g., "A", "BC") to a column number (1-based).
    pub(crate) fn column_to_number(column: &str) -> Result<u32, ParsingError> {
        let mut result = 0u32;
        for c in column.chars() {
            if !c.is_ascii_alphabetic() {
                return Err(ParsingError::InvalidReference(format!(
                    "Invalid column: {column}"
                )));
            }
            result = result * 26 + (c.to_ascii_uppercase() as u32 - 'A' as u32 + 1);
        }
        Ok(result)
    }

    /// Convert a column number to a column letter.
    pub(crate) fn number_to_column(mut num: u32) -> String {
        let mut result = String::new();
        while num > 0 {
            num -= 1;
            result.insert(0, ((num % 26) as u8 + b'A') as char);
            num /= 26;
        }
        result
    }

    /// Get the Excel-style string representation of this reference
    pub fn to_excel_string(&self) -> String {
        match self {
            ReferenceType::Cell { sheet, row, col } => {
                if let Some(s) = sheet {
                    format!("{}!{}{}", s, Self::number_to_column(*col), row)
                } else {
                    format!("{}{}", Self::number_to_column(*col), row)
                }
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                // Format start reference
                let start_ref = match (start_col, start_row) {
                    (Some(col), Some(row)) => format!("{}{}", Self::number_to_column(*col), row),
                    (Some(col), None) => Self::number_to_column(*col),
                    (None, Some(row)) => row.to_string(),
                    (None, None) => "".to_string(), // Should not happen in normal usage
                };

                // Format end reference
                let end_ref = match (end_col, end_row) {
                    (Some(col), Some(row)) => format!("{}{}", Self::number_to_column(*col), row),
                    (Some(col), None) => Self::number_to_column(*col),
                    (None, Some(row)) => row.to_string(),
                    (None, None) => "".to_string(), // Should not happen in normal usage
                };

                let range_part = format!("{start_ref}:{end_ref}");

                if let Some(s) = sheet {
                    if s.contains(' ') {
                        format!("'{s}'!{range_part}")
                    } else {
                        format!("{s}!{range_part}")
                    }
                } else {
                    range_part
                }
            }
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
        args: Vec<ASTNode>,
    },
    Array(Vec<Vec<ASTNode>>),
}

impl Display for ASTNodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ASTNodeType::Literal(value) => write!(f, "{value:?}"),
            ASTNodeType::Reference { original, .. } => write!(f, "Reference({original})"),
            ASTNodeType::UnaryOp { op, expr } => write!(f, "UnaryOp({op}, {expr:?})"),
            ASTNodeType::BinaryOp { op, left, right } => {
                write!(f, "BinaryOp({op}, {left:?}, {right:?})")
            }
            ASTNodeType::Function { name, args } => write!(f, "Function({name}, {args:?})"),
            ASTNodeType::Array(rows) => write!(f, "Array({rows:?})"),
        }
    }
}

/// A node in the abstract syntax tree.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ASTNode {
    pub node_type: ASTNodeType,
    pub source_token: Option<Token>,
}

impl Display for ASTNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ASTNode(node_type={:?}, source_token={:?})",
            self.node_type, self.source_token
        )
    }
}

impl Eq for ASTNode {}

impl ASTNode {
    /// Create a new AST node.
    pub fn new(node_type: ASTNodeType, source_token: Option<Token>) -> Self {
        ASTNode {
            node_type,
            source_token,
        }
    }

    /// Hash of structure and literal values; ignores whitespace,
    /// source_token field, and reference *casing* (use .normalise()).
    /// This produces a deterministic fingerprint that can be used for caching.
    pub fn fingerprint(&self) -> u64 {
        let mut hasher = FormulaHasher::new();

        self.hash_structure(&mut hasher);
        hasher.finish()
    }

    /// Helper method to hash the AST structure recursively
    fn hash_structure(&self, hasher: &mut FormulaHasher) {
        // Hash the node type discriminant to distinguish between different node types
        // We create a single byte to represent the variant
        match &self.node_type {
            ASTNodeType::Literal(value) => {
                hasher.write(&[1]); // Discriminant for Literal
                match value {
                    LiteralValue::Int(i) => {
                        hasher.write(&[1]); // Int subtype
                        hasher.write(&i.to_le_bytes());
                    }
                    LiteralValue::Number(n) => {
                        hasher.write(&[2]); // Number subtype
                        hasher.write(&n.to_bits().to_le_bytes());
                    }
                    LiteralValue::Text(s) => {
                        hasher.write(&[3]); // Text subtype
                        hasher.write(s.as_bytes());
                    }
                    LiteralValue::Boolean(b) => {
                        hasher.write(&[4]); // Boolean subtype
                        hasher.write(&[*b as u8]);
                    }
                    LiteralValue::Error(e) => {
                        hasher.write(&[5]); // Error subtype
                        hasher.write(e.to_string().as_bytes());
                    }
                    LiteralValue::Date(d) => {
                        hasher.write(&[6]); // Date subtype
                        hasher.write(d.to_string().as_bytes());
                    }
                    LiteralValue::Time(t) => {
                        hasher.write(&[7]); // Time subtype
                        hasher.write(t.to_string().as_bytes());
                    }
                    LiteralValue::DateTime(dt) => {
                        hasher.write(&[8]); // DateTime subtype
                        hasher.write(dt.to_string().as_bytes());
                    }
                    LiteralValue::Duration(dur) => {
                        hasher.write(&[9]); // Duration subtype
                        hasher.write(dur.to_string().as_bytes());
                    }
                    LiteralValue::Array(a) => {
                        hasher.write(&[10]); // Array subtype
                        // Hash array dimensions
                        hasher.write(&(a.len() as u64).to_le_bytes());
                        for row in a {
                            hasher.write(&(row.len() as u64).to_le_bytes());
                            for cell in row {
                                // Recursively hash each value in the array
                                match cell {
                                    LiteralValue::Int(i) => {
                                        hasher.write(&[1]);
                                        hasher.write(&i.to_le_bytes());
                                    }
                                    LiteralValue::Number(n) => {
                                        hasher.write(&[2]);
                                        hasher.write(&n.to_bits().to_le_bytes());
                                    }
                                    LiteralValue::Text(s) => {
                                        hasher.write(&[3]);
                                        hasher.write(s.as_bytes());
                                    }
                                    LiteralValue::Boolean(b) => {
                                        hasher.write(&[4]);
                                        hasher.write(&[*b as u8]);
                                    }
                                    LiteralValue::Error(e) => {
                                        hasher.write(&[5]);
                                        hasher.write(e.to_string().as_bytes());
                                    }
                                    LiteralValue::Date(d) => {
                                        hasher.write(&[6]); // Date subtype
                                        hasher.write(d.to_string().as_bytes());
                                    }
                                    LiteralValue::Time(t) => {
                                        hasher.write(&[7]); // Time subtype
                                        hasher.write(t.to_string().as_bytes());
                                    }
                                    LiteralValue::DateTime(dt) => {
                                        hasher.write(&[8]); // DateTime subtype
                                        hasher.write(dt.to_string().as_bytes());
                                    }
                                    LiteralValue::Duration(dur) => {
                                        hasher.write(&[9]); // Duration subtype
                                        hasher.write(dur.to_string().as_bytes());
                                    }
                                    LiteralValue::Array(_) => {
                                        // For simplicity, we don't support nested arrays
                                        hasher.write(&[10]);
                                    }
                                    LiteralValue::Empty => {
                                        hasher.write(&[11]);
                                    }
                                    LiteralValue::Pending => {
                                        hasher.write(&[12]); // Pending subtype
                                    }
                                }
                            }
                        }
                    }
                    LiteralValue::Empty => {
                        hasher.write(&[11]); // Empty subtype
                    }
                    LiteralValue::Pending => {
                        hasher.write(&[12]); // Pending subtype
                    }
                }
            }
            ASTNodeType::Reference { reference, .. } => {
                hasher.write(&[2]); // Discriminant for Reference

                // Use the normalized form to ignore case differences
                let normalized = reference.normalise();
                hasher.write(normalized.as_bytes());
            }
            ASTNodeType::UnaryOp { op, expr } => {
                hasher.write(&[3]); // Discriminant for UnaryOp
                hasher.write(op.as_bytes());
                expr.hash_structure(hasher);
            }
            ASTNodeType::BinaryOp { op, left, right } => {
                hasher.write(&[4]); // Discriminant for BinaryOp
                hasher.write(op.as_bytes());
                left.hash_structure(hasher);
                right.hash_structure(hasher);
            }
            ASTNodeType::Function { name, args } => {
                hasher.write(&[5]); // Discriminant for Function

                // Use lowercase function name to be case-insensitive
                let name_lower = name.to_lowercase();
                hasher.write(name_lower.as_bytes());

                // Hash the number of arguments
                hasher.write(&(args.len() as u64).to_le_bytes());

                // Hash each argument in order
                for arg in args {
                    arg.hash_structure(hasher);
                }
            }
            ASTNodeType::Array(rows) => {
                hasher.write(&[6]); // Discriminant for Array

                // Hash array dimensions
                hasher.write(&(rows.len() as u64).to_le_bytes());

                // Hash each row in row-major order
                for row in rows {
                    hasher.write(&(row.len() as u64).to_le_bytes());
                    for cell in row {
                        cell.hash_structure(hasher);
                    }
                }
            }
        }
    }

    /// Create a new reference node, parsing the reference string immediately.
    pub fn new_reference(
        reference_str: String,
        source_token: Option<Token>,
    ) -> Result<Self, ParserError> {
        // Parse the reference string right away
        let reference = ReferenceType::parse(&reference_str).map_err(|e| ParserError {
            message: format!("Failed to parse reference '{reference_str}': {e}"),
            position: None,
        })?;

        Ok(ASTNode {
            node_type: ASTNodeType::Reference {
                original: reference_str,
                reference,
            },
            source_token,
        })
    }

    /// Get the reference type directly from a Reference node
    pub fn get_reference_type(&self) -> Option<&ReferenceType> {
        match &self.node_type {
            ASTNodeType::Reference { reference, .. } => Some(reference),
            _ => None,
        }
    }

    /// Get all reference dependencies of this AST node.
    pub fn get_dependencies(&self) -> Vec<&ReferenceType> {
        let mut dependencies = Vec::new();
        self.collect_dependencies(&mut dependencies);
        dependencies
    }

    /// Get string representations of all dependencies in this AST node.
    pub fn get_dependency_strings(&self) -> Vec<String> {
        self.get_dependencies()
            .into_iter()
            .map(|r| r.to_excel_string())
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
            ASTNodeType::Array(rows) => {
                for row in rows {
                    for cell in row {
                        cell.collect_dependencies(dependencies);
                    }
                }
            }
            _ => {}
        }
    }
}
/// A parser for Excel formulas.
pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
    include_whitespace: bool,
}

impl Parser {
    /// Create a new parser from a formula string.
    pub fn from(formula: &str) -> Result<Self, TokenizerError> {
        let tokenizer = crate::Tokenizer::new(formula)?;
        Ok(Parser::new(tokenizer.items, false))
    }

    /// Create a new parser.
    pub fn new(tokens: Vec<Token>, include_whitespace: bool) -> Self {
        Parser {
            tokens,
            current: 0,
            include_whitespace,
        }
    }

    /// Parse the formula into an AST.
    pub fn parse(&mut self) -> Result<ASTNode, ParserError> {
        // Handle literal formulas (non-formulas)
        if !self.tokens.is_empty() && self.tokens[0].token_type == TokenType::Literal {
            return Ok(ASTNode::new(
                ASTNodeType::Literal(LiteralValue::Text(self.tokens[0].value.clone())),
                Some(self.tokens[0].clone()),
            ));
        }

        // Skip whitespace if we're not including it
        if !self.include_whitespace {
            self.skip_whitespace();
        }

        let result = self.expression(0)?; // Start with lowest precedence

        // Make sure we consumed all tokens
        if self.current < self.tokens.len() {
            if self.tokens[self.current].token_type == TokenType::Whitespace
                && !self.include_whitespace
            {
                self.skip_whitespace();
            }

            if self.current < self.tokens.len() {
                return Err(ParserError {
                    message: format!("Unexpected token: {}", self.tokens[self.current]),
                    position: Some(self.current),
                });
            }
        }

        Ok(result)
    }

    /// Skip whitespace tokens.
    fn skip_whitespace(&mut self) {
        while self.current < self.tokens.len()
            && self.tokens[self.current].token_type == TokenType::Whitespace
        {
            self.current += 1;
        }
    }

    /// Parse an expression with a minimum precedence level.
    fn expression(&mut self, min_precedence: u8) -> Result<ASTNode, ParserError> {
        if !self.include_whitespace {
            self.skip_whitespace();
        }

        // Parse prefix operators or primary expression
        let mut expr = if self.check_token(TokenType::OpPrefix) {
            let op_token = self.tokens[self.current].clone();
            self.current += 1;

            if !self.include_whitespace {
                self.skip_whitespace();
            }

            let right = self.expression(7)?; // Prefix has high precedence

            ASTNode::new(
                ASTNodeType::UnaryOp {
                    op: op_token.value.clone(),
                    expr: Box::new(right),
                },
                Some(op_token),
            )
        } else {
            self.primary()?
        };

        // Parse postfix operators
        if self.check_token(TokenType::OpPostfix) {
            let op_token = self.tokens[self.current].clone();
            self.current += 1;

            expr = ASTNode::new(
                ASTNodeType::UnaryOp {
                    op: op_token.value.clone(),
                    expr: Box::new(expr),
                },
                Some(op_token),
            );
        }

        // Parse infix operators with precedence climbing
        while self.current < self.tokens.len() {
            if !self.include_whitespace {
                self.skip_whitespace();
            }

            if self.current >= self.tokens.len() {
                break;
            }

            let token = &self.tokens[self.current];

            if token.token_type != TokenType::OpInfix {
                break;
            }

            let (precedence, associativity) = match token.get_precedence() {
                Some(p) => p,
                None => break,
            };

            if precedence < min_precedence {
                break;
            }

            let op_token = token.clone();
            self.current += 1;

            if !self.include_whitespace {
                self.skip_whitespace();
            }

            // For right-associative operators, use same precedence
            // For left-associative operators, use precedence + 1
            let next_min_precedence = match associativity {
                Associativity::Left => precedence + 1,
                Associativity::Right => precedence,
            };

            let right = self.expression(next_min_precedence)?;

            expr = ASTNode::new(
                ASTNodeType::BinaryOp {
                    op: op_token.value.clone(),
                    left: Box::new(expr),
                    right: Box::new(right),
                },
                Some(op_token),
            );
        }

        Ok(expr)
    }

    /// Parse a primary expression (literal, reference, function call, etc.).
    fn primary(&mut self) -> Result<ASTNode, ParserError> {
        if self.current >= self.tokens.len() {
            return Err(ParserError {
                message: "Unexpected end of formula".to_string(),
                position: Some(self.current),
            });
        }

        if !self.include_whitespace {
            self.skip_whitespace();
        }

        let token = &self.tokens[self.current];

        match token.token_type {
            TokenType::Operand => {
                self.current += 1;

                match token.subtype {
                    TokenSubType::Number => {
                        let value = token.value.parse::<f64>().map_err(|_| ParserError {
                            message: format!("Failed to parse number: {}", token.value),
                            position: Some(self.current - 1),
                        })?;

                        Ok(ASTNode::new(
                            ASTNodeType::Literal(LiteralValue::Number(value)),
                            Some(token.clone()),
                        ))
                    }
                    TokenSubType::Text => {
                        // Handle text literals, possibly stripping quotes
                        let text = if token.value.starts_with('"')
                            && token.value.ends_with('"')
                            && token.value.len() >= 2
                        {
                            // Process Excel's double-quote escaping (where "" represents a single " inside a string)
                            token.value[1..token.value.len() - 1].replace("\"\"", "\"")
                        } else {
                            token.value.clone()
                        };

                        Ok(ASTNode::new(
                            ASTNodeType::Literal(LiteralValue::Text(text)),
                            Some(token.clone()),
                        ))
                    }
                    TokenSubType::Logical => {
                        let value = token.value == "TRUE";

                        Ok(ASTNode::new(
                            ASTNodeType::Literal(LiteralValue::Boolean(value)),
                            Some(token.clone()),
                        ))
                    }
                    TokenSubType::Error => Ok(ASTNode::new(
                        ASTNodeType::Literal(LiteralValue::Error(ExcelError::from_error_string(
                            &token.value,
                        ))),
                        Some(token.clone()),
                    )),
                    TokenSubType::Range => {
                        ASTNode::new_reference(token.value.clone(), Some(token.clone()))
                    }
                    _ => Err(ParserError {
                        message: format!("Unexpected operand subtype: {}", token.subtype),
                        position: Some(self.current - 1),
                    }),
                }
            }
            TokenType::Paren => {
                if token.subtype == TokenSubType::Open {
                    self.current += 1;

                    if !self.include_whitespace {
                        self.skip_whitespace();
                    }

                    let expr = self.expression(0)?;

                    // Expect closing parenthesis
                    if !self.include_whitespace {
                        self.skip_whitespace();
                    }

                    if !self.check_token_with_subtype(TokenType::Paren, TokenSubType::Close) {
                        return Err(ParserError {
                            message: "Expected closing parenthesis".to_string(),
                            position: Some(self.current),
                        });
                    }

                    self.current += 1;
                    Ok(expr)
                } else {
                    Err(ParserError {
                        message: "Unexpected closing parenthesis".to_string(),
                        position: Some(self.current),
                    })
                }
            }
            TokenType::Func => self.parse_function_call(),
            TokenType::Array => self.parse_array_literal(),
            _ => Err(ParserError {
                message: format!("Unexpected token: {token}"),
                position: Some(self.current),
            }),
        }
    }

    /// Parse a function call.
    fn parse_function_call(&mut self) -> Result<ASTNode, ParserError> {
        let func_token = self.tokens[self.current].clone();
        self.current += 1;

        // Extract function name (remove trailing '(')
        let name = func_token.value[..func_token.value.len() - 1].to_string();

        let args = self.parse_function_arguments()?;

        Ok(ASTNode::new(
            ASTNodeType::Function { name, args },
            Some(func_token),
        ))
    }

    /// Parse function arguments.
    fn parse_function_arguments(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut args = Vec::new();

        // Check for empty argument list
        if !self.include_whitespace {
            self.skip_whitespace();
        }

        if self.check_token_with_subtype(TokenType::Func, TokenSubType::Close) {
            self.current += 1;
            return Ok(args);
        }

        // Flag to track if we're at the beginning of an argument list or after a comma
        let mut expect_arg = true;

        loop {
            if !self.include_whitespace {
                self.skip_whitespace();
            }

            // Check if we're at the end of the argument list
            if self.check_token_with_subtype(TokenType::Func, TokenSubType::Close) {
                // End of function arguments
                self.current += 1;

                // If we were expecting an argument but found closing parenthesis,
                // it means there's a trailing comma, which implies an empty last argument
                if expect_arg {
                    // Add an empty argument (represented as an empty string)
                    args.push(ASTNode::new(
                        ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                        None,
                    ));
                }

                break;
            }

            // Check for consecutive commas (empty argument)
            if expect_arg && self.check_token_with_subtype(TokenType::Sep, TokenSubType::Arg) {
                // Found an empty argument
                args.push(ASTNode::new(
                    ASTNodeType::Literal(LiteralValue::Text("".to_string())),
                    None,
                ));

                // Skip the comma and continue
                self.current += 1;
                expect_arg = true;
                continue;
            }

            if expect_arg {
                // Parse one argument
                let arg = self.expression(0)?;
                args.push(arg);
            }

            if !self.include_whitespace {
                self.skip_whitespace();
            }

            if self.current >= self.tokens.len() {
                return Err(ParserError {
                    message: "Unexpected end of formula in function arguments".to_string(),
                    position: Some(self.current),
                });
            }

            if self.check_token_with_subtype(TokenType::Sep, TokenSubType::Arg) {
                // Argument separator - continue to next argument
                self.current += 1;
                expect_arg = true;
            } else if self.check_token_with_subtype(TokenType::Func, TokenSubType::Close) {
                // End of function arguments
                self.current += 1;
                break;
            } else {
                return Err(ParserError {
                    message: format!(
                        "Expected argument separator or closing parenthesis, got: {}",
                        self.tokens[self.current]
                    ),
                    position: Some(self.current),
                });
            }
        }

        Ok(args)
    }

    /// Parse an array literal.
    fn parse_array_literal(&mut self) -> Result<ASTNode, ParserError> {
        let array_token = self.tokens[self.current].clone();
        self.current += 1;

        let mut array = Vec::new();
        let mut current_row = Vec::new();

        loop {
            if !self.include_whitespace {
                self.skip_whitespace();
            }

            if self.current >= self.tokens.len() {
                return Err(ParserError {
                    message: "Unexpected end of formula in array".to_string(),
                    position: Some(self.current),
                });
            }

            if self.check_token_with_subtype(TokenType::Array, TokenSubType::Close) {
                // End of array
                self.current += 1;

                if !current_row.is_empty() {
                    array.push(current_row);
                }

                return Ok(ASTNode::new(ASTNodeType::Array(array), Some(array_token)));
            } else if self.check_token(TokenType::Sep) {
                self.current += 1;

                if self.tokens[self.current - 1].subtype == TokenSubType::Row {
                    // End of row
                    array.push(current_row);
                    current_row = Vec::new();
                }
                // Else it's an argument separator within the current row
            } else {
                let element = self.expression(0)?;
                current_row.push(element);
            }
        }
    }

    /// Check if the current token matches the expected type.
    fn check_token(&self, expected_type: TokenType) -> bool {
        self.current < self.tokens.len() && self.tokens[self.current].token_type == expected_type
    }

    /// Check if the current token matches the expected type and subtype.
    fn check_token_with_subtype(
        &self,
        expected_type: TokenType,
        expected_subtype: TokenSubType,
    ) -> bool {
        self.current < self.tokens.len()
            && self.tokens[self.current].token_type == expected_type
            && self.tokens[self.current].subtype == expected_subtype
    }
}
