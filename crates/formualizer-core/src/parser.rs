use crate::tokenizer::{Associativity, Token, TokenSubType, TokenType, TokenizerError};
use crate::{ExcelError, LiteralValue};
use crate::{ParsingError, Tokenizer};

use crate::hasher::FormulaHasher;
use once_cell::sync::Lazy;
use std::error::Error;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};

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
            TableSpecifier::Row(row) => write!(f, "{row}"),
            TableSpecifier::Column(column) => write!(f, "{column}"),
            TableSpecifier::ColumnRange(start, end) => write!(f, "{start}:{end}"),
            TableSpecifier::SpecialItem(item) => write!(f, "{item}"),
            TableSpecifier::Combination(specs) => {
                // Emit nested bracketed parts so the surrounding Table formatter prints
                // canonical structured refs like Table[[#Headers],[Column1]:[Column2]]
                let parts: Vec<String> = specs.iter().map(|s| format!("[{s}]")).collect();
                write!(f, "{}", parts.join(","))
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

impl ReferenceType {
    /// Create a reference from a string. Can be A1, A:A, A1:B2, Table1[Column], etc.
    pub fn from_string(reference: &str) -> Result<Self, ParsingError> {
        // First check if this is a table reference (contains '[')
        if reference.contains('[') {
            return Self::parse_table_reference(reference);
        }

        // Extract sheet name if present
        let (sheet, ref_part) = Self::extract_sheet_name(reference);

        if ref_part.contains(':') {
            // Range reference
            Self::parse_range_reference(&ref_part, sheet)
        } else {
            // Try to parse as a single cell reference
            match Self::parse_cell_reference(&ref_part) {
                Ok((col, row)) => Ok(ReferenceType::Cell { sheet, row, col }),
                Err(_) => {
                    // Treat it as a named range
                    Ok(ReferenceType::NamedRange(reference.to_string()))
                }
            }
        }
    }

    /// Parse a range reference like "A1:B2", "A:A", or "1:1"
    fn parse_range_reference(reference: &str, sheet: Option<String>) -> Result<Self, ParsingError> {
        let mut parts = reference.splitn(2, ':');
        let start = parts.next().unwrap();
        let end = parts
            .next()
            .ok_or_else(|| ParsingError::InvalidReference(format!("Invalid range: {reference}")))?;

        let (start_col, start_row) = Self::parse_range_part(start)?;
        let (end_col, end_row) = Self::parse_range_part(end)?;

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

        // Try to parse as column-only or row-only
        let bytes = part.as_bytes();
        let mut i = 0;

        // Skip optional $
        if i < bytes.len() && bytes[i] == b'$' {
            i += 1;
        }

        // Check if we have letters (column)
        let col_start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
            i += 1;
        }

        if i > col_start {
            // We have a column
            let col_str = &part[col_start..i];
            let col = Self::column_to_number(col_str)?;

            // Skip optional $ before row
            if i < bytes.len() && bytes[i] == b'$' {
                i += 1;
            }

            // Check if we have digits (row)
            let row_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }

            if i > row_start && i == bytes.len() {
                // We have both column and row (shouldn't happen as parse_cell_reference should have caught it)
                let row_str = &part[row_start..i];
                let row = row_str.parse::<u32>().map_err(|_| {
                    ParsingError::InvalidReference(format!("Invalid row: {row_str}"))
                })?;
                return Ok((Some(col), Some(row)));
            } else if i == col_start + col_str.len()
                || (i == col_start + col_str.len() + 1 && bytes[col_start + col_str.len()] == b'$')
            {
                // Just a column
                return Ok((Some(col), None));
            }
        } else {
            // No column, check for row-only reference
            i = 0;
            if i < bytes.len() && bytes[i] == b'$' {
                i += 1;
            }

            let row_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }

            if i > row_start && i == bytes.len() {
                let row_str = &part[row_start..i];
                let row = row_str.parse::<u32>().map_err(|_| {
                    ParsingError::InvalidReference(format!("Invalid row: {row_str}"))
                })?;
                return Ok((None, Some(row)));
            }
        }

        Err(ParsingError::InvalidReference(format!(
            "Invalid range part: {part}"
        )))
    }

    /// Parse a cell reference like "A1" into (column, row) using byte-based parsing.
    fn parse_cell_reference(reference: &str) -> Result<(u32, u32), ParsingError> {
        let bytes = reference.as_bytes();
        let mut i = 0;

        // Skip optional $ for absolute column reference
        if i < bytes.len() && bytes[i] == b'$' {
            i += 1;
        }

        // Parse column letters
        let col_start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
            i += 1;
        }

        if i == col_start {
            return Err(ParsingError::InvalidReference(format!(
                "Invalid cell reference: {reference}"
            )));
        }

        let col_str = &reference[col_start..i];
        let col = Self::column_to_number(col_str)?;

        // Skip optional $ for absolute row reference
        if i < bytes.len() && bytes[i] == b'$' {
            i += 1;
        }

        // Parse row number
        let row_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }

        if i == row_start || i != bytes.len() {
            return Err(ParsingError::InvalidReference(format!(
                "Invalid cell reference: {reference}"
            )));
        }

        let row_str = &reference[row_start..i];
        let row = row_str
            .parse::<u32>()
            .map_err(|_| ParsingError::InvalidReference(format!("Invalid row: {row_str}")))?;

        Ok((col, row))
    }

    /// Convert a column letter (e.g., "A", "BC") to a column number (1-based) using byte operations.
    pub(crate) fn column_to_number(column: &str) -> Result<u32, ParsingError> {
        let bytes = column.as_bytes();

        // Excel column names have a practical limit (XFD = 16384 is the max in Excel)
        // Anything longer than 3 characters is likely not a column reference
        if bytes.is_empty() || bytes.len() > 3 {
            return Err(ParsingError::InvalidReference(format!(
                "Invalid column: {column}"
            )));
        }

        let mut result = 0u32;

        for &b in bytes {
            if !b.is_ascii_alphabetic() {
                return Err(ParsingError::InvalidReference(format!(
                    "Invalid column: {column}"
                )));
            }
            // Use checked arithmetic to prevent overflow
            result = result
                .checked_mul(26)
                .and_then(|r| r.checked_add((b.to_ascii_uppercase() - b'A' + 1) as u32))
                .ok_or_else(|| {
                    ParsingError::InvalidReference(format!("Invalid column: {column}"))
                })?;
        }

        Ok(result)
    }

    /// Convert a column number to a column letter using lookup table for common values.
    pub(crate) fn number_to_column(mut num: u32) -> String {
        // Use lookup table for common columns (1-702 covers A-ZZ)
        if num > 0 && num <= 702 {
            return COLUMN_LOOKUP[(num - 1) as usize].clone();
        }

        // Fallback for larger column numbers
        let mut result = String::with_capacity(3);
        while num > 0 {
            num -= 1;
            result.insert(0, ((num % 26) as u8 + b'A') as char);
            num /= 26;
        }
        result
    }
}

impl Display for ReferenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ReferenceType::Cell { sheet, row, col } => {
                    let col_str = Self::number_to_column(*col);
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
                        (Some(col), Some(row)) => {
                            format!("{}{}", Self::number_to_column(*col), row)
                        }
                        (Some(col), None) => Self::number_to_column(*col),
                        (None, Some(row)) => row.to_string(),
                        (None, None) => "".to_string(), // Should not happen in normal usage
                    };

                    // Format end reference
                    let end_ref = match (end_col, end_row) {
                        (Some(col), Some(row)) => {
                            format!("{}{}", Self::number_to_column(*col), row)
                        }
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
        )
    }
}

impl ReferenceType {
    /// Normalise the reference string (convert to canonical form)
    pub fn normalise(&self) -> String {
        format!("{self}")
    }

    /// Extract a sheet name from a reference using byte operations.
    fn extract_sheet_name(reference: &str) -> (Option<String>, String) {
        let bytes = reference.as_bytes();
        let mut i = 0;

        // Handle quoted sheet names
        if i < bytes.len() && bytes[i] == b'\'' {
            i += 1;
            let start = i;

            // Find closing quote
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    // Check if next char is '!'
                    if i + 1 < bytes.len() && bytes[i + 1] == b'!' {
                        let sheet = String::from(&reference[start..i]);
                        let ref_part = String::from(&reference[i + 2..]);
                        return (Some(sheet), ref_part);
                    }
                }
                i += 1;
            }
        }

        // Handle unquoted sheet names
        i = 0;
        while i < bytes.len() {
            if bytes[i] == b'!' && i > 0 {
                let sheet = String::from(&reference[0..i]);
                let ref_part = String::from(&reference[i + 1..]);
                return (Some(sheet), ref_part);
            }
            i += 1;
        }

        (None, reference.to_string())
    }

    /// Parse a table reference like "Table1[Column1]" or more complex ones like "Table1[[#All],[Column1]:[Column2]]".
    fn parse_table_reference(reference: &str) -> Result<Self, ParsingError> {
        // Find the first '[' to separate table name from specifier
        if let Some(bracket_pos) = reference.find('[') {
            let table_name = reference[..bracket_pos].trim();
            if table_name.is_empty() {
                return Err(ParsingError::InvalidReference(reference.to_string()));
            }

            let specifier_str = &reference[bracket_pos..];
            let specifier = Self::parse_table_specifier(specifier_str)?;

            Ok(ReferenceType::Table(TableReference {
                name: table_name.to_string(),
                specifier,
            }))
        } else {
            Err(ParsingError::InvalidReference(reference.to_string()))
        }
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
            // Check for column range using iterator instead of split().collect()
            if let Some(colon_pos) = content.find(':') {
                let start = content[..colon_pos].trim();
                let end = content[colon_pos + 1..].trim();
                return Ok(Some(TableSpecifier::ColumnRange(
                    start.to_string(),
                    end.to_string(),
                )));
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
            || content.contains("[@]")
        {
            // This is a combination of specifiers
            // Parse them into a vector
            let mut specifiers = Vec::new();

            // Simple parsing - this would need enhancement for full support
            if content.contains("[#Headers]") {
                specifiers.push(Box::new(TableSpecifier::SpecialItem(SpecialItem::Headers)));
            }
            if content.contains("[#Data]") {
                specifiers.push(Box::new(TableSpecifier::SpecialItem(SpecialItem::Data)));
            }
            if content.contains("[#Totals]") {
                specifiers.push(Box::new(TableSpecifier::SpecialItem(SpecialItem::Totals)));
            }
            if content.contains("[#All]") {
                specifiers.push(Box::new(TableSpecifier::SpecialItem(SpecialItem::All)));
            }

            if !specifiers.is_empty() {
                return Ok(Some(TableSpecifier::Combination(specifiers)));
            }
        }

        // Fallback to storing as a column specifier
        Ok(Some(TableSpecifier::Column(content.trim().to_string())))
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
        args: Vec<ASTNode>, // Most functions have <= 4 args
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
            ASTNodeType::Array(rows) => write!(f, "Array({rows:?})"),
        }
    }
}

/// An AST node represents a parsed formula element
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
    tokens: Vec<Token>,
    position: usize,
    /// Optional classifier to determine whether a function name is volatile.
    volatility_classifier: Option<Box<dyn Fn(&str) -> bool + Send + Sync + 'static>>,
}

impl<T> From<T> for Parser
where
    T: AsRef<str>,
{
    fn from(formula: T) -> Self {
        let tokens = Tokenizer::new(formula.as_ref()).unwrap().items;
        Self::new(tokens, false)
    }
}

impl Parser {
    pub fn new(tokens: Vec<Token>, include_whitespace: bool) -> Self {
        let filtered_tokens = if include_whitespace {
            tokens
        } else {
            tokens
                .into_iter()
                .filter(|t| t.token_type != TokenType::Whitespace)
                .collect()
        };
        Parser {
            tokens: filtered_tokens,
            position: 0,
            volatility_classifier: None,
        }
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

    /// Parse the tokens into an AST.
    pub fn parse(&mut self) -> Result<ASTNode, ParserError> {
        if self.tokens.is_empty() {
            return Err(ParserError {
                message: "No tokens to parse".to_string(),
                position: None,
            });
        }

        // Check for literal formula (doesn't start with '=')
        if self.tokens[0].token_type == TokenType::Literal {
            let token = self.tokens[0].clone();
            return Ok(ASTNode::new(
                ASTNodeType::Literal(LiteralValue::Text(token.value.clone())),
                Some(token),
            ));
        }

        let ast = self.parse_expression()?;
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
        self.parse_binary_op(0)
    }

    fn parse_binary_op(&mut self, min_precedence: u8) -> Result<ASTNode, ParserError> {
        let mut left = self.parse_unary_op()?;

        while self.position < self.tokens.len() {
            let token = &self.tokens[self.position];
            if token.token_type != TokenType::OpInfix {
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

            let right = self.parse_binary_op(next_min_precedence)?;
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

    fn parse_unary_op(&mut self) -> Result<ASTNode, ParserError> {
        if self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::OpPrefix
        {
            let op_token = self.tokens[self.position].clone();
            self.position += 1;
            let expr = self.parse_unary_op()?;
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
        self.parse_postfix_op()
    }

    fn parse_postfix_op(&mut self) -> Result<ASTNode, ParserError> {
        let mut expr = self.parse_primary()?;

        while self.position < self.tokens.len()
            && self.tokens[self.position].token_type == TokenType::OpPostfix
        {
            let op_token = self.tokens[self.position].clone();
            self.position += 1;
            let contains_volatile = expr.contains_volatile;
            expr = ASTNode::new_with_volatile(
                ASTNodeType::UnaryOp {
                    op: op_token.value.clone(),
                    expr: Box::new(expr),
                },
                Some(op_token),
                contains_volatile,
            );
        }

        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<ASTNode, ParserError> {
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
                let value = token.value.to_uppercase() == "TRUE";
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
                let reference =
                    ReferenceType::from_string(&token.value).map_err(|e| ParserError {
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

/// Normalise a reference string to its canonical form
pub fn normalise_reference(reference: &str) -> Result<String, ParsingError> {
    let ref_type = ReferenceType::from_string(reference)?;
    Ok(ref_type.to_string())
}

pub fn parse<T: AsRef<str>>(formula: T) -> Result<ASTNode, ParserError> {
    Parser::from(formula.as_ref()).parse()
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
    let tokens = Tokenizer::new(formula.as_ref())?.items;
    let mut parser = Parser::new_with_classifier(tokens, false, classifier);
    parser.parse()
}

/// Efficient batch parser with an internal token cache and optional volatility classifier.
///
/// The cache is keyed by the original formula string; repeated formulas across a batch
/// (very common in spreadsheets) will avoid re-tokenization and whitespace filtering.
pub struct BatchParser {
    include_whitespace: bool,
    volatility_classifier: Option<std::sync::Arc<dyn Fn(&str) -> bool + Send + Sync + 'static>>,
    token_cache: std::collections::HashMap<String, Vec<Token>>, // filtered tokens
}

impl BatchParser {
    pub fn builder() -> BatchParserBuilder {
        BatchParserBuilder::default()
    }

    /// Parse a formula using the internal cache and configured classifier.
    pub fn parse(&mut self, formula: &str) -> Result<ASTNode, ParserError> {
        // Get or build filtered tokens
        let filtered = if let Some(tokens) = self.token_cache.get(formula) {
            tokens.clone()
        } else {
            let mut tokens = Tokenizer::new(formula)?.items;
            if !self.include_whitespace {
                tokens = tokens
                    .into_iter()
                    .filter(|t| t.token_type != TokenType::Whitespace)
                    .collect();
            }
            self.token_cache.insert(formula.to_string(), tokens.clone());
            tokens
        };

        let mut parser = Parser::new(filtered, true); // already filtered per include_whitespace
        if let Some(classifier) = self.volatility_classifier.clone() {
            let arc = classifier.clone();
            parser = parser.with_volatility_classifier(move |name| arc(name));
        }
        parser.parse()
    }
}

#[derive(Default)]
pub struct BatchParserBuilder {
    include_whitespace: bool,
    volatility_classifier: Option<std::sync::Arc<dyn Fn(&str) -> bool + Send + Sync + 'static>>,
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
        self.volatility_classifier = Some(std::sync::Arc::new(f));
        self
    }

    pub fn build(self) -> BatchParser {
        BatchParser {
            include_whitespace: self.include_whitespace,
            volatility_classifier: self.volatility_classifier,
            token_cache: std::collections::HashMap::new(),
        }
    }
}
