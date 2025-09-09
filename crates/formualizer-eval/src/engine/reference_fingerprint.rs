use formualizer_parse::parser::ReferenceType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Extension trait for ReferenceType to generate deterministic fingerprints
pub trait ReferenceFingerprint {
    /// Generate a stable, deterministic fingerprint for this reference
    /// Used as a cache key for flattened ranges
    fn fingerprint(&self) -> String;
}

impl ReferenceFingerprint for ReferenceType {
    fn fingerprint(&self) -> String {
        // Create a deterministic string representation
        // Avoid using Debug trait which might change
        match self {
            ReferenceType::Cell { sheet, row, col } => {
                format!("cell:{}:{}:{}", sheet.as_deref().unwrap_or("_"), row, col)
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                format!(
                    "range:{}:{}:{}:{}:{}",
                    sheet.as_deref().unwrap_or("_"),
                    start_row.map_or("*".to_string(), |r| r.to_string()),
                    start_col.map_or("*".to_string(), |c| c.to_string()),
                    end_row.map_or("*".to_string(), |r| r.to_string()),
                    end_col.map_or("*".to_string(), |c| c.to_string())
                )
            }
            ReferenceType::Table(table_ref) => {
                // Use a hash for complex table references
                let mut hasher = DefaultHasher::new();
                table_ref.hash(&mut hasher);
                format!("table:{:x}", hasher.finish())
            }
            ReferenceType::NamedRange(name) => {
                format!("named:{name}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_fingerprint() {
        let ref1 = ReferenceType::Cell {
            sheet: None,
            row: 5,
            col: 10,
        };

        let ref2 = ReferenceType::Cell {
            sheet: None,
            row: 5,
            col: 10,
        };

        assert_eq!(ref1.fingerprint(), ref2.fingerprint());
        assert_eq!(ref1.fingerprint(), "cell:_:5:10");
    }

    #[test]
    fn test_range_fingerprint() {
        let ref1 = ReferenceType::Range {
            sheet: Some("Sheet1".to_string()),
            start_row: Some(0),
            start_col: Some(0),
            end_row: Some(99),
            end_col: Some(0),
        };

        let ref2 = ReferenceType::Range {
            sheet: Some("Sheet1".to_string()),
            start_row: Some(0),
            start_col: Some(0),
            end_row: Some(99),
            end_col: Some(0),
        };

        assert_eq!(ref1.fingerprint(), ref2.fingerprint());
        assert_eq!(ref1.fingerprint(), "range:Sheet1:0:0:99:0");
    }

    #[test]
    fn test_whole_column_fingerprint() {
        let ref1 = ReferenceType::Range {
            sheet: None,
            start_row: None,
            start_col: Some(0),
            end_row: None,
            end_col: Some(0),
        };

        assert_eq!(ref1.fingerprint(), "range:_:*:0:*:0");
    }

    #[test]
    fn test_named_range_fingerprint() {
        let ref1 = ReferenceType::NamedRange("MyRange".to_string());
        let ref2 = ReferenceType::NamedRange("MyRange".to_string());

        assert_eq!(ref1.fingerprint(), ref2.fingerprint());
        assert_eq!(ref1.fingerprint(), "named:MyRange");
    }

    #[test]
    fn test_different_refs_different_fingerprints() {
        let ref1 = ReferenceType::Cell {
            sheet: None,
            row: 1,
            col: 1,
        };

        let ref2 = ReferenceType::Cell {
            sheet: None,
            row: 1,
            col: 2,
        };

        assert_ne!(ref1.fingerprint(), ref2.fingerprint());
    }
}
