//! Calamine expands shared-formula followers by offsetting every maximal run
//! of `[A-Za-z0-9._\\$:]` that parses as an A1 cell or range, outside
//! `"`-delimited spans. It does not understand `'` quoting, so a sheet
//! qualifier such as `'FY2024'!` or `'Q1 2024'!` is rewritten for every
//! follower (silently pointing at another sheet), and a `"` inside a quoted
//! sheet name flips its string state for the rest of the formula. Such
//! shared formulas cannot be replayed exactly and are refused.
use super::{IoError, unsupported};

/// Whether Calamine's shared expansion would alter `qualifier` (the complete
/// `'Sheet'!` or `Sheet!` text) for some follower. Followers sit below or to
/// the right of the top-left master, so offsets are non-negative; a run that
/// survives a one-row and a one-column step unchanged is either absolute,
/// not a reference, or already at the grid edge, where Calamine keeps it.
pub(super) fn shifted_by_shared_expansion(qualifier: &str) -> bool {
    qualifier.contains('"')
        || [(1, 0), (0, 1)].into_iter().any(|end| {
            calamine::expand_shared_formula(qualifier, (0, 0), end)
                .map_or(true, |expanded| expanded != qualifier)
        })
}

/// Characters that end an unquoted sheet qualifier when scanning backwards
/// from its `!`.
fn delimiter(b: u8) -> bool {
    matches!(
        b,
        b'(' | b')'
            | b','
            | b';'
            | b'+'
            | b'-'
            | b'*'
            | b'/'
            | b'^'
            | b'&'
            | b'='
            | b'<'
            | b'>'
            | b'{'
            | b'}'
            | b' '
            | b'%'
            | b'!'
            | b'"'
            | b'\''
            | b'\n'
            | b'\r'
            | b'\t'
    )
}

/// Refuse a shared master formula (with followers) whose text contains a
/// sheet qualifier, quoted or not, that [`shifted_by_shared_expansion`]
/// would alter. Excel string literals (with `""` escapes) are skipped.
pub(super) fn validate(formula: &str, location: &str) -> Result<(), IoError> {
    if !formula.contains('!') {
        return Ok(());
    }
    let refuse = |qualifier: &str| {
        unsupported(
            "shared formula sheet qualifier would be rewritten by shared-formula expansion",
            format!("{location} qualifier {qualifier}"),
        )
    };
    let b = formula.as_bytes();
    let mut i = 0;
    let mut segment_start = 0;
    while i < b.len() {
        match b[i] {
            b'"' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'"' {
                        if b.get(i + 1) == Some(&b'"') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
                segment_start = i;
            }
            b'\'' => {
                let start = i;
                i += 1;
                while i < b.len() {
                    if b[i] == b'\'' {
                        if b.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                if i < b.len() && b.get(i + 1) == Some(&b'!') {
                    let qualifier = &formula[start..i + 2];
                    if shifted_by_shared_expansion(qualifier) {
                        return Err(refuse(qualifier));
                    }
                    i += 1;
                }
                i += 1;
                segment_start = i;
            }
            b'!' => {
                let mut start = i;
                while start > segment_start && !delimiter(b[start - 1]) {
                    start -= 1;
                }
                let qualifier = &formula[start..=i];
                if start < i && shifted_by_shared_expansion(qualifier) {
                    return Err(refuse(qualifier));
                }
                i += 1;
                segment_start = i;
            }
            _ => i += 1,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{shifted_by_shared_expansion as shifted, validate};
    #[test]
    fn common_sheet_names_are_not_shifted() {
        for q in [
            "'Sheet 1'!",
            "'Data'!",
            "'Summary'!",
            "Sheet1!",
            "Sheet2!",
            "'It''s'!",
            "'2024'!",
            "'Données'!",
            "#REF!",
        ] {
            assert!(!shifted(q), "{q}");
        }
        for q in [
            "'Q1'!",
            "'FY2024'!",
            "'Q1 2024'!",
            "'Q1 Sales'!",
            "'H1'!",
            "ABC1!",
            "'x\"y'!",
            "'Q1:Q4'!",
            "'a1'!",
        ] {
            assert!(shifted(q), "{q}");
        }
    }
    #[test]
    fn validation_skips_string_literals_and_scans_every_qualifier() {
        assert!(validate("IF(\"'Q1'!\"=\"\",0,A1)", "A1").is_ok());
        assert!(validate("Sheet2!A1+'Sheet 1'!B2", "A1").is_ok());
        assert!(validate("Sheet2!A1+'Q1'!B2", "A1").is_err());
        assert!(validate("SUM(FY2024!A1)", "A1").is_err());
        assert!(validate("\"a\"\"b\"&'x\"y'!A1", "A1").is_err());
        assert!(validate("'Données'!A1+1", "A1").is_ok());
    }
}
