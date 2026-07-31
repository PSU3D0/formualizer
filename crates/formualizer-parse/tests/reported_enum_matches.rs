//! These integration tests compile as an external downstream crate. The wildcard
//! arms prove the required match shape; assertions cover only variants that exist now.

use formualizer_parse::{ParsingError, RecoveryAction};

fn recovery_label(value: RecoveryAction) -> &'static str {
    match value {
        RecoveryAction::SkippedUnmatchedCloser => "skipped_unmatched_closer",
        _ => "other",
    }
}

fn parsing_error_label(value: ParsingError) -> &'static str {
    match value {
        ParsingError::InvalidReference(_) => "invalid_reference",
        _ => "other",
    }
}

#[test]
fn external_wildcard_matches_compile_and_map_known_variants() {
    assert_eq!(
        recovery_label(RecoveryAction::SkippedUnmatchedCloser),
        "skipped_unmatched_closer"
    );
    assert_eq!(
        parsing_error_label(ParsingError::InvalidReference("A0".to_string())),
        "invalid_reference"
    );
}
