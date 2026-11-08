//! `ParserError::position` is a byte offset into the formula source, the
//! same unit as `TokenizerError::pos` and the operator errors pinned in
//! `operators.rs`.

use formualizer_parse::parse;

#[test]
fn parser_error_position_is_a_byte_offset() {
    for (formula, position) in [
        // A trailing operand: the offset of that operand.
        ("=1 2", 3),
        ("=IF(1,2)3", 8),
        // `é` is two bytes, so the byte offset is one past the character count.
        ("=LEN(\"é\") 2", 11),
        // The end of the input: the source length.
        ("=1+", 3),
        ("=SUM(1,2)+", 10),
        // A ragged array: the offset of its closing brace.
        ("={1,2;3}", 7),
    ] {
        let error = parse(formula).unwrap_err();
        assert_eq!(error.position, Some(position), "{formula}: {error}");
    }
}
