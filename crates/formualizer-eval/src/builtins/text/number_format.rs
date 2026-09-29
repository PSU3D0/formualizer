//! Excel number-format rendering for `TEXT`.
//!
//! This covers the digit-placeholder formats: `0` and `#` placeholders, the
//! decimal point, thousands grouping, trailing-comma scaling, `%`, literal
//! text (quoted, backslash-escaped or bare), colour tags and up to three
//! `;`-separated sections. Codes outside that surface (scientific, fractions,
//! `?` alignment, `*` fill, `_` padding, conditions, locale tags, the text
//! section, dates and times) return [`Fallback::Unsupported`], and `TEXT`
//! keeps its previous rendering for them.
//!
//! Digits come from the same 15-significant-digit decimal view that `ROUND`
//! uses, so display rounding matches `ROUND` (half away from zero on that
//! view) and never expands the exact binary value.

use crate::builtins::math::numeric::fifteen_digit_view;

/// Excel returns `#VALUE!` when the rendered text would be longer than this.
const MAX_RENDERED_CHARS: usize = 255;

/// Why the renderer produced no text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fallback {
    /// Syntax outside the supported surface; `TEXT` keeps its previous path.
    Unsupported,
    /// A format or result for which Excel returns `#VALUE!`.
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Placeholder(char),
    Decimal,
    Comma,
    Percent,
    Literal(char),
}

#[derive(Debug, PartialEq, Eq)]
enum Section {
    /// A section with no digit placeholder renders its literal text.
    Literal(String),
    Number(NumberSection),
}

#[derive(Debug, PartialEq, Eq)]
struct NumberSection {
    prefix: String,
    suffix: String,
    min_integer_digits: usize,
    has_decimal_point: bool,
    min_fraction_digits: usize,
    max_fraction_digits: usize,
    grouping: bool,
    percent_count: usize,
    comma_scale_count: usize,
}

/// Renders `value` with the number-format `code`.
pub(super) fn format_number(value: f64, code: &str) -> Result<String, Fallback> {
    if !value.is_finite() {
        return Err(Fallback::Unsupported);
    }
    let raw_sections = split_sections(code)?;
    match raw_sections.len() {
        1..=3 => {}
        // The fourth section formats text; Excel rejects a fifth.
        4 => return Err(Fallback::Unsupported),
        _ => return Err(Fallback::Invalid),
    }
    let sections = raw_sections
        .into_iter()
        .map(parse_section)
        .collect::<Result<Vec<_>, _>>()?;

    // The section is chosen by the sign of the unrounded value.
    let (index, automatic_minus) = if value < 0.0 {
        if sections.len() >= 2 {
            (1, false)
        } else {
            (0, true)
        }
    } else if value == 0.0 && sections.len() >= 3 {
        (2, false)
    } else {
        (0, false)
    };
    let rendered = match &sections[index] {
        // A single literal section still shows the minus sign
        // (`TEXT(-5,"cat")` is `-cat`).
        Section::Literal(text) if automatic_minus => format!("-{text}"),
        Section::Literal(text) => text.clone(),
        Section::Number(section) => render_number(section, value.abs(), automatic_minus)?,
    };
    if rendered.chars().count() > MAX_RENDERED_CHARS {
        return Err(Fallback::Invalid);
    }
    Ok(rendered)
}

/// Splits on `;` outside quotes, escapes and brackets.
fn split_sections(code: &str) -> Result<Vec<&str>, Fallback> {
    let mut sections = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    let mut bracketed = false;
    for (index, ch) in code.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted {
            if ch == '"' {
                quoted = false;
            }
            continue;
        }
        if bracketed {
            if ch == ']' {
                bracketed = false;
            }
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '"' => quoted = true,
            '[' => bracketed = true,
            ';' => {
                sections.push(&code[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    if quoted || escaped {
        // Excel: `TEXT(5,"0""")` and `TEXT(5,"0\")` are `#VALUE!`.
        return Err(Fallback::Invalid);
    }
    if bracketed {
        return Err(Fallback::Unsupported);
    }
    sections.push(&code[start..]);
    Ok(sections)
}

/// Letters that are date, time or exponent codes. Unquoted beside a digit
/// placeholder they make Excel return `#VALUE!` (`TEXT(5,"0 d")`,
/// `TEXT(5,"s0")`); other letters are literal (`TEXT(5,"0 x")` is `5 x`).
fn is_code_letter(ch: char) -> bool {
    matches!(
        ch.to_ascii_lowercase(),
        'b' | 'd' | 'e' | 'g' | 'h' | 'm' | 'n' | 's' | 'y'
    )
}

fn parse_section(code: &str) -> Result<Section, Fallback> {
    let mut units = Vec::with_capacity(code.len());
    let mut code_letter = false;
    let mut colour = false;
    // The previous character was an unquoted `s` (a seconds code).
    let mut after_seconds = false;
    let mut chars = code.chars().peekable();
    while let Some(ch) = chars.next() {
        let seconds_code = std::mem::replace(&mut after_seconds, false);
        match ch {
            '"' => {
                for literal in chars.by_ref() {
                    if literal == '"' {
                        break;
                    }
                    units.push(Unit::Literal(literal));
                }
            }
            '\\' => units.push(Unit::Literal(chars.next().ok_or(Fallback::Invalid)?)),
            '[' => {
                let mut tag = String::new();
                for tagged in chars.by_ref() {
                    if tagged == ']' {
                        break;
                    }
                    tag.push(tagged);
                }
                // Colours only; conditions, locale and elapsed-time tags are
                // outside the renderer.
                match colour_tag(&tag) {
                    Some(true) => colour = true,
                    // `[Color0]` and `[Color57]` are `#VALUE!`.
                    Some(false) => return Err(Fallback::Invalid),
                    None => return Err(Fallback::Unsupported),
                }
            }
            '0' | '#' => units.push(Unit::Placeholder(ch)),
            '.' => {
                // `s.0` is a fractional-seconds time code.
                if seconds_code {
                    return Err(Fallback::Unsupported);
                }
                units.push(Unit::Decimal);
            }
            ',' => units.push(Unit::Comma),
            '%' => units.push(Unit::Percent),
            // `?` alignment, `@` text, `*` fill, `_` padding and `/`
            // fractions or AM/PM are outside the renderer.
            '?' | '@' | '*' | '_' | '/' => return Err(Fallback::Unsupported),
            'e' | 'E' if matches!(chars.peek(), Some('+' | '-')) => {
                return Err(Fallback::Unsupported);
            }
            other => {
                code_letter |= is_code_letter(other);
                after_seconds = matches!(other, 's' | 'S');
                units.push(Unit::Literal(other));
            }
        }
    }

    let Some(first) = units
        .iter()
        .position(|unit| matches!(unit, Unit::Placeholder(_)))
    else {
        // No digit placeholder: the section is literal text (`TEXT(5,"cat")`
        // is `cat`), unless it holds codes this renderer does not own, such
        // as a date, `General`, or a bare colour. A `%` here is literal
        // (`TEXT(5,"%")` is `%`).
        if code_letter || (colour && units.is_empty()) {
            return Err(Fallback::Unsupported);
        }
        return units
            .iter()
            .map(|unit| match unit {
                Unit::Literal(ch) => Ok(*ch),
                Unit::Percent => Ok('%'),
                _ => Err(Fallback::Unsupported),
            })
            .collect::<Result<String, _>>()
            .map(Section::Literal);
    };
    if code_letter {
        return Err(Fallback::Invalid);
    }
    let last = units
        .iter()
        .rposition(|unit| matches!(unit, Unit::Placeholder(_)))
        .unwrap_or(first);

    // The decimal point is the first `.`. It may sit inside the placeholder
    // run, or directly before or after it (`.00`, `0.`).
    let decimal = units.iter().position(|unit| matches!(unit, Unit::Decimal));
    if decimal.is_some_and(|index| index + 1 < first || index > last + 1) {
        return Err(Fallback::Unsupported);
    }
    // Between the first and last placeholder only placeholders, the one
    // decimal point and grouping commas may appear.
    for (offset, unit) in units[first..=last].iter().enumerate() {
        match unit {
            Unit::Placeholder(_) => {}
            Unit::Decimal if Some(first + offset) == decimal => {}
            Unit::Comma if decimal.is_none_or(|index| first + offset < index) => {}
            _ => return Err(Fallback::Unsupported),
        }
    }

    let run_end = match decimal {
        Some(index) if index > last => index + 1,
        _ => last + 1,
    };
    let run_start = match decimal {
        Some(index) if index < first => index,
        _ => first,
    };
    // Commas straight after the run scale by 1,000 each (`0.0,,`).
    let comma_scale_count = units[run_end..]
        .iter()
        .take_while(|unit| matches!(unit, Unit::Comma))
        .count();
    let prefix = affix(&units[..run_start])?;
    let suffix = affix(&units[run_end + comma_scale_count..])?;

    let (integer, fraction) = match decimal {
        Some(index) if index >= first => {
            (&units[first..index], &units[index + 1..=last.max(index)])
        }
        Some(_) => (&units[0..0], &units[first..=last]),
        None => (&units[first..=last], &units[0..0]),
    };
    let zeros = |part: &[Unit]| {
        part.iter()
            .filter(|unit| matches!(unit, Unit::Placeholder('0')))
            .count()
    };
    Ok(Section::Number(NumberSection {
        prefix,
        suffix,
        min_integer_digits: zeros(integer),
        has_decimal_point: decimal.is_some(),
        min_fraction_digits: zeros(fraction),
        max_fraction_digits: fraction
            .iter()
            .filter(|unit| matches!(unit, Unit::Placeholder(_)))
            .count(),
        grouping: integer.iter().any(|unit| matches!(unit, Unit::Comma)),
        percent_count: units
            .iter()
            .filter(|unit| matches!(unit, Unit::Percent))
            .count(),
        comma_scale_count,
    }))
}

/// `Some(true)` for a colour tag, `Some(false)` for a `[ColorN]` outside
/// 1 to 56, `None` for any other tag.
fn colour_tag(tag: &str) -> Option<bool> {
    let lower = tag.to_ascii_lowercase();
    if let Some(index) = lower.strip_prefix("color")
        && !index.is_empty()
        && index.bytes().all(|digit| digit.is_ascii_digit())
    {
        return Some(
            index
                .parse::<u16>()
                .is_ok_and(|index| (1..=56).contains(&index)),
        );
    }
    matches!(
        lower.as_str(),
        "black" | "blue" | "cyan" | "green" | "magenta" | "red" | "white" | "yellow"
    )
    .then_some(true)
}

/// Literal text around the digits. `%` renders as itself; a `.` after the
/// decimal point is punctuation (`0.00%.`).
fn affix(units: &[Unit]) -> Result<String, Fallback> {
    let mut out = String::new();
    for unit in units {
        match unit {
            Unit::Literal(ch) => out.push(*ch),
            Unit::Percent => out.push('%'),
            Unit::Decimal => out.push('.'),
            _ => return Err(Fallback::Unsupported),
        }
    }
    Ok(out)
}

fn render_number(
    section: &NumberSection,
    magnitude: f64,
    automatic_minus: bool,
) -> Result<String, Fallback> {
    let mut scaled = magnitude;
    for _ in 0..section.percent_count {
        scaled *= 100.0;
    }
    for _ in 0..section.comma_scale_count {
        scaled /= 1_000.0;
    }
    if !scaled.is_finite() {
        return Err(Fallback::Invalid);
    }
    let (mut integer, mut fraction) = decimal_digits(scaled, section.max_fraction_digits);
    // No minus sign when the value rounds to zero (`TEXT(-0.004,"0.00")` is
    // `0.00`).
    let is_zero = integer == "0" && fraction.bytes().all(|digit| digit == b'0');

    if integer == "0" && section.min_integer_digits == 0 {
        integer.clear();
    }
    if integer.len() < section.min_integer_digits {
        integer.insert_str(0, &"0".repeat(section.min_integer_digits - integer.len()));
    }
    if section.grouping && !integer.is_empty() {
        integer = group_thousands(&integer);
    }
    while fraction.len() > section.min_fraction_digits && fraction.ends_with('0') {
        fraction.pop();
    }

    let mut out = String::with_capacity(
        section.prefix.len() + integer.len() + fraction.len() + section.suffix.len() + 2,
    );
    if automatic_minus && !is_zero {
        out.push('-');
    }
    out.push_str(&section.prefix);
    out.push_str(&integer);
    if section.has_decimal_point {
        out.push('.');
        out.push_str(&fraction);
    }
    out.push_str(&section.suffix);
    Ok(out)
}

/// The integer digits and exactly `fraction_digits` fraction digits of
/// `magnitude`, rounded half away from zero on its 15-significant-digit
/// decimal view. Digits past the view are zeros.
fn decimal_digits(magnitude: f64, fraction_digits: usize) -> (String, String) {
    let units = if magnitude == 0.0 {
        "0".to_string()
    } else {
        // `magnitude` is about `coefficient * 10^exponent`.
        let (coefficient, exponent) = fifteen_digit_view(magnitude, true);
        let unit_exponent = -(fraction_digits as i64);
        if exponent >= unit_exponent {
            let mut units = coefficient.to_string();
            units.push_str(&"0".repeat((exponent - unit_exponent) as usize));
            units
        } else {
            let discarded = unit_exponent - exponent;
            if discarded > 15 {
                // The whole view is below half a unit.
                "0".to_string()
            } else {
                let unit = 10_u64.pow(discarded as u32);
                let carry = (coefficient % unit) * 2 >= unit;
                (coefficient / unit + u64::from(carry)).to_string()
            }
        }
    };
    // `units` counts multiples of 10^-fraction_digits.
    if units.len() > fraction_digits {
        let split = units.len() - fraction_digits;
        (units[..split].to_string(), units[split..].to_string())
    } else {
        let mut fraction = "0".repeat(fraction_digits - units.len());
        fraction.push_str(&units);
        ("0".to_string(), fraction)
    }
}

fn group_thousands(integer: &str) -> String {
    let mut out = String::with_capacity(integer.len() + integer.len() / 3);
    for (index, ch) in integer.chars().enumerate() {
        if index > 0 && (integer.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: f64, code: &str) -> String {
        format_number(value, code).unwrap_or_else(|error| panic!("{value} {code:?}: {error:?}"))
    }

    #[test]
    fn placeholders_decimal_point_and_grouping() {
        let cases = [
            (7.0, "0000", "0007"),
            (0.5, "#.##", ".5"),
            (0.0, "#", ""),
            (0.0, "0", "0"),
            (3.0, "#.00", "3.00"),
            (0.5, "0.0#", "0.5"),
            (0.567, "0.0#", "0.57"),
            (0.5, ".00", ".50"),
            (5.0, "0.#", "5."),
            (0.0, "#.##", "."),
            (10.0, "#.#", "10."),
            (12345.6, "#,##0.00", "12,345.60"),
            (1234567.0, "#,##0", "1,234,567"),
            (1234.5, "0,0.0", "1,234.5"),
            (12.0, "#,###", "12"),
            (0.0, "#,###", ""),
            (999.999, "#,##0.00", "1,000.00"),
        ];
        for (value, code, expected) in cases {
            assert_eq!(text(value, code), expected, "{value} {code:?}");
        }
    }

    #[test]
    fn percent_and_trailing_commas_scale() {
        let cases = [
            (0.0975, "0.00%", "9.75%"),
            (0.256, "0%", "26%"),
            (5.0, "0%%", "50000%%"),
            (5.0, "%0", "%500"),
            (5.0, "0 %", "500 %"),
            (12_200_000.0, "0.0,,", "12.2"),
            (1_234_567.0, "#,##0,", "1,235"),
            (999.0, "0,", "1"),
            (1234.5, "#,##0.00,", "1.23"),
        ];
        for (value, code, expected) in cases {
            assert_eq!(text(value, code), expected, "{value} {code:?}");
        }
    }

    #[test]
    fn literals() {
        let cases = [
            (0.09, "0.00% Cap", "9.00% Cap"),
            (0.09, r"0.00\%", "0.09%"),
            (0.09, r#"0.00"%""#, "0.09%"),
            (0.09, r#""Cap "0.00%"#, "Cap 9.00%"),
            (7.0, r#"0";units""#, "7;units"),
            (0.001, "0.00%.", "0.10%."),
            (5.0, "$0.00", "$5.00"),
            (5.0, "0 x", "5 x"),
            (5.0, "(0)", "(5)"),
            (5.0, "0-", "5-"),
            (5.0, "+0", "+5"),
            (5.0, "cat", "cat"),
            (5.0, r#""abc""#, "abc"),
            (5.0, "%", "%"),
            (7.0, r"0\;x", "7;x"),
            (-7.0, r"0\;x", "-7;x"),
            (1.0, r#"0"m""#, "1m"),
            (1.0, r"0\m", "1m"),
        ];
        for (value, code, expected) in cases {
            assert_eq!(text(value, code), expected, "{value} {code:?}");
        }
    }

    #[test]
    fn sections_and_signs() {
        let cases = [
            (-1234.5, "#,##0.00;[Red](#,##0.00)", "(1,234.50)"),
            (0.0, r#"0.00;[Red]-0.00;"zero""#, "zero"),
            (0.004, r#"0.00;[Red]-0.00;"zero""#, "0.00"),
            (-0.004, "0.00;(0.00)", "(0.00)"),
            (-1.0, "0;;", ""),
            (0.0, "0;-0;", ""),
            (-5.0, "0;-", "-"),
            (-5.0, "0;cat", "cat"),
            // One literal section keeps the minus sign.
            (-5.0, "cat", "-cat"),
            (-0.4, "cat", "-cat"),
            (-5.0, "$", "-$"),
            (-5.0, r#""abc""#, "-abc"),
            (1.0, "[Blue]0.0", "1.0"),
            (5.0, "[Color10]0", "5"),
            (-1.0, "0.0;[Red]0.0", "1.0"),
            (-5.0, "[Red]-0", "--5"),
            (-5.0, "$#,##0.00", "-$5.00"),
            (-0.5, "0%;(0%)", "(50%)"),
            (-0.5, "0", "-1"),
            // A value that rounds to zero loses its minus sign.
            (-0.004, "0.00", "0.00"),
            (-0.4, "#", ""),
            (-0.4, "$0", "$0"),
            (-400.0, "0,", "0"),
            (-0.00004, "0.00%", "0.00%"),
        ];
        for (value, code, expected) in cases {
            assert_eq!(text(value, code), expected, "{value} {code:?}");
        }
    }

    #[test]
    fn rounding_uses_the_fifteen_digit_view() {
        let cases = [
            // Below the binary midpoint, but the decimal view is a tie.
            (1.005, "0.00", "1.01"),
            (0.145, "0.00", "0.15"),
            (2.675, "0.00", "2.68"),
            (8.835, "0.00", "8.84"),
            (1.225, "0.00", "1.23"),
            (2.5, "0", "3"),
            (-2.5, "0", "-3"),
            // Digits past the fifteenth significant digit are zeros.
            (0.1 + 0.2, "0.00000000000000000", "0.30000000000000000"),
            (
                1.0 / 3.0,
                "0.00000000000000000000",
                "0.33333333333333300000",
            ),
            (
                2.0 / 3.0,
                "0.00000000000000000000",
                "0.66666666666666700000",
            ),
            (2.0 / 3.0 * 1e20, "0", "66666666666666700000"),
            (1e20, "0.00", "100000000000000000000.00"),
            (1.23e-10, "0.000000000000", "0.000000000123"),
        ];
        for (value, code, expected) in cases {
            assert_eq!(text(value, code), expected, "{value} {code:?}");
        }
        let hundred = text(1e100, "0");
        assert_eq!(hundred, format!("1{}", "0".repeat(100)));
    }

    #[test]
    fn results_longer_than_255_characters_are_value_errors() {
        assert_eq!(text(1e254, "0").len(), 255);
        assert_eq!(text(1e252, "0").len(), 253);
        assert_eq!(text(1.0, &"0".repeat(40)), format!("{}1", "0".repeat(39)));
        for (value, code) in [
            (1e255, "0".to_string()),
            (1e252, "0.00".to_string()),
            (1e307, "0.00".to_string()),
            (-1e307, "0.00".to_string()),
            (1e307, "0.00%".to_string()),
            (f64::MAX, "0.00".to_string()),
            (1.0, "0".repeat(300)),
        ] {
            assert_eq!(
                format_number(value, &code),
                Err(Fallback::Invalid),
                "{value:e} {code:?}"
            );
        }
    }

    #[test]
    fn code_letters_beside_placeholders_are_value_errors() {
        for letter in ['b', 'd', 'e', 'g', 'h', 'm', 'n', 's', 'y'] {
            for code in [
                format!("0 {letter}"),
                format!("{letter}0"),
                format!("0 {}", letter.to_ascii_uppercase()),
            ] {
                assert_eq!(
                    format_number(5.0, &code),
                    Err(Fallback::Invalid),
                    "{code:?}"
                );
            }
        }
        for code in ["0m", "0.00m", "0 mm", "0 units", "0 kg", "0E0"] {
            assert_eq!(format_number(1.0, code), Err(Fallback::Invalid), "{code:?}");
        }
        for letter in "acfijklopqrtuvwxzACFIJKLOPQRTUVWXZ".chars() {
            assert_eq!(text(5.0, &format!("0 {letter}")), format!("5 {letter}"));
        }
    }

    #[test]
    fn malformed_codes_are_value_errors() {
        for code in [r#"0""#, r"0\", "0;0;0;0;0", "[Color0]0", "[Color57]0"] {
            assert_eq!(format_number(5.0, code), Err(Fallback::Invalid), "{code:?}");
        }
    }

    #[test]
    fn other_codes_are_left_to_the_caller() {
        for code in [
            "0.00E+00",
            "# ?/?",
            "000-000",
            "_(* #,##0.00_)",
            "[>=1]0.0",
            "0;0;0;@",
            "m",
            "mm",
            "yyyy-mm-dd",
            "hh:mm",
            "hh:mm:ss.000",
            "ss.0",
            "[h]:mm",
            "h:mm AM/PM",
            "0 AM/PM",
            "General",
            "0;General",
            "[Red]",
            "0;[Red]",
            "0.0.0",
            ",0",
            "0.0,0",
            "[$-409]0",
        ] {
            assert_eq!(
                format_number(1.0, code),
                Err(Fallback::Unsupported),
                "{code:?}"
            );
        }
    }
}
