use std::sync::OnceLock;

/// The calculation-relevant class of an Excel number-format code.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormatClass {
    General,
    Number { decimals: u8, thousands: bool },
    Date,
    Time,
    DateTime,
    Duration,
    Percent { decimals: u8 },
    Currency { decimals: u8 },
    Text,
    Scientific,
    Fraction,
    Other,
}

/// A classified Excel number-format code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumberFormat {
    code: Box<str>,
    class: FormatClass,
}

impl NumberFormat {
    /// Classify a format code. Parsing is total; unsupported codes are `Other`.
    pub fn parse(code: &str) -> Self {
        let code = canonicalize(code);
        let class = classify(&code);
        Self { code, class }
    }

    pub fn class(&self) -> &FormatClass {
        &self.class
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    /// Return an OOXML built-in number format (ids 0 through 49).
    pub fn builtin(id: u16) -> Option<&'static NumberFormat> {
        builtin_code(id).map(|_| {
            static BUILTINS: OnceLock<Vec<Option<NumberFormat>>> = OnceLock::new();
            BUILTINS
                .get_or_init(|| {
                    (0..=49)
                        .map(|candidate| builtin_code(candidate).map(NumberFormat::parse))
                        .collect()
                })
                .get(id as usize)
                .and_then(Option::as_ref)
                .expect("known builtin id")
        })
    }
}

fn canonicalize(code: &str) -> Box<str> {
    code.trim().into()
}

fn decimal_count(section: &str) -> u8 {
    let visible = visible_format(section);
    let Some(dot) = visible.find('.') else {
        return 0;
    };
    visible[dot + 1..]
        .chars()
        .take_while(|ch| matches!(ch, '0' | '#' | '?'))
        .count()
        .min(u8::MAX as usize) as u8
}

fn visible_format(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    let mut chars = code.chars().peekable();
    let mut quoted = false;
    while let Some(ch) = chars.next() {
        if quoted {
            if ch == '"' {
                quoted = false;
            }
            continue;
        }
        match ch {
            '"' => quoted = true,
            '\\' | '_' | '*' => {
                chars.next();
            }
            '[' => {
                let mut bracket = String::new();
                for next in chars.by_ref() {
                    if next == ']' {
                        break;
                    }
                    bracket.push(next);
                }
                let lower = bracket.to_ascii_lowercase();
                if lower.chars().all(|c| matches!(c, 'h' | 'm' | 's' | ':'))
                    && lower.chars().any(|c| matches!(c, 'h' | 'm' | 's'))
                {
                    out.push('[');
                    out.push_str(&lower);
                    out.push(']');
                }
            }
            _ => out.push(ch.to_ascii_lowercase()),
        }
    }
    out
}

fn classify(code: &str) -> FormatClass {
    if code.eq_ignore_ascii_case("general") {
        return FormatClass::General;
    }
    let first = code.split(';').next().unwrap_or(code);
    let visible = visible_format(first);
    if visible.trim() == "@" {
        return FormatClass::Text;
    }
    if visible.contains("[h]") || visible.contains("[m]") || visible.contains("[s]") {
        return FormatClass::Duration;
    }

    let am_pm = visible.contains("am/pm") || visible.contains("a/p");
    let has_year = visible.contains('y');
    let has_day = visible.contains('d');
    let has_hour = visible.contains('h');
    let has_second = visible.contains('s');
    // `m` is a month beside date tokens and minutes beside time tokens.
    let has_date = has_year || has_day;
    let has_time = has_hour || has_second || am_pm;
    if has_date && has_time {
        return FormatClass::DateTime;
    }
    if has_date {
        return FormatClass::Date;
    }
    if has_time {
        return FormatClass::Time;
    }
    if visible.contains('%') {
        return FormatClass::Percent {
            decimals: decimal_count(first),
        };
    }
    if visible.contains("e+") || visible.contains("e-") {
        return FormatClass::Scientific;
    }
    if visible.contains('/') && visible.chars().any(|ch| ch == '?' || ch == '#') {
        return FormatClass::Fraction;
    }
    let currency = visible.contains('$')
        || visible.contains('€')
        || visible.contains('£')
        || visible.contains('¥');
    if currency {
        return FormatClass::Currency {
            decimals: decimal_count(first),
        };
    }
    if visible.chars().any(|ch| matches!(ch, '0' | '#' | '?')) {
        return FormatClass::Number {
            decimals: decimal_count(first),
            thousands: visible.contains(','),
        };
    }
    FormatClass::Other
}

/// OOXML built-in format codes. IDs 23-36 are locale-dependent/reserved.
pub fn builtin_code(id: u16) -> Option<&'static str> {
    match id {
        0 => Some("General"),
        1 => Some("0"),
        2 => Some("0.00"),
        3 => Some("#,##0"),
        4 => Some("#,##0.00"),
        5 => Some("$#,##0_);($#,##0)"),
        6 => Some("$#,##0_);[Red]($#,##0)"),
        7 => Some("$#,##0.00_);($#,##0.00)"),
        8 => Some("$#,##0.00_);[Red]($#,##0.00)"),
        9 => Some("0%"),
        10 => Some("0.00%"),
        11 => Some("0.00E+00"),
        12 => Some("# ?/?"),
        13 => Some("# ??/??"),
        14 => Some("m/d/yy"),
        15 => Some("d-mmm-yy"),
        16 => Some("d-mmm"),
        17 => Some("mmm-yy"),
        18 => Some("h:mm AM/PM"),
        19 => Some("h:mm:ss AM/PM"),
        20 => Some("h:mm"),
        21 => Some("h:mm:ss"),
        22 => Some("m/d/yy h:mm"),
        37 => Some("#,##0_);(#,##0)"),
        38 => Some("#,##0_);[Red](#,##0)"),
        39 => Some("#,##0.00_);(#,##0.00)"),
        40 => Some("#,##0.00_);[Red](#,##0.00)"),
        41 => Some("_(* #,##0_);_(* (#,##0);_(* \"-\"_);_(@_)"),
        42 => Some("_($* #,##0_);_($* (#,##0);_($* \"-\"_);_(@_)"),
        43 => Some("_(* #,##0.00_);_(* (#,##0.00);_(* \"-\"??_);_(@_)"),
        44 => Some("_($* #,##0.00_);_($* (#,##0.00);_($* \"-\"??_);_(@_)"),
        45 => Some("mm:ss"),
        46 => Some("[h]:mm:ss"),
        47 => Some("mmss.0"),
        48 => Some("##0.0E+0"),
        49 => Some("@"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_temporal_codes_without_being_fooled_by_literals() {
        assert_eq!(
            NumberFormat::parse("yyyy-mm-dd").class(),
            &FormatClass::Date
        );
        assert_eq!(NumberFormat::parse("h:mm:ss").class(), &FormatClass::Time);
        assert_eq!(
            NumberFormat::parse("yyyy-mm-dd hh:mm").class(),
            &FormatClass::DateTime
        );
        assert_eq!(
            NumberFormat::parse("[h]:mm:ss").class(),
            &FormatClass::Duration
        );
        assert_eq!(
            NumberFormat::parse("0.00 \"days\"").class(),
            &FormatClass::Number {
                decimals: 2,
                thousands: false
            }
        );
    }

    #[test]
    fn classifies_non_temporal_codes() {
        assert_eq!(
            NumberFormat::parse("General").class(),
            &FormatClass::General
        );
        assert_eq!(NumberFormat::parse("@").class(), &FormatClass::Text);
        assert_eq!(
            NumberFormat::parse("0.00%").class(),
            &FormatClass::Percent { decimals: 2 }
        );
        assert_eq!(
            NumberFormat::parse("0.00E+00").class(),
            &FormatClass::Scientific
        );
        assert_eq!(
            NumberFormat::parse("# ??/??").class(),
            &FormatClass::Fraction
        );
        assert_eq!(
            NumberFormat::parse("$#,##0.00").class(),
            &FormatClass::Currency { decimals: 2 }
        );
    }

    #[test]
    fn builtin_table_has_expected_generic_classes() {
        assert_eq!(
            NumberFormat::builtin(0).unwrap().class(),
            &FormatClass::General
        );
        assert_eq!(
            NumberFormat::builtin(14).unwrap().class(),
            &FormatClass::Date
        );
        assert_eq!(
            NumberFormat::builtin(21).unwrap().class(),
            &FormatClass::Time
        );
        assert_eq!(
            NumberFormat::builtin(22).unwrap().class(),
            &FormatClass::DateTime
        );
        assert_eq!(
            NumberFormat::builtin(46).unwrap().class(),
            &FormatClass::Duration
        );
        assert_eq!(
            NumberFormat::builtin(49).unwrap().class(),
            &FormatClass::Text
        );
        assert!(NumberFormat::builtin(23).is_none());
        assert!(NumberFormat::builtin(50).is_none());
    }
}
