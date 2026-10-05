//! Excel date and time number-format rendering for `TEXT`.
//!
//! Covers the en-US date and time codes: `d`, `dd`, `ddd`, `dddd`, `m` to
//! `mmmmm`, `y`/`yy`, `yyy`/`yyyy`, `h`, `hh`, `m`/`mm` as minutes (straight
//! after an hour code or straight before a seconds code), `s`, `ss`,
//! fractional seconds (`ss.0` to `ss.000`), `AM/PM`, `A/P`, the elapsed-time
//! tags `[h]`, `[m]` and `[s]`, colour and locale tags, quoted text,
//! backslash escapes and bare literal characters.
//!
//! The value is rounded to the smallest unit the format can show (a whole
//! second, or the fractional-second digits) before it is split into parts, so
//! `23:59:59.6` formats as the next day's `00:00:00`; minutes and hours are
//! never rounded on their own (`h:mm` of `10:29:45` is `10:29`).
//!
//! Codes outside that surface (era and Buddhist years, `General`, digit
//! placeholders other than fractional seconds, fill and padding, conditions
//! and the system date tags) return [`Fallback::Unsupported`].

use super::number_format::{Fallback, colour_tag, split_sections};
use formualizer_common::{DateSystem, try_serial_to_display_date_parts_for};

const SECONDS_PER_DAY: u64 = 86_400;

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

/// Indexed by `serial mod 7` in the 1900 system (serial 0 is a Saturday).
const WEEKDAYS: [&str; 7] = [
    "Saturday",
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
];

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Literal(String),
    Year(usize),
    /// `m` run; `m`/`mm` may turn into minutes once neighbours are known.
    Month(usize),
    Minute(usize),
    Day(usize),
    Hour(usize),
    Second(usize),
    /// Fractional-second digits after a seconds code (`ss.00`).
    Fraction(usize),
    ElapsedHours(usize),
    ElapsedMinutes(usize),
    ElapsedSeconds(usize),
    /// `AM/PM` (always upper case).
    AmPm,
    /// `A/P`; the two letters keep the case written in the code.
    AP(char, char),
}

impl Token {
    fn is_code(&self) -> bool {
        !matches!(self, Token::Literal(_))
    }
}

/// Renders `value` with a date or time format `code` in `system`.
///
/// Returns [`Fallback::Unsupported`] when the selected section holds no date
/// or time code, so the caller can try its other renderers.
pub(super) fn format_datetime(
    system: DateSystem,
    value: f64,
    code: &str,
) -> Result<String, Fallback> {
    if !value.is_finite() {
        return Err(Fallback::Unsupported);
    }
    let sections = split_sections(code)?;
    if sections.len() > 4 {
        return Err(Fallback::Invalid);
    }
    let (section, magnitude, automatic_minus) = if value < 0.0 && sections.len() >= 2 {
        (sections[1], -value, false)
    } else if value == 0.0 && sections.len() >= 3 {
        (sections[2], value, false)
    } else {
        (sections[0], value.abs(), value < 0.0)
    };
    let tokens = tokenize(section)?;
    if !tokens.iter().any(Token::is_code) {
        return Err(Fallback::Unsupported);
    }
    // Excel cannot show a negative date or time (`TEXT(-1,"yyyy")` is
    // `#VALUE!`).
    if automatic_minus {
        return Err(Fallback::Invalid);
    }
    render(system, magnitude, &tokens)
}

fn tokenize(section: &str) -> Result<Vec<Token>, Fallback> {
    let chars: Vec<char> = section.chars().collect();
    let mut tokens = Vec::new();
    let mut literal = String::new();
    let mut i = 0;
    let flush = |literal: &mut String, tokens: &mut Vec<Token>| {
        if !literal.is_empty() {
            tokens.push(Token::Literal(std::mem::take(literal)));
        }
    };
    let run = |chars: &[char], start: usize, lower: char| {
        chars[start..]
            .iter()
            .take_while(|ch| ch.to_ascii_lowercase() == lower)
            .count()
    };
    while i < chars.len() {
        let ch = chars[i];
        let lower = ch.to_ascii_lowercase();
        match lower {
            '"' => {
                i += 1;
                while i < chars.len() && chars[i] != '"' {
                    literal.push(chars[i]);
                    i += 1;
                }
                i += 1;
            }
            '\\' => {
                literal.push(*chars.get(i + 1).ok_or(Fallback::Invalid)?);
                i += 2;
            }
            '[' => {
                let close = chars[i..]
                    .iter()
                    .position(|c| *c == ']')
                    .ok_or(Fallback::Unsupported)?;
                let tag: String = chars[i + 1..i + close].iter().collect();
                i += close + 1;
                let tag_lower = tag.to_ascii_lowercase();
                let elapsed =
                    |unit: char| !tag_lower.is_empty() && tag_lower.chars().all(|c| c == unit);
                if elapsed('h') {
                    flush(&mut literal, &mut tokens);
                    tokens.push(Token::ElapsedHours(tag.len()));
                } else if elapsed('m') {
                    flush(&mut literal, &mut tokens);
                    tokens.push(Token::ElapsedMinutes(tag.len()));
                } else if elapsed('s') {
                    flush(&mut literal, &mut tokens);
                    tokens.push(Token::ElapsedSeconds(tag.len()));
                    i = fraction_after_seconds(&chars, i, &mut tokens)?;
                } else if let Some(locale) = tag.strip_prefix('$') {
                    // `[$-409]` selects a locale; `[$USD-409]` also shows its
                    // symbol. The system date and time tags (`[$-F800]`,
                    // `[$-F400]`) replace the whole format.
                    let (symbol, id) = locale.split_once('-').unwrap_or((locale, ""));
                    if id.to_ascii_lowercase().starts_with("f") && id.len() >= 4 {
                        return Err(Fallback::Unsupported);
                    }
                    literal.push_str(symbol);
                } else {
                    match colour_tag(&tag) {
                        Some(true) => {}
                        Some(false) => return Err(Fallback::Invalid),
                        None => return Err(Fallback::Unsupported),
                    }
                }
            }
            'y' | 'd' | 'm' | 'h' => {
                let n = run(&chars, i, lower);
                flush(&mut literal, &mut tokens);
                tokens.push(match lower {
                    'y' => Token::Year(n),
                    'd' => Token::Day(n),
                    'm' => Token::Month(n),
                    _ => Token::Hour(n),
                });
                i += n;
            }
            's' => {
                let n = run(&chars, i, 's');
                flush(&mut literal, &mut tokens);
                tokens.push(Token::Second(n));
                i = fraction_after_seconds(&chars, i + n, &mut tokens)?;
            }
            'a' => {
                let ahead: String = chars[i..chars.len().min(i + 5)].iter().collect();
                if ahead.eq_ignore_ascii_case("am/pm") {
                    flush(&mut literal, &mut tokens);
                    tokens.push(Token::AmPm);
                    i += 5;
                } else if chars.len() >= i + 3
                    && chars[i + 1] == '/'
                    && chars[i + 2].eq_ignore_ascii_case(&'p')
                {
                    flush(&mut literal, &mut tokens);
                    tokens.push(Token::AP(ch, chars[i + 2]));
                    i += 3;
                } else {
                    literal.push(ch);
                    i += 1;
                }
            }
            // Era and Buddhist years, Mac minutes, digit placeholders, text,
            // fill and padding are outside this renderer.
            'e' | 'g' | 'b' | 'n' | '0' | '#' | '?' | '%' | '@' | '*' | '_' => {
                return Err(Fallback::Unsupported);
            }
            _ => {
                literal.push(ch);
                i += 1;
            }
        }
    }
    flush(&mut literal, &mut tokens);
    resolve_minutes(&mut tokens);
    Ok(tokens)
}

/// Fractional-second digits (`.0` to `.000`) straight after a seconds code
/// starting at `i`; returns the index after them.
fn fraction_after_seconds(
    chars: &[char],
    i: usize,
    tokens: &mut Vec<Token>,
) -> Result<usize, Fallback> {
    if chars.get(i) != Some(&'.') || chars.get(i + 1) != Some(&'0') {
        return Ok(i);
    }
    let digits = chars[i + 1..].iter().take_while(|ch| **ch == '0').count();
    if digits > 3 {
        return Err(Fallback::Unsupported);
    }
    tokens.push(Token::Fraction(digits));
    Ok(i + 1 + digits)
}

/// `m` and `mm` are minutes straight after an hour code or straight before a
/// seconds code (literals between them do not count).
fn resolve_minutes(tokens: &mut [Token]) {
    let codes: Vec<usize> = (0..tokens.len()).filter(|&i| tokens[i].is_code()).collect();
    for (pos, &index) in codes.iter().enumerate() {
        let Token::Month(n) = tokens[index] else {
            continue;
        };
        if n > 2 {
            continue;
        }
        let after_hour = pos > 0
            && matches!(
                tokens[codes[pos - 1]],
                Token::Hour(_) | Token::ElapsedHours(_)
            );
        let before_second = codes.get(pos + 1).is_some_and(|&next| {
            matches!(tokens[next], Token::Second(_) | Token::ElapsedSeconds(_))
        });
        if after_hour || before_second {
            tokens[index] = Token::Minute(n);
        }
    }
}

fn pad(value: u64, width: usize) -> String {
    format!("{value:0width$}", width = width.min(2))
}

fn render(system: DateSystem, value: f64, tokens: &[Token]) -> Result<String, Fallback> {
    let digits = tokens
        .iter()
        .filter_map(|token| match token {
            Token::Fraction(n) => Some(*n),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let ticks_per_second = 10_u64.pow(digits as u32);
    let ticks_per_day = SECONDS_PER_DAY * ticks_per_second;

    // Round the time of day to the displayed precision; a carry advances the
    // date.
    let mut days = value.trunc();
    let mut ticks = ((value - days) * ticks_per_day as f64).round() as u64;
    if ticks >= ticks_per_day {
        ticks -= ticks_per_day;
        days += 1.0;
    }
    let parts =
        try_serial_to_display_date_parts_for(system, days).map_err(|_| Fallback::Invalid)?;
    let whole_days = days as u64;
    let weekday = match system {
        DateSystem::Excel1900 => (whole_days % 7) as usize,
        // 1904-01-01 (serial 0) is a Friday.
        DateSystem::Excel1904 => ((whole_days + 6) % 7) as usize,
    };

    let seconds_of_day = ticks / ticks_per_second;
    let fraction = ticks % ticks_per_second;
    let twelve_hour = tokens
        .iter()
        .any(|token| matches!(token, Token::AmPm | Token::AP(..)));
    let hour = seconds_of_day / 3600;
    let pm = hour >= 12;
    let shown_hour = if twelve_hour {
        match hour % 12 {
            0 => 12,
            h => h,
        }
    } else {
        hour
    };
    let elapsed_seconds = whole_days * SECONDS_PER_DAY + seconds_of_day;

    let mut out = String::new();
    for token in tokens {
        match token {
            Token::Literal(text) => out.push_str(text),
            Token::Year(n) if *n <= 2 => out.push_str(&pad(u64::from(parts.year as u32 % 100), 2)),
            Token::Year(_) => out.push_str(&format!("{:04}", parts.year)),
            Token::Month(n) => {
                let name = MONTHS[(parts.month - 1) as usize];
                match n {
                    1 | 2 => out.push_str(&pad(u64::from(parts.month), *n)),
                    3 => out.push_str(&name[..3]),
                    5 => out.push_str(&name[..1]),
                    _ => out.push_str(name),
                }
            }
            Token::Day(n) => match n {
                1 | 2 => out.push_str(&pad(u64::from(parts.day), *n)),
                3 => out.push_str(&WEEKDAYS[weekday][..3]),
                _ => out.push_str(WEEKDAYS[weekday]),
            },
            Token::Hour(n) => out.push_str(&pad(shown_hour, *n)),
            Token::Minute(n) => out.push_str(&pad(seconds_of_day / 60 % 60, *n)),
            Token::Second(n) => out.push_str(&pad(seconds_of_day % 60, *n)),
            Token::Fraction(n) => {
                out.push('.');
                let text = format!("{fraction:0digits$}");
                out.push_str(&text[..*n]);
            }
            Token::ElapsedHours(n) => out.push_str(&pad(elapsed_seconds / 3600, *n)),
            Token::ElapsedMinutes(n) => out.push_str(&pad(elapsed_seconds / 60, *n)),
            Token::ElapsedSeconds(n) => out.push_str(&pad(elapsed_seconds, *n)),
            Token::AmPm => out.push_str(if pm { "PM" } else { "AM" }),
            Token::AP(a, p) => out.push(if pm { *p } else { *a }),
        }
    }
    if out.chars().count() > 255 {
        return Err(Fallback::Invalid);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: f64, code: &str) -> String {
        format_datetime(DateSystem::Excel1900, value, code)
            .unwrap_or_else(|error| panic!("{value} {code:?}: {error:?}"))
    }

    // 37000.41799768519 is 2001-04-19 (a Thursday) 10:01:55.
    const SERIAL: f64 = 37_000.417_997_685_19;

    #[test]
    fn date_codes() {
        let cases = [
            ("mm/dd/yy", "04/19/01"),
            ("mm/dd/yyyy", "04/19/2001"),
            ("m/d/y", "4/19/01"),
            ("yyy", "2001"),
            ("d", "19"),
            ("dd", "19"),
            ("ddd", "Thu"),
            ("dddd", "Thursday"),
            ("ddddd", "Thursday"),
            ("m", "4"),
            ("mm", "04"),
            ("mmm", "Apr"),
            ("mmmm", "April"),
            ("mmmmm", "A"),
            ("MMM D, YYYY", "Apr 19, 2001"),
            ("dddd, mmmm dd, yyyy", "Thursday, April 19, 2001"),
            ("d-mmm-yy", "19-Apr-01"),
            ("yyyy-mm-dd", "2001-04-19"),
        ];
        for (code, expected) in cases {
            assert_eq!(text(SERIAL, code), expected, "{code:?}");
        }
    }

    #[test]
    fn weekday_names_follow_the_1900_serial_calendar() {
        assert_eq!(text(0.0, "dddd"), "Saturday");
        assert_eq!(text(1.0, "dddd"), "Sunday");
        assert_eq!(text(2.0, "dddd"), "Monday");
        assert_eq!(text(6.0, "dddd"), "Friday");
        assert_eq!(text(36_745.0, "ddd"), "Mon");
        assert_eq!(text(60.0, "dddd mm/dd/yyyy"), "Wednesday 02/29/1900");
        assert_eq!(text(0.0, "mm/dd/yyyy"), "01/00/1900");
        assert_eq!(
            format_datetime(DateSystem::Excel1904, 0.0, "dddd yyyy-mm-dd").unwrap(),
            "Friday 1904-01-01"
        );
    }

    #[test]
    fn minutes_versus_months() {
        let cases = [
            ("h:mm", "10:01"),
            ("hh:mm:ss", "10:01:55"),
            ("mm:ss", "01:55"),
            ("m:ss", "1:55"),
            ("h m", "10 1"),
            ("h\"h\" mm\"m\"", "10h 01m"),
            ("mm/dd/yy hh:mm", "04/19/01 10:01"),
            ("hh:mm mm/dd", "10:01 04/19"),
            ("hh mmm", "10 Apr"),
        ];
        for (code, expected) in cases {
            assert_eq!(text(SERIAL, code), expected, "{code:?}");
        }
    }

    #[test]
    fn twelve_hour_clock() {
        assert_eq!(text(SERIAL, "h:mm AM/PM"), "10:01 AM");
        assert_eq!(text(0.75, "h:mm am/pm"), "6:00 PM");
        assert_eq!(text(0.0, "h AM/PM"), "12 AM");
        assert_eq!(text(0.5, "hh:mm:ss A/P"), "12:00:00 P");
        assert_eq!(text(0.25, "h a/p"), "6 a");
    }

    #[test]
    fn rounding_to_the_displayed_precision() {
        // 10:29:45 shows as 10:29 (minutes are not rounded on their own).
        assert_eq!(text(37_800.0 / 86_400.0 - 15.0 / 86_400.0, "h:mm"), "10:29");
        // 23:59:59.6 rounds to the next day's midnight.
        let late = 45_306.0 + 86_399.6 / 86_400.0;
        assert_eq!(text(late, "yyyy-mm-dd hh:mm:ss"), "2024-01-16 00:00:00");
        assert_eq!(text(late, "yyyy-mm-dd hh:mm"), "2024-01-16 00:00");
        assert_eq!(text(late, "hh:mm:ss.0"), "23:59:59.6");
        // A hair below 13:00 is 13:00.
        assert_eq!(text(37_054.541_666_666_664, "h:mm:ss"), "13:00:00");
        assert_eq!(text(0.5 + 1.234 / 86_400.0, "ss.00"), "01.23");
        assert_eq!(text(0.5 + 1.234 / 86_400.0, "ss.000"), "01.234");
        assert_eq!(text(0.5 + 1.234 / 86_400.0, "s.0"), "1.2");
    }

    #[test]
    fn elapsed_time() {
        assert_eq!(text(1.5, "[h]:mm:ss"), "36:00:00");
        assert_eq!(text(1.5, "[mm]:ss"), "2160:00");
        assert_eq!(text(0.001, "[ss].00"), "86.40");
        assert_eq!(text(0.25, "[hh]"), "06");
        assert_eq!(text(1.5, "h:mm"), "12:00");
    }

    #[test]
    fn literals() {
        assert_eq!(text(SERIAL, "\"Day \"d"), "Day 19");
        assert_eq!(text(SERIAL, "\\d\\a\\y d"), "day 19");
        assert_eq!(text(SERIAL, "(yyyy)!$-+^&'~{}<>=:"), "(2001)!$-+^&'~{}<>=:");
        assert_eq!(text(SERIAL, "[Red]yyyy"), "2001");
        assert_eq!(text(SERIAL, "[$-409]mmm yyyy"), "Apr 2001");
        assert_eq!(text(SERIAL, "yyyy.mm.dd"), "2001.04.19");
    }

    #[test]
    fn sections_negatives_and_bounds() {
        assert_eq!(text(SERIAL, "mm/dd/yy;@"), "04/19/01");
        assert_eq!(
            format_datetime(DateSystem::Excel1900, -1.0, "yyyy"),
            Err(Fallback::Invalid)
        );
        assert_eq!(
            format_datetime(DateSystem::Excel1900, 2_958_466.0, "yyyy"),
            Err(Fallback::Invalid)
        );
        assert_eq!(text(2_958_465.0, "yyyy-mm-dd"), "9999-12-31");
    }

    #[test]
    fn other_codes_are_left_to_the_caller() {
        for code in [
            "0.00", "General", "\"abc\"", "yyyy 0", "[$-F800]", "e", "[>1]yyyy",
        ] {
            assert_eq!(
                format_datetime(DateSystem::Excel1900, SERIAL, code),
                Err(Fallback::Unsupported),
                "{code:?}"
            );
        }
    }
}
