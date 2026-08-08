use formualizer_common::{a1_sheet_name_needs_quoting, format_a1_sheet_name};
use std::borrow::Cow;

fn adversarial_names() -> Vec<String> {
    let mut names = vec![
        "",
        "A",
        "Data",
        "123abc",
        "1",
        "999999",
        "0",
        "O'Brien",
        "it''s",
        "'quoted'",
        "'",
        "''",
        "'''",
        " lead",
        "trail ",
        " both ",
        "  ",
        "A1",
        "ZZ99",
        "XFD1048576",
        "R1C1",
        "RC",
        "R",
        "C",
        "A1:B2",
        "A:A",
        "1:1",
        "TRUE",
        "true",
        "TrUe",
        "FALSE",
        "NULL",
        "REF",
        "DIV",
        "NAME",
        "NUM",
        "VALUE",
        "N/A",
        "n/a",
        "TRUEX",
        "XTRUE",
        "REF!",
        "#REF!",
        "Sheet!Name",
        "a\"b",
        "a#b",
        "a$b",
        "a%b",
        "a&b",
        "a(b",
        "a)b",
        "a*b",
        "a+b",
        "a,b",
        "a-b",
        "a.b",
        "a/b",
        "a:b",
        "a;b",
        "a<b",
        "a=b",
        "a>b",
        "a?b",
        "a@b",
        "a[b",
        "a\\b",
        "a]b",
        "a^b",
        "a`b",
        "a{b",
        "a|b",
        "a}b",
        "a~b",
        "a b",
        "a_b",
        "_leading",
        "a1b",
        "Ünïcödé",
        "日本語",
        "😀emoji",
        "Ünïcödé Sheet",
        "naïve'test",
        "Ω",
        "\u{200b}zerowidth",
        "tab\there",
        "new\nline",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    names.push("x".repeat(255));
    names.push("9".repeat(255));
    names.push(format!("{}'{}", "a".repeat(120), "b".repeat(120)));
    for byte in 0x20u8..=0x7e {
        let character = char::from(byte);
        names.push(character.to_string());
        names.push(format!("a{character}b"));
        names.push(format!("{character}ab"));
    }
    names
}

#[test]
fn formatter_ownership_agrees_with_non_allocating_predicate_for_adversarial_corpus() {
    for name in adversarial_names() {
        let formatted = format_a1_sheet_name(&name);
        assert_eq!(
            matches!(formatted, Cow::Owned(_)),
            a1_sheet_name_needs_quoting(&name),
            "sheet name {name:?}"
        );
    }
}
