use super::*;

fn paths(text: &str) -> ((usize, usize), bool) {
    let mut counts = (0, 0);
    let result = scan_inner::<true>(
        text.as_bytes(),
        &XlsxRecalculateOptions::default(),
        Mode::Plain,
        (&mut 0, &mut 0),
        &mut false,
        &mut |_| Ok(()),
        &mut counts,
    );
    (counts, result.is_ok())
}

#[test]
fn canonical_tags_take_the_hot_path() {
    let xml = format!(
        "<worksheet xmlns='{}'><sheetData><row r='1'><c r='A1'><f>1</f><v>1</v></c></row></sheetData></worksheet>",
        xml::MAIN
    );
    assert_eq!(paths(&xml), ((4, 2), true));
}

#[test]
fn prefixes_and_declarations_stay_cold() {
    let xml = format!(
        "<x:worksheet xmlns:x='{}'><x:sheetData><x:row r='1'><x:c r='A1'><x:f>1</x:f><x:v>1</x:v></x:c></x:row></x:sheetData></x:worksheet>",
        xml::MAIN
    );
    assert_eq!(paths(&xml), ((0, 6), true));
    let xml = format!(
        "<worksheet xmlns='{}'><sheetData><row r='1'><c xmlns='{}' r='A1'><f>1</f><v>1</v></c></row></sheetData></worksheet>",
        xml::MAIN,
        xml::MAIN
    );
    // Even a no-op namespace declaration is fully validated on the cold path.
    assert_eq!(paths(&xml), ((3, 3), true));
}

#[test]
fn rebinding_invalidates_the_witness_before_interpretation() {
    for attrs in ["xmlns='urn:foreign'", "xmlns:p='urn:foreign' p:extra='1'"] {
        let xml = format!(
            "<worksheet xmlns='{}'><sheetData><row r='1'><c {attrs} r='A1'/></row></sheetData></worksheet>",
            xml::MAIN
        );
        let (counts, admitted) = paths(&xml);
        assert_eq!(counts, (1, 3));
        assert_eq!(admitted, !attrs.starts_with("xmlns='"));
    }
}
