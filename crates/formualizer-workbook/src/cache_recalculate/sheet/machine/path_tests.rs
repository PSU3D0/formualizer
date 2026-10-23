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
fn raw_literal_provenance_preserves_encoded_and_unsigned_spellings() {
    let xml = format!(
        "<worksheet xmlns='{}'><sheetData><row r='1'><c r='A1' t='&#110;' s='&#49;'><v>+1</v></c><c r='B1' t='s'><v>+1</v></c><c r='C1'/><c r='D1'><v/></c></row></sheetData></worksheet>",
        xml::MAIN
    );
    let bytes = xml.as_bytes();
    let mut values = Vec::new();
    scan_emitting(
        bytes,
        &XlsxRecalculateOptions::default(),
        Mode::Plain,
        &mut 0,
        &mut 0,
        |literal| {
            values.push((
                literal.raw_type_span.and_then(|span| bytes.get(span)),
                literal.raw_style_span.and_then(|span| bytes.get(span)),
                literal.raw_value_span.and_then(|span| bytes.get(span)),
                literal.style.map(|value| value.into_owned()),
                format!("{:?}", literal.value),
            ));
            Ok(())
        },
    )
    .expect("admitted original decoded projection");
    assert_eq!(
        values[0],
        (
            Some(b"&#110;".as_slice()),
            Some(b"&#49;".as_slice()),
            Some(b"+1".as_slice()),
            Some("1".to_owned()),
            "Number(1.0)".to_owned()
        )
    );
    assert_eq!(values[1].0, Some(b"s".as_slice()));
    assert_eq!(values[1].2, Some(b"+1".as_slice()));
    assert_eq!(values[1].4, "Shared(1)");
    assert_eq!(values[2].2, None);
    assert_eq!(values[3].2, Some(b"".as_slice()));
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
