use super::*;

fn paths(text: &str) -> ((usize, usize), bool) {
    let mut counts = (0, 0);
    let result = scan_inner::<true, true>(
        text.as_bytes(),
        &ReadOptions::from(&XlsxRecalculateOptions::default()),
        ScanRequest::recalculate(ScanStrategy::Plain),
        (&mut 0, &mut 0),
        &mut false,
        &mut emission::Literals(|_: emission::Literal<'_>| Ok(())),
        &mut counts,
    );
    (counts, result.is_ok())
}

#[test]
fn raw_coordinate_dimension_and_row_provenance_does_not_change_admission() {
    for prefix in ["", "x:"] {
        let namespace = if prefix.is_empty() {
            "xmlns"
        } else {
            "xmlns:x"
        };
        let xml = format!(
            "<{prefix}worksheet {namespace}='{}'><{prefix}dimension ref='&#65;1:B1'/><{prefix}sheetData><{prefix}row r='&#49;'><{prefix}c r='&#65;1'><{prefix}v>1</{prefix}v></{prefix}c><{prefix}c r='&#66;1'/></{prefix}row><{prefix}row r='+2'/></{prefix}sheetData></{prefix}worksheet>",
            xml::MAIN
        );
        let bytes = xml.as_bytes();
        let mut coordinates = Vec::new();
        let result = scan_emitting(
            bytes,
            &XlsxRecalculateOptions::default(),
            ScanStrategy::Plain,
            &mut 0,
            &mut 0,
            |literal| {
                coordinates.push((
                    literal.row,
                    literal.col,
                    literal.raw_coordinate_span.and_then(|span| bytes.get(span)),
                ));
                Ok(())
            },
        )
        .unwrap();
        let Scanned::Done(scan) = result else {
            panic!("plain sheet must not index")
        };
        assert_eq!(
            coordinates,
            vec![
                (1, 1, Some(b"&#65;1".as_slice())),
                (1, 2, Some(b"&#66;1".as_slice()))
            ]
        );
        assert_eq!(
            scan.raw_dimension_span.and_then(|span| bytes.get(span)),
            Some(b"&#65;1:B1".as_slice())
        );
        assert_eq!(
            scan.raw_row_refusal_span.and_then(|span| bytes.get(span)),
            Some(b"&#49;".as_slice())
        );
    }
    for (row, incompatible) in [("&#49;", true), ("+1", true), ("0001", false), ("1", false)] {
        for cells in ["", "<c r='A1'><v>1</v></c>"] {
            let xml = format!(
                "<worksheet xmlns='{}'><sheetData><row r='{row}'>{cells}</row></sheetData></worksheet>",
                xml::MAIN
            );
            let result = scan_emitting(
                xml.as_bytes(),
                &XlsxRecalculateOptions::default(),
                ScanStrategy::Plain,
                &mut 0,
                &mut 0,
                |_| Ok(()),
            )
            .unwrap();
            let Scanned::Done(scan) = result else {
                panic!("plain sheet must not index")
            };
            assert_eq!(
                scan.raw_row_refusal_span
                    .as_ref()
                    .map(|span| &xml[span.clone()]),
                incompatible.then_some(row)
            );
        }
    }
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
        ScanStrategy::Plain,
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
fn generated_child_hint_covers_seen_legacy_and_metadata_array_extents() {
    for metadata in ["", " cm='1'"] {
        let xml = format!(
            "<worksheet xmlns='{}'><sheetData><row r='1'><c r='A1'{metadata}><f t='array' ref='A1:B2'>1</f><v>1</v></c><c r='B1'><v>2</v></c><c r='C1'><v>3</v></c></row><row r='2'><c r='A2'/><c r='B2'><v>4</v></c><c r='C2'/></row><row r='3'><c r='A3'/></row></sheetData></worksheet>",
            xml::MAIN
        );
        let mut hints = Vec::new();
        let result = scan_emitting(
            xml.as_bytes(),
            &XlsxRecalculateOptions::default(),
            ScanStrategy::Indexed,
            &mut 0,
            &mut 0,
            |literal| {
                hints.push(((literal.row, literal.col), literal.generated_child_hint));
                Ok(())
            },
        );
        assert!(matches!(result, Ok(Scanned::Done(_))));
        assert_eq!(
            hints,
            [
                ((1, 2), true),
                ((1, 3), false),
                ((2, 1), true),
                ((2, 2), true),
                ((2, 3), false),
                ((3, 1), false)
            ]
        );
    }
}

#[test]
fn generated_child_hint_does_not_look_ahead_or_change_refusal_order() {
    // A child before its top-left anchor cannot be admitted in row-major
    // order. Its emitted prefix must remain unhinted, and the old coordinate
    // refusal (not an eager ownership/metadata decision) remains authoritative.
    let xml = format!(
        "<worksheet xmlns='{}'><sheetData><row r='1'><c r='B1'><v>2</v></c><c r='A1'><f t='array' ref='A1:B2'>1</f><v>1</v></c></row></sheetData></worksheet>",
        xml::MAIN
    );
    let options = XlsxRecalculateOptions::default();
    let mut hints = Vec::new();
    let result = scan_emitting(
        xml.as_bytes(),
        &options,
        ScanStrategy::Indexed,
        &mut 0,
        &mut 0,
        |literal| {
            hints.push(((literal.row, literal.col), literal.generated_child_hint));
            Ok(())
        },
    );
    let old = super::super::scan_uncounted(
        xml.as_bytes(),
        &options,
        ScanStrategy::Indexed,
        &mut 0,
        &mut 0,
    );
    assert_eq!(hints, [((1, 2), false)]);
    assert!(result.is_err());
    assert_eq!(format!("{:?}", result.err()), format!("{:?}", old.err()));
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
