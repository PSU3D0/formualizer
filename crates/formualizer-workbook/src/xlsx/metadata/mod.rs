//! Workbook metadata shared by source ingestion and strict publication admission.
//! Parsed declarations never change source bytes or evaluation formula text.
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod dynamic;
pub(crate) mod names;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod strings;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod styles;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod tables;
#[cfg(feature = "xlsx-recalc")]
pub(in crate::xlsx) use strings::SharedStringTable;
#[cfg(feature = "xlsx-recalc")]
pub(in crate::xlsx) use styles::Styles;

#[cfg(all(test, feature = "xlsx-recalc"))]
#[test]
fn compatible_metadata_retains_cdata_and_honors_empty_section_closes() {
    use crate::xlsx::options::{ReadOptions, ReadPolicy};
    let compatible = ReadOptions::default();
    let strict = ReadOptions {
        policy: ReadPolicy::Strict,
        ..ReadOptions::default()
    };
    let strings = br#"<sst><si><t>a<![CDATA[b]]>c</t></si></sst>"#;
    assert_eq!(
        SharedStringTable::parse_read(strings, &compatible)
            .unwrap()
            .get(0)
            .unwrap(),
        "abc"
    );
    assert!(SharedStringTable::parse_read(strings, &strict).is_err());
    for styles in [
        br#"<styleSheet><cellXfs><cellXfs/><xf numFmtId="14"/></cellXfs></styleSheet>"#.as_slice(),
        br#"<styleSheet><numFmts><numFmts/><numFmt numFmtId="164" formatCode="yyyy"/></numFmts><cellXfs><xf numFmtId="164"/></cellXfs></styleSheet>"#.as_slice(),
    ] {
        assert_eq!(Styles::parse_read(styles, &compatible).unwrap().format(Some("0"), 1.0), None);
        assert!(Styles::parse_read(styles, &strict).unwrap().format(Some("0"), 1.0).is_some());
    }
}

#[cfg(all(test, feature = "xlsx-recalc"))]
#[test]
fn value_metadata_extraction_is_independent_of_publication_policy() {
    use crate::xlsx::options::{ReadOptions, ReadPolicy};
    let compatible = ReadOptions::default();
    let strict = ReadOptions {
        policy: ReadPolicy::Strict,
        ..ReadOptions::default()
    };
    let strings = br#"<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><si><t>A&amp;B</t></si><si/><si><t>_x0041_</t></si></sst>"#;
    let left = SharedStringTable::parse_read(strings, &strict).unwrap();
    let right = SharedStringTable::parse_read(strings, &compatible).unwrap();
    assert_eq!(left.len(), 3);
    assert_eq!(left.len(), right.len());
    for index in 0..left.len() {
        assert_eq!(left.get(index).unwrap(), right.get(index).unwrap());
    }
    assert_eq!(right.get(0).unwrap(), "A&B");
    assert_eq!(right.get(2).unwrap(), "A");
    let styles = br#"<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellXfs count="2"><xf numFmtId="14"/><xf numFmtId="46"/></cellXfs></styleSheet>"#;
    let left = Styles::parse_read(styles, &strict).unwrap();
    let right = Styles::parse_read(styles, &compatible).unwrap();
    assert_eq!(left.len(), 2);
    assert_eq!(left.len(), right.len());
    for style in [None, Some("0"), Some("1"), Some("bad")] {
        for number in [0.5, 1.0, 1.5] {
            assert_eq!(left.format(style, number), right.format(style, number));
        }
    }
}

#[cfg(all(test, feature = "xlsx-recalc"))]
#[test]
fn bulk_direct_metadata_matches_edge_case_extraction() {
    let options = crate::xlsx::options::ReadOptions::default();
    for source in [
        r#"<sst><si/><si><t>A&amp;B_x0041_</t></si></sst>"#,
        "<sst><si><t>a<![CDATA[b]]>c</t><rPh><t>hidden</t></rPh></si></sst>",
        "<sst><si><r><t xml:space=\"preserve\"> a </t></r><r><t> b </t></r></si><si><t/></si></sst>",
        "<sst><si><t>a\u{85}b\u{2028}c</t></si></sst>",
    ] {
        let expected = SharedStringTable::parse_read(source.as_bytes(), &options).unwrap();
        let actual = SharedStringTable::parse_direct(source.as_bytes(), &options).unwrap();
        assert_eq!(expected.len(), actual.len(), "{source}");
        for i in 0..expected.len() {
            assert_eq!(expected.get(i).unwrap(), actual.get(i).unwrap(), "{source}");
        }
    }
    for source in [
        r#"<styleSheet><numFmts><numFmt numFmtId="164" formatCode="&quot;x&quot;yyyy"/></numFmts><cellXfs><xf numFmtId="14"/><xf numFmtId="46"/><xf numFmtId="164"/><xf/></cellXfs></styleSheet>"#,
        r#"<styleSheet><cellXfs><cellXfs/><xf numFmtId="14"/></cellXfs></styleSheet>"#,
        r#"<styleSheet><numFmts><numFmts/><numFmt numFmtId="164" formatCode="yyyy"/></numFmts><cellXfs><xf numFmtId="164"/></cellXfs></styleSheet>"#,
    ] {
        let expected = Styles::parse_read(source.as_bytes(), &options).unwrap();
        let actual = Styles::parse_direct(source.as_bytes(), &options).unwrap();
        assert_eq!(expected.len(), actual.len());
        for style in [
            None,
            Some("0"),
            Some("1"),
            Some("2"),
            Some("bad"),
            Some("999999999999999999999999"),
        ] {
            for n in [0.5, 1.0, 1.5] {
                assert_eq!(
                    expected.format(style, n),
                    actual.format(style, n),
                    "{source}"
                );
            }
        }
    }
    for source in [
        "<!DOCTYPE sst><sst/>",
        "<sst><si></sst>",
        "<sst><si><t>&unknown;</t></si></sst>",
    ] {
        assert!(SharedStringTable::parse_direct(source.as_bytes(), &options).is_err());
    }
}

/// Stream immutable declarations under shared XML safety/read bounds.
/// This entry does not grant calculation or source-publication authority.
/// Consumers bind declarations in the callback to preserve refusal priority;
/// compatible consumers can instead collect only the fields they understand.
#[cfg(feature = "xlsx-recalc")]
pub(in crate::xlsx) fn read_source<'a>(
    bytes: &'a [u8],
    options: &crate::xlsx::options::ReadOptions,
    visit: impl FnMut(
        &[crate::xlsx::xml::Element<'a>],
        crate::xlsx::xml::Node<'_, 'a>,
    ) -> Result<(), crate::IoError>,
) -> Result<(), crate::IoError> {
    crate::xlsx::xml::walk(bytes, options, visit)
}

#[cfg(all(test, feature = "xlsx-recalc"))]
#[test]
fn compatible_metadata_source_stream_does_not_apply_strict_binding() {
    use crate::xlsx::options::ReadOptions;
    let inputs = [
        "<workbook><definedNames><definedName name=\"same\">NOW()</definedName><definedName name=\"same\">2</definedName></definedNames></workbook>",
        "<table name=\"invalid name\" tableType=\"queryTable\"><tableColumns count=\"9\"><tableColumn id=\"0\" name=\"same\"/></tableColumns></table>",
        "<metadata><metadataTypes><metadataType name=\"unknown\"/></metadataTypes><cellMetadata><bk><rc t=\"bad\" v=\"bad\"/><rc/></bk></cellMetadata></metadata>",
        "<Types><Override PartName=\"/opaque.xml\" ContentType=\"unknown\"/></Types>",
    ];
    for input in inputs {
        let mut opens = 0;
        read_source(input.as_bytes(), &ReadOptions::default(), |_, node| {
            opens += usize::from(matches!(node.kind, crate::xlsx::xml::Kind::Open { .. }));
            Ok(())
        })
        .unwrap();
        assert!(opens > 1);
    }
    for input in [
        "<table>",
        "<!DOCTYPE table><table/>",
        "<metadata>&unknown;</metadata>",
    ] {
        assert!(read_source(input.as_bytes(), &ReadOptions::default(), |_, _| Ok(())).is_err());
    }
}
