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
#[cfg(test)]
#[cfg(feature = "xlsx-recalc")]
pub(in crate::xlsx) use styles::raw_attribute;

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
