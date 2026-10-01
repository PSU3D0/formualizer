//! FORM211-E: XLDAPR binding reuse, append and synthesis. Every edited or
//! synthesized part is parsed back through the admission parser.
use super::super::XlsxRecalculateOptions;
use super::super::dynamic_metadata::{
    Binder, DYNAMIC_ARRAY_NS, DynamicBinding, DynamicMetadata, METADATA_PART, XLDAPR_URI,
    parse_bytes,
};
use super::dynamic_admission::{MAIN, refused};

const TYPES: &str = "<metadataTypes count=\"1\"><metadataType name=\"XLDAPR\" minSupportedVersion=\"120000\"/></metadataTypes>";

fn future(collapsed: &[bool]) -> String {
    let blocks: String = collapsed
        .iter()
        .map(|c| {
            format!(
                "<bk><extLst><ext uri=\"{XLDAPR_URI}\"><xda:dynamicArrayProperties fDynamic=\"1\" fCollapsed=\"{}\"/></ext></extLst></bk>",
                u8::from(*c)
            )
        })
        .collect();
    format!(
        "<futureMetadata name=\"XLDAPR\" count=\"{}\">{blocks}</futureMetadata>",
        collapsed.len()
    )
}
fn cells(v: &[u32]) -> String {
    let blocks: String = v
        .iter()
        .map(|v| format!("<bk><rc t=\"1\" v=\"{v}\"/></bk>"))
        .collect();
    format!(
        "<cellMetadata count=\"{}\">{blocks}</cellMetadata>",
        v.len()
    )
}
fn part(body: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<metadata xmlns=\"{MAIN}\" xmlns:xda=\"{DYNAMIC_ARRAY_NS}\">{body}</metadata>"
    )
}
fn parsed(xml: &str) -> DynamicMetadata {
    parse_bytes(xml.as_bytes(), METADATA_PART, &Default::default()).expect("valid metadata")
}
fn binding(cm: u32, future_block: u32, collapsed: bool) -> DynamicBinding {
    DynamicBinding {
        cell_metadata: cm,
        metadata_type: 1,
        future_block,
        collapsed,
    }
}

#[test]
fn a_compatible_block_is_reused_without_any_edit() {
    // cm 1 is collapsed, cm 2 is compatible: the lowest compatible one wins.
    let m = parsed(&part(&format!(
        "{TYPES}{}{}",
        future(&[true, false]),
        cells(&[0, 1])
    )));
    let options = XlsxRecalculateOptions::default();
    let mut binder = Binder::new(Some(&m));
    assert_eq!(binder.bind(&options).unwrap(), 2);
    assert_eq!(binder.bind(&options).unwrap(), 2);
    assert!(binder.finish(&options).unwrap().is_none());
}

#[test]
fn a_collapsed_only_part_gets_exactly_one_appended_block() {
    let source = part(&format!("{TYPES}{}{}", future(&[true]), cells(&[0])));
    let m = parsed(&source);
    let options = XlsxRecalculateOptions::default();
    let mut binder = Binder::new(Some(&m));
    // Every request shares the one appended record.
    assert_eq!(binder.bind(&options).unwrap(), 2);
    assert_eq!(binder.bind(&options).unwrap(), 2);
    let edit = binder.finish(&options).unwrap().expect("edited part");
    assert!(!edit.added);
    assert_eq!(edit.part, METADATA_PART);
    let text = String::from_utf8(edit.bytes.clone()).unwrap();
    let expected = source
        .replace(
            "<futureMetadata name=\"XLDAPR\" count=\"1\">",
            "<futureMetadata name=\"XLDAPR\" count=\"2\">",
        )
        .replace(
            "</futureMetadata>",
            &format!(
                "<bk><extLst><ext uri=\"{XLDAPR_URI}\" xmlns:xda=\"{DYNAMIC_ARRAY_NS}\"><xda:dynamicArrayProperties fDynamic=\"1\" fCollapsed=\"0\"/></ext></extLst></bk></futureMetadata>"
            ),
        )
        .replace("<cellMetadata count=\"1\">", "<cellMetadata count=\"2\">")
        .replace(
            "</cellMetadata>",
            "<bk><rc t=\"1\" v=\"1\"/></bk></cellMetadata>",
        );
    assert_eq!(text, expected);
    // The shared collapsed record keeps its bytes and index; the new one is
    // a non-collapsed block at the next index.
    let again = parsed(&text);
    assert_eq!(again.resolve(1).unwrap(), binding(1, 0, true));
    assert_eq!(again.resolve(2).unwrap(), binding(2, 1, false));
    assert!(text.contains("fCollapsed=\"1\""));
}

#[test]
fn prefixed_and_self_closing_sections_are_extended_in_their_own_style() {
    // The main namespace is bound to `xda`, so the new extension must use a
    // different dynamic-array prefix; empty sections are expanded.
    let source = format!(
        "<xda:metadata xmlns:xda=\"{MAIN}\"><xda:metadataTypes count=\"1\"><xda:metadataType name=\"XLDAPR\"/></xda:metadataTypes><xda:futureMetadata name=\"XLDAPR\" count=\"0\"/><xda:cellMetadata/></xda:metadata>"
    );
    let m = parsed(&source);
    let options = XlsxRecalculateOptions::default();
    let mut binder = Binder::new(Some(&m));
    assert_eq!(binder.bind(&options).unwrap(), 1);
    let edit = binder.finish(&options).unwrap().unwrap();
    let text = String::from_utf8(edit.bytes).unwrap();
    assert_eq!(
        text,
        format!(
            "<xda:metadata xmlns:xda=\"{MAIN}\"><xda:metadataTypes count=\"1\"><xda:metadataType name=\"XLDAPR\"/></xda:metadataTypes><xda:futureMetadata name=\"XLDAPR\" count=\"1\"><xda:bk><xda:extLst><xda:ext uri=\"{XLDAPR_URI}\" xmlns:xda1=\"{DYNAMIC_ARRAY_NS}\"><xda1:dynamicArrayProperties fDynamic=\"1\" fCollapsed=\"0\"/></xda:ext></xda:extLst></xda:bk></xda:futureMetadata><xda:cellMetadata><xda:bk><xda:rc t=\"1\" v=\"0\"/></xda:bk></xda:cellMetadata></xda:metadata>"
        )
    );
    assert_eq!(parsed(&text).resolve(1).unwrap(), binding(1, 0, false));
}

#[test]
fn a_missing_part_is_synthesized_canonically_and_parses_back() {
    let options = XlsxRecalculateOptions::default();
    let mut binder = Binder::new(None);
    assert_eq!(binder.bind(&options).unwrap(), 1);
    assert_eq!(binder.bind(&options).unwrap(), 1);
    let edit = binder.finish(&options).unwrap().unwrap();
    assert!(edit.added);
    assert_eq!(edit.part, METADATA_PART);
    let text = String::from_utf8(edit.bytes).unwrap();
    assert!(text.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"));
    let m = parsed(&text);
    assert_eq!(m.resolve(1).unwrap(), binding(1, 0, false));
    refused(m.resolve(2), "dangling");
}

#[test]
fn no_request_means_no_metadata_edit() {
    let options = XlsxRecalculateOptions::default();
    assert!(Binder::new(None).finish(&options).unwrap().is_none());
    let m = parsed(&part(&format!("{TYPES}{}{}", future(&[true]), cells(&[0]))));
    assert!(Binder::new(Some(&m)).finish(&options).unwrap().is_none());
}

#[test]
fn a_part_without_xldapr_sections_is_not_extended() {
    let m = parsed(&part(""));
    let options = XlsxRecalculateOptions::default();
    let mut binder = Binder::new(Some(&m));
    assert_eq!(binder.bind(&options).unwrap(), 1);
    refused(binder.finish(&options), "cannot be extended");
    let m = parsed(&part(TYPES));
    let mut binder = Binder::new(Some(&m));
    binder.bind(&options).unwrap();
    refused(binder.finish(&options), "cannot be extended");
}

#[test]
fn the_record_limit_bounds_appends() {
    let m = parsed(&part(&format!("{TYPES}{}{}", future(&[true]), cells(&[0]))));
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_cells = 1;
    refused(
        Binder::new(Some(&m)).bind(&options),
        "sheet metadata record limit",
    );
}
