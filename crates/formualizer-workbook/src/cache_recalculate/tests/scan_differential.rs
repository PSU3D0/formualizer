//! Differential oracle for the borrowed XML walker and worksheet scanner:
//! both must agree with the owned-string implementations they replaced
//! (`xml::reference`, `sheet::reference`) on every event, span, scan output,
//! counter and error.
//!
//! Every walk and scan made by the crate's unit tests is shadowed by these
//! checks; this module adds a generated malformed-input set, a mutation loop
//! over worksheets from the committed XLSX fixtures and an ignored check
//! over a list of XLSX files.
use super::super::{IoError, XlsxRecalculateLimits, XlsxRecalculateOptions, sheet, xml};
use std::cell::Cell;
use std::io::Read;

thread_local! {
    /// Set while a comparison runs, so that it is not itself shadowed.
    static COMPARING: Cell<bool> = const { Cell::new(false) };
}
/// Run `f` unless a comparison is already running on this thread.
fn exclusive(f: impl FnOnce()) {
    if COMPARING.with(Cell::get) {
        return;
    }
    COMPARING.with(|c| c.set(true));
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            COMPARING.with(|c| c.set(false));
        }
    }
    let _reset = Reset;
    f();
}

type Trace = (Vec<String>, String);

fn walk_trace(bytes: &[u8], options: &XlsxRecalculateOptions) -> Trace {
    let mut events = Vec::new();
    let result = xml::walk(bytes, options, |path, node| {
        let path: Vec<(&str, &str, &str)> = path
            .iter()
            .map(|e| (&*e.ns, e.local, e.qualified))
            .collect();
        let kind = match node.kind {
            xml::Kind::Open { empty, attributes } => format!(
                "open {empty} {:?}",
                attributes
                    .iter()
                    .map(|a| (&*a.ns, a.local, a.qualified, &*a.value, a.span.clone()))
                    .collect::<Vec<_>>()
            ),
            xml::Kind::Close => "close".to_owned(),
            xml::Kind::Text(text) => format!("text {:?}", &*text),
        };
        events.push(format!("{path:?} {kind} {:?}", node.span));
        Ok(())
    });
    (events, format!("{result:?}"))
}
fn reference_walk_trace(bytes: &[u8], options: &XlsxRecalculateOptions) -> Trace {
    use xml::reference as old;
    let mut events = Vec::new();
    let result = old::walk(bytes, options, |path, node| {
        let path: Vec<(&str, &str, &str)> = path
            .iter()
            .map(|e| (e.ns.as_str(), e.local.as_str(), e.qualified.as_str()))
            .collect();
        let kind = match &node.kind {
            old::Kind::Open { empty, attributes } => format!(
                "open {empty} {:?}",
                attributes
                    .iter()
                    .map(|a| (
                        a.ns.as_str(),
                        a.local.as_str(),
                        a.qualified.as_str(),
                        a.value.as_str(),
                        a.span.clone()
                    ))
                    .collect::<Vec<_>>()
            ),
            old::Kind::Close => "close".to_owned(),
            old::Kind::Text(text) => format!("text {:?}", text.as_str()),
        };
        events.push(format!("{path:?} {kind} {:?}", node.span));
        Ok(())
    });
    (events, format!("{result:?}"))
}
fn compare_walk(bytes: &[u8], options: &XlsxRecalculateOptions) {
    let new = walk_trace(bytes, options);
    let old = reference_walk_trace(bytes, options);
    if new != old {
        let at = new.0.iter().zip(&old.0).position(|(a, b)| a != b);
        panic!(
            "XML walk diverged from the reference at event {at:?}\n new: {:?}\n old: {:?}\n new result: {}\n old result: {}\ninput: {:?}",
            at.and_then(|i| new.0.get(i)),
            at.and_then(|i| old.0.get(i)),
            new.1,
            old.1,
            String::from_utf8_lossy(&bytes[..bytes.len().min(2000)]),
        );
    }
}
fn scan_outcome(
    f: impl FnOnce(&mut usize, &mut u64) -> Result<sheet::Scanned, IoError>,
    observed: usize,
    logical: u64,
) -> String {
    let (mut observed, mut logical) = (observed, logical);
    let result = f(&mut observed, &mut logical);
    format!("{result:?} observed={observed} logical={logical}")
}
fn compare_scan(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: sheet::Mode,
    observed: usize,
    logical: u64,
) {
    let new = scan_outcome(
        |o, l| sheet::scan_uncounted(bytes, options, mode, o, l),
        observed,
        logical,
    );
    let old = scan_outcome(
        |o, l| sheet::reference::scan(bytes, options, mode, o, l),
        observed,
        logical,
    );
    if new != old {
        let at = new
            .bytes()
            .zip(old.bytes())
            .position(|(a, b)| a != b)
            .unwrap_or(new.len().min(old.len()));
        let from = at.saturating_sub(300);
        panic!(
            "worksheet scan ({mode:?}) diverged from the reference at byte {at}\n new: …{}\n old: …{}\ninput: {:?}",
            &new[from..(at + 300).min(new.len())],
            &old[from..(at + 300).min(old.len())],
            String::from_utf8_lossy(&bytes[..bytes.len().min(2000)]),
        );
    }
}
/// Shadow check made by every `xml::walk` under test.
pub(in crate::cache_recalculate) fn shadow_walk(bytes: &[u8], options: &XlsxRecalculateOptions) {
    exclusive(|| compare_walk(bytes, options));
}
/// Shadow check made by every `sheet::scan` under test.
pub(in crate::cache_recalculate) fn shadow_scan(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: sheet::Mode,
    observed: usize,
    logical: u64,
) {
    exclusive(|| compare_scan(bytes, options, mode, observed, logical));
}
const MODES: [sheet::Mode; 3] = [
    sheet::Mode::Plain,
    sheet::Mode::TablePlain,
    sheet::Mode::Indexed,
];
/// Walk and scan `bytes` in every mode under `options`.
fn compare_all(bytes: &[u8], options: &XlsxRecalculateOptions) {
    exclusive(|| {
        compare_walk(bytes, options);
        for mode in MODES {
            compare_scan(bytes, options, mode, 0, 0);
        }
    });
}
/// Default limits and a tight set that trips every count/size bound.
fn option_sets() -> Vec<XlsxRecalculateOptions> {
    let tight = XlsxRecalculateLimits {
        max_xml_depth: 5,
        max_cells: 6,
        max_formula_cells: 2,
        max_columns: 3,
        max_entries: 1,
        ..XlsxRecalculateLimits::default()
    };
    vec![
        XlsxRecalculateOptions::default(),
        XlsxRecalculateOptions {
            limits: tight,
            ..XlsxRecalculateOptions::default()
        },
    ]
}

const MAIN: &str = xml::MAIN;
const X14AC: &str = "http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac";
const MC: &str = "http://schemas.openxmlformats.org/markup-compatibility/2006";
const XM: &str = "http://schemas.microsoft.com/office/excel/2006/main";
const XDR: &str = "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing";

/// A worksheet exercising every element and attribute the scanner reads.
fn rich_sheet() -> String {
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
            "<worksheet xmlns=\"{main}\" xmlns:r=\"{office}\" xmlns:mc=\"{mc}\" ",
            "xmlns:x14ac=\"{x14ac}\" xmlns:xdr=\"{xdr}\" xmlns:xm=\"{xm}\" mc:Ignorable=\"x14ac\">",
            "<dimension ref=\"A1:D6\"/>",
            "<sheetFormatPr defaultRowHeight=\"15\" x14ac:dyDescent=\"0.25\"/>",
            "<!-- a comment --><?pi target?>",
            "<sheetData>",
            "<row r=\"1\" spans=\"1:4\" x14ac:dyDescent=\"0.25\">",
            "<c r=\"A1\" t=\"s\"><v>0</v></c>",
            "<c r=\"B1\"><v>1.5</v></c>",
            "<c r=\"C1\" t=\"str\"><f>A1&amp;\"x&lt;y\"</f><v>a&gt;b</v></c>",
            "<c r=\"D1\" t=\"inlineStr\"><is><t xml:space=\"preserve\"> hi&#x41;&#66; </t></is></c>",
            "</row>",
            "<row r=\"2\" hidden=\"1\" outlineLevel=\"1\">",
            "<c r=\"A2\"><f t=\"shared\" ref=\"A2:A4\" si=\"0\">B1*2</f><v>3</v></c>",
            "<c r=\"B2\" t=\"b\"><v>1</v></c><c r=\"C2\" t=\"e\"><v>#N/A</v></c><c r=\"D2\" s=\"1\"/>",
            "</row>",
            "<row r=\"3\" ht=\"0\" customHeight=\"1\" outlineLevel=\"1\" collapsed=\"1\">",
            "<c r=\"A3\"><f t=\"shared\" si=\"0\"/><v>3</v></c>",
            "<c r=\"B3\"><f>SUM(\r\nA1:A2)</f><v/></c>",
            "</row>",
            "<row r=\"4\"><c r=\"A4\"><f t=\"shared\" si=\"0\"></f><v>3</v></c>",
            "<c r=\"B4\" t=\"d\"><v>2024-01-01</v></c></row>",
            "<row r=\"6\" collapsed=\"false\"><c r=\"A6\"><f>A4</f></c></row>",
            "</sheetData>",
            "<autoFilter ref=\"A1:D4\"><filterColumn colId=\"0\"><filters><filter val=\"1\"/></filters></filterColumn></autoFilter>",
            "<mergeCells count=\"1\"><mergeCell ref=\"C3:D3\"/></mergeCells>",
            "<mc:AlternateContent><mc:Choice Requires=\"x14\"><controls><control>",
            "<controlPr><anchor><from><xdr:col>1</xdr:col><xdr:row>2</xdr:row></from>",
            "<to><xdr:col>2</xdr:col><xdr:row>3</xdr:row></to></anchor></controlPr>",
            "</control></controls></mc:Choice></mc:AlternateContent>",
            "<tableParts count=\"1\"><tablePart r:id=\"rId1\"/></tableParts>",
            "<extLst><ext uri=\"{{78C0D931-6437-407d-A8EE-F0AAD7539E65}}\">",
            "<x14:conditionalFormattings xmlns:x14=\"http://schemas.microsoft.com/office/spreadsheetml/2009/9/main\">",
            "<x14:conditionalFormatting><x14:cfRule type=\"cellIs\"><xm:f>$A$1</xm:f></x14:cfRule>",
            "<xm:sqref>A1</xm:sqref></x14:conditionalFormatting></x14:conditionalFormattings>",
            "</ext></extLst>",
            "</worksheet>"
        ),
        main = MAIN,
        office = xml::OFFICE,
        mc = MC,
        x14ac = X14AC,
        xdr = XDR,
        xm = XM,
    )
}
/// A worksheet with dynamic-array metadata (`cm`) and a legacy array formula.
fn dynamic_sheet() -> String {
    format!(
        concat!(
            "<x:worksheet xmlns:x=\"{main}\"><x:sheetData>",
            "<x:row r=\"1\"><x:c r=\"A1\" cm=\"1\"><x:f t=\"array\" ref=\"A1:A3\">SEQUENCE(3)</x:f><x:v>1</x:v></x:c>",
            "<x:c r=\"B1\"><x:f t=\"array\" ref=\"B1\">1</x:f><x:v>1</x:v></x:c></x:row>",
            "<x:row r=\"2\"><x:c r=\"A2\"><x:v>2</x:v></x:c><x:c r=\"B2\" t=\"e\"><x:v>#SPILL!</x:v></x:c></x:row>",
            "<x:row r=\"3\"><x:c r=\"A3\"><x:v>3</x:v></x:c></x:row>",
            "</x:sheetData></x:worksheet>"
        ),
        main = MAIN
    )
}
/// Small documents that each hit one walker or scanner rule.
fn malformed_cases() -> Vec<String> {
    let w = |body: &str| format!("<worksheet xmlns=\"{MAIN}\">{body}</worksheet>");
    let sd = |rows: &str| w(&format!("<sheetData>{rows}</sheetData>"));
    let cell = |c: &str| sd(&format!("<row r=\"1\">{c}</row>"));
    let mut cases = vec![
        String::new(),
        " ".into(),
        "<a/>".into(),
        "<a/><b/>".into(),
        "text<a/>".into(),
        "<a/>text".into(),
        "&amp;<a/>".into(),
        "<a>&#0;</a>".into(),
        "<a>&#xD800;</a>".into(),
        "<a>&#x1F600;&#65;&#x;</a>".into(),
        "<a>&nbsp;</a>".into(),
        "<a>&amp</a>".into(),
        "<a><![CDATA[x]]></a>".into(),
        "<a>x]]>y</a>".into(),
        "<a>x]]&gt;y</a>".into(),
        "<!DOCTYPE a><a/>".into(),
        "<!DOCTYPE a [<!ENTITY e \"x\">]><a>&e;</a>".into(),
        "<?xml version=\"1.1\"?><a/>".into(),
        "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><a/>".into(),
        "<?xml version=\"1.0\" encoding=\"us-ascii\"?><a>é</a>".into(),
        "<?xml version=\"1.0\" encoding=\"US-ASCII\"?><a>e</a>".into(),
        "<?xml version='1.0'?><?xml version='1.0'?><a/>".into(),
        "<a/><?xml version='1.0'?>".into(),
        "<?xml?><a/>".into(),
        "<a><!-- x -- y --></a>".into(),
        "<a><!-- ok --></a>".into(),
        "<a><?pi x?></a>".into(),
        "<a></b>".into(),
        "<a>".into(),
        "</a>".into(),
        "<a b=\"1\" b=\"2\"/>".into(),
        "<a b=\"1\" b=\"2\" c=\"3/>".into(),
        "<a b=\"1\" c=\"2\" b=\"3\"/>".into(),
        "<a xmlns:p=\"u\" xmlns:q=\"u\" p:b=\"1\" q:b=\"2\"/>".into(),
        "<a p:b=\"1\" xmlns:p=\"u\"/>".into(),
        "<a xmlns:p=\"u\" xmlns:p=\"v\" p:b=\"1\"/>".into(),
        "<a xmlns=\"u\" xmlns=\"v\"/>".into(),
        "<a xmlns:xml=\"u\"/>".into(),
        "<a xmlns:xmlns=\"u\"/>".into(),
        "<a xmlns:p=\"http://www.w3.org/XML/1998/namespace\"/>".into(),
        "<a xml:space=\"preserve\"/>".into(),
        "<p:a/>".into(),
        "<a p:b=\"1\"/>".into(),
        "<a:b:c/>".into(),
        "<1a/>".into(),
        "<a 1b=\"2\"/>".into(),
        "<é/>".into(),
        "<a b=1/>".into(),
        "<a b/>".into(),
        "<a b=\"x\ty\"/>".into(),
        "<a b=\"x\ny\"/>".into(),
        "<a b=\"x<y\"/>".into(),
        "<a b=\"x&lt;y&#9;\"/>".into(),
        "<a b=\"&#1;\"/>".into(),
        "<a b=\"&bogus;\"/>".into(),
        "<a b=\"&amp\"/>".into(),
        "<a b='single'/>".into(),
        "<a  b = \"spaced\"  />".into(),
        "<a xmlns:p=\"u&amp;v\" p:b=\"1\"/>".into(),
        "<a>\r\nx\ry\u{85}z\u{2028}</a>".into(),
        "<a>\u{1}</a>".into(),
        "<a>\u{FFFE}</a>".into(),
        "<a>ok\u{10FFFF}</a>".into(),
        "<a/>\r\n\t ".into(),
        "<a></a >".into(),
        "<a\n/>".into(),
        // Worksheet rules.
        "<worksheet xmlns=\"urn:other\"><sheetData/></worksheet>".to_owned(),
        w(""),
        w("<sheetData/><sheetData/>"),
        w("<sheetData/><dimension ref=\"A1\"/>"),
        w("<dimension ref=\"A1\"/><dimension ref=\"A1\"/><sheetData/>"),
        w("<dimension ref=\"A1:IW1\"/><sheetData/>"),
        w("<dimension ref=\"B2:A1\"/><sheetData/>"),
        w("<dimension ref=\"$A$1\"/><sheetData/>"),
        w("<dimension/><sheetData/>"),
        w("<dimension ref=\"A1:XFD1048576\"/><sheetData/>"),
        sd("<row r=\"2\"/><row r=\"1\"/>"),
        sd("<row r=\"0\"/>"),
        sd("<row r=\"1048577\"/>"),
        sd("<row/>"),
        sd("<row r=\"x\"/>"),
        sd("<row r=\"1\" hidden=\"yes\"/>"),
        sd("<row r=\"1\" ht=\"abc\"/><row r=\"2\" ht=\" 0 \"/><row r=\"3\" ht=\"-1\"/>"),
        sd(
            "<row r=\"1\" outlineLevel=\" 0 \"/><row r=\"2\" outlineLevel=\"2\" collapsed=\"true\"/>",
        ),
        sd("<row r=\"1\"><c r=\"B1\"/><c r=\"A1\"/></row>"),
        sd("<row r=\"1\"><c r=\"A2\"/></row>"),
        sd("<row r=\"1\"><c/></row>"),
        sd("<row r=\"1\"><c r=\"a1\"/></row>"),
        sd("<row r=\"1\"><c r=\"$A1\"/></row>"),
        sd("<row r=\"1\"><c r=\"Z1\"/></row>"),
        sd("<row r=\"1\"><c r=\"A1\"><c r=\"B1\"/></c></row>"),
        w("<dimension ref=\"A1:B2\"/><sheetData><row r=\"3\"><c r=\"A3\"/></row></sheetData>"),
        cell("<c r=\"A1\" vm=\"1\"/>"),
        cell("<c r=\"A1\" cm=\"0\"><f>1</f></c>"),
        cell("<c r=\"A1\" cm=\"x\"><f>1</f></c>"),
        cell("<c r=\"A1\" cm=\"1\"><f>1</f></c>"),
        cell("<c r=\"A1\" cm=\"1\"/>"),
        cell("<c r=\"A1\" t=\"q\"/>"),
        cell("<c r=\"A1\"><f>1</f><f>2</f></c>"),
        cell("<c r=\"A1\"><v>1</v><f>2</f></c>"),
        cell("<c r=\"A1\"><v>1</v><v>2</v></c>"),
        cell("<c r=\"A1\"><is/><v>2</v></c>"),
        cell("<c r=\"A1\"><v>1</v><is/></c>"),
        cell("<c r=\"A1\"><f t=\"dataTable\">1</f></c>"),
        cell("<c r=\"A1\"><f t=\"other\">1</f></c>"),
        cell("<c r=\"A1\"><f ref=\"A1:A2\">1</f></c>"),
        cell("<c r=\"A1\"><f t=\"shared\">1</f></c>"),
        cell("<c r=\"A1\"><f t=\"shared\" si=\"x\" ref=\"A1\">1</f></c>"),
        cell(
            "<c r=\"A1\"><f t=\"shared\" si=\"0\" ref=\"A1:B1\">1</f></c><c r=\"B1\"><f t=\"shared\" si=\"1\"/></c>",
        ),
        cell(
            "<c r=\"A1\"><f t=\"shared\" si=\"0\" ref=\"A1:B1\">1</f></c><c r=\"C1\"><f t=\"shared\" si=\"0\"/></c>",
        ),
        cell("<c r=\"A1\"><f t=\"shared\" si=\"0\">1</f></c>"),
        cell("<c r=\"B1\"><f t=\"shared\" si=\"0\" ref=\"A1:B1\">1</f></c>"),
        cell(
            "<c r=\"A1\"><f t=\"shared\" si=\"0\" ref=\"A1:B1\">1</f></c><c r=\"B1\"><f t=\"shared\" si=\"0\" ref=\"B1\"/></c>",
        ),
        cell(
            "<c r=\"A1\"><f t=\"shared\" si=\"0\" ref=\"A1:A2\">1</f></c><c r=\"B1\"><f t=\"shared\" si=\"0\" ref=\"B1:B2\">2</f></c>",
        ),
        cell("<c r=\"A1\"><f t=\"shared\" si=\"0\" ref=\"A1:B1\">Sheet2!A1</f></c>"),
        cell("<c r=\"A1\"><f t=\"shared\" si=\"0\" ref=\"A1:B1\">[1]Sheet2!A1</f></c>"),
        cell("<c r=\"A1\"><f> </f></c>"),
        cell("<c r=\"A1\"><f/></c>"),
        cell("<c r=\"A1\"><f t=\"array\" ref=\"A1:B1\">1</f></c>"),
        cell("<c r=\"A1\"><f t=\"array\">1</f></c>"),
        cell("<c r=\"A1\"><f>1<b/></f></c>"),
        cell("<c r=\"A1\"><v>1<b/></v></c>"),
        cell("<c r=\"A1\"><v><v/></v></c>"),
        cell("<c r=\"A1\"><is><t>x</t><r><rPr/><t>y</t></r></is></c>"),
        cell("<c r=\"A1\"><x/></c>"),
        cell("<c r=\"A1\" t=\"e\"><v>#BOGUS</v></c>"),
        cell("<c r=\"A1\" t=\"e\"/>"),
        cell("<c r=\"A1\" t=\"e\"><v/></c>"),
        cell("<c r=\"A1\" t=\"s\"><v>-1</v></c>"),
        cell("<c r=\"A1\" t=\"s\"><v></v></c>"),
        cell("<c r=\"A1\" t=\"b\"><v>2</v></c>"),
        cell("<c r=\"A1\"><v>inf</v></c>"),
        cell("<c r=\"A1\"><v>1&#48;</v></c>"),
        cell("<c r=\"A1\"><v> 1</v></c>"),
        cell("<c r=\"A1\"><v>1</v><v/></c>"),
        cell("<c r=\"A1\" t=\"str\"><v>&amp;</v></c>"),
        cell("<c r=\"A1\" t=\"n\"><v></v></c>"),
        cell("<c r=\"A1\"><f>A&amp;B&#x26;C</f><v>1</v></c>"),
        cell("<c r=\"A1\"><f>\r\nA1</f><v>1</v></c>"),
        w("<sheetData/><v/>"),
        w("<f/><sheetData/>"),
        w("<sheetData><row r=\"1\"/><is/></sheetData>"),
        w("<sheetData><c r=\"A1\"/></sheetData>"),
        w("<sheetData/><row r=\"1\"/>"),
        w("<sheetData/><mergeCell ref=\"A1\"/>"),
        w("<sheetData/><mergeCells><mergeCell/></mergeCells>"),
        w("<sheetData/><mergeCells><mergeCell ref=\"B1:A1\"/></mergeCells>"),
        w("<sheetData/><mergeCells><mergeCell ref=\"A1:B2\"/><mergeCell ref=\"C1\"/></mergeCells>"),
        w("<sheetData/><autoFilter/>"),
        w("<sheetData/><autoFilter ref=\"A1:B5\"/><autoFilter ref=\"A1\"/>"),
        w("<sheetData/><autoFilter ref=\"A1:B1\"><filterColumn colId=\"0\"/></autoFilter>"),
        w("<sheetData/><sheetFormatPr zeroHeight=\"true\"/>"),
        w("<sheetData/><tablePart/>"),
        w("<sheetData/><tableParts><tablePart/></tableParts>"),
        w("<sheetData/><tableParts count=\"x\"/>"),
        w("<sheetData/><tableParts count=\"1\"/><tableParts count=\"1\"/>"),
        w("<sheetData/><tableParts count=\"0\"><tablePart r:id=\"x\" xmlns:r=\"u\"/></tableParts>"),
        w(&format!(
            "<sheetData/><tableParts count=\"2\"><tablePart r:id=\"rId1\" xmlns:r=\"{}\"/></tableParts>",
            xml::OFFICE
        )),
        w(&format!(
            "<sheetData/><tableParts count=\"1\"><tablePart xmlns:r=\"{}\"/></tableParts>",
            xml::OFFICE
        )),
        w(&format!(
            "<sheetData/><tableParts count=\"99999999\"><tablePart r:id=\"a\" xmlns:r=\"{}\"/></tableParts>",
            xml::OFFICE
        )),
        w("<sheetData/><x:row xmlns:x=\"urn:x\"/>"),
        w(&format!(
            "<sheetData/><extLst><ext><xm:f xmlns:xm=\"{XM}\">A1</xm:f></ext></extLst>"
        )),
        w(&format!(
            "<sheetData/><extLst><xm:f xmlns:xm=\"{XM}\">A1</xm:f></extLst>"
        )),
        w(&format!(
            "<sheetData><row r=\"1\"><xm:f xmlns:xm=\"{XM}\"/></row></sheetData>"
        )),
        w(&format!(
            "<sheetData/><mc:AlternateContent xmlns:mc=\"{MC}\"><anchor><from><xdr:row xmlns:xdr=\"{XDR}\">1</xdr:row></from></anchor></mc:AlternateContent>"
        )),
        w(&format!(
            "<sheetData/><anchor><from><xdr:row xmlns:xdr=\"{XDR}\">1</xdr:row></from></anchor>"
        )),
        w("<sheetData/><a><b><c><d><e><f/></e></d></c></b></a>"),
    ];
    // Many attributes, exceeding the inline duplicate check.
    let many: String = (0..40).map(|i| format!(" a{i}=\"{i}\"")).collect();
    cases.push(format!("<a{many}/>"));
    cases.push(format!("<a{many} a39=\"x\"/>"));
    cases.push(format!(
        "<a xmlns:p=\"u\" xmlns:q=\"u\"{many} p:z=\"1\" q:z=\"2\"/>"
    ));
    let rows: String = (1..=12)
        .map(|r| format!("<row r=\"{r}\"><c r=\"A{r}\"><f>{r}</f><v>{r}</v></c><c r=\"B{r}\"><v>{r}</v></c></row>"))
        .collect();
    cases.push(sd(&rows));
    cases.push(rich_sheet());
    cases.push(dynamic_sheet());
    cases
}

#[test]
fn malformed_and_edge_inputs_match_the_reference() {
    for options in option_sets() {
        for case in malformed_cases() {
            compare_all(case.as_bytes(), &options);
        }
    }
}

#[test]
fn bad_utf8_and_cancellation_match_the_reference() {
    let mut bad = rich_sheet().into_bytes();
    let at = bad.len() / 2;
    bad.insert(at, 0xff);
    compare_all(&bad, &XlsxRecalculateOptions::default());
    compare_all(b"<a>\xc3</a>", &XlsxRecalculateOptions::default());
    compare_all(b"\xef\xbb\xbf<a/>", &XlsxRecalculateOptions::default());
    let token = formualizer_eval::engine::CancelToken::new();
    token.cancel();
    let options = XlsxRecalculateOptions {
        cancel: Some(token),
        ..XlsxRecalculateOptions::default()
    };
    compare_all(rich_sheet().as_bytes(), &options);
}

#[test]
fn many_distinct_namespaces_and_prefixes_match_the_reference() {
    // Interning is hashed; a linear intern list made these quadratic.
    let n = 2_000;
    let ext: String = (0..n)
        .map(|i| format!("<x:e xmlns:x=\"urn:{i:08}\" x:a=\"1\"/>"))
        .collect();
    let rows: String = (1..=n)
        .map(|r| {
            format!(
                "<row r=\"{r}\" xmlns:p{r}=\"{MAIN}\"><p{r}:c r=\"A{r}\"><p{r}:f>1+1</p{r}:f><p{r}:v>0</p{r}:v></p{r}:c></row>"
            )
        })
        .collect();
    let sheet = format!(
        "<worksheet xmlns=\"{MAIN}\"><sheetData>{rows}</sheetData><extLst><ext uri=\"{{A}}\">{ext}</ext></extLst></worksheet>"
    );
    compare_all(sheet.as_bytes(), &XlsxRecalculateOptions::default());
}

#[test]
fn every_truncation_matches_the_reference() {
    for doc in [rich_sheet(), dynamic_sheet()] {
        let bytes = doc.as_bytes();
        for options in option_sets() {
            for end in 0..bytes.len() {
                compare_all(&bytes[..end], &options);
            }
        }
    }
}

/// Worksheets of the committed XLSX fixtures plus the generated sheets.
fn seed_worksheets() -> Vec<Vec<u8>> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut files = Vec::new();
    let mut stack = vec![dir];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "xlsx") {
                files.push(path);
            }
        }
    }
    files.sort();
    let mut sheets = vec![rich_sheet().into_bytes(), dynamic_sheet().into_bytes()];
    for file in files {
        for (name, data) in xml_parts(&std::fs::read(&file).unwrap()) {
            if name.starts_with("xl/worksheets/") && !name.contains("_rels") {
                sheets.push(data);
            }
        }
    }
    sheets
}
/// Every XML member (`*.xml`, `*.rels`) of a ZIP package.
fn xml_parts(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    let Ok(mut archive) = zip::ZipArchive::new(std::io::Cursor::new(bytes)) else {
        return Vec::new();
    };
    let mut parts = Vec::new();
    for i in 0..archive.len() {
        let Ok(mut file) = archive.by_index(i) else {
            continue;
        };
        let name = file.name().to_owned();
        if !(name.ends_with(".xml") || name.ends_with(".rels")) {
            continue;
        }
        let mut data = Vec::new();
        if (&mut file).take(256 << 20).read_to_end(&mut data).is_ok() {
            parts.push((name, data));
        }
    }
    parts
}
/// Deterministic xorshift generator.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}
const TOKENS: &[&str] = &[
    "<",
    ">",
    "/>",
    "</",
    "\"",
    "'",
    "=",
    " ",
    "&",
    ";",
    "&amp;",
    "&lt;",
    "&#65;",
    "&#0;",
    "&bogus;",
    "]]>",
    "<![CDATA[x]]>",
    "<!--c-->",
    "<?p?>",
    "\r\n",
    "\t",
    "\u{0}",
    "\u{e9}",
    "xmlns=\"urn:x\"",
    "xmlns:x=\"urn:x\"",
    "x:",
    " r=\"A1\"",
    " t=\"s\"",
    " t=\"e\"",
    " t=\"shared\"",
    " si=\"0\"",
    " ref=\"A1:B2\"",
    " cm=\"1\"",
    " vm=\"1\"",
    " hidden=\"1\"",
    "<f>",
    "</f>",
    "<v>",
    "</v>",
    "<c r=\"A1\">",
    "</c>",
    "<row r=\"1\">",
    "</row>",
    "<is>",
    "<sheetData>",
    "</sheetData>",
    "<dimension ref=\"A1\"/>",
    "<mergeCell ref=\"A1\"/>",
];
/// One random edit: delete, duplicate or replace a span, or insert a token.
fn mutate(rng: &mut Rng, data: &[u8]) -> Vec<u8> {
    let mut out = data.to_vec();
    for _ in 0..1 + rng.below(3) {
        let at = rng.below(out.len() + 1);
        let len = 1 + rng.below(16);
        let end = (at + len).min(out.len());
        match rng.below(4) {
            0 => {
                out.drain(at..end);
            }
            1 => {
                let span = out[at..end].to_vec();
                out.splice(at..at, span);
            }
            2 => {
                let token = TOKENS[rng.below(TOKENS.len())].as_bytes();
                out.splice(at..end, token.iter().copied());
            }
            _ => {
                let token = TOKENS[rng.below(TOKENS.len())].as_bytes();
                out.splice(at..at, token.iter().copied());
            }
        }
    }
    out
}

#[test]
fn mutated_worksheets_match_the_reference() {
    let rounds: usize = std::env::var("FORMUALIZER_SCAN_MUTATION_ROUNDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(300);
    let sheets = seed_worksheets();
    assert!(sheets.len() > 2, "fixtures provide worksheets");
    let options = option_sets();
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for round in 0..rounds {
        for sheet in &sheets {
            let mutated = mutate(&mut rng, sheet);
            compare_all(&mutated, &options[round % options.len()]);
        }
    }
}

#[test]
fn committed_fixture_parts_match_the_reference() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut stack = vec![dir];
    let mut parts = 0;
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "xlsx") {
                parts += check_package(&std::fs::read(&path).unwrap());
            }
        }
    }
    assert!(parts > 0);
}
/// Compare every XML part (and, for worksheets, every scan mode) of one
/// package; returns the number of parts compared.
fn check_package(bytes: &[u8]) -> usize {
    let options = XlsxRecalculateOptions::default();
    let parts = xml_parts(bytes);
    for (name, data) in &parts {
        exclusive(|| {
            compare_walk(data, &options);
            if name.starts_with("xl/worksheets/") && !name.contains("_rels") {
                for mode in MODES {
                    compare_scan(data, &options, mode, 0, 0);
                }
            }
        });
    }
    parts.len()
}

/// Old and new walk/scan over every XML part of the XLSX files listed (one
/// path per line) in the file named by `FORMUALIZER_SCAN_DIFF_FILES`:
///
/// ```text
/// FORMUALIZER_SCAN_DIFF_FILES=list.txt cargo test --release -p formualizer-workbook \
///     --all-features --lib scan_differential -- --ignored
/// ```
#[test]
#[ignore = "needs FORMUALIZER_SCAN_DIFF_FILES"]
fn listed_packages_match_the_reference() {
    let list = std::env::var("FORMUALIZER_SCAN_DIFF_FILES")
        .expect("FORMUALIZER_SCAN_DIFF_FILES names a file list");
    let list = std::fs::read_to_string(list).unwrap();
    let (mut files, mut parts) = (0, 0);
    for path in list.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("{path}: {e}"));
        parts += check_package(&bytes);
        files += 1;
    }
    eprintln!("compared {parts} XML parts of {files} packages");
    assert!(files > 0);
}
