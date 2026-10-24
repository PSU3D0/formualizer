//! Source acceptance policy is independent of retained outputs and XML safety.
use super::*;
use crate::xlsx::options::ScanRetention;
use std::io::{Cursor as IoCursor, Read};

#[derive(Default, Debug, PartialEq)]
struct Collected {
    values: Vec<(u32, u32, String)>,
    formulas: Vec<String>,
}
impl emission::Sink for Collected {
    fn literal(&mut self, literal: emission::Literal<'_>) -> Result<(), IoError> {
        let value = match literal.value {
            emission::Value::Number(n) => format!("number:{:016x}", n.to_bits()),
            value => format!("{value:?}"),
        };
        self.values.push((literal.row, literal.col, value));
        Ok(())
    }
    fn formula(&mut self, formula: emission::Formula<'_>) -> Result<(), IoError> {
        self.formulas.push(format!("{formula:?}"));
        Ok(())
    }
}
fn collect(bytes: &[u8], policy: ReadPolicy) -> Result<(Scanned, Collected), IoError> {
    let mut options = ReadOptions::from(&XlsxRecalculateOptions::default());
    options.policy = policy;
    let request = ScanRequest {
        strategy: ScanStrategy::Indexed,
        retention: ScanRetention {
            values: true,
            formula_events: true,
            marks: false,
            geometry: policy == ReadPolicy::Strict,
        },
    };
    let mut collected = Collected::default();
    let result = scan_with(bytes, &options, request, &mut 0, &mut 0, &mut collected)?;
    Ok((result, collected))
}
fn sheet(body: &str) -> Vec<u8> {
    format!("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>{body}</sheetData></worksheet>").into_bytes()
}

#[test]
fn publication_only_refusals_are_not_loading_admission() {
    let cases = [
        (
            "<row r=\"1\"><c r=\"A1\"><v>1</v></c><c r=\"A1\"><v>2</v></c></row>",
            2,
            0,
        ),
        (
            "<row r=\"2\"><c r=\"A2\"/></row><row r=\"1\"><c r=\"A1\"/></row>",
            2,
            0,
        ),
        ("<row><c><v>1</v></c></row>", 1, 0),
        ("<row r=\"1\"><c r=\"$A$1\"><v>1</v></c></row>", 1, 0),
        ("<row r=\"1\"><c r=\"A1\" vm=\"7\"><v>1</v></c></row>", 1, 0),
        ("<row r=\"1\"><c r=\"A1\" cm=\"0\"><v>1</v></c></row>", 1, 0),
        (
            "<row r=\"1\"><c r=\"A1\" t=\"future\"><v>1</v></c></row>",
            1,
            0,
        ),
        (
            "<row r=\"1\"><c r=\"A1\" t=\"b\"><v>yes</v></c></row>",
            1,
            0,
        ),
        (
            "<row r=\"1\"><c r=\"A1\" t=\"str\"><v><![CDATA[text]]></v></c></row>",
            1,
            0,
        ),
        ("<row r=\"1\"><c r=\"A1\"><f/></c></row>", 0, 1),
        (
            "<row r=\"1\"><c r=\"A1\"><f t=\"dataTable\">A1</f></c></row>",
            0,
            1,
        ),
        (
            "<row r=\"1\"><c r=\"A1\"><f t=\"future\">A1</f></c></row>",
            0,
            1,
        ),
        (
            "<row r=\"1\"><c r=\"A1\"><f t=\"array\">A1</f></c></row>",
            0,
            1,
        ),
        (
            "<row r=\"1\"><c r=\"A1\"><f t=\"shared\" si=\"9\"/></c></row>",
            0,
            1,
        ),
        (
            "<row r=\"1\"><c r=\"A1\"><f t=\"shared\" si=\"bad\"/></c></row>",
            0,
            1,
        ),
        ("<row r=\"1\"><c r=\"A1\"><v>1</v><v>2</v></c></row>", 1, 0),
        (
            "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>a</t></is><is><t>b</t></is></c></row>",
            1,
            0,
        ),
    ];
    const ONE: &str = "number:3ff0000000000000";
    let expected_values: &[&[(u32, u32, &str)]] = &[
        &[(1, 1, ONE), (1, 1, "number:4000000000000000")],
        &[(2, 1, "Empty"), (1, 1, "Empty")],
        &[(1, 1, ONE)],
        &[(1, 1, ONE)],
        &[(1, 1, ONE)],
        &[(1, 1, ONE)],
        &[(1, 1, "Unreadable")],
        &[(1, 1, "Boolean(true)")],
        &[(1, 1, "Text(\"text\")")],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
        &[(1, 1, "number:4000000000000000")],
        &[(1, 1, "Text(\"b\")")],
    ];
    let expected_formulas = [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some("Ordinary { text: \"\" }"),
        Some("Unsupported { kind: \"dataTable\", text: \"A1\" }"),
        Some("Unsupported { kind: \"unsupported\", text: \"A1\" }"),
        Some("Unsupported { kind: \"array\", text: \"A1\" }"),
        Some("SharedDescendant { shared_index: Some(9) }"),
        Some("SharedDescendant { shared_index: None }"),
        None,
        None,
    ];
    for (case, (body, values, formulas)) in cases.into_iter().enumerate() {
        let bytes = sheet(body);
        let refused = match collect(&bytes, ReadPolicy::Strict) {
            // Indexed strict scans defer scalar refusals to ownership.
            Ok((Scanned::Done(scan), _)) => scan.index.as_ref().is_some_and(|index| {
                index
                    .cells
                    .iter()
                    .any(|cell| cell.literal_refusal.is_some())
            }),
            _ => true,
        };
        assert!(refused, "strict must refuse {body}");
        let (Scanned::Done(scan), result) = collect(&bytes, ReadPolicy::Compatible).unwrap() else {
            panic!("compatible must not request an ownership index")
        };
        assert_eq!(
            (result.values.len(), result.formulas.len()),
            (values, formulas),
            "{body}"
        );
        assert_eq!(
            result.values,
            expected_values[case]
                .iter()
                .map(|(row, col, value)| (*row, *col, (*value).to_owned()))
                .collect::<Vec<_>>(),
            "{body}"
        );
        if let Some(expected) = expected_formulas[case] {
            assert!(result.formulas[0].starts_with("Formula { row: 1, col: 1, sequence: 0,"));
            assert!(
                result.formulas[0].contains(expected),
                "{body}: {}",
                result.formulas[0]
            );
        }
        assert!(scan.cells.is_empty());
        assert!(scan.index.is_none());
    }
    let (_, missing) =
        collect(&sheet("<row><c><v>1</v></c></row>"), ReadPolicy::Compatible).unwrap();
    assert_eq!(
        missing.values,
        [(1, 1, "number:3ff0000000000000".to_owned())]
    );
    let (_, unknown) = collect(
        &sheet("<row r=\"1\"><c r=\"A1\" t=\"future\"><v>1</v></c></row>"),
        ReadPolicy::Compatible,
    )
    .unwrap();
    assert_eq!(unknown.values, [(1, 1, "Unreadable".to_owned())]);
    let (_, cdata) = collect(
        &sheet("<row><c t=\"str\"><v><![CDATA[text]]></v></c></row>"),
        ReadPolicy::Compatible,
    )
    .unwrap();
    assert_eq!(cdata.values, [(1, 1, "Text(\"text\")".to_owned())]);
}

#[test]
fn compatibility_keeps_xml_grid_budget_and_cancellation_safety() {
    for bytes in [
        b"<!DOCTYPE worksheet><worksheet/>".as_slice(),
        b"<worksheet>&unknown;</worksheet>",
        b"<worksheet><sheetData></worksheet>",
        b"<worksheet>\xff</worksheet>",
        &sheet("<row r=\"1\"><c r=\"XFE1\"/></row>"),
        &sheet("<row r=\"1048577\"/>"),
    ] {
        assert!(collect(bytes, ReadPolicy::Compatible).is_err());
    }
    let bytes = sheet("<row r=\"1\"><c r=\"A1\"><f>1</f></c></row>");
    let mut options = ReadOptions::default();
    options.limits.max_formula_cells = 0;
    let request = ScanRequest {
        strategy: ScanStrategy::Plain,
        retention: ScanRetention {
            values: true,
            formula_events: true,
            marks: false,
            geometry: false,
        },
    };
    assert!(
        scan_with(
            &bytes,
            &options,
            request,
            &mut 0,
            &mut 0,
            Collected::default()
        )
        .is_err()
    );
    options.limits.max_formula_cells = 100;
    options.limits.max_cells = 0;
    assert!(
        scan_with(
            &bytes,
            &options,
            request,
            &mut 0,
            &mut 0,
            Collected::default()
        )
        .is_err()
    );
    options.limits.max_cells = 100;
    options.limits.max_xml_depth = 1;
    assert!(
        scan_with(
            &bytes,
            &options,
            request,
            &mut 0,
            &mut 0,
            Collected::default()
        )
        .is_err()
    );
    options.limits.max_xml_depth = 128;
    for (kind, range) in [
        ("shared", "A1:XFD1048576"),
        ("array", "A1:XFD1048576"),
        ("shared", "A1:XFE2"),
        ("array", "A1:XFE2"),
    ] {
        let bytes = sheet(&format!(
            "<row r=\"1\"><c r=\"A1\"><f t=\"{kind}\" si=\"0\" ref=\"{range}\">1</f></c></row>"
        ));
        assert!(
            scan_with(
                &bytes,
                &options,
                request,
                &mut 0,
                &mut 0,
                Collected::default()
            )
            .is_err()
        );
    }
    let token = formualizer_eval::engine::CancelToken::new();
    token.cancel();
    options.cancel = Some(token);
    assert!(
        scan_with(
            &bytes,
            &options,
            request,
            &mut 0,
            &mut 0,
            Collected::default()
        )
        .is_err()
    );
}

#[test]
#[ignore = "needs FORMUALIZER_SCAN_DIFF_FILES"]
fn listed_compatible_parts_preserve_strict_values_and_source_events() {
    let list =
        std::fs::read_to_string(std::env::var("FORMUALIZER_SCAN_DIFF_FILES").unwrap()).unwrap();
    let (mut packages, mut parts, mut accepted) = (0, 0, 0);
    for path in list.lines().map(str::trim).filter(|path| !path.is_empty()) {
        let data = std::fs::read(path).unwrap();
        let mut archive = zip::ZipArchive::new(IoCursor::new(data)).unwrap();
        for i in 0..archive.len() {
            let mut member = archive.by_index(i).unwrap();
            if !member.name().ends_with(".xml") && !member.name().ends_with(".rels") {
                continue;
            }
            let mut bytes = Vec::new();
            member.read_to_end(&mut bytes).unwrap();
            parts += 1;
            let compatible = collect(&bytes, ReadPolicy::Compatible);
            if let Ok((Scanned::Done(_), strict)) = collect(&bytes, ReadPolicy::Strict) {
                let (Scanned::Done(_), compatible) =
                    compatible.unwrap_or_else(|error| panic!("{path}: {}: {error}", member.name()))
                else {
                    panic!("compatible rescan")
                };
                assert_eq!(strict, compatible, "{path}: {}", member.name());
                accepted += 1;
            }
        }
        packages += 1;
    }
    eprintln!(
        "compatible scan: {parts} XML parts / {packages} packages; {accepted} strict-accepted parts preserve values and formula events"
    );
    assert!(packages > 0 && accepted > 0);
}
