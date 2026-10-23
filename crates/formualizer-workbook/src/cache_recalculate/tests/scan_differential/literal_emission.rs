//! End-to-end value-plane oracle: candidate events versus the actual pinned
//! Calamine cell stream over the existing recalc ingestion view. No engine
//! construction, formula evaluation, or production ingestion wiring.
use super::*;
use crate::cache_recalculate::{admit_source, apply_patches, inert_sheets, ingest_view, package};
use calamine::{DataRef, Reader, Xlsx};
use sheet::machine::{
    emission::{DisplayString, Value},
    scan_emitting,
};
use std::collections::BTreeMap;
use std::io::Cursor;

fn strings(data: &[u8], options: &XlsxRecalculateOptions) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = None;
    xml::walk(data, options, |path, node| {
        if xml::path_is(path, xml::MAIN, &["sst", "si"]) {
            match node.kind {
                xml::Kind::Open { empty: true, .. } => out.push(String::new()),
                xml::Kind::Open { .. } => current = Some(DisplayString::default()),
                xml::Kind::Close => {
                    if let Some(value) = current.take() {
                        out.push(value.finish());
                    }
                }
                _ => {}
            }
        } else if let Some(value) = current.as_mut() {
            value.event(path, &node, data)?;
        }
        Ok(())
    })
    .unwrap();
    out
}
fn number(value: f64) -> String {
    format!("number:{:016x}", value.to_bits())
}
fn oracle(value: &DataRef<'_>) -> Option<String> {
    Some(match value {
        DataRef::Empty => return None,
        DataRef::Float(n) => number(*n),
        DataRef::Int(n) => number(*n as f64),
        DataRef::DateTime(n) => number(n.as_f64()),
        DataRef::Bool(b) => format!("bool:{b}"),
        DataRef::Error(e) => format!("error:{e}"),
        DataRef::String(s) | DataRef::DateTimeIso(s) | DataRef::DurationIso(s) => {
            format!("text:{s:?}")
        }
        DataRef::SharedString(s) => format!("text:{s:?}"),
    })
}

/// Returns serialized literal-event and populated literal-value counts.
fn compare_package(bytes: &[u8], label: &str) -> Result<(usize, usize), IoError> {
    let options = XlsxRecalculateOptions::default();
    let mut admitted = admit_source(bytes, &options)?;
    let table = if admitted
        .archive
        .file_names()
        .any(|n| n == "xl/sharedStrings.xml")
    {
        let data = package::read_part(
            &mut admitted.archive,
            "xl/sharedStrings.xml",
            options.limits.max_worksheet_bytes,
        )?;
        strings(&data, &options)
    } else {
        Vec::new()
    };
    // These are the unchanged old-scanner/ownership-derived edits used by
    // today's Calamine recalc stream, not edits made from candidate events.
    let mut edits = package::Edits {
        replace: BTreeMap::new(),
        add: BTreeMap::new(),
    };
    for (sheet, plan) in admitted.sheets.iter().zip(&admitted.plans) {
        let patches = ingest_view::patches(plan, &options.cancel)?;
        if !patches.is_empty() {
            edits.replace.insert(
                sheet.part.clone(),
                apply_patches(&plan.data, patches, options.limits.max_worksheet_bytes)?,
            );
        }
    }
    inert_sheets::project(
        &mut admitted.archive,
        &admitted.sheets,
        &mut edits,
        &options,
    )?;
    let view = package::rewrite(bytes, &mut admitted.archive, &edits, &options)?;
    let mut workbook = Xlsx::new(Cursor::new(view)).unwrap();
    let (mut events, mut populated) = (0, 0);
    for (sheet, plan) in admitted.sheets.iter().zip(&admitted.plans) {
        let mut expected = BTreeMap::new();
        let mut reader = workbook.worksheet_cells_reader(&sheet.name).unwrap();
        while let Some(record) = reader.next_cell_with_formula_metadata().unwrap() {
            if record.formula.is_none()
                && let Some(value) = oracle(&record.value)
            {
                expected.insert((record.pos.0 + 1, record.pos.1 + 1), value);
            }
        }
        let mut actual = BTreeMap::new();
        let result = scan_emitting(
            &plan.data,
            &options,
            sheet::Mode::Indexed,
            &mut 0,
            &mut 0,
            |literal| {
                events += 1;
                // The contextual sink omits the same proven generated children
                // as today's ingestion view, without changing source marks.
                if plan
                    .ownership
                    .children
                    .contains_key(&(literal.row, literal.col))
                {
                    return Ok(());
                }
                let _style_index = literal.style.as_deref();
                let value = match literal.value {
                    Value::Empty => return Ok(()),
                    Value::Number(n) => number(n),
                    Value::Boolean(b) => format!("bool:{b}"),
                    Value::Error(e) => format!("error:{e}"),
                    Value::Text(s) => format!("text:{s:?}"),
                    Value::Shared(index) => format!(
                        "text:{:?}",
                        table
                            .get(index as usize)
                            .unwrap_or_else(|| panic!("{label}: missing SST index {index}"))
                    ),
                    Value::Unreadable => panic!(
                        "{label}: unmasked unreadable value at {}:{}",
                        literal.row, literal.col
                    ),
                };
                assert!(actual.insert((literal.row, literal.col), value).is_none());
                Ok(())
            },
        )?;
        assert!(matches!(result, sheet::Scanned::Done(_)));
        assert_eq!(
            actual, expected,
            "literal emission diverged: {label} / {}",
            sheet.name
        );
        populated += actual.len();
    }
    Ok((events, populated))
}

#[test]
fn literal_empty_rich_escape_and_xml10_values_match_calamine() {
    exclusive(|| {
        let base = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/zip_containers/openpyxl.xlsx"),
        )
        .unwrap();
        let options = XlsxRecalculateOptions::default();
        let mut archive = package::admit(&base, &options).unwrap();
        let doc = format!(
            concat!(
                "<worksheet xmlns=\"{}\"><sheetData><row r=\"1\">",
                "<c r=\"A1\" t=\"str\"><v/></c><c r=\"B1\" t=\"d\"><v/></c>",
                "<c r=\"C1\" t=\"inlineStr\"><is/></c><c r=\"D1\"><v/></c>",
                "<c r=\"E1\" t=\"inlineStr\"><is><r><t> a </t></r><rPh><t>omit</t></rPh><r><t xml:space=\"preserve\"> b </t></r></is></c>",
                "<c r=\"F1\" t=\"inlineStr\"><is><t>_x000D__x005F_x000A_ </t></is></c>",
                "<c r=\"G1\" t=\"str\"><v>a\u{85}b\u{2028}c\r\nd&amp;e</v></c>",
                "<c r=\"H1\" t=\"inlineStr\"><is><t>a\u{85}b\u{2028}c\r\nd&amp;e</t></is></c>",
                "<c r=\"I1\"/><c r=\"J1\" t=\"b\"><v>0</v></c><c r=\"K1\" t=\"e\"><v>#N/A</v></c>",
                "</row></sheetData></worksheet>"
            ),
            xml::MAIN
        );
        let edits = package::Edits {
            replace: BTreeMap::from([("xl/worksheets/sheet1.xml".into(), doc.into_bytes())]),
            add: BTreeMap::new(),
        };
        let bytes = package::rewrite(&base, &mut archive, &edits, &options).unwrap();
        let (events, values) = compare_package(&bytes, "literal edge fixture").unwrap();
        assert_eq!((events, values), (11, 9));
    });
}

#[test]
fn committed_literal_emissions_match_calamine() {
    exclusive(|| {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let mut stack = vec![dir];
        let (mut accepted, mut refused, mut events, mut values) = (0, 0, 0, 0);
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "xlsx") {
                    match compare_package(
                        &std::fs::read(&path).unwrap(),
                        &path.display().to_string(),
                    ) {
                        Ok((e, v)) => {
                            accepted += 1;
                            events += e;
                            values += v;
                        }
                        Err(_) => refused += 1,
                    }
                }
            }
        }
        eprintln!(
            "literal fixtures: {accepted} admitted, {refused} source-admission refusals, {events} events, {values} populated values"
        );
        assert!(accepted > 0 && values > 0);
    });
}
#[test]
#[ignore = "needs FORMUALIZER_SCAN_DIFF_FILES"]
fn listed_literal_emissions_match_calamine() {
    exclusive(|| {
        let list =
            std::fs::read_to_string(std::env::var("FORMUALIZER_SCAN_DIFF_FILES").unwrap()).unwrap();
        let (mut files, mut events, mut values) = (0, 0, 0);
        for path in list.lines().map(str::trim).filter(|s| !s.is_empty()) {
            let (e, v) = compare_package(&std::fs::read(path).unwrap(), path)
                .unwrap_or_else(|e| panic!("{path}: {e:?}"));
            files += 1;
            events += e;
            values += v;
        }
        eprintln!(
            "literal corpus: {files} packages, {events} events, {values} populated values, zero divergences"
        );
        assert!(files > 0);
    });
}
