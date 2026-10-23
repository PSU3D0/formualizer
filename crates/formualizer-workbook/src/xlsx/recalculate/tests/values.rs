use super::super::{
    IoError, XlsxRecalculateOptions,
    metadata::{SharedStringTable, Styles, raw_attribute},
    sheet,
    values::{Context, IngestedValues, RawCell, ValueSink, dimension_hint},
    xml,
};
use super::metadata::{annotation, probe};
use calamine::{DataRef, Reader, Xlsx};
use formualizer_common::{DateSystem, LiteralValue};
use formualizer_eval::engine::WorkbookLoadLimits;
use std::{collections::BTreeMap, io::Cursor};
const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";

// Test-only raw-source adapter until the coordinated additive emission fields
// arrive. It is deliberately not a second production worksheet pass.
fn raw_cells<'a>(
    bytes: &'a [u8],
    options: &XlsxRecalculateOptions,
) -> BTreeMap<(u32, u32), RawCell<'a>> {
    let mut cells = BTreeMap::new();
    let (mut current, mut start) = (None, 0);
    xml::walk(bytes, options, |path, node| {
        if xml::path_is(path, xml::MAIN, &["worksheet", "sheetData", "row", "c"])
            && let xml::Kind::Open { .. } = node.kind
        {
            let pos = sheet::SourceRect::parse(node.value("r").unwrap()).unwrap();
            current = Some((pos.first_row, pos.first_col));
            cells.insert(
                current.unwrap(),
                RawCell {
                    kind: raw_attribute(bytes, &node, "t"),
                    style: raw_attribute(bytes, &node, "s"),
                    payload: None,
                },
            );
        }
        if xml::path_is(
            path,
            xml::MAIN,
            &["worksheet", "sheetData", "row", "c", "v"],
        ) {
            match node.kind {
                xml::Kind::Open { empty, .. } => {
                    start = node.span.end;
                    if empty {
                        cells.get_mut(&current.unwrap()).unwrap().payload =
                            Some(&bytes[start..start]);
                    }
                }
                xml::Kind::Close => {
                    cells.get_mut(&current.unwrap()).unwrap().payload =
                        Some(&bytes[start..node.span.start])
                }
                _ => {}
            }
        }
        Ok(())
    })
    .unwrap();
    cells
}
fn literal(value: &DataRef<'_>) -> Option<LiteralValue> {
    Some(match value {
        DataRef::Empty => return None,
        DataRef::Float(n) => LiteralValue::Number(*n).finite_or_num_error(),
        DataRef::Int(n) => LiteralValue::Number(*n as f64).finite_or_num_error(),
        DataRef::DateTime(dt) => LiteralValue::Number(dt.as_f64()).finite_or_num_error(),
        DataRef::Bool(b) => LiteralValue::Boolean(*b),
        DataRef::String(s) | DataRef::DateTimeIso(s) | DataRef::DurationIso(s) => {
            LiteralValue::Text(s.clone())
        }
        DataRef::SharedString(s) => LiteralValue::Text((*s).to_owned()),
        DataRef::Error(e) => {
            use formualizer_common::{ExcelError, ExcelErrorKind as K};
            let kind = match e {
                calamine::CellErrorType::Div0 => K::Div,
                calamine::CellErrorType::NA => K::Na,
                calamine::CellErrorType::Name => K::Name,
                calamine::CellErrorType::Null => K::Null,
                calamine::CellErrorType::Num => K::Num,
                calamine::CellErrorType::Ref => K::Ref,
                calamine::CellErrorType::Value => K::Value,
                _ => K::Error,
            };
            LiteralValue::Error(ExcelError::new(kind))
        }
    })
}
fn compare(
    data: &[u8],
    sst: &[u8],
    styles: &[u8],
    sparse: bool,
) -> Result<IngestedValues, IoError> {
    let options = XlsxRecalculateOptions::default();
    let strings = SharedStringTable::parse(sst, &options)?;
    let formats = Styles::parse(styles, &options)?;
    let raw = raw_cells(data, &options);
    let mut book = Xlsx::new(Cursor::new(probe(Some(sst), Some(styles), data))).unwrap();
    let mut reader = book.worksheet_cells_reader("Probe").unwrap();
    let dimensions = dimension_hint(data, &options);
    let mut expected = BTreeMap::new();
    let mut expected_error = None;
    loop {
        match reader.next_cell_with_formula_metadata() {
            Ok(Some(record)) if record.formula.is_none() => {
                if let Some(value) = literal(&record.value) {
                    expected.insert(record.pos, (value, annotation(&record.value)));
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(error) => {
                expected_error = Some(IoError::Calamine(calamine::Error::Xlsx(error)).to_string());
                break;
            }
        }
    }
    let limits = WorkbookLoadLimits::default();
    let mut sink = ValueSink::new(Context {
        name: "Probe",
        source: data,
        strings: &strings,
        styles: &formats,
        dimensions,
        chunk_rows: 2,
        date_system: DateSystem::Excel1904,
        limits: &limits,
    })?;
    let mut pending_error = None;
    let result = sheet::scanner::scan_emitting(
        data,
        &options,
        sheet::ScanRetention::Indexed,
        &mut 0,
        &mut 0,
        |event| {
            let source = raw[&(event.row, event.col)];
            if pending_error.is_none()
                && let Err(error) = sink.push(event, source)
            {
                pending_error = Some(error);
            }
            Ok(())
        },
    )?;
    if let Some(error) = pending_error {
        assert_eq!(Some(error.to_string()), expected_error);
        return Err(error);
    }
    assert!(
        expected_error.is_none(),
        "native accepted a Calamine refusal"
    );
    let sheet::Scanned::Done(scan) = result else {
        panic!("indexed scan requires completion")
    };
    for formula in &scan.cells {
        sink.observe_formula(formula.row, formula.col)?;
    }
    let plane = sink.finish(scan.bounds)?;
    assert_eq!(plane.values_observed, expected.len());
    assert_eq!(plane.values_handed_to_engine, expected.len());
    assert_eq!(plane.used_sparse_fallback, sparse);
    assert_eq!(plane.arrow.date_system, DateSystem::Excel1904);
    assert!(
        plane.dimensions.0 >= scan.bounds.0 as usize
            && plane.dimensions.1 >= scan.bounds.1 as usize
    );
    for ((row, col), (value, format)) in expected {
        let actual = plane.arrow.get_cell_value(row as usize, col as usize);
        if let (LiteralValue::Number(a), LiteralValue::Number(b)) = (&actual, &value) {
            assert_eq!(a.to_bits(), b.to_bits());
        } else {
            assert_eq!(actual, value, "value at {row}:{col}");
        }
        assert_eq!(
            plane.arrow.format_id(row as usize, col as usize),
            format,
            "format at {row}:{col}"
        );
    }
    Ok(plane)
}
fn context() -> (String, String) {
    (
        format!("<sst xmlns=\"{MAIN}\"><si><t>zero</t></si><si><t>one</t></si><si><t/></si></sst>"),
        format!(
            "<styleSheet xmlns=\"{MAIN}\"><cellXfs><xf numFmtId=\"46\"/><xf numFmtId=\"14\"/></cellXfs></styleSheet>"
        ),
    )
}
#[test]
fn dense_borrowed_values_empty_text_dates_and_formula_caches() {
    let (sst, styles) = context();
    let data = format!(
        concat!(
            "<worksheet xmlns=\"{}\"><dimension ref=\"A1:H3\"/><sheetData>",
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>1</v></c><c r=\"B1\" t=\"s\"><v>2</v></c>",
            "<c r=\"C1\" t=\"str\"><v/></c><c r=\"D1\"><v/></c><c r=\"E1\" s=\"1\"><v>1.25</v></c>",
            "<c r=\"F1\" t=\"b\"><v>1</v></c><c r=\"G1\" t=\"e\"><v>#N/A</v></c><c r=\"H1\"><f>1+1</f><v>999</v></c></row>",
            "<row r=\"2\"><c r=\"A2\" t=\"str\"><v>source text</v></c><c r=\"B2\" t=\"inlineStr\"><is><t>a&amp;b</t></is></c>",
            "<c r=\"C2\" s=\"&#49;\"><v>0.5</v></c><c r=\"D2\" s=\"+1\"><v>60</v></c><c r=\"E2\" s=\"1\"><v>0.5</v></c></row>",
            "</sheetData></worksheet>"
        ),
        MAIN
    );
    let plane = compare(data.as_bytes(), sst.as_bytes(), styles.as_bytes(), false).unwrap();
    assert_eq!(plane.arrow.get_cell_value(0, 7), LiteralValue::Empty);
    assert_eq!(plane.arrow.get_cell_value(0, 3), LiteralValue::Empty);
    assert_eq!(
        plane.arrow.get_cell_value(0, 2),
        LiteralValue::Text(String::new())
    );
}
#[test]
fn sparse_gap_and_no_dimension_column_growth_match_calamine() {
    let (sst, styles) = context();
    for dimension in ["<dimension ref=\"A1:C300\"/>", ""] {
        let data = format!(
            "<worksheet xmlns=\"{MAIN}\">{dimension}<sheetData><row r=\"1\"><c r=\"A1\" t=\"s\"><v>1</v></c></row><row r=\"200\"><c r=\"C200\" s=\"1\"><v>60</v></c></row></sheetData></worksheet>"
        );
        compare(data.as_bytes(), sst.as_bytes(), styles.as_bytes(), true).unwrap();
    }
}
#[test]
fn raw_shared_indices_and_out_of_range_match_calamine() {
    let (sst, styles) = context();
    for payload in ["+1", "01", " 1", "999", "4294967296"] {
        let data = format!(
            "<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"s\"><v>{payload}</v></c></row></sheetData></worksheet>"
        );
        let result = compare(data.as_bytes(), sst.as_bytes(), styles.as_bytes(), false);
        assert_eq!(result.is_err(), matches!(payload, "999" | "4294967296"));
    }
}
#[test]
fn advisory_prefix_is_bounded_and_machine_remains_authoritative() {
    let options = XlsxRecalculateOptions::default();
    let prefixed = format!(
        "<x:worksheet xmlns:x=\"{MAIN}\"><x:dimension ref=\"A1:E9\"/><x:sheetData/></x:worksheet>"
    );
    assert_eq!(dimension_hint(prefixed.as_bytes(), &options), (9, 5));
    let truncated = format!("<worksheet xmlns=\"{MAIN}\"><dimension ref=\"A1:C3\"/><!--");
    assert_eq!(dimension_hint(truncated.as_bytes(), &options), (3, 3));
    assert!(
        sheet::scanner::scan(
            truncated.as_bytes(),
            &options,
            sheet::ScanRetention::Plain,
            &mut 0,
            &mut 0
        )
        .is_err()
    );
    let (sst, styles) = context();
    let data = format!(
        "<worksheet xmlns=\"{MAIN}\"><!--{}--><dimension ref=\"A1:C2\"/><sheetData><row r=\"1\"><c r=\"C1\" t=\"s\"><v>1</v></c></row></sheetData></worksheet>",
        "x".repeat(80_000)
    );
    assert_eq!(dimension_hint(data.as_bytes(), &options), (1, 1));
    let plane = compare(data.as_bytes(), sst.as_bytes(), styles.as_bytes(), true).unwrap();
    assert_eq!(plane.dimensions, (2, 3));
}
#[test]
#[ignore = "needs FORMUALIZER_SCAN_DIFF_FILES"]
fn listed_native_value_planes_match_calamine() {
    super::scan_differential::exclusive(|| {
        use super::super::{admit_source, apply_patches, inert_sheets, ingest_view, package};
        let paths =
            std::fs::read_to_string(std::env::var("FORMUALIZER_SCAN_DIFF_FILES").unwrap()).unwrap();
        let options = XlsxRecalculateOptions::default();
        let (mut files, mut sheets, mut values, mut sparse) = (0, 0, 0, 0);
        for path in paths.lines().map(str::trim).filter(|s| !s.is_empty()) {
            let bytes = std::fs::read(path).unwrap();
            let mut admitted = admit_source(&bytes, &options).unwrap();
            let mut read = |name: &str| {
                let exists = admitted.archive.file_names().any(|n| n == name);
                exists.then(|| {
                    package::read_part(
                        &mut admitted.archive,
                        name,
                        options.limits.max_worksheet_bytes,
                    )
                    .unwrap()
                })
            };
            let strings = read("xl/sharedStrings.xml")
                .map(|data| SharedStringTable::parse(&data, &options).unwrap())
                .unwrap_or_default();
            let styles = read("xl/styles.xml")
                .map(|data| Styles::parse(&data, &options).unwrap())
                .unwrap_or_default();
            let mut edits = package::Edits {
                replace: BTreeMap::new(),
                add: BTreeMap::new(),
            };
            for (sheet, plan) in admitted.sheets.iter().zip(&admitted.plans) {
                let patches = ingest_view::patches(plan, &options.cancel).unwrap();
                if !patches.is_empty() {
                    edits.replace.insert(
                        sheet.part.clone(),
                        apply_patches(&plan.data, patches, options.limits.max_worksheet_bytes)
                            .unwrap(),
                    );
                }
            }
            inert_sheets::project(
                &mut admitted.archive,
                &admitted.sheets,
                &mut edits,
                &options,
            )
            .unwrap();
            let view = package::rewrite(&bytes, &mut admitted.archive, &edits, &options).unwrap();
            let mut workbook = Xlsx::new(Cursor::new(view)).unwrap();
            for (sheet, plan) in admitted.sheets.iter().zip(&admitted.plans) {
                let limits = WorkbookLoadLimits::default();
                let raw = raw_cells(&plan.data, &options);
                let mut sink = ValueSink::new(Context {
                    name: &sheet.name,
                    source: &plan.data,
                    strings: &strings,
                    styles: &styles,
                    dimensions: dimension_hint(&plan.data, &options),
                    chunk_rows: 2048,
                    date_system: admitted.date_system,
                    limits: &limits,
                })
                .unwrap();
                let result = sheet::scanner::scan_emitting(
                    &plan.data,
                    &options,
                    sheet::ScanRetention::Indexed,
                    &mut 0,
                    &mut 0,
                    |event| {
                        if !plan
                            .ownership
                            .children
                            .contains_key(&(event.row, event.col))
                        {
                            let source = raw[&(event.row, event.col)];
                            sink.push(event, source)?;
                        }
                        Ok(())
                    },
                )
                .unwrap();
                assert!(matches!(result, sheet::Scanned::Done(_)));
                for formula in &plan.cells {
                    sink.observe_formula(formula.row, formula.col).unwrap();
                }
                let plane = sink.finish(plan.bounds).unwrap();
                let mut reader = workbook.worksheet_cells_reader(&sheet.name).unwrap();
                let mut expected_count = 0;
                while let Some(record) = reader.next_cell_with_formula_metadata().unwrap() {
                    if record.formula.is_some() {
                        continue;
                    }
                    let Some(expected) = literal(&record.value) else {
                        continue;
                    };
                    let (row, col) = (record.pos.0 as usize, record.pos.1 as usize);
                    let actual = plane.arrow.get_cell_value(row, col);
                    if let (LiteralValue::Number(a), LiteralValue::Number(b)) = (&actual, &expected)
                    {
                        assert_eq!(
                            a.to_bits(),
                            b.to_bits(),
                            "{path} / {} / {row}:{col}",
                            sheet.name
                        );
                    } else {
                        assert_eq!(actual, expected, "{path} / {} / {row}:{col}", sheet.name);
                    }
                    assert_eq!(
                        plane.arrow.format_id(row, col),
                        annotation(&record.value),
                        "{path} / {} / {row}:{col} format",
                        sheet.name
                    );
                    expected_count += 1;
                }
                assert_eq!(plane.values_observed, expected_count);
                assert_eq!(plane.values_handed_to_engine, expected_count);
                assert_eq!(plane.arrow.date_system, admitted.date_system);
                sheets += 1;
                values += expected_count;
                sparse += usize::from(plane.used_sparse_fallback);
            }
            files += 1;
        }
        eprintln!(
            "native Arrow values: {files} packages / {sheets} sheets / {values} populated values / {sparse} sparse sheets, zero value/format divergences"
        );
        assert!(files > 0);
    });
}
#[test]
fn encoded_raw_type_refusal_matches_calamine() {
    let (sst, styles) = context();
    let data = format!(
        "<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"&#110;\"><v>1</v></c></row></sheetData></worksheet>"
    );
    assert!(compare(data.as_bytes(), sst.as_bytes(), styles.as_bytes(), false).is_err());
}
