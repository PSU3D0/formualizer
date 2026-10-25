#![cfg(feature = "xlsx-recalc")]
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_workbook::{
    CancelToken, NativeXlsxAdapter, SpreadsheetReader, Workbook, XlsxLoadLimits,
    XlsxRecalculateLimits,
};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
};
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const SHEET: &str = "xl/worksheets/sheet1.xml";
const LINK: &str = "xl/externalLinks/externalLink1.xml";
fn parts(external: bool) -> BTreeMap<String, String> {
    let mut p = BTreeMap::from([
        (
            "_rels/.rels".into(),
            format!(
                "<Relationships xmlns=\"{RELS}\"><Relationship Id=\"r\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>"
            ),
        ),
        (
            "xl/workbook.xml".into(),
            format!(
                "<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"s\"/></sheets>{}</workbook>",
                if external {
                    "<externalReferences><externalReference r:id=\"e\"/></externalReferences>"
                } else {
                    ""
                }
            ),
        ),
        (
            "xl/_rels/workbook.xml.rels".into(),
            format!(
                "<Relationships xmlns=\"{RELS}\"><Relationship Id=\"s\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/>{}</Relationships>",
                if external {
                    format!(
                        "<Relationship Id=\"e\" Type=\"{OFFICE}/externalLink\" Target=\"externalLinks/externalLink1.xml\"/>"
                    )
                } else {
                    String::new()
                }
            ),
        ),
        (
            SHEET.into(),
            format!(
                "<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\"><f>{}</f><v>7</v></c></row></sheetData></worksheet>",
                if external { "[1]Data!A1" } else { "1+6" }
            ),
        ),
    ]);
    if external {
        p.insert("[Content_Types].xml".into(), format!("<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Override PartName=\"/{LINK}\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml\"/></Types>"));
        p.insert(LINK.into(), format!("<externalLink xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><externalBook r:id=\"r\"><sheetNames><sheetName val=\"Data\"/></sheetNames><sheetDataSet><sheetData sheetId=\"0\"><row r=\"1\"><cell r=\"A1\"><v>7</v></cell></row></sheetData></sheetDataSet></externalBook></externalLink>"));
    }
    p
}
fn pack(p: &BTreeMap<String, String>, method: CompressionMethod) -> Vec<u8> {
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, text) in p {
        z.start_file(
            name,
            SimpleFileOptions::default().compression_method(method),
        )
        .unwrap();
        z.write_all(text.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}
fn damage(bytes: &mut [u8], name: &str, size: bool) {
    let mut z = ZipArchive::new(Cursor::new(&*bytes)).unwrap();
    let file = z.by_name(name).unwrap();
    let local = file.header_start() as usize;
    let crc = file.crc32();
    let declared = file.size() as u32;
    drop(file);
    drop(z);
    let central = bytes
        .windows(4)
        .enumerate()
        .find(|(i, w)| {
            *w == b"PK\x01\x02" && bytes.get(i + 46..i + 46 + name.len()) == Some(name.as_bytes())
        })
        .unwrap()
        .0;
    if size {
        bytes[local + 22..local + 26].copy_from_slice(&(declared + 1).to_le_bytes());
        bytes[central + 24..central + 28].copy_from_slice(&(declared + 1).to_le_bytes());
    } else {
        bytes[local + 14..local + 18].copy_from_slice(&(crc ^ 1).to_le_bytes());
        bytes[central + 16..central + 20].copy_from_slice(&(crc ^ 1).to_le_bytes());
    }
}

#[test]
fn unused_corruption_and_huge_output_are_not_consumed() {
    let mut p = parts(false);
    p.insert(
        "xl/embeddings/unused.bin".into(),
        "x".repeat(2 * 1024 * 1024),
    );
    for corrupt in [false, true] {
        let mut bytes = pack(&p, CompressionMethod::Deflated);
        if corrupt {
            damage(&mut bytes, "xl/embeddings/unused.bin", false);
        }
        for aggregate in [4096, 4 * 1024 * 1024] {
            let limits = XlsxLoadLimits {
                max_part_bytes: 4096,
                max_expanded_bytes: aggregate,
                ..Default::default()
            };
            let mut a = NativeXlsxAdapter::open_bytes_with_limits(bytes.clone(), limits).unwrap();
            assert!(a.read_sheet("Sheet1").is_ok());
        }
    }
}
#[test]
fn selected_strings_and_styles_integrity_failures_are_not_skipped() {
    let mut p = parts(false);
    let relations = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
    *relations = relations.replace("</Relationships>", &format!("<Relationship Id=\"strings\" Type=\"{OFFICE}/sharedStrings\" Target=\"sharedStrings.xml\"/><Relationship Id=\"styles\" Type=\"{OFFICE}/styles\" Target=\"styles.xml\"/></Relationships>"));
    p.insert(
        "xl/sharedStrings.xml".into(),
        format!("<sst xmlns=\"{MAIN}\"><si><t>x</t></si></sst>"),
    );
    p.insert(
        "xl/styles.xml".into(),
        format!("<styleSheet xmlns=\"{MAIN}\"/>"),
    );
    for target in ["xl/sharedStrings.xml", "xl/styles.xml"] {
        for method in [CompressionMethod::Stored, CompressionMethod::Deflated] {
            let mut bytes = pack(&p, method);
            damage(&mut bytes, target, false);
            assert!(
                NativeXlsxAdapter::open_bytes(bytes).is_err(),
                "{target} {method:?}"
            );
        }
    }
}
#[test]
fn selected_crc_and_wrong_size_are_refused_for_both_methods() {
    for method in [CompressionMethod::Stored, CompressionMethod::Deflated] {
        for size in [false, true] {
            let mut bytes = pack(&parts(false), method);
            damage(&mut bytes, SHEET, size);
            assert!(
                NativeXlsxAdapter::open_bytes(bytes).is_err(),
                "{method:?} size={size}"
            );
        }
    }
}
#[test]
fn limits_are_applied_before_reader_access_and_at_current_position() {
    let bytes = pack(&parts(false), CompressionMethod::Stored);
    let limits = XlsxLoadLimits {
        max_input_bytes: bytes.len(),
        ..Default::default()
    };
    let mut prefixed = b"prefix".to_vec();
    prefixed.extend_from_slice(&bytes);
    let mut cursor = Cursor::new(prefixed);
    cursor.set_position(6);
    assert!(NativeXlsxAdapter::open_reader_with_limits(Box::new(cursor), limits.clone()).is_ok());
    assert!(
        NativeXlsxAdapter::open_bytes_with_limits(
            bytes.clone(),
            XlsxLoadLimits {
                max_input_bytes: bytes.len() - 1,
                ..limits
            }
        )
        .is_err()
    );
    struct NoRead;
    impl Read for NoRead {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("invalid config read input")
        }
    }
    assert!(
        NativeXlsxAdapter::open_reader_with_limits(
            Box::new(NoRead),
            XlsxLoadLimits {
                max_part_bytes: usize::MAX,
                ..Default::default()
            }
        )
        .is_err()
    );
    let cancel = CancelToken::new();
    cancel.cancel();
    assert!(
        NativeXlsxAdapter::open_bytes_cancellable_with_limits(bytes, cancel, Default::default())
            .is_err()
    );
}
#[test]
fn aggregate_is_actual_and_exact_boundaries_work() {
    let p = parts(false);
    let consumed: usize = p.values().map(String::len).sum();
    let bytes = pack(&p, CompressionMethod::Deflated);
    for (limit, accepted) in [
        (0, false),
        (consumed - 1, false),
        (consumed, true),
        (consumed + 1, true),
    ] {
        assert_eq!(
            NativeXlsxAdapter::open_bytes_with_limits(
                bytes.clone(),
                XlsxLoadLimits {
                    max_expanded_bytes: limit,
                    ..Default::default()
                }
            )
            .is_ok(),
            accepted,
            "{limit}"
        );
    }
}
#[test]
fn external_safety_failures_survive_retry_and_second_ingestion() {
    for method in [CompressionMethod::Stored, CompressionMethod::Deflated] {
        for size in [false, true] {
            let mut bytes = pack(&parts(true), method);
            damage(&mut bytes, LINK, size);
            let full = NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap();
            assert!(
                Workbook::from_reader(
                    full,
                    formualizer_workbook::LoadStrategy::EagerAll,
                    formualizer_workbook::WorkbookConfig::ephemeral()
                )
                .is_err()
            );
            let mut adapter = NativeXlsxAdapter::open_bytes(bytes).unwrap();
            assert!(adapter.cached_external_link_values().is_none());
            assert!(adapter.cached_external_link_values().is_none());
            assert!(
                adapter
                    .stream_into_engine(Workbook::new().engine_mut())
                    .is_err()
            );
            assert!(
                adapter
                    .stream_into_engine(Workbook::new().engine_mut())
                    .is_err()
            );
            assert!(adapter.read_sheet("Sheet1").is_err());
        }
    }
}
#[test]
fn external_budget_includes_real_metadata_rereads_and_is_memoized() {
    let p = parts(true);
    let opening: usize = [
        "_rels/.rels",
        "xl/_rels/workbook.xml.rels",
        "xl/workbook.xml",
        SHEET,
    ]
    .iter()
    .map(|n| p[*n].len())
    .sum();
    let lazy: usize = [
        "xl/_rels/workbook.xml.rels",
        "xl/workbook.xml",
        "[Content_Types].xml",
        LINK,
    ]
    .iter()
    .map(|n| p[*n].len())
    .sum();
    let bytes = pack(&p, CompressionMethod::Deflated);
    for (limit, accepted) in [
        (opening, false),
        (opening + lazy - 1, false),
        (opening + lazy, true),
    ] {
        let mut adapter = NativeXlsxAdapter::open_bytes_with_limits(
            bytes.clone(),
            XlsxLoadLimits {
                max_expanded_bytes: limit,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            adapter.cached_external_link_values().is_some(),
            accepted,
            "{limit}"
        );
        assert_eq!(
            adapter.cached_external_link_values().is_some(),
            accepted,
            "memoization/retry {limit}"
        );
        assert_eq!(
            adapter
                .stream_into_engine(Workbook::new().engine_mut())
                .is_ok(),
            accepted
        );
        assert_eq!(
            adapter
                .stream_into_engine(Workbook::new().engine_mut())
                .is_ok(),
            accepted
        );
    }
}
#[test]
fn semantic_invalid_cache_is_still_best_effort() {
    for body in [
        "<invalid/>",
        &format!("<externalLink xmlns=\"{MAIN}\"><ddeLink/></externalLink>"),
    ] {
        let mut p = parts(true);
        p.insert(LINK.into(), body.into());
        let mut adapter =
            NativeXlsxAdapter::open_bytes(pack(&p, CompressionMethod::Stored)).unwrap();
        assert!(adapter.cached_external_link_values().is_some());
        assert!(
            adapter
                .stream_into_engine(Workbook::new().engine_mut())
                .is_ok()
        );
    }
}
#[test]
fn external_depth_part_and_cancellation_failures_are_terminal() {
    let p = parts(true);
    let bytes = pack(&p, CompressionMethod::Deflated);
    let mut adapter = NativeXlsxAdapter::open_bytes_with_limits(
        bytes.clone(),
        XlsxLoadLimits {
            max_xml_depth: 5,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(adapter.cached_external_link_values().is_none());
    assert!(
        adapter
            .stream_into_engine(Workbook::new().engine_mut())
            .is_err()
    );
    let cancel = CancelToken::new();
    let mut adapter = NativeXlsxAdapter::open_bytes_cancellable_with_limits(
        bytes,
        cancel.clone(),
        Default::default(),
    )
    .unwrap();
    cancel.cancel();
    assert!(adapter.cached_external_link_values().is_none());
    assert!(adapter.cached_external_link_values().is_none());
    assert!(
        adapter
            .stream_into_engine(Workbook::new().engine_mut())
            .is_err()
    );
    let mut p = p;
    p.get_mut(LINK).unwrap().push_str(&" ".repeat(4096));
    let mut adapter = NativeXlsxAdapter::open_bytes_with_limits(
        pack(&p, CompressionMethod::Deflated),
        XlsxLoadLimits {
            max_part_bytes: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(adapter.cached_external_link_values().is_none());
    assert!(
        adapter
            .stream_into_engine(Workbook::new().engine_mut())
            .is_err()
    );
}
#[test]
fn external_unsupported_targets_and_absent_descriptions_remain_semantic() {
    for target in ["../escape.xml", "https://example.invalid/cache.xml"] {
        let mut p = parts(true);
        p.insert(
            "xl/_rels/workbook.xml.rels".into(),
            p["xl/_rels/workbook.xml.rels"].replace("externalLinks/externalLink1.xml", target),
        );
        let mut adapter =
            NativeXlsxAdapter::open_bytes(pack(&p, CompressionMethod::Deflated)).unwrap();
        assert!(adapter.cached_external_link_values().is_some());
        assert!(
            adapter
                .stream_into_engine(Workbook::new().engine_mut())
                .is_ok()
        );
    }
    let mut p = parts(true);
    p.remove(LINK);
    let mut adapter = NativeXlsxAdapter::open_bytes(pack(&p, CompressionMethod::Deflated)).unwrap();
    assert!(adapter.cached_external_link_values().is_some());
    assert!(
        adapter
            .stream_into_engine(Workbook::new().engine_mut())
            .is_ok()
    );
}
#[test]
fn external_does_not_sum_unused_expansion() {
    let mut p = parts(true);
    p.insert("unused.xml".into(), "x".repeat(1024 * 1024));
    let mut adapter = NativeXlsxAdapter::open_bytes_with_limits(
        pack(&p, CompressionMethod::Deflated),
        XlsxLoadLimits {
            max_expanded_bytes: 8192,
            max_part_bytes: 4096,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(adapter.cached_external_link_values().is_some());
    assert!(
        adapter
            .stream_into_engine(Workbook::new().engine_mut())
            .is_ok()
    );
}
#[test]
fn repeated_worksheet_targets_charge_each_actual_read() {
    let mut p = parts(false);
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</sheets>",
        "<sheet name=\"Second\" sheetId=\"2\" r:id=\"s\"/></sheets>",
    );
    let actual: usize = p.values().map(String::len).sum::<usize>() + p[SHEET].len();
    let bytes = pack(&p, CompressionMethod::Deflated);
    for (limit, accepted) in [(actual - 1, false), (actual, true)] {
        assert_eq!(
            NativeXlsxAdapter::open_bytes_with_limits(
                bytes.clone(),
                XlsxLoadLimits {
                    max_expanded_bytes: limit,
                    ..Default::default()
                }
            )
            .is_ok(),
            accepted
        );
    }
}
#[test]
fn explicit_path_limits_and_retained_source_apply_to_both_aliases() {
    use formualizer_workbook::{CalamineAdapter, XlsxPathSource};
    let bytes = pack(&parts(true), CompressionMethod::Deflated);
    for source in [XlsxPathSource::SharedFile, XlsxPathSource::DirectMmap] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.xlsx");
        std::fs::write(&path, &bytes).unwrap();
        let low = XlsxLoadLimits {
            max_input_bytes: bytes.len() - 1,
            ..Default::default()
        };
        assert!(
            NativeXlsxAdapter::open_path_with_source_and_limits(&path, source, low.clone())
                .is_err()
        );
        assert!(CalamineAdapter::open_path_with_source_and_limits(&path, source, low).is_err());
        let limits = XlsxLoadLimits {
            max_input_bytes: bytes.len(),
            ..Default::default()
        };
        let mut a =
            NativeXlsxAdapter::open_path_with_source_and_limits(&path, source, limits).unwrap();
        std::fs::rename(&path, dir.path().join("retained.xlsx")).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert!(a.cached_external_link_values().is_some());
        assert!(a.read_sheet("Sheet1").is_ok());
    }
}
#[test]
fn bounds_resource_failure_is_terminal_for_all_reads() {
    let mut p = parts(false);
    p.get_mut("xl/workbook.xml").unwrap().clone_from(&format!(
        "<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"s\"/><sheet name=\"Good\" sheetId=\"2\" r:id=\"good\"/></sheets></workbook>"
    ));
    p.get_mut("xl/_rels/workbook.xml.rels").unwrap().clone_from(&format!(
        "<Relationships xmlns=\"{RELS}\"><Relationship Id=\"s\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"good\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/good.xml\"/></Relationships>"
    ));
    p.insert(SHEET.into(), format!(
        "<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>too deep</t></is></c></row></sheetData></worksheet>"
    ));
    p.insert("xl/worksheets/good.xml".into(), format!(
        "<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\"><v>7</v></c></row></sheetData></worksheet>"
    ));
    let bytes = pack(&p, CompressionMethod::Deflated);
    let mut adapter = NativeXlsxAdapter::open_bytes_with_limits(
        bytes,
        XlsxLoadLimits {
            max_xml_depth: 5,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(adapter.sheet_bounds("Good"), Some((1, 1)));
    assert!(adapter.sheet_bounds("Sheet1").is_none());
    assert!(
        adapter.sheet_bounds("Good").is_none(),
        "cached bounds cannot escape terminal failure"
    );
    assert!(adapter.read_sheet("Good").is_err());
    assert!(adapter.read_cell("Good", 1, 1).is_err());
    assert!(adapter.read_range("Good", (1, 1), (1, 1)).is_err());
    assert!(
        adapter
            .stream_into_engine(Workbook::new().engine_mut())
            .is_err()
    );
}

#[test]
fn bounds_are_unavailable_after_external_safety_failure() {
    let p = parts(true);
    let opening: usize = [
        "_rels/.rels",
        "xl/_rels/workbook.xml.rels",
        "xl/workbook.xml",
        SHEET,
    ]
    .iter()
    .map(|name| p[*name].len())
    .sum();
    for corruption in [false, true] {
        let mut bytes = pack(&p, CompressionMethod::Deflated);
        let mut limits = XlsxLoadLimits::default();
        if corruption {
            damage(&mut bytes, LINK, false);
        } else {
            limits.max_expanded_bytes = opening;
        }
        let mut adapter = NativeXlsxAdapter::open_bytes_with_limits(bytes, limits).unwrap();
        assert!(adapter.cached_external_link_values().is_none());
        assert!(
            adapter.sheet_bounds("Sheet1").is_none(),
            "terminal external failure cannot permit bounds: corrupt={corruption}"
        );
        assert!(adapter.read_sheet("Sheet1").is_err());
    }
}

#[test]
fn ordinary_defaults_are_independent_of_recalculation() {
    let strict = XlsxRecalculateLimits::default();
    let limits = XlsxLoadLimits::default();
    assert_eq!(limits.max_input_bytes, 133_169_152);
    assert_eq!(limits.max_entries, 10_000);
    assert_eq!(limits.max_expanded_bytes, 256 * 1024 * 1024);
    assert_eq!(limits.max_part_bytes, 128 * 1024 * 1024);
    assert_eq!(limits.max_xml_depth, 128);
    assert_eq!(strict.max_input_bytes, 64 * 1024 * 1024);
    assert!(NativeXlsxAdapter::open_bytes_with_limits(Vec::new(), limits).is_err());
}
