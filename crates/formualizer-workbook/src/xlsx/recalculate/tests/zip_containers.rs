//! Package admission of the ZIP containers real producers write: the
//! metadata-only extra fields and 32-bit data descriptors are admitted;
//! unknown, malformed, ZIP64 and encryption metadata is still refused. The
//! committed fixtures come from `tests/fixtures/zip_containers/generate.py`;
//! the raw writer below builds the edge cases.
use super::super::{XlsxRecalculateOptions, package};
use super::dynamic_admission::refused;
use crate::test_source_xlsx as source_xlsx;
use std::io::{Cursor, Read};
use zip::ZipArchive;

macro_rules! fixture {
    ($name:literal) => {
        (
            $name,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/zip_containers/",
                $name
            ))
            .as_slice(),
        )
    };
}
const FIXTURES: [(&str, &[u8]); 6] = [
    fixture!("openpyxl.xlsx"),
    fixture!("excel_growth_hint.xlsx"),
    fixture!("descriptor_unsigned.xlsx"),
    fixture!("ntfs_times.xlsx"),
    fixture!("info_zip.xlsx"),
    fixture!("libreoffice.xlsx"),
];

fn admit(bytes: &[u8]) -> Result<(), crate::IoError> {
    package::admit(bytes, &XlsxRecalculateOptions::default()).map(|_| ())
}

#[test]
fn ordinary_discovery_errors_are_not_replaced_by_unread_payload_errors() {
    use crate::xlsx::options::ReadOptions;
    use std::collections::HashSet;
    for duplicate in [false, true] {
        let mut entries = workbook();
        if duplicate {
            entries.push(entries[0].clone());
        }
        for damaged in [0, entries.len() - 1] {
            let mut bytes = build(&entries);
            let mut archive = ZipArchive::new(Cursor::new(&bytes)).unwrap();
            let offset = if duplicate && damaged == entries.len() - 1 {
                // Both named entries must be audited; damage the final local
                // payload by walking the writer's raw local records below.
                let local = bytes.windows(4).rposition(|w| w == b"PK\x03\x04").unwrap();
                local + 30 + entries[damaged].name.len() + entries[damaged].local_extra.len()
            } else {
                archive.by_index(damaged).unwrap().data_start() as usize
            };
            drop(archive);
            bytes[offset] ^= 1;
            let result = package::admit_retaining(&bytes, &ReadOptions::default(), |archive| {
                let _ = package::read_part(archive, &entries[0].name, 1 << 20);
                Err::<((), HashSet<String>), _>(crate::IoError::Backend {
                    backend: "semantic".into(),
                    message: "must follow CRC".into(),
                })
            });
            let error = match result {
                Ok(_) => panic!("damaged payload admitted"),
                Err(e) => e.to_string(),
            };
            assert!(
                error.contains("must follow CRC"),
                "discovery failure must win: {error}"
            );
        }
        let bytes = build(&entries);
        let mut options = ReadOptions::default();
        options.limits.max_expanded_bytes = entries.iter().map(|e| e.data.len()).sum::<usize>() - 1;
        assert!(package::admit_retaining(&bytes, &options, |_| Ok(((), HashSet::new()))).is_ok());
    }
}

#[test]
fn ordinary_decoded_unicode_name_collisions_skip_shadowed_payloads() {
    use crate::xlsx::options::ReadOptions;
    let mut entries = workbook();
    for (i, entry) in entries.iter_mut().take(2).enumerate() {
        entry.name = format!("raw-{i}.xml");
        let mut extra = vec![1];
        extra.extend_from_slice(&crc32(entry.name.as_bytes()).to_le_bytes());
        extra.extend_from_slice(b"same.xml");
        entry.central_extra = field(0x7075, &extra);
        entry.local_extra = entry.central_extra.clone();
    }
    let bytes = build(&entries);
    let archive = ZipArchive::new(Cursor::new(&bytes)).unwrap();
    assert!(
        archive.len() < entries.len(),
        "fixture must collide after Unicode name decoding"
    );
    // Corrupt the first physical payload, which the aggregate name map hid.
    let local_name_len = entries[0].name.len();
    let offset = 30 + local_name_len + entries[0].local_extra.len();
    let mut bytes = bytes;
    bytes[offset] ^= 1;
    assert!(
        package::admit_retaining(&bytes, &ReadOptions::default(), |_| Ok((
            (),
            std::collections::HashSet::new()
        )))
        .is_ok()
    );
    assert!(
        admit(&bytes).is_err(),
        "Strict still refuses Unicode aliases"
    );
}

#[test]
fn ordinary_admission_inflates_nothing_until_consumed() {
    use crate::xlsx::options::ReadOptions;
    use std::collections::HashSet;
    let entries = workbook();
    let bytes = build(&entries);
    let needed: HashSet<_> = [entries[1].name.clone()].into_iter().collect();
    let mut options = ReadOptions::default();
    options.limits.max_worksheet_bytes = 8;
    package::take_part_inflates();
    let (mut archive, ()) =
        package::admit_retaining(&bytes, &options, |_| Ok(((), needed))).unwrap();
    assert_eq!(
        package::retained_bytes(&archive),
        0,
        "no speculative consumer buffers retained"
    );
    assert!(package::take_part(&mut archive, &entries[1].name, 8).is_err());
    assert!(package::take_part_inflates().values().all(|&n| n == 1));
}

#[test]
fn compatible_container_reads_skip_rewrite_only_admission() {
    use crate::xlsx::options::ReadOptions;
    for case in 0..6 {
        let mut entries = workbook();
        match case {
            0 => entries[0].central_extra = field(0xBEEF, &[1, 2]),
            1 => entries[0].local_extra = field(0xBEEF, &[1, 2]),
            2 => entries[0].comment = b"producer comment".to_vec(),
            3 => entries[0].gap = 3,
            4 => entries[0].name = "_xmlsignatures/sig1.xml".into(),
            5 => entries[0].name = "xl/richData/value.xml".into(),
            _ => unreachable!(),
        }
        let bytes = build(&entries);
        assert!(admit(&bytes).is_err(), "strict case {case}");
        let mut archive = package::admit_with(&bytes, &ReadOptions::default(), |_| {
            panic!("compatible package read must not invoke strict binding")
        })
        .unwrap_or_else(|e| panic!("compatible case {case}: {e}"));
        assert_eq!(
            package::read_part(&mut archive, &entries[1].name, 1 << 20).unwrap(),
            entries[1].data
        );
    }
}

#[test]
fn compatible_container_reads_keep_payload_and_resource_defences() {
    use crate::xlsx::options::ReadOptions;
    for case in 0..7 {
        let mut entries = workbook();
        let mut options = ReadOptions::default();
        match case {
            0 => entries[0].central_extra = vec![0xEF, 0xBE, 255, 255],
            1 => entries[0].flags = 1,
            2 => options.limits.max_entries = 1,
            3 => options.limits.max_expanded_bytes = 1,
            4 => options.limits.max_input_bytes = 1,
            5 => {
                let token = formualizer_eval::engine::CancelToken::new();
                token.cancel();
                options.cancel = Some(token);
            }
            6 => {}
            _ => unreachable!(),
        }
        let mut bytes = build(&entries);
        if case == 6 {
            bytes[30 + entries[0].name.len()] ^= 1;
        }
        let result = package::admit_with(&bytes, &options, |_| panic!("strict binding"))
            .and_then(|mut archive| package::read_part(&mut archive, &entries[0].name, 1 << 20));
        assert!(result.is_err(), "selected safety case {case}");
    }
}

#[test]
fn compatible_relationship_extraction_has_no_strict_binding_authority() {
    use crate::xlsx::options::ReadOptions;
    let ordinary = r#"<Relationship Id="x" Type="urn:sheet" Target="xl/workbook.xml"/>"#;
    let bodies = [
        ordinary.to_owned(),
        format!("<wrapper>{ordinary}</wrapper>"),
        ordinary.replace("Target=", "TargetMode=\"other\" Target="),
        ordinary.replace("xl/workbook.xml", "xl//workbook.xml"),
        format!("<Relationship Id=\"x\" Type=\"urn:first\" Target=\"xl/missing.xml\"/>{ordinary}"),
        ordinary.replace(" Type=\"urn:sheet\"", ""),
        ordinary.replace("xl/workbook.xml", "../../escape.xml"),
    ];
    for (case, body) in bodies.into_iter().enumerate() {
        let ns = if case == 0 {
            "urn:foreign"
        } else {
            crate::xlsx::xml::RELS
        };
        let mut entries = workbook();
        entries
            .iter_mut()
            .find(|e| e.name == "_rels/.rels")
            .unwrap()
            .data = format!("<Relationships xmlns=\"{ns}\">{body}</Relationships>").into_bytes();
        let bytes = build(&entries);
        let strict = XlsxRecalculateOptions::default();
        let mut archive = package::admit(&bytes, &strict).unwrap();
        assert!(
            package::relationships(&mut archive, "", &strict).is_err(),
            "strict case {case}"
        );
        let relationships =
            package::relationships_read(&mut archive, "", &ReadOptions::default()).unwrap();
        if case >= 5 {
            assert!(relationships.is_empty());
        } else {
            assert_eq!(relationships.len(), 1);
            assert_eq!(relationships["x"].kind, "urn:sheet");
            assert_eq!(
                relationships["x"].target.as_deref(),
                Some("xl/workbook.xml")
            );
        }
    }
}

#[test]
fn ordinary_duplicate_crc_checks_only_selected_identity() {
    use crate::xlsx::options::ReadOptions;
    for corrupt_first in [true, false] {
        let mut entries = workbook();
        entries.push(entries[0].clone());
        let mut bytes = build(&entries);
        let index = if corrupt_first { 0 } else { entries.len() - 1 };
        // This raw writer uses stored members without extras/descriptors here.
        let local: usize = entries[..index]
            .iter()
            .map(|e| 30 + e.name.len() + e.data.len())
            .sum();
        bytes[local + 30 + entries[index].name.len()] ^= 1;
        let mut archive = package::admit_with(&bytes, &ReadOptions::default(), |_| {
            panic!("strict binding")
        })
        .unwrap();
        assert_eq!(
            package::read_part(&mut archive, &entries[0].name, 1 << 20).is_ok(),
            corrupt_first,
            "only last-wins selected member is decoded"
        );
        assert!(admit(&bytes).is_err());
    }
}

#[test]
fn ordinary_duplicate_expansion_does_not_count_shadowed_members() {
    use crate::xlsx::options::ReadOptions;
    let mut entries = workbook();
    entries.push(entries[0].clone());
    let physical: usize = entries.iter().map(|e| e.data.len()).sum();
    let mut options = ReadOptions::default();
    options.limits.max_expanded_bytes = physical - entries[0].data.len();
    let bytes = build(&entries);
    assert!(
        package::admit_with(&bytes, &options, |_| panic!("strict binding")).is_ok(),
        "unread shadowed output is not charged"
    );
    // Even an oversized shadowed member emits no output and is not charged.
    entries[0].data = vec![b'x'; options.limits.max_expanded_bytes + 1];
    let bytes = build(&entries);
    assert!(package::admit_with(&bytes, &options, |_| panic!("strict binding")).is_ok());
}

#[test]
fn duplicate_physical_valid_members_keep_permissive_name_resolution() {
    use crate::xlsx::options::ReadOptions;
    let mut entries = workbook();
    let mut winner = entries[0].clone();
    winner.data = b"duplicate winner".to_vec();
    entries.push(winner.clone());
    let bytes = build(&entries);
    let mut archive = package::admit_with(&bytes, &ReadOptions::default(), |_| {
        panic!("strict binding")
    })
    .unwrap();
    assert_eq!(archive.len(), entries.len() - 1);
    assert_eq!(
        package::read_part(&mut archive, &winner.name, 1024).unwrap(),
        winner.data
    );
}

#[test]
fn ordinary_main_and_external_readers_agree_on_duplicate_and_unicode_selection() {
    use crate::{NativeXlsxAdapter, SpreadsheetReader};
    use source_xlsx::*;
    let bytes = book(&[Ws::new("Sheet1", vec![formula("A1", "[1]Data!A1")])], "");
    let mut p = unpack(&bytes);
    let link = "xl/externalLinks/externalLink1.xml";
    p.insert(link.into(), format!("<externalLink xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><externalBook r:id=\"r\"><sheetNames><sheetName val=\"Data\"/></sheetNames><sheetDataSet><sheetData sheetId=\"0\"><row r=\"1\"><cell r=\"A1\"><v>7</v></cell></row></sheetData></sheetDataSet></externalBook></externalLink>"));
    p = edit(
        p,
        "xl/workbook.xml",
        "</sheets>",
        "</sheets><externalReferences><externalReference r:id=\"e\"/></externalReferences>",
    );
    p = edit(
        p,
        WB_RELS,
        "</Relationships>",
        &format!(
            "<Relationship Id=\"e\" Type=\"{OFFICE}/externalLink\" Target=\"externalLinks/externalLink1.xml\"/></Relationships>"
        ),
    );
    p = edit(
        p,
        TYPES,
        "</Types>",
        &format!(
            "<Override PartName=\"/{link}\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml\"/></Types>"
        ),
    );
    for target in ["xl/workbook.xml", WB_RELS, SHEET, link] {
        for unicode in [false, true] {
            for selected in [false, true] {
                let mut entries = entries_from(&pack(&p));
                let shadow = entries.iter().position(|e| e.name == target).unwrap();
                let winner = entries[shadow].clone();
                if unicode {
                    let e = &mut entries[shadow];
                    e.name = "raw-shadow.xml".into();
                    let mut extra = vec![1];
                    extra.extend_from_slice(&crc32(e.name.as_bytes()).to_le_bytes());
                    extra.extend_from_slice(target.as_bytes());
                    e.local_extra = field(0x7075, &extra);
                    e.central_extra = e.local_extra.clone();
                }
                entries.push(winner);
                let damaged = if selected { entries.len() - 1 } else { shadow };
                let offset: usize = entries[..damaged]
                    .iter()
                    .map(|e| 30 + e.name.len() + e.local_extra.len() + e.data.len())
                    .sum();
                let mut bytes = build(&entries);
                bytes[offset
                    + 30
                    + entries[damaged].name.len()
                    + entries[damaged].local_extra.len()] ^= 1;
                let result = NativeXlsxAdapter::open_bytes(bytes).and_then(|mut a| {
                    if a.cached_external_link_values().is_none() {
                        return Err(crate::xlsx::options::unsupported("failed cache", "test"));
                    }
                    a.read_sheet("Sheet1").map(|_| ())
                });
                assert_eq!(
                    result.is_ok(),
                    !selected,
                    "target={target} unicode={unicode} selected={selected}"
                );
            }
        }
    }
}

#[test]
fn producer_containers_are_admitted() {
    for (name, bytes) in FIXTURES {
        if let Err(e) = admit(bytes) {
            panic!("{name}: {e}");
        }
        package::admit_with(bytes, &crate::xlsx::options::ReadOptions::default(), |_| {
            panic!("compatible producer fixture must not invoke strict binding")
        })
        .unwrap_or_else(|e| panic!("compatible physical audit {name}: {e}"));
    }
}

// Put large opaque payloads before *all* discovery metadata. Releasing a
// cache only after discovery/finalisation cannot bound this fixture's peak.
fn opaque_first(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    let block = vec![b'x'; 1024 * 1024];
    for i in 0..2 {
        writer
            .start_file(format!("opaque/{i}.bin"), options)
            .unwrap();
        for _ in 0..16 {
            writer.write_all(&block).unwrap();
        }
    }
    let mut archive = ZipArchive::new(Cursor::new(bytes)).unwrap();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        if file.name() == "[Content_Types].xml" {
            data = String::from_utf8(data)
                .unwrap()
                .replace(
                    "</Types>",
                    "<Default Extension=\"bin\" ContentType=\"application/octet-stream\"/></Types>",
                )
                .into_bytes();
        }
        let options = if file.name() == "unused.bin" {
            options.compression_method(zip::CompressionMethod::Stored)
        } else {
            options
        };
        writer.start_file(file.name(), options).unwrap();
        writer.write_all(&data).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

#[test]
fn opaque_payloads_are_streamed_before_discovery_not_retained() {
    let bytes = opaque_first(FIXTURES[0].1);
    package::take_part_inflates();
    package::take_audit_buffer_peak();
    let source = super::super::admit_source(&bytes, &XlsxRecalculateOptions::default()).unwrap();
    let counts = package::take_part_inflates();
    assert!(
        package::retained_bytes(&source.archive) < 64 * 1024,
        "opaque payloads must not be retained"
    );
    assert_eq!(counts["opaque/0.bin"], 1);
    assert_eq!(counts["opaque/1.bin"], 1);
    // B6 reuses verified bootstrap metadata and transfers each worksheet's
    // single audited buffer into its retained publication marks.
    for sheet in source.sheets {
        assert_eq!(counts[&sheet.part], 1);
    }
    assert!(counts.values().all(|count| *count == 1), "{counts:?}");
    // This metric covers the actual streaming audit buffer, not all source
    // allocations. The companion process-RSS regression checks retained heap.
    assert_eq!(package::take_audit_buffer_peak(), 64 * 1024);
}

#[test]
fn ordinary_opaque_members_are_never_inflated() {
    use crate::xlsx::options::ReadOptions;
    let bytes = opaque_first(FIXTURES[0].1);
    package::take_part_inflates();
    package::take_audit_buffer_peak();
    let (archive, ()) = package::admit_retaining(&bytes, &ReadOptions::default(), |archive| {
        package::read_part(archive, "_rels/.rels", 64 * 1024)?;
        Ok((
            (),
            ["xl/worksheets/sheet1.xml".to_owned()]
                .into_iter()
                .collect(),
        ))
    })
    .unwrap();
    assert!(package::retained_bytes(&archive) < 64 * 1024);
    let counts = package::take_part_inflates();
    assert!(counts.values().all(|&n| n == 1), "{counts:?}");
    assert!(!counts.contains_key("opaque/0.bin"));
    assert!(!counts.contains_key("opaque/1.bin"));
    assert_eq!(package::take_audit_buffer_peak(), 0);
}

#[test]
fn opaque_audit_finishes_before_semantic_refusal_and_late_crc_error() {
    let mut entries = workbook();
    entries
        .iter_mut()
        .find(|e| e.name == "xl/worksheets/sheet1.xml")
        .unwrap()
        .data = b"<bad/>".to_vec();
    let mut unused = entries[0].clone();
    unused.name = "unused.bin".into();
    unused.data = b"unused payload".to_vec();
    entries.push(unused);
    let mut bytes = opaque_first(&build(&entries));
    let end = find(&bytes, "unused.bin").data_end;
    bytes[end - 1] ^= 1;
    package::take_audit_buffer_peak();
    let expected = admit(&bytes).expect_err("late CRC audit refuses");
    let Err(actual) = super::super::admit_source(&bytes, &XlsxRecalculateOptions::default()) else {
        panic!("CRC must outrank the worksheet root refusal");
    };
    assert_eq!(format!("{expected:?}"), format!("{actual:?}"));
    assert_eq!(package::take_audit_buffer_peak(), 64 * 1024);
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
        }
    }
    !crc
}
#[derive(Clone, Copy, PartialEq)]
enum Descriptor {
    None,
    Signed,
    Unsigned,
    /// A ZIP64 descriptor: signature, CRC and 64-bit sizes.
    Zip64,
}
/// One stored member of the raw writer.
#[derive(Clone)]
struct Entry {
    name: String,
    data: Vec<u8>,
    flags: u16,
    method: u16,
    local_extra: Vec<u8>,
    central_extra: Vec<u8>,
    comment: Vec<u8>,
    descriptor: Descriptor,
    /// Local CRC/sizes of a descriptor member: zero, or the final values.
    local_sizes: bool,
    /// Bytes inserted after the member (a gap).
    gap: usize,
}
fn workbook() -> Vec<Entry> {
    entries_from(FIXTURES[0].1)
}
fn entries_from(bytes: &[u8]) -> Vec<Entry> {
    let mut z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    (0..z.len())
        .map(|i| {
            let mut f = z.by_index(i).unwrap();
            let mut data = Vec::new();
            f.read_to_end(&mut data).unwrap();
            Entry {
                name: f.name().to_owned(),
                data,
                flags: 0,
                method: 0,
                local_extra: Vec::new(),
                central_extra: Vec::new(),
                comment: Vec::new(),
                descriptor: Descriptor::None,
                local_sizes: true,
                gap: 0,
            }
        })
        .collect()
}
fn build(entries: &[Entry]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut central = Vec::new();
    for e in entries {
        let offset = out.len() as u32;
        let flags = e.flags
            | if e.descriptor == Descriptor::None {
                0
            } else {
                8
            };
        let mut fixed = Vec::new();
        for v in [20u16, flags, e.method, 0, 0x0021] {
            fixed.extend_from_slice(&v.to_le_bytes());
        }
        let mut sizes = crc32(&e.data).to_le_bytes().to_vec();
        sizes.extend_from_slice(&(e.data.len() as u32).to_le_bytes());
        sizes.extend_from_slice(&(e.data.len() as u32).to_le_bytes());
        out.extend_from_slice(b"PK\x03\x04");
        out.extend_from_slice(&fixed);
        if e.descriptor == Descriptor::None || e.local_sizes {
            out.extend_from_slice(&sizes);
        } else {
            out.extend_from_slice(&[0; 12]);
        }
        out.extend_from_slice(&(e.name.len() as u16).to_le_bytes());
        out.extend_from_slice(&(e.local_extra.len() as u16).to_le_bytes());
        out.extend_from_slice(e.name.as_bytes());
        out.extend_from_slice(&e.local_extra);
        out.extend_from_slice(&e.data);
        match e.descriptor {
            Descriptor::None => {}
            Descriptor::Signed => {
                out.extend_from_slice(b"PK\x07\x08");
                out.extend_from_slice(&sizes);
            }
            Descriptor::Unsigned => out.extend_from_slice(&sizes),
            Descriptor::Zip64 => {
                out.extend_from_slice(b"PK\x07\x08");
                out.extend_from_slice(&sizes[..4]);
                out.extend_from_slice(&(e.data.len() as u64).to_le_bytes());
                out.extend_from_slice(&(e.data.len() as u64).to_le_bytes());
            }
        }
        out.extend(std::iter::repeat_n(0u8, e.gap));
        central.extend_from_slice(b"PK\x01\x02");
        central.extend_from_slice(&45u16.to_le_bytes());
        central.extend_from_slice(&fixed);
        central.extend_from_slice(&sizes);
        for v in [
            e.name.len() as u16,
            e.central_extra.len() as u16,
            e.comment.len() as u16,
            0,
            0,
        ] {
            central.extend_from_slice(&v.to_le_bytes());
        }
        central.extend_from_slice(&0u32.to_le_bytes());
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(e.name.as_bytes());
        central.extend_from_slice(&e.central_extra);
        central.extend_from_slice(&e.comment);
    }
    let start = out.len() as u32;
    out.extend_from_slice(&central);
    out.extend_from_slice(b"PK\x05\x06");
    for v in [0u16, 0, entries.len() as u16, entries.len() as u16] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.extend_from_slice(&(central.len() as u32).to_le_bytes());
    out.extend_from_slice(&start.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}
fn field(id: u16, data: &[u8]) -> Vec<u8> {
    let mut out = id.to_le_bytes().to_vec();
    out.extend_from_slice(&(data.len() as u16).to_le_bytes());
    out.extend_from_slice(data);
    out
}
/// Excel's growth hint: signature 0xA028, padding size, zero padding.
fn growth_hint(value: u16, padding: usize) -> Vec<u8> {
    let mut data = 0xA028u16.to_le_bytes().to_vec();
    data.extend_from_slice(&value.to_le_bytes());
    data.extend(std::iter::repeat_n(0u8, padding));
    field(0xA220, &data)
}
fn timestamp(flags: u8, times: usize) -> Vec<u8> {
    let mut data = vec![flags];
    data.extend(std::iter::repeat_n(0x5Fu8, 4 * times));
    field(0x5455, &data)
}
fn unix_owner() -> Vec<u8> {
    field(0x7875, &[1, 4, 0xE8, 3, 0, 0, 4, 0xE8, 3, 0, 0])
}
fn ntfs() -> Vec<u8> {
    let mut data = vec![0; 4];
    data.extend_from_slice(&1u16.to_le_bytes());
    data.extend_from_slice(&24u16.to_le_bytes());
    data.extend_from_slice(&[7; 24]);
    field(0x000A, &data)
}
/// Apply `change` to the sheet member (index 4 has stored data and a
/// successor) and to nothing else.
fn variant(change: impl Fn(&mut Entry)) -> Vec<u8> {
    let mut entries = workbook();
    let target = entries
        .iter()
        .position(|e| e.name == "xl/worksheets/sheet1.xml")
        .unwrap();
    change(&mut entries[target]);
    build(&entries)
}

#[test]
fn metadata_only_extra_fields_and_descriptors_are_admitted() {
    admit(&build(&workbook())).unwrap();
    // Every member at once, in each admitted form.
    let mut all = workbook();
    for (i, e) in all.iter_mut().enumerate() {
        match i % 5 {
            0 => {
                e.local_extra = growth_hint(512, 512);
                e.descriptor = Descriptor::Signed;
                e.local_sizes = false;
            }
            // Excel also writes a zero padding value.
            1 => e.local_extra = growth_hint(0, 101),
            2 => {
                e.local_extra = [timestamp(3, 2), unix_owner()].concat();
                e.central_extra = [timestamp(3, 1), unix_owner()].concat();
                e.descriptor = Descriptor::Unsigned;
                e.local_sizes = false;
            }
            3 => {
                e.local_extra = [ntfs(), growth_hint(4, 4)].concat();
                e.central_extra = [ntfs(), growth_hint(0, 0)].concat();
            }
            _ => {
                // NTFS without attributes; final local sizes with a descriptor.
                e.local_extra = field(0x000A, &[0; 4]);
                e.central_extra = timestamp(7, 3);
                e.descriptor = Descriptor::Unsigned;
            }
        }
        e.method = 0;
    }
    admit(&build(&all)).unwrap();
}

#[test]
fn unknown_malformed_and_zip64_extra_fields_are_refused() {
    let cases: Vec<(Vec<u8>, bool, &str)> = vec![
        // Unknown IDs: Info-ZIP Unicode path (renames), NT security
        // descriptor, strong encryption header.
        (field(0x7075, &[1, 0, 0, 0, 0, b'x']), false, "0x7075"),
        (field(0x4453, &[0; 4]), true, "0x4453"),
        (field(0x0017, &[0; 8]), false, "0x0017"),
        (field(0x0001, &[0; 16]), false, "ZIP64 member"),
        (field(0x0001, &[0; 16]), true, "ZIP64 member"),
        (
            field(0x9901, &[2, 0, b'A', b'E', 3, 8, 0]),
            false,
            "encrypted",
        ),
        // Malformed blocks.
        (vec![0x20, 0xA2, 4], false, "malformed ZIP extra field"),
        (
            [growth_hint(4, 4), vec![0, 0, 0]].concat(),
            false,
            "malformed ZIP extra field",
        ),
        (
            {
                let mut f = growth_hint(4, 4);
                f[2] = 200;
                f
            },
            false,
            "malformed ZIP extra field",
        ),
        (field(0xA220, &[0x28, 0xA0, 4]), false, "0xA220"),
        (field(0xA220, &[0x29, 0xA0, 4, 0, 0, 0]), false, "0xA220"),
        (
            field(0xA220, &[0x28, 0xA0, 4, 0, 0, 1, 0, 0]),
            false,
            "0xA220",
        ),
        (
            [growth_hint(4, 4), growth_hint(4, 4)].concat(),
            false,
            "duplicate ZIP extra field",
        ),
        (timestamp(3, 1), false, "0x5455"),
        (timestamp(9, 1), true, "0x5455"),
        (field(0x5455, &[]), true, "0x5455"),
        (
            field(0x7875, &[2, 4, 0, 0, 0, 0, 4, 0, 0, 0, 0]),
            false,
            "0x7875",
        ),
        (
            field(0x7875, &[1, 4, 0, 0, 0, 0, 4, 0, 0, 0]),
            true,
            "0x7875",
        ),
        (field(0x7875, &[1, 0, 0]), true, "0x7875"),
        (field(0x000A, &[1, 0, 0, 0]), false, "0x000A"),
        (field(0x000A, &[0; 31]), true, "0x000A"),
        (
            {
                let mut f = ntfs();
                f[8] = 2;
                f
            },
            false,
            "0x000A",
        ),
    ];
    for (extra, central, needle) in cases {
        let bytes = variant(|e| {
            if central {
                e.central_extra = extra.clone();
            } else {
                e.local_extra = extra.clone();
            }
        });
        refused(admit(&bytes), needle);
    }
}

#[test]
fn inconsistent_and_zip64_descriptors_are_refused() {
    let descriptor = |e: &mut Entry| {
        e.descriptor = Descriptor::Signed;
        e.local_sizes = false;
    };
    // Baseline: admitted.
    admit(&variant(descriptor)).unwrap();
    refused(
        admit(&variant(|e| {
            descriptor(e);
            e.descriptor = Descriptor::Zip64;
        })),
        "ZIP data descriptor mismatch",
    );
    // Empty member: a ZIP64 descriptor whose high words happen to match
    // leaves eight unaccounted bytes.
    refused(
        admit(&variant(|e| {
            descriptor(e);
            e.data.clear();
            e.descriptor = Descriptor::Zip64;
        })),
        "unaccounted ZIP bytes",
    );
    // Flag set, no descriptor bytes.
    let mut entries = workbook();
    entries[4].flags = 8;
    refused(admit(&build(&entries)), "ZIP data descriptor mismatch");
    // Descriptor CRC/size differs from the authoritative central record.
    for at in [4, 8, 12] {
        let mut bytes = variant(descriptor);
        let sheet = find(&bytes, "xl/worksheets/sheet1.xml");
        bytes[sheet.data_end + at] ^= 1;
        refused(admit(&bytes), "ZIP data descriptor mismatch");
    }
    // A local CRC/size that is neither zero nor final.
    for at in [14, 18, 22] {
        let mut bytes = variant(descriptor);
        let sheet = find(&bytes, "xl/worksheets/sheet1.xml");
        bytes[sheet.local + at] = 1;
        refused(admit(&bytes), "inconsistent ZIP local/central metadata");
    }
}

#[test]
fn flags_methods_comments_and_gaps_are_refused() {
    for (flags, needle) in [
        (0x0001, "encrypted ZIP member"),
        (0x0040, "encrypted ZIP member"),
        (0x2000, "encrypted ZIP member"),
        (0x0020, "general-purpose flags"),
        (0x0010, "general-purpose flags"),
    ] {
        refused(admit(&variant(|e| e.flags = flags)), needle);
    }
    refused(
        admit(&variant(|e| e.method = 12)),
        "unsupported ZIP compression method",
    );
    refused(
        admit(&variant(|e| e.comment = b"note".to_vec())),
        "ZIP entry comment",
    );
    refused(admit(&variant(|e| e.gap = 3)), "unaccounted ZIP bytes");
}

struct Found {
    local: usize,
    data_end: usize,
}
fn find(bytes: &[u8], name: &str) -> Found {
    let z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut at = z.central_directory_start() as usize;
    let h16 = |i: usize| u16::from_le_bytes([bytes[i], bytes[i + 1]]) as usize;
    let h32 = |i: usize| u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
    loop {
        let len = h16(at + 28);
        if &bytes[at + 46..at + 46 + len] == name.as_bytes() {
            let local = h32(at + 42);
            let data = local + 30 + h16(local + 26) + h16(local + 28);
            return Found {
                local,
                data_end: data + h32(at + 20),
            };
        }
        at += 46 + len + h16(at + 30) + h16(at + 32);
    }
}

/// Every member carries extras and a descriptor; one in the middle is
/// replaced (growing) and one is added.
#[test]
fn rewrite_relocates_untouched_members_verbatim_and_simplifies_replaced_ones() {
    use package::Edits;
    let mut entries = workbook();
    for (i, e) in entries.iter_mut().enumerate() {
        e.local_extra = [growth_hint(256, 256), timestamp(1, 1)].concat();
        e.central_extra = [timestamp(1, 1), ntfs()].concat();
        e.descriptor = if i % 2 == 0 {
            Descriptor::Signed
        } else {
            Descriptor::Unsigned
        };
        e.local_sizes = i % 3 == 0;
    }
    let input = build(&entries);
    let sheet = "xl/worksheets/sheet1.xml";
    let body = b"<worksheet/>".repeat(300);
    let added = "xl/metadata.xml";
    let options = XlsxRecalculateOptions::default();
    let mut archive = package::admit(&input, &options).unwrap();
    let edits = Edits {
        replace: [(sheet.to_owned(), body.clone())].into(),
        add: [(added.to_owned(), b"<metadata/>".to_vec())].into(),
    };
    let output = package::rewrite(&input, &mut archive, &edits, &options).unwrap();
    admit(&output).unwrap();
    let mut z = ZipArchive::new(Cursor::new(&output)).unwrap();
    for e in &entries {
        let mut data = Vec::new();
        z.by_name(&e.name).unwrap().read_to_end(&mut data).unwrap();
        let expected = if e.name == sheet { &body } else { &e.data };
        assert_eq!(&data, expected, "{}", e.name);
    }
    let mut data = Vec::new();
    z.by_name(added).unwrap().read_to_end(&mut data).unwrap();
    assert_eq!(data, b"<metadata/>");
    let (old, new) = (raw(&input), raw(&output));
    for e in &entries {
        let ((oc, om), (nc, nm)) = (&old[&e.name], &new[&e.name]);
        if e.name == sheet {
            // No extras, no descriptor, final local CRC/sizes.
            assert_eq!(nm[6] & 8, 0);
            assert_eq!(&nm[28..30], &[0, 0]);
            assert_eq!(&nc[30..32], &[0, 0]);
            assert_eq!(nm[14..26], nc[16..28]);
            assert_eq!(nm.len(), 30 + sheet.len() + h32(nc, 20));
        } else {
            assert_eq!(om, nm, "{} local bytes", e.name);
            assert_eq!(oc, nc, "{} central record", e.name);
        }
    }
}
fn h32(b: &[u8], i: usize) -> usize {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize
}
/// Per member: central record (offset zeroed) and the local bytes up to
/// the next member or the directory.
fn raw(bytes: &[u8]) -> std::collections::BTreeMap<String, (Vec<u8>, Vec<u8>)> {
    let z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let start = z.central_directory_start() as usize;
    let h16 = |i: usize| u16::from_le_bytes([bytes[i], bytes[i + 1]]) as usize;
    let mut at = start;
    let mut entries = Vec::new();
    for _ in 0..z.len() {
        let len = 46 + h16(at + 28) + h16(at + 30) + h16(at + 32);
        entries.push((at, len, h32(bytes, at + 42)));
        at += len;
    }
    let mut starts: Vec<_> = entries.iter().map(|e| e.2).collect();
    starts.push(start);
    starts.sort_unstable();
    entries
        .into_iter()
        .map(|(c, len, local)| {
            let name = String::from_utf8(bytes[c + 46..c + 46 + h16(c + 28)].to_vec()).unwrap();
            let end = starts[starts.partition_point(|s| *s <= local)];
            let mut central = bytes[c..c + len].to_vec();
            central[42..46].fill(0);
            (name, (central, bytes[local..end].to_vec()))
        })
        .collect()
}
