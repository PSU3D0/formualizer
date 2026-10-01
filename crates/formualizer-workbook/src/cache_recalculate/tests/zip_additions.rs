//! FORM211-E: surgical ZIP32 member addition. Outputs are re-read by the
//! `zip` crate (which verifies CRCs) and re-audited by package admission.
use super::super::XlsxRecalculateOptions;
use super::super::package::{self, Edits};
use super::dynamic_admission::refused;
use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

const COMMENT: &[u8] = b"archive comment kept";
const NEW: &str = "xl/metadata.xml";

/// A stored member, deflated members and an archive comment.
fn source() -> Vec<u8> {
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    z.set_comment("archive comment kept");
    let time = zip::DateTime::from_date_and_time(2020, 1, 2, 3, 4, 6).unwrap();
    let deflated = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .last_modified_time(time);
    let stored = deflated.compression_method(CompressionMethod::Stored);
    for (name, body, options) in [
        ("[Content_Types].xml", "<Types/>".repeat(40), deflated),
        ("docProps/stored.bin", "stored bytes".repeat(10), stored),
        (
            "xl/worksheets/sheet1.xml",
            "<worksheet/>".repeat(50),
            deflated,
        ),
        ("zz/opaque.bin", "opaque".repeat(30), deflated),
    ] {
        z.start_file(name, options).unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}
fn contents(bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    (0..z.len())
        .map(|i| {
            let mut f = z.by_index(i).unwrap();
            let mut out = Vec::new();
            // read_to_end verifies the CRC-32.
            f.read_to_end(&mut out).unwrap();
            (f.name().to_owned(), out)
        })
        .collect()
}
fn h16(b: &[u8], i: usize) -> usize {
    u16::from_le_bytes(b[i..i + 2].try_into().unwrap()) as usize
}
fn h32(b: &[u8], i: usize) -> usize {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize
}
/// `(central offset, central length)` per entry, and the end-record offset.
fn directory(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut at = z.central_directory_start() as usize;
    let mut out = Vec::new();
    for _ in 0..z.len() {
        let len = 46 + h16(bytes, at + 28) + h16(bytes, at + 30) + h16(bytes, at + 32);
        out.push((at, len));
        at += len;
    }
    (out, at)
}
fn rewrite(
    bytes: &[u8],
    edits: &Edits,
    options: &XlsxRecalculateOptions,
) -> Result<Vec<u8>, crate::IoError> {
    let mut archive = package::admit(bytes, options)?;
    package::rewrite(bytes, &mut archive, edits, options)
}
fn edits(replace: &[(&str, &[u8])], add: &[(&str, &[u8])]) -> Edits {
    let map = |v: &[(&str, &[u8])]| {
        v.iter()
            .map(|(k, v)| ((*k).to_owned(), v.to_vec()))
            .collect()
    };
    Edits {
        replace: map(replace),
        add: map(add),
    }
}

#[test]
fn a_new_member_is_appended_and_old_records_and_payloads_are_preserved() {
    let input = source();
    let sheet = b"<worksheet><sheetData/></worksheet>".repeat(20);
    let metadata = b"<metadata>new member</metadata>".repeat(5);
    let options = XlsxRecalculateOptions::default();
    let output = rewrite(
        &input,
        &edits(&[("xl/worksheets/sheet1.xml", &sheet)], &[(NEW, &metadata)]),
        &options,
    )
    .unwrap();
    // Independent reader: every member, CRCs verified, the new one last.
    let before = contents(&input);
    let after = contents(&output);
    let mut expected = before.clone();
    expected.insert("xl/worksheets/sheet1.xml".into(), sheet.clone());
    expected.insert(NEW.into(), metadata.clone());
    assert_eq!(after, expected);
    let mut z = ZipArchive::new(Cursor::new(&output)).unwrap();
    assert_eq!(z.comment(), COMMENT);
    assert_eq!(z.len(), 5);
    assert_eq!(z.by_index(4).unwrap().name(), NEW);
    assert_eq!(
        z.by_name(NEW).unwrap().compression(),
        CompressionMethod::Deflated
    );
    assert_eq!(
        z.by_name("docProps/stored.bin").unwrap().compression(),
        CompressionMethod::Stored
    );
    // Old central records: unchanged except CRC/sizes of the replaced
    // member and relocated local offsets.
    let (old, old_end) = directory(&input);
    let (new, new_end) = directory(&output);
    let mut source = ZipArchive::new(Cursor::new(&input)).unwrap();
    for (i, (&(a, len), &(b, _))) in old.iter().zip(&new).enumerate() {
        let replaced = i == 2;
        for k in 0..len {
            let offset = (42..46).contains(&k);
            let sizes = replaced && (16..28).contains(&k);
            if !offset && !sizes {
                assert_eq!(input[a + k], output[b + k], "central {i} byte {k}");
            }
        }
        let (la, lb) = (h32(&input, a + 42), h32(&output, b + 42));
        let local_len = 30 + h16(&input, la + 26) + h16(&input, la + 28);
        for k in 0..local_len {
            if !(replaced && (14..26).contains(&k)) {
                assert_eq!(input[la + k], output[lb + k], "local {i} byte {k}");
            }
        }
        if !replaced {
            let f = source.by_index(i).unwrap();
            let size = f.compressed_size() as usize;
            let data = f.data_start() as usize;
            let shift = lb as isize - la as isize;
            let moved = (data as isize + shift) as usize;
            assert_eq!(
                input[data..data + size],
                output[moved..moved + size],
                "payload {i}"
            );
        }
    }
    // The new central record follows the old ones; its local record sits
    // right before the directory.
    let (nb, nlen) = new[4];
    assert_eq!(nb + nlen, new_end);
    let local = h32(&output, nb + 42);
    assert_eq!(&output[local..local + 4], b"PK\x03\x04");
    let directory_start = new[0].0;
    let local_len = 30 + NEW.len() + h32(&output, nb + 20);
    assert_eq!(local + local_len, directory_start);
    // End record: counts, size and offset patched; comment bytes kept.
    assert_eq!(h16(&output, new_end + 8), 5);
    assert_eq!(h16(&output, new_end + 10), 5);
    assert_eq!(h32(&output, new_end + 12), new_end - directory_start);
    assert_eq!(h32(&output, new_end + 16), directory_start);
    assert_eq!(output[new_end + 20..], input[old_end + 20..]);
    // The output passes the same bounded admission audit as any input.
    package::admit(&output, &options).unwrap();
}

#[test]
fn an_addition_alone_relocates_nothing_before_the_directory() {
    let input = source();
    let output = rewrite(&input, &edits(&[], &[(NEW, b"<m/>")]), &Default::default()).unwrap();
    let directory_start = ZipArchive::new(Cursor::new(&input))
        .unwrap()
        .central_directory_start() as usize;
    assert_eq!(output[..directory_start], input[..directory_start]);
    assert_eq!(contents(&output)[NEW], b"<m/>");
}

#[test]
fn duplicate_invalid_and_over_budget_additions_are_refused() {
    let input = source();
    let options = XlsxRecalculateOptions::default();
    for name in ["zz/opaque.bin", "ZZ/Opaque.BIN"] {
        refused(
            rewrite(&input, &edits(&[], &[(name, b"x")]), &options),
            "duplicate ZIP member",
        );
    }
    refused(
        rewrite(&input, &edits(&[], &[("xl/../evil.xml", b"x")]), &options),
        "non-canonical ZIP part",
    );
    let mut limited = options.clone();
    limited.limits.max_entries = 4;
    refused(
        rewrite(&input, &edits(&[], &[(NEW, b"x")]), &limited),
        "ZIP entry count limit",
    );
    let mut small = options.clone();
    small.limits.max_output_bytes = input.len() + 10;
    assert!(rewrite(&input, &edits(&[], &[(NEW, &[b'x'; 4096])]), &small).is_err());
}
