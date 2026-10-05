#![cfg(feature = "xlsx-recalc")]
//! Source recalculation of the ZIP containers real producers write (see
//! `tests/fixtures/zip_containers/generate.py`). Untouched members keep
//! every byte; rewritten members take one simple, consistent form.
use calamine::{Data, Reader, Xlsx};
use formualizer_workbook::recalculate_xlsx_bytes;
use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
};
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

fn h16(b: &[u8], i: usize) -> usize {
    u16::from_le_bytes([b[i], b[i + 1]]) as usize
}
fn h32(b: &[u8], i: usize) -> usize {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize
}
/// A member's raw bytes, parsed independently of the library.
struct Record {
    /// Central record with its local-offset field zeroed.
    central: Vec<u8>,
    /// Local header, extra fields, payload and data descriptor.
    member: Vec<u8>,
    flags: usize,
    local_extra: usize,
    central_extra: usize,
    /// Local header plus payload, without any descriptor.
    header_and_payload: usize,
}
fn records(bytes: &[u8]) -> BTreeMap<String, Record> {
    let footer = bytes.windows(4).rposition(|w| w == b"PK\x05\x06").unwrap();
    let start = h32(bytes, footer + 16);
    let mut at = start;
    let mut entries = Vec::new();
    while at < footer {
        assert_eq!(&bytes[at..at + 4], b"PK\x01\x02");
        let len = 46 + h16(bytes, at + 28) + h16(bytes, at + 30) + h16(bytes, at + 32);
        entries.push((at, len, h32(bytes, at + 42)));
        at += len;
    }
    let mut starts: Vec<_> = entries.iter().map(|e| e.2).collect();
    starts.push(start);
    starts.sort_unstable();
    entries
        .into_iter()
        .map(|(c, len, local)| {
            let name =
                String::from_utf8(bytes[c + 46..c + 46 + h16(bytes, c + 28)].to_vec()).unwrap();
            let end = starts[starts.partition_point(|s| *s <= local)];
            let mut central = bytes[c..c + len].to_vec();
            central[42..46].fill(0);
            let local_extra = h16(bytes, local + 28);
            let record = Record {
                central,
                member: bytes[local..end].to_vec(),
                flags: h16(bytes, c + 8),
                local_extra,
                central_extra: h16(bytes, c + 30),
                header_and_payload: 30 + h16(bytes, local + 26) + local_extra + h32(bytes, c + 20),
            };
            (name, record)
        })
        .collect()
}
fn contents(bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    (0..z.len())
        .map(|i| {
            let mut f = z.by_index(i).unwrap();
            let mut out = Vec::new();
            // Verifies the CRC-32 against the central record.
            f.read_to_end(&mut out).unwrap();
            (f.name().to_owned(), out)
        })
        .collect()
}
fn value(bytes: &[u8], sheet: &str, row: u32, col: u32) -> Data {
    let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
    x.worksheet_range(sheet)
        .unwrap()
        .get_value((row, col))
        .cloned()
        .unwrap()
}

#[test]
fn producer_containers_recalculate_and_keep_untouched_members_byte_identical() {
    for (name, input) in FIXTURES {
        let result = recalculate_xlsx_bytes(input, Default::default())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let output = &result.bytes;
        assert_eq!(value(output, "Data", 0, 1), Data::Float(20.0), "{name}");
        assert_eq!(value(output, "Summary", 0, 0), Data::Float(300.0), "{name}");
        // Recalculating the output changes nothing.
        let again = recalculate_xlsx_bytes(output, Default::default()).unwrap();
        assert_eq!(&again.bytes, output, "{name}: second run");
        let (before, after) = (contents(input), contents(output));
        assert_eq!(
            before.keys().collect::<Vec<_>>(),
            after.keys().collect::<Vec<_>>()
        );
        let (old, new) = (records(input), records(output));
        let mut rewritten = 0;
        for (member, data) in &before {
            let (a, b) = (&old[member], &new[member]);
            if after[member] == *data {
                assert_eq!(a.member, b.member, "{name}: {member} local bytes");
                assert_eq!(a.central, b.central, "{name}: {member} central record");
                continue;
            }
            rewritten += 1;
            // Simple form: no extra fields, no descriptor, local fields equal
            // to central ones; name, version, method and time kept.
            assert_eq!(b.flags, a.flags & !8, "{name}: {member} flags");
            assert_eq!((b.local_extra, b.central_extra), (0, 0), "{name}: {member}");
            assert_eq!(b.member.len(), b.header_and_payload, "{name}: {member}");
            assert_eq!(b.member[4..14], b.central[6..16], "{name}: {member}");
            assert_eq!(b.member[14..26], b.central[16..28], "{name}: {member}");
            assert_eq!(b.central[4..6], a.central[4..6], "{name}: {member}");
            assert_eq!(b.central[36..42], a.central[36..42], "{name}: {member}");
            assert_eq!(b.member[8..14], a.member[8..14], "{name}: {member}");
        }
        assert_eq!(rewritten, result.worksheet_parts_changed, "{name}");
        // Every fixture but LibreOffice's (which wrote caches) is stale.
        assert_eq!(rewritten > 0, name != "libreoffice.xlsx", "{name}");
    }
}
