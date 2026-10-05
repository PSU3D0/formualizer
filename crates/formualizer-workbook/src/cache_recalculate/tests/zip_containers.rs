//! Package admission of the ZIP containers real producers write: the
//! metadata-only extra fields and 32-bit data descriptors are admitted;
//! unknown, malformed, ZIP64 and encryption metadata is still refused. The
//! committed fixtures come from `tests/fixtures/zip_containers/generate.py`;
//! the raw writer below builds the edge cases.
use super::super::{XlsxRecalculateOptions, package};
use super::dynamic_admission::refused;
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
fn producer_containers_are_admitted() {
    for (name, bytes) in FIXTURES {
        if let Err(e) = admit(bytes) {
            panic!("{name}: {e}");
        }
    }
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
    let mut z = ZipArchive::new(Cursor::new(FIXTURES[0].1)).unwrap();
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
