//! Surgical ZIP32 package edits. ZIP7 supplies compression and CRC generation;
//! original local/central metadata is retained, not normalized by raw_copy_file.
//! Admission has rejected ZIP64, encryption and extra fields outside its
//! metadata-only allow-list.
//!
//! Untouched members keep every byte (local header, extra fields, payload,
//! data descriptor and central record); only central local-offset fields of
//! relocated members change. A replaced member is rewritten in one simple
//! form: its name, original version, flags (without the data-descriptor
//! bit), method and DOS time, the new CRC/sizes, no extra fields and no data
//! descriptor, in both its local and its central record (which also keeps
//! made-by version and attributes). Added members get a fresh minimal ZIP32
//! local record inserted before the original central directory and a
//! matching central record appended to it. The end record's counts,
//! directory size and offset are patched and the archive comment is kept.
use super::super::{BoundedOutput, Patch, apply_patches};
use super::{
    Archive, BTreeMap, IoError, XlsxRecalculateOptions, audit_directory, checkpoint, part_name,
    u16_at, unsupported,
};
use std::io::{Cursor, Write};
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

/// Container edits: existing members to replace and validated new members
/// to add. A name is in at most one of the two maps.
#[derive(Debug, Default)]
pub(in crate::cache_recalculate) struct Edits {
    pub replace: BTreeMap<String, Vec<u8>>,
    pub add: BTreeMap<String, Vec<u8>>,
}
impl Edits {
    pub fn is_empty(&self) -> bool {
        self.replace.is_empty() && self.add.is_empty()
    }
}

/// CRC-32, compressed size and the compressed payload of `data`, produced
/// by ZIP7 with `options`.
fn encode(
    data: &[u8],
    options: SimpleFileOptions,
    name: &str,
    limits: &XlsxRecalculateOptions,
) -> Result<(u32, Vec<u8>), IoError> {
    let mut writer = ZipWriter::new(BoundedOutput {
        cursor: Cursor::new(Vec::new()),
        limit: limits.limits.max_output_bytes,
    });
    writer
        .start_file("payload", options)
        .map_err(|e| IoError::from_backend("zip", e))?;
    for chunk in data.chunks(64 * 1024) {
        checkpoint(&limits.cancel)?;
        writer.write_all(chunk)?;
    }
    let encoded = writer
        .finish()
        .map_err(|e| IoError::from_backend("zip", e))?
        .cursor
        .into_inner();
    let mut temporary =
        ZipArchive::new(Cursor::new(&encoded)).map_err(|e| IoError::from_backend("zip", e))?;
    let payload = temporary
        .by_index(0)
        .map_err(|e| IoError::from_backend("zip", e))?;
    let body = payload.data_start() as usize;
    let compressed = encoded
        .get(body..body + payload.compressed_size() as usize)
        .ok_or_else(|| unsupported("ZIP payload range", name))?
        .to_vec();
    Ok((payload.crc32(), compressed))
}
fn zip32(value: usize, name: &str) -> Result<[u8; 4], IoError> {
    u32::try_from(value)
        .ok()
        .filter(|n| *n != u32::MAX)
        .map(u32::to_le_bytes)
        .ok_or_else(|| unsupported("ZIP32 size or offset overflow", name.to_owned()))
}

pub(in crate::cache_recalculate) fn rewrite(
    bytes: &[u8],
    archive: &mut Archive<'_>,
    edits: &Edits,
    options: &XlsxRecalculateOptions,
) -> Result<Vec<u8>, IoError> {
    let replacements = &edits.replace;
    let directory = audit_directory(bytes, options)?;
    let directory_start = directory.start;
    let at = directory.footer;
    let mut changes = Vec::new();
    let mut patches = Vec::new();
    let mut directory_delta = 0isize;
    let mut fresh = BTreeMap::new();
    for member in &directory.members {
        checkpoint(&options.cancel)?;
        let c = member.central;
        let length = u16_at(bytes, c + 28)?;
        let raw_name = &bytes[c + 46..c + 46 + length];
        let name =
            std::str::from_utf8(raw_name).map_err(|e| IoError::from_backend("zip-name", e))?;
        if edits.add.keys().any(|n| n.eq_ignore_ascii_case(name)) {
            return Err(unsupported("duplicate ZIP member", name));
        }
        let Some(data) = replacements.get(name) else {
            continue;
        };
        let source = archive
            .by_name(name)
            .map_err(|e| IoError::from_backend("zip", e))?;
        if source.data_start() as usize != member.data
            || source.compressed_size() as usize != member.data_end - member.data
        {
            return Err(unsupported("inconsistent ZIP member range", name));
        }
        let (crc, compressed) = encode(data, source.options(), name, options)?;
        drop(source);
        // The replaced member's simple form: its name, the original
        // version, flags without the data-descriptor bit, method and time;
        // final CRC/sizes; no extra fields and no data descriptor.
        let mut common = Vec::with_capacity(24);
        common.extend_from_slice(&bytes[c + 6..c + 8]);
        let flags = (u16_at(bytes, c + 8)? & !0x0008) as u16;
        common.extend_from_slice(&flags.to_le_bytes());
        common.extend_from_slice(&bytes[c + 10..c + 16]);
        common.extend_from_slice(&crc.to_le_bytes());
        common.extend_from_slice(
            &u32::try_from(compressed.len())
                .ok()
                .filter(|n| *n != u32::MAX)
                .ok_or_else(|| unsupported("ZIP32 compressed size overflow", name))?
                .to_le_bytes(),
        );
        common.extend_from_slice(
            &u32::try_from(data.len())
                .ok()
                .filter(|n| *n != u32::MAX)
                .ok_or_else(|| unsupported("ZIP32 expanded size overflow", name))?
                .to_le_bytes(),
        );
        common.extend_from_slice(&bytes[c + 28..c + 30]);
        common.extend_from_slice(&0u16.to_le_bytes());
        let mut record = Vec::with_capacity(30 + length + compressed.len());
        record.extend_from_slice(b"PK\x03\x04");
        record.extend_from_slice(&common);
        record.extend_from_slice(raw_name);
        record.extend_from_slice(&compressed);
        let old = member.end - member.local;
        changes.push((member.end, record.len() as i128 - old as i128));
        patches.push(Patch {
            span: member.local..member.end,
            replacement: record,
        });
        // Central record: made-by version, the common fields, comment
        // length 0, disk 0, the original attributes and (patched below) the
        // relocated offset, then the name.
        let mut central = Vec::with_capacity(46 + length);
        central.extend_from_slice(b"PK\x01\x02");
        central.extend_from_slice(&bytes[c + 4..c + 6]);
        central.extend_from_slice(&common);
        central.extend_from_slice(&[0; 4]);
        central.extend_from_slice(&bytes[c + 36..c + 46]);
        central.extend_from_slice(raw_name);
        directory_delta += central.len() as isize - member.central_len as isize;
        fresh.insert(c, central);
    }
    if fresh.len() != replacements.len() {
        return Err(unsupported("unmatched package replacement", "XLSX output"));
    }
    changes.sort_by_key(|(end, _)| *end);
    let mut delta = 0;
    for (_, change) in &mut changes {
        delta += *change;
        *change = delta;
    }
    let relocate = |old: usize| -> Result<usize, IoError> {
        let index = changes.partition_point(|(end, _)| *end <= old);
        let delta = if index == 0 { 0 } else { changes[index - 1].1 };
        usize::try_from(old as i128 + delta)
            .map_err(|_| unsupported("ZIP32 relocated offset overflow", "XLSX output"))
    };
    // Relocated local offsets: in place for untouched central records, in
    // the fresh record of a replaced member.
    for member in &directory.members {
        let offset = zip32(relocate(member.local)?, "XLSX output")?;
        if let Some(mut record) = fresh.remove(&member.central) {
            record[42..46].copy_from_slice(&offset);
            patches.push(Patch {
                span: member.central..member.central + member.central_len,
                replacement: record,
            });
        } else {
            patches.push(Patch {
                span: member.central + 42..member.central + 46,
                replacement: offset.to_vec(),
            });
        }
    }
    // New members: local records before the original central directory,
    // central records after its last entry.
    let entries = archive
        .len()
        .checked_add(edits.add.len())
        .filter(|n| *n <= options.limits.max_entries && *n < u16::MAX as usize)
        .ok_or_else(|| unsupported("ZIP entry count limit or ZIP64", "XLSX output"))?;
    let mut locals = Vec::new();
    let mut centrals = Vec::new();
    let mut offset = relocate(directory_start)?;
    for (name, data) in &edits.add {
        checkpoint(&options.cancel)?;
        part_name(name)?;
        if !name.is_ascii() || replacements.contains_key(name) {
            return Err(unsupported(
                "invalid added ZIP member name",
                name.to_owned(),
            ));
        }
        let (crc, compressed) = encode(
            data,
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
            name,
            options,
        )?;
        let name_len = u16::try_from(name.len())
            .map_err(|_| unsupported("ZIP member name length", name.to_owned()))?;
        // version needed 2.0, no flags (ASCII name, no data descriptor),
        // deflate, DOS time 00:00 on 1980-01-01, CRC and ZIP32 sizes.
        let mut common = Vec::with_capacity(26);
        common.extend_from_slice(&20u16.to_le_bytes());
        common.extend_from_slice(&0u16.to_le_bytes());
        common.extend_from_slice(&8u16.to_le_bytes());
        common.extend_from_slice(&0u16.to_le_bytes());
        common.extend_from_slice(&0x0021u16.to_le_bytes());
        common.extend_from_slice(&crc.to_le_bytes());
        common.extend_from_slice(&zip32(compressed.len(), name)?);
        common.extend_from_slice(&zip32(data.len(), name)?);
        common.extend_from_slice(&name_len.to_le_bytes());
        common.extend_from_slice(&0u16.to_le_bytes());
        let record_start = locals.len();
        locals.extend_from_slice(b"PK\x03\x04");
        locals.extend_from_slice(&common);
        locals.extend_from_slice(name.as_bytes());
        locals.extend_from_slice(&compressed);
        centrals.extend_from_slice(b"PK\x01\x02");
        centrals.extend_from_slice(&20u16.to_le_bytes());
        centrals.extend_from_slice(&common);
        // comment length, disk, internal and external attributes.
        centrals.extend_from_slice(&[0; 10]);
        centrals.extend_from_slice(&zip32(offset, name)?);
        centrals.extend_from_slice(name.as_bytes());
        offset = offset
            .checked_add(locals.len() - record_start)
            .ok_or_else(|| unsupported("ZIP32 size or offset overflow", name.to_owned()))?;
    }
    let size = (at - directory_start)
        .checked_add_signed(directory_delta)
        .and_then(|n| n.checked_add(centrals.len()))
        .ok_or_else(|| unsupported("ZIP32 directory size overflow", "XLSX output"))?;
    // The directory must end below the ZIP64 sentinel as well.
    zip32(offset.saturating_add(size), "XLSX output")?;
    if !edits.add.is_empty() {
        patches.push(Patch {
            span: directory_start..directory_start,
            replacement: locals,
        });
        patches.push(Patch {
            span: at..at,
            replacement: centrals,
        });
    }
    // End record: entry counts, directory size and offset; the archive
    // comment is kept.
    let count = (entries as u16).to_le_bytes();
    patches.push(Patch {
        span: at + 8..at + 20,
        replacement: [
            &count[..],
            &count[..],
            &zip32(size, "XLSX output")?[..],
            &zip32(offset, "XLSX output")?[..],
        ]
        .concat(),
    });
    checkpoint(&options.cancel)?;
    let result = apply_patches(bytes, patches, options.limits.max_output_bytes)?;
    checkpoint(&options.cancel)?;
    Ok(result)
}
