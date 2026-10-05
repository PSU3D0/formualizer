//! Surgical ZIP32 package edits. ZIP7 supplies compression and CRC generation;
//! original local/central metadata is retained, not normalized by raw_copy_file.
//! Admission has rejected ZIP64, encryption and extra fields outside its
//! metadata-only allow-list.
//!
//! Replaced members keep their local/central records (only CRC/sizes and
//! relocated offsets change); replacing a member that has extra fields or a
//! data descriptor is refused. Untouched members keep every byte. Added members get a fresh minimal ZIP32 local
//! record inserted before the original central directory and a matching
//! central record appended to it; the end record's counts, directory size
//! and offset are patched and the archive comment is kept.
use super::super::{BoundedOutput, Patch, apply_patches};
use super::{
    Archive, BTreeMap, IoError, XlsxRecalculateOptions, audit_directory, checkpoint, part_name,
    u16_at, u32_at, unsupported,
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
    let directory_start = archive.central_directory_start() as usize;
    let mut at = directory_start;
    let mut headers = Vec::new();
    let mut changes = Vec::new();
    let mut patches = Vec::new();
    for _ in 0..archive.len() {
        checkpoint(&options.cancel)?;
        let length = u16_at(bytes, at + 28)?;
        let name = std::str::from_utf8(&bytes[at + 46..at + 46 + length])
            .map_err(|e| IoError::from_backend("zip-name", e))?;
        if edits.add.keys().any(|n| n.eq_ignore_ascii_case(name)) {
            return Err(unsupported("duplicate ZIP member", name));
        }
        let local = u32_at(bytes, at + 42)?;
        headers.push((at, local));
        if let Some(data) = replacements.get(name) {
            let member = directory
                .members
                .iter()
                .find(|m| m.central == at)
                .ok_or_else(|| unsupported("unaudited ZIP member", name))?;
            if member.end != member.data_end
                || member.data != member.local + 30 + length
                || member.central_len != 46 + length
            {
                return Err(unsupported(
                    "rewrite of a ZIP member with extra fields or a data descriptor",
                    name,
                ));
            }
            let source = archive
                .by_name(name)
                .map_err(|e| IoError::from_backend("zip", e))?;
            let start = source.data_start() as usize;
            let end = start + source.compressed_size() as usize;
            let (crc, compressed) = encode(data, source.options(), name, options)?;
            let mut fields = Vec::with_capacity(12);
            fields.extend_from_slice(&crc.to_le_bytes());
            fields.extend_from_slice(
                &u32::try_from(compressed.len())
                    .map_err(|_| unsupported("ZIP32 compressed size overflow", name))?
                    .to_le_bytes(),
            );
            fields.extend_from_slice(
                &u32::try_from(data.len())
                    .map_err(|_| unsupported("ZIP32 expanded size overflow", name))?
                    .to_le_bytes(),
            );
            changes.push((end, compressed.len() as i128 - (end - start) as i128));
            patches.push(Patch {
                span: local + 14..local + 26,
                replacement: fields.clone(),
            });
            patches.push(Patch {
                span: at + 16..at + 28,
                replacement: fields,
            });
            patches.push(Patch {
                span: start..end,
                replacement: compressed,
            });
        }
        at += 46 + length + u16_at(bytes, at + 30)? + u16_at(bytes, at + 32)?;
    }
    if at != directory.footer || changes.len() != replacements.len() {
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
    for (central, local) in headers {
        patches.push(Patch {
            span: central + 42..central + 46,
            replacement: zip32(relocate(local)?, "XLSX output")?.to_vec(),
        });
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
    if !edits.add.is_empty() {
        let size = (at - directory_start)
            .checked_add(centrals.len())
            .ok_or_else(|| unsupported("ZIP32 directory size overflow", "XLSX output"))?;
        // The directory must end below the ZIP64 sentinel as well.
        zip32(offset.saturating_add(size), "XLSX output")?;
        patches.push(Patch {
            span: directory_start..directory_start,
            replacement: locals,
        });
        patches.push(Patch {
            span: at..at,
            replacement: centrals,
        });
        let count = (entries as u16).to_le_bytes();
        patches.push(Patch {
            span: at + 8..at + 16,
            replacement: [&count[..], &count[..], &zip32(size, "XLSX output")?[..]].concat(),
        });
    }
    patches.push(Patch {
        span: at + 16..at + 20,
        replacement: zip32(offset, "XLSX output")?.to_vec(),
    });
    checkpoint(&options.cancel)?;
    let result = apply_patches(bytes, patches, options.limits.max_output_bytes)?;
    checkpoint(&options.cancel)?;
    Ok(result)
}
