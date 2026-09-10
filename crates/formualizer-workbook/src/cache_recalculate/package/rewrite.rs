//! Surgical ZIP32 package edits. ZIP7 supplies compression and CRC generation;
//! original local/central metadata is retained, not normalized by raw_copy_file.
//! Admission has rejected ZIP64, descriptors and unknown extra metadata.
use super::super::{BoundedOutput, Patch, apply_patches};
use super::{
    Archive, BTreeMap, IoError, XlsxRecalculateOptions, checkpoint, u16_at, u32_at, unsupported,
};
use std::io::{Cursor, Write};
use zip::{ZipArchive, ZipWriter};

pub(in crate::cache_recalculate) fn rewrite(
    bytes: &[u8],
    archive: &mut Archive<'_>,
    replacements: &BTreeMap<String, Vec<u8>>,
    options: &XlsxRecalculateOptions,
) -> Result<Vec<u8>, IoError> {
    let mut at = archive.central_directory_start() as usize;
    let mut headers = Vec::new();
    let mut changes = Vec::new();
    let mut patches = Vec::new();
    for _ in 0..archive.len() {
        checkpoint(&options.cancel)?;
        let length = u16_at(bytes, at + 28)?;
        let name = std::str::from_utf8(&bytes[at + 46..at + 46 + length])
            .map_err(|e| IoError::from_backend("zip-name", e))?;
        let local = u32_at(bytes, at + 42)?;
        headers.push((at, local));
        if let Some(data) = replacements.get(name) {
            let source = archive
                .by_name(name)
                .map_err(|e| IoError::from_backend("zip", e))?;
            let start = source.data_start() as usize;
            let end = start + source.compressed_size() as usize;
            let mut writer = ZipWriter::new(BoundedOutput {
                cursor: Cursor::new(Vec::new()),
                limit: options.limits.max_output_bytes,
            });
            writer
                .start_file("payload", source.options())
                .map_err(|e| IoError::from_backend("zip", e))?;
            for chunk in data.chunks(64 * 1024) {
                checkpoint(&options.cancel)?;
                writer.write_all(chunk)?;
            }
            let encoded = writer
                .finish()
                .map_err(|e| IoError::from_backend("zip", e))?
                .cursor
                .into_inner();
            let mut temporary = ZipArchive::new(Cursor::new(&encoded))
                .map_err(|e| IoError::from_backend("zip", e))?;
            let payload = temporary
                .by_index(0)
                .map_err(|e| IoError::from_backend("zip", e))?;
            let mut fields = Vec::with_capacity(12);
            fields.extend_from_slice(&payload.crc32().to_le_bytes());
            fields.extend_from_slice(
                &u32::try_from(payload.compressed_size())
                    .map_err(|_| unsupported("ZIP32 compressed size overflow", name))?
                    .to_le_bytes(),
            );
            fields.extend_from_slice(
                &u32::try_from(data.len())
                    .map_err(|_| unsupported("ZIP32 expanded size overflow", name))?
                    .to_le_bytes(),
            );
            let body = payload.data_start() as usize;
            let compressed = encoded[body..body + payload.compressed_size() as usize].to_vec();
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
    if changes.len() != replacements.len() {
        return Err(unsupported("unmatched package replacement", "XLSX output"));
    }
    changes.sort_by_key(|(end, _)| *end);
    let mut delta = 0;
    for (_, change) in &mut changes {
        delta += *change;
        *change = delta;
    }
    let relocate = |old: usize| -> Result<Vec<u8>, IoError> {
        let index = changes.partition_point(|(end, _)| *end <= old);
        let delta = if index == 0 { 0 } else { changes[index - 1].1 };
        Ok(u32::try_from(old as i128 + delta)
            .map_err(|_| unsupported("ZIP32 relocated offset overflow", "XLSX output"))?
            .to_le_bytes()
            .to_vec())
    };
    for (central, local) in headers {
        patches.push(Patch {
            span: central + 42..central + 46,
            replacement: relocate(local)?,
        });
    }
    patches.push(Patch {
        span: at + 16..at + 20,
        replacement: relocate(archive.central_directory_start() as usize)?,
    });
    checkpoint(&options.cancel)?;
    let result = apply_patches(bytes, patches, options.limits.max_output_bytes)?;
    checkpoint(&options.cancel)?;
    Ok(result)
}
