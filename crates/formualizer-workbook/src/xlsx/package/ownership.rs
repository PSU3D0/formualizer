//! Provisional metadata discovery followed by the authoritative directory audit.
//! Provisional reads cannot establish ownership or bypass final directory checks.
use super::*;

fn closure(
    archive: &mut Archive<'_>,
    options: &XlsxRecalculateOptions,
) -> Result<HashSet<String>, IoError> {
    let discovery = discover(archive, options)?;
    let mut needed = HashSet::new();
    for sheet in discovery.sheets.iter().filter(|sheet| !sheet.inert) {
        needed.insert(sheet.part.clone());
        needed.extend(sheet.tables.values().cloned());
    }
    needed.extend(discovery.metadata);
    if !discovery.external_links.is_empty() {
        let links = crate::xlsx::recalculate::external_links::parse(
            archive,
            &discovery.external_links,
            options,
        )?;
        links.check_kinds()?;
    }
    if let Some(part) = calc_chain_part(archive, options) {
        needed.insert(part);
    }
    Ok(needed)
}

/// A provisional read never leaks payload failures ahead of earlier members.
/// Its actual byte count and failure are replayed at its directory position.
pub(super) fn bootstrap_read(
    archive: &mut Archive<'_>,
    name: &str,
    limit: usize,
    options: &ReadOptions,
) -> Result<Vec<u8>, IoError> {
    if !archive.parts.contains_key(name) {
        let index = archive
            .index_for_name(name)
            .ok_or_else(|| IoError::from_backend("zip", zip::result::ZipError::FileNotFound))?;
        let part = read(archive, index, Some(limit), options);
        archive.parts.insert(name.to_owned(), part);
    }
    let part = &archive.parts[name];
    if part.failure.is_some() {
        return Err(unsupported("provisional payload failure", name));
    }
    if part.bytes.len() > limit {
        return Err(unsupported("XML part byte limit", name));
    }
    Ok(part.bytes.clone())
}

fn read(
    archive: &mut Archive<'_>,
    index: usize,
    retain: Option<usize>,
    options: &ReadOptions,
) -> VerifiedPart {
    let mut part = VerifiedPart {
        bytes: Vec::new(),
        actual: 0,
        failure: None,
    };
    let result = (|| {
        let mut file = archive
            .zip
            .by_index(index)
            .map_err(|error| IoError::from_backend("zip", error))?;
        #[cfg(test)]
        record_inflate(file.name());
        if let Some(limit) = retain {
            part.bytes.reserve(
                usize::try_from(file.size())
                    .unwrap_or(limit.saturating_add(1))
                    .min(limit.saturating_add(1)),
            );
        }
        let mut buffer = [0u8; 64 * 1024];
        #[cfg(test)]
        AUDIT_BUFFER_PEAK.with(|peak| peak.set(peak.get().max(buffer.len())));
        loop {
            checkpoint(&options.cancel)?;
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            part.actual = part
                .actual
                .checked_add(n)
                .ok_or_else(|| unsupported("expanded byte overflow", "XLSX package"))?;
            // Never retain beyond the consumer's existing XML-part bound. A
            // bounded prefix preserves that consumer's later limit refusal;
            // the rest is still streamed to EOF for CRC/size verification.
            if let Some(limit) = retain {
                let keep = n.min(limit.saturating_add(1).saturating_sub(part.bytes.len()));
                part.bytes.extend_from_slice(&buffer[..keep]);
            }
            // The audit will apply the cumulative limit in directory order.
            // No legal package can need more than this many bytes in a member.
            if part.actual > options.limits.max_expanded_bytes {
                return Err(unsupported("actual ZIP expansion limit", "XLSX package"));
            }
        }
        if part.actual as u64 != file.size() {
            return Err(unsupported("ZIP expanded-size mismatch", file.name()));
        }
        Ok(())
    })();
    part.failure = result.err();
    part
}

pub(super) fn audit<'a>(
    mut archive: Archive<'a>,
    options: &XlsxRecalculateOptions,
) -> Result<Archive<'a>, IoError> {
    archive.provisional = Some(ReadOptions::from(options));
    // Semantic and provisional read failures are deliberately ignored here.
    // The real discovery repeats from verified metadata after the full audit.
    let needed = closure(&mut archive, options).unwrap_or_default();
    archive.provisional = None;
    audit_unique_payloads(archive, &needed, &ReadOptions::from(options))
}

/// Strict directory admission rejects duplicate names and `open` verifies
/// equal raw/indexed counts: indices therefore cover every physical member.
/// Preserve this single-inflate path and its provisional-error replay order.
/// Permissive admission instead audits raw members before name resolution.
pub(super) fn audit_unique_payloads<'a>(
    mut archive: Archive<'a>,
    needed: &HashSet<String>,
    options: &ReadOptions,
) -> Result<Archive<'a>, IoError> {
    let mut total = 0usize;
    for index in 0..archive.len() {
        checkpoint(&options.cancel)?;
        let file = archive
            .zip
            .by_index_raw(index)
            .map_err(|error| IoError::from_backend("zip", error))?;
        let name = file.name().to_owned();
        if file.encrypted() {
            return Err(unsupported("encrypted ZIP member", &name));
        }
        if options.policy == ReadPolicy::Strict
            && (name.starts_with("_xmlsignatures/") || name.ends_with("origin.sigs"))
        {
            return Err(unsupported("package digital signature", "XLSX package"));
        }
        if options.policy == ReadPolicy::Strict && name.starts_with("xl/richData/") {
            return Err(unsupported("rich value data", "XLSX package"));
        }
        drop(file);
        let retain = archive.parts.contains_key(&name) || needed.contains(&name);
        let mut part = archive.parts.remove(&name).unwrap_or_else(|| {
            read(
                &mut archive,
                index,
                retain.then_some(options.limits.max_worksheet_bytes),
                options,
            )
        });
        total = total
            .checked_add(part.actual)
            .ok_or_else(|| unsupported("expanded byte overflow", "XLSX package"))?;
        if total > options.limits.max_expanded_bytes {
            return Err(unsupported("actual ZIP expansion limit", "XLSX package"));
        }
        if let Some(error) = part.failure.take() {
            return Err(error);
        }
        if retain {
            archive.parts.insert(name, part);
        }
    }
    Ok(archive)
}
