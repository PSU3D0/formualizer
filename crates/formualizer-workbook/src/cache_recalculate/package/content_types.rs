use super::{
    Archive, BTreeMap, IoError, Sheet, XlsxRecalculateOptions, append_child, part_name, read_part,
    unsupported, xml,
};

const PART: &str = "[Content_Types].xml";

/// Add an `Override` for `part` (a validated package part name) to the
/// content types, unless any declaration already names it: the result is
/// refused rather than given a duplicate. All other bytes are kept.
pub(in crate::cache_recalculate) fn add_override(
    archive: &mut Archive<'_>,
    part: &str,
    content_type: &str,
    options: &XlsxRecalculateOptions,
) -> Result<Vec<u8>, IoError> {
    part_name(part)?;
    let data = read_part(archive, PART, options.limits.max_worksheet_bytes)?;
    let name = format!("/{part}");
    xml::walk(&data, options, |_, node| {
        if node
            .value("PartName")
            .is_some_and(|p| p.eq_ignore_ascii_case(&name))
        {
            return Err(unsupported("duplicate content-type override", PART));
        }
        Ok(())
    })?;
    append_child(
        &data,
        PART,
        |prefix| format!("<{prefix}Override PartName=\"{name}\" ContentType=\"{content_type}\"/>"),
        options,
    )
}

/// `metadata` is the relationship-resolved sheet metadata part, if any;
/// `links` are the workbook's external link parts.
pub(super) fn validate(
    archive: &mut Archive<'_>,
    sheets: &[Sheet],
    metadata: Option<&str>,
    links: &[String],
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    use super::super::dynamic_metadata::SHEET_METADATA_CONTENT_TYPE;
    use super::super::external_links::LINK_CONTENT_TYPE;
    const NS: &str = "http://schemas.openxmlformats.org/package/2006/content-types";
    const PREFIX: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.";
    let data = read_part(archive, PART, options.limits.max_worksheet_bytes)?;
    let mut defaults = BTreeMap::new();
    let mut overrides = BTreeMap::new();
    xml::walk(&data, options, |path, node| {
        if !matches!(node.kind, xml::Kind::Open { .. }) {
            return Ok(());
        }
        let e = path.last().expect("open XML element");
        if path.len() == 1 && !xml::path_is(path, NS, &["Types"]) {
            return Err(unsupported("content types root/namespace", "XLSX package"));
        }
        if path.len() > 1 {
            if path.len() != 2 || e.ns != NS || !matches!(e.local, "Default" | "Override") {
                return Err(unsupported(
                    "unknown content-type declaration",
                    "XLSX package",
                ));
            }
            let content = node.required("ContentType")?;
            if content.is_empty()
                || content.contains("digital-signature")
                || (metadata.is_none() && content.contains("sheetMetadata"))
                || (content.contains("externalLink")
                    && (content != LINK_CONTENT_TYPE || e.local != "Override"))
            {
                return Err(unsupported("unsupported content type", "XLSX package"));
            }
            if let Some(metadata) = metadata
                && content.contains("sheetMetadata")
            {
                if e.local != "Override" || content != SHEET_METADATA_CONTENT_TYPE {
                    return Err(unsupported(
                        "sheet metadata content-type disagreement",
                        "XLSX package",
                    ));
                }
                if node.required("PartName")?.strip_prefix('/') != Some(metadata) {
                    return Err(unsupported("unrelated sheet metadata part", "XLSX package"));
                }
            }
            if e.local == "Default" {
                let extension = node.required("Extension")?;
                if extension.is_empty()
                    || extension.contains(['.', '/', '\\'])
                    || defaults
                        .insert(extension.to_ascii_lowercase(), content.to_owned())
                        .is_some()
                {
                    return Err(unsupported(
                        "invalid/duplicate default content type",
                        "XLSX package",
                    ));
                }
            } else {
                let name = node
                    .required("PartName")?
                    .strip_prefix('/')
                    .ok_or_else(|| unsupported("relative content-type part", "XLSX package"))?;
                part_name(name)?;
                if overrides
                    .insert(name.to_owned(), content.to_owned())
                    .is_some()
                {
                    return Err(unsupported(
                        "duplicate content-type override",
                        "XLSX package",
                    ));
                }
            }
        }
        Ok(())
    })?;
    for name in archive.file_names() {
        if name == "[Content_Types].xml" || name.ends_with('/') {
            continue;
        }
        let content = overrides
            .get(name)
            .or_else(|| {
                name.rsplit_once('.')
                    .and_then(|(_, e)| defaults.get(&e.to_ascii_lowercase()))
            })
            .ok_or_else(|| unsupported("part without content type", name))?;
        if Some(name) == metadata {
            if content != SHEET_METADATA_CONTENT_TYPE {
                return Err(unsupported(
                    "sheet metadata content-type disagreement",
                    name,
                ));
            }
            continue;
        }
        let link = links.iter().any(|p| p == name);
        if !link && name.starts_with("xl/externalLinks/") && !name.ends_with(".rels") {
            return Err(unsupported("unreferenced external link part", name));
        }
        if link != (content == LINK_CONTENT_TYPE) {
            return Err(unsupported("external link content-type disagreement", name));
        }
        if link {
            continue;
        }
        let table = sheets.iter().any(|s| s.tables.values().any(|p| p == name));
        if !table && (content == &format!("{PREFIX}table+xml") || name.starts_with("xl/tables/")) {
            return Err(unsupported("orphan table part", name));
        }
        let expected = if table {
            Some(format!("{PREFIX}table+xml"))
        } else if name == "xl/workbook.xml" {
            Some(format!("{PREFIX}sheet.main+xml"))
        } else if let Some(sheet) = sheets.iter().find(|s| s.part == name) {
            Some(format!("{PREFIX}{}+xml", sheet.kind))
        } else if name == "xl/styles.xml" {
            Some(format!("{PREFIX}styles+xml"))
        } else if name == "xl/sharedStrings.xml" {
            Some(format!("{PREFIX}sharedStrings+xml"))
        } else if name.ends_with(".rels") {
            Some("application/vnd.openxmlformats-package.relationships+xml".into())
        } else {
            None
        };
        if expected
            .as_ref()
            .is_some_and(|expected| content != expected)
        {
            return Err(unsupported("part/content-type mismatch", name));
        }
    }
    Ok(())
}
