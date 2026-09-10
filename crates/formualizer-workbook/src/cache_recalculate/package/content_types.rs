use super::{
    Archive, BTreeMap, IoError, Sheet, XlsxRecalculateOptions, part_name, read_part, unsupported,
    xml,
};

pub(super) fn validate(
    archive: &mut Archive<'_>,
    sheets: &[Sheet],
    options: &XlsxRecalculateOptions,
) -> Result<(), IoError> {
    const NS: &str = "http://schemas.openxmlformats.org/package/2006/content-types";
    const PREFIX: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.";
    let data = read_part(
        archive,
        "[Content_Types].xml",
        options.limits.max_worksheet_bytes,
    )?;
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
            if path.len() != 2 || e.ns != NS || !matches!(e.local.as_str(), "Default" | "Override")
            {
                return Err(unsupported(
                    "unknown content-type declaration",
                    "XLSX package",
                ));
            }
            let content = node.required("ContentType")?;
            if content.is_empty()
                || content.contains("digital-signature")
                || content.contains("sheetMetadata")
                || content.contains("externalLink")
            {
                return Err(unsupported("unsupported content type", "XLSX package"));
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
        let expected = if name == "xl/workbook.xml" {
            Some(format!("{PREFIX}sheet.main+xml"))
        } else if sheets.iter().any(|s| s.part == name) {
            Some(format!("{PREFIX}worksheet+xml"))
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
