use super::*;
use std::io::{Read, Seek};
use zip::ZipArchive;

/// Read-only values cached in an XLSX package for linked workbooks.
/// Constructed only by workbook readers; linked files are never opened.
pub struct CachedExternalLinkValues {
    pub(crate) values: Arc<ExternalValues>,
    pub(crate) indices: Vec<usize>,
    pub(crate) refused_formulas: HashMap<(String, u32, u32), String>,
    pub(crate) refused_names: HashSet<String>,
    pub(crate) skipped_names: Vec<String>,
}

pub(crate) fn has_links(reader: impl Read + Seek) -> bool {
    ZipArchive::new(reader).is_ok_and(|archive| {
        archive
            .file_names()
            .any(|p| p.starts_with("xl/externalLinks/"))
    })
}

fn read_part<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    part: &str,
    options: &CacheOptions,
) -> Result<Vec<u8>, IoError> {
    let entry = archive.by_name(part).map_err(|_| malformed(part))?;
    if entry.size() > options.limits.max_worksheet_bytes as u64 {
        return Err(unsupported("external link part byte limit", part));
    }
    let mut bytes = Vec::new();
    entry
        .take(options.limits.max_worksheet_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| malformed(part))?;
    if bytes.len() > options.limits.max_worksheet_bytes {
        return Err(unsupported("external link part byte limit", part));
    }
    Ok(bytes)
}

fn read_links(reader: impl Read + Seek, options: &CacheOptions) -> Result<Links, IoError> {
    let mut archive = ZipArchive::new(reader).map_err(|_| malformed("XLSX package"))?;
    if archive.len() > options.limits.max_entries
        || archive
            .file_names()
            .map(|p| p.to_string())
            .collect::<HashSet<_>>()
            .len()
            != archive.len()
    {
        return Err(unsupported(
            "external link package entry limit or duplicate entry",
            "XLSX package",
        ));
    }
    let expanded = (0..archive.len())
        .try_fold(0u64, |sum, i| {
            sum.checked_add(archive.by_index(i).ok()?.size())
        })
        .ok_or_else(|| malformed("XLSX package"))?;
    if expanded > options.limits.max_expanded_bytes as u64 {
        return Err(unsupported(
            "external link expanded byte limit",
            "XLSX package",
        ));
    }
    let rels = read_part(&mut archive, "xl/_rels/workbook.xml.rels", options)?;
    let mut targets = HashMap::new();
    xml::walk(&rels, options, |path, node| {
        if xml::path_is(path, xml::RELS, &["Relationships", "Relationship"])
            && matches!(node.kind, xml::Kind::Open { .. })
            && node.value("Type") == Some(LINK_RELATIONSHIP)
            && node.value("TargetMode") != Some("External")
        {
            let id = node
                .value("Id")
                .ok_or_else(|| malformed("workbook relationships"))?;
            let target = node
                .value("Target")
                .ok_or_else(|| malformed("workbook relationships"))?;
            let part = if let Some(target) = target.strip_prefix('/') {
                target.to_string()
            } else {
                format!("xl/{target}")
            };
            // Do not follow traversal, URI or ambiguous noncanonical targets.
            if !part.starts_with("xl/externalLinks/")
                || part
                    .split('/')
                    .any(|s| s == ".." || s == "." || s.is_empty())
                || part.contains('\\')
                || part.contains(':')
            {
                return Err(malformed("workbook relationships"));
            }
            if targets.insert(id.to_string(), part).is_some() {
                return Err(malformed("workbook relationships"));
            }
        }
        Ok(())
    })?;
    let workbook = read_part(&mut archive, "xl/workbook.xml", options)?;
    let mut parts = Vec::new();
    xml::walk(&workbook, options, |path, node| {
        if xml::path_is(
            path,
            xml::MAIN,
            &["workbook", "externalReferences", "externalReference"],
        ) && matches!(node.kind, xml::Kind::Open { .. })
        {
            let id = node
                .attribute(xml::OFFICE, "id")
                .map(|a| &*a.value)
                .ok_or_else(|| malformed("workbook external references"))?;
            parts.push(targets.get(id).cloned());
        }
        Ok(())
    })?;
    let mut counted = 0;
    let mut seen = HashSet::new();
    let links = parts
        .into_iter()
        .map(|part| {
            part.filter(|p| seen.insert(p.clone()))
                .and_then(|p| {
                    read_part(&mut archive, &p, options)
                        .ok()
                        .and_then(|data| parse_part(&data, &p, options, &mut counted).ok())
                })
                .unwrap_or(Link::Other("invalid"))
        })
        .collect();
    Ok(Links { links })
}

pub(crate) fn load(
    reader: impl Read + Seek,
    formulas: &[(String, u32, u32, String)],
    names: &[(String, String, Option<usize>)],
    options: &CacheOptions,
) -> CachedExternalLinkValues {
    let links = read_links(reader, options).unwrap_or(Links { links: Vec::new() });
    let parsed_names: Vec<_> = names
        .iter()
        .map(|(_, text, _)| parse_formula(text))
        .collect();
    let mut external_names = HashSet::new();
    let mut name_refs = Vec::new();
    for ((name, text, _), ast) in names.iter().zip(&parsed_names) {
        let mut reads = Reads::default();
        if let Some(ast) = ast {
            collect(ast, &mut reads);
        }
        if !reads.externals.is_empty()
            || reads.names.iter().any(|n| is_external_name(n))
            || (ast.is_none() && text.contains('['))
        {
            external_names.insert(name.to_ascii_lowercase());
        }
        name_refs.push(reads.names.iter().map(|n| name_key(n)).collect::<Vec<_>>());
    }
    loop {
        let before = external_names.len();
        for ((name, _, _), refs) in names.iter().zip(&name_refs) {
            if refs.iter().any(|n| external_names.contains(n)) {
                external_names.insert(name.to_ascii_lowercase());
            }
        }
        if before == external_names.len() {
            break;
        }
    }
    let mut used: HashSet<_> = name_refs.iter().flatten().cloned().collect();
    let mut values = ExternalValues::default();
    let mut indices = BTreeSet::new();
    let mut area = 0;
    let mut serve = |ast: &ASTNode| -> Result<(), IoError> {
        let mut reads = Reads::default();
        collect(ast, &mut reads);
        for ext in reads.externals {
            if values.contains(&ext.raw) {
                continue;
            }
            let (index, served) = links.serve(ext, &ext.raw, &mut area, options)?;
            indices.insert(index);
            match served {
                Served::Scalar(v) => {
                    values.scalars.insert(ext.raw.clone(), v);
                }
                Served::Range(v) => {
                    values.ranges.insert(ext.raw.clone(), v);
                }
            }
        }
        Ok(())
    };
    let mut refused_formulas = HashMap::new();
    for (sheet, row, col, text) in formulas {
        if let Some(ast) = parse_formula(text) {
            let mut reads = Reads::default();
            collect(&ast, &mut reads);
            used.extend(reads.names.iter().map(|n| name_key(n)));
            if check_shape(&ast, &external_names, text)
                .and_then(|()| serve(&ast))
                .is_err()
            {
                refused_formulas.insert((sheet.clone(), *row, *col), text.clone());
            }
        }
    }
    let mut refused_names = HashSet::new();
    let mut skipped_names = Vec::new();
    for ((name, _, _), ast) in names.iter().zip(&parsed_names) {
        let key = name.to_ascii_lowercase();
        if !external_names.contains(&key) {
            continue;
        }
        if !used.contains(&key) {
            skipped_names.push(key);
            continue;
        }
        let valid = ast.as_ref().is_some_and(|ast| {
            (is_external_range(ast) || !array_valued(ast))
                && check_shape(ast, &external_names, name)
                    .and_then(|()| serve(ast))
                    .is_ok()
        });
        if !valid {
            refused_names.insert(key);
        }
    }
    // Propagate unsupported externally computed names through the name graph.
    loop {
        let before = refused_names.len();
        for ((name, _, _), refs) in names.iter().zip(&name_refs) {
            if refs.iter().any(|n| refused_names.contains(n)) {
                refused_names.insert(name.to_ascii_lowercase());
            }
        }
        if before == refused_names.len() {
            break;
        }
    }
    for (sheet, row, col, text) in formulas {
        if let Some(ast) = parse_formula(text) {
            let mut reads = Reads::default();
            collect(&ast, &mut reads);
            if reads
                .names
                .iter()
                .any(|n| refused_names.contains(&name_key(n)))
            {
                refused_formulas.insert((sheet.clone(), *row, *col), text.clone());
            }
        }
    }
    CachedExternalLinkValues {
        values: Arc::new(values),
        indices: indices.into_iter().collect(),
        refused_formulas,
        refused_names,
        skipped_names,
    }
}
