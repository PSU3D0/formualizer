//! Mark deliberately absent caches for host recalculation without changing
//! workbook calculation/iteration mode or claiming that the host has run.
use super::*;
pub(super) fn patch(
    data: &[u8],
    options: &XlsxRecalculateOptions,
) -> Result<Option<Vec<u8>>, IoError> {
    let mut patches = Vec::new();
    let mut seen = false;
    let mut insert = None;
    let mut qualified = "calcPr".to_owned();
    xml::walk(data, options, |path, node| {
        if xml::path_is(path, xml::MAIN, &["workbook"]) {
            match &node.kind {
                xml::Kind::Open { .. } => {
                    if let Some((prefix, _)) = path[0].qualified.split_once(':') {
                        qualified = format!("{prefix}:calcPr");
                    }
                }
                xml::Kind::Close => {
                    insert.get_or_insert(node.span.start);
                }
                _ => {}
            }
        }
        if path.len() == 2
            && path[1].ns.is_main()
            && matches!(node.kind, xml::Kind::Open { .. })
            && matches!(
                path[1].local,
                "oleSize"
                    | "customWorkbookViews"
                    | "pivotCaches"
                    | "smartTagPr"
                    | "smartTagTypes"
                    | "webPublishing"
                    | "fileRecoveryPr"
                    | "webPublishObjects"
                    | "extLst"
            )
        {
            insert.get_or_insert(node.span.start);
        }
        if xml::path_is(path, xml::MAIN, &["workbook", "calcPr"]) {
            if let xml::Kind::Open { empty, .. } = &node.kind {
                seen = true;
                let mut missing = String::new();
                for name in ["fullCalcOnLoad", "forceFullCalc"] {
                    match node.attribute("", name) {
                        Some(a) if matches!(&*a.value, "1" | "true") => {}
                        Some(a) => patches.push(Patch {
                            span: a.span.clone(),
                            replacement: format!("{name}=\"1\"").into_bytes(),
                        }),
                        None => missing.push_str(&format!(" {name}=\"1\"")),
                    }
                }
                if !missing.is_empty() {
                    let at = node.span.end - if *empty { 2 } else { 1 };
                    patches.push(Patch {
                        span: at..at,
                        replacement: missing.into_bytes(),
                    });
                }
            }
        }
        Ok(())
    })?;
    if !seen {
        let at = insert.ok_or_else(|| {
            unsupported("missing workbook insertion boundary", "partial cache flags")
        })?;
        patches.push(Patch {
            span: at..at,
            replacement: format!("<{qualified} fullCalcOnLoad=\"1\" forceFullCalc=\"1\"/>")
                .into_bytes(),
        });
    }
    if patches.is_empty() {
        return Ok(None);
    }
    apply_patches(data, patches, options.limits.max_worksheet_bytes).map(Some)
}
