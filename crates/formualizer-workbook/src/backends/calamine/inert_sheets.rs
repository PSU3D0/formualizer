//! Ordinary loads omit non-calculating sheet entries, never create engine sheets.
use super::*;
use std::io::{Cursor, Write};

type ModuleProjection = (
    Option<Vec<u8>>,
    Vec<String>,
    Vec<crate::traits::SheetImportDiagnostic>,
);

/// Remove empty-relationship module entries before Calamine opens the package.
/// Keep the original order separately: localSheetId counts these entries too.
pub(super) fn omit_modules(
    source: &SharedXlsxReader,
    cancel: Option<CancelToken>,
) -> Result<ModuleProjection, calamine::Error> {
    let io = |e: std::io::Error| calamine::Error::Io(e);
    let zip_error = |e| io(std::io::Error::other(e));
    let mut archive = ZipArchive::new(CancellableReader::new(source.reader(), cancel.clone()))
        .map_err(zip_error)?;
    let mut workbook = Vec::new();
    archive
        .by_name("xl/workbook.xml")
        .map_err(zip_error)?
        .read_to_end(&mut workbook)
        .map_err(io)?;
    let mut xml = quick_xml::NsReader::from_reader(workbook.as_slice());
    let mut names = Vec::new();
    let mut omitted = Vec::new();
    let mut spans = Vec::new();
    let mut in_sheets = false;
    loop {
        CalamineAdapter::cancellation_checkpoint(cancel.as_ref())?;
        let start = xml.buffer_position() as usize;
        match xml.read_event().map_err(|e| io(std::io::Error::other(e)))? {
            Event::Start(e) if e.local_name().as_ref() == b"sheets" => in_sheets = true,
            Event::End(e) if e.local_name().as_ref() == b"sheets" => in_sheets = false,
            Event::Start(e) | Event::Empty(e)
                if in_sheets
                    && e.local_name().as_ref() == b"sheet"
                    && matches!(xml.resolver().resolve_element(e.name()).0,
                    quick_xml::name::ResolveResult::Bound(ns)
                    if ns.as_ref() == b"http://schemas.openxmlformats.org/spreadsheetml/2006/main") =>
            {
                if let Some(name) = CalamineAdapter::decode_attr(&xml, &e, b"name") {
                    names.push(name.clone());
                    let empty_relationship = e.attributes().flatten().any(|attr| {
                        let (ns, local) = xml.resolver().resolve_attribute(attr.key);
                        local.as_ref() == b"id"
                            && matches!(ns, quick_xml::name::ResolveResult::Bound(ns)
                                if ns.as_ref() == b"http://schemas.openxmlformats.org/officeDocument/2006/relationships")
                            && attr.decode_and_unescape_value(xml.decoder()).is_ok_and(|id| id.is_empty())
                    });
                    if empty_relationship {
                        // A sheet may use an explicit closing tag rather than '/>'.
                        if !workbook[start..xml.buffer_position() as usize].ends_with(b"/>") {
                            xml.read_to_end(e.name())
                                .map_err(|e| io(std::io::Error::other(e)))?;
                        }
                        spans.push(start..xml.buffer_position() as usize);
                        omitted.push(crate::traits::SheetImportDiagnostic {
                            name,
                            kind: "module".into(),
                        });
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if spans.is_empty() {
        return Ok((None, names, omitted));
    }
    for span in spans.into_iter().rev() {
        workbook.drain(span);
    }
    let mut output = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for i in 0..archive.len() {
        let entry = archive.by_index(i).map_err(zip_error)?;
        if entry.name() == "xl/workbook.xml" {
            output
                .start_file(entry.name(), zip::write::SimpleFileOptions::default())
                .map_err(zip_error)?;
            output.write_all(&workbook).map_err(io)?;
        } else {
            output.raw_copy_file(entry).map_err(zip_error)?;
        }
    }
    Ok((
        Some(output.finish().map_err(zip_error)?.into_inner()),
        names,
        omitted,
    ))
}
