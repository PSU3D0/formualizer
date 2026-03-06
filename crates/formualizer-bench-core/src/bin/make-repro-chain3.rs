#[cfg(feature = "xlsx")]
use anyhow::{Context, Result};
#[cfg(feature = "xlsx")]
use std::{
    fs::File,
    io::{Cursor, Read, Write},
};

#[cfg(not(feature = "xlsx"))]
fn main() {
    eprintln!(
        "This binary requires feature `xlsx`: cargo run -p formualizer-bench-core --features xlsx --bin make-repro-chain3 -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "xlsx")]
fn main() -> anyhow::Result<()> {
    use formualizer_testkit::write_workbook;
    use std::path::PathBuf;

    let path = PathBuf::from("benchmarks/corpus/synthetic/repro_chain3.xlsx");
    write_workbook(&path, |book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        sh.get_cell_mut((1, 1)).set_value_number(1.0);
        sh.get_cell_mut((1, 2)).set_formula("=A1+1");
        sh.get_cell_mut((1, 3)).set_formula("=A2+1");
    });
    normalize_xlsx_styles_for_cross_engine(&path)?;
    println!("{}", path.display());
    Ok(())
}

#[cfg(feature = "xlsx")]
fn normalize_xlsx_styles_for_cross_engine(path: &std::path::Path) -> Result<()> {
    let src =
        File::open(path).with_context(|| format!("open xlsx for normalize: {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(src)
        .with_context(|| format!("read xlsx zip for normalize: {}", path.display()))?;

    let mut files: Vec<(String, zip::CompressionMethod, Vec<u8>)> =
        Vec::with_capacity(archive.len());
    for idx in 0..archive.len() {
        let mut entry = archive.by_index(idx)?;
        let name = entry.name().to_string();
        let method = entry.compression();
        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;
        if name == "xl/styles.xml" {
            data = normalize_styles_xml(&data)?;
        }
        files.push((name, method, data));
    }
    drop(archive);

    let mut out_buf = Cursor::new(Vec::<u8>::new());
    {
        let mut writer = zip::ZipWriter::new(&mut out_buf);
        for (name, method, data) in files {
            let options = zip::write::FileOptions::default().compression_method(method);
            writer.start_file(name, options)?;
            writer.write_all(&data)?;
        }
        writer.finish()?;
    }

    std::fs::write(path, out_buf.into_inner())
        .with_context(|| format!("write normalized xlsx: {}", path.display()))?;
    Ok(())
}

#[cfg(feature = "xlsx")]
fn normalize_styles_xml(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut xml = String::from_utf8(bytes.to_vec()).context("styles.xml must be utf-8")?;

    if !xml.contains("<numFmts") {
        insert_after_stylesheet_open(&mut xml, "<numFmts count=\"0\"/>")?;
    }
    if !xml.contains("<cellStyleXfs") {
        insert_before_marker_or_stylesheet_end(
            &mut xml,
            "<cellXfs",
            "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>",
        )?;
    }
    if !xml.contains("<cellStyles") {
        insert_after_marker_or_stylesheet_open(
            &mut xml,
            "</cellXfs>",
            "<cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles>",
        )?;
    }

    Ok(xml.into_bytes())
}

#[cfg(feature = "xlsx")]
fn insert_after_stylesheet_open(xml: &mut String, snippet: &str) -> Result<()> {
    let open = xml
        .find("<styleSheet")
        .with_context(|| "styles.xml missing <styleSheet> root")?;
    let gt_rel = xml[open..]
        .find('>')
        .with_context(|| "styles.xml malformed <styleSheet> open tag")?;
    let insert_at = open + gt_rel + 1;
    xml.insert_str(insert_at, snippet);
    Ok(())
}

#[cfg(feature = "xlsx")]
fn insert_before_marker_or_stylesheet_end(
    xml: &mut String,
    marker: &str,
    snippet: &str,
) -> Result<()> {
    if let Some(pos) = xml.find(marker) {
        xml.insert_str(pos, snippet);
        return Ok(());
    }
    if let Some(end) = xml.find("</styleSheet>") {
        xml.insert_str(end, snippet);
        return Ok(());
    }
    anyhow::bail!("styles.xml missing marker and closing styleSheet: {marker}")
}

#[cfg(feature = "xlsx")]
fn insert_after_marker_or_stylesheet_open(
    xml: &mut String,
    marker: &str,
    snippet: &str,
) -> Result<()> {
    if let Some(pos) = xml.find(marker) {
        let insert_at = pos + marker.len();
        xml.insert_str(insert_at, snippet);
        return Ok(());
    }
    insert_after_stylesheet_open(xml, snippet)
}
