//! Cold-import compatibility for upstream Umya 3.1.0's discarded border colours.
//!
//! Only original border colour metadata is read here. Umya remains the document
//! parser and authority. No XML, patch map or alternate document survives import.
use quick_xml::{Reader, events::Event};
use std::collections::BTreeMap;
use std::io::{self, BufReader, Cursor, Read};
use umya_spreadsheet3::{Color, Style, Workbook, XlsxError};

type Attributes = BTreeMap<String, String>;
type Colours = BTreeMap<String, Color>;
const PART_LIMIT: u64 = 256 * 1024 * 1024;
const TOTAL_LIMIT: u64 = 1024 * 1024 * 1024;

pub(super) fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Read a single bounded source snapshot; parsing and repair see identical bytes.
pub fn read_document_path(path: impl AsRef<std::path::Path>) -> Result<Workbook, XlsxError> {
    read_document_reader(std::fs::File::open(path)?)
}

/// Buffer one bounded input stream, then use the same document importer.
pub fn read_document_reader(reader: impl Read) -> Result<Workbook, XlsxError> {
    read_document(&read_source(reader)?)
}

pub(super) fn read_source(reader: impl Read) -> Result<Vec<u8>, XlsxError> {
    let mut bytes = Vec::new();
    reader.take(PART_LIMIT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > PART_LIMIT {
        return Err(invalid("XLSX input limit exceeded").into());
    }
    Ok(bytes)
}

/// Eagerly import XLSX, restoring border colours lost by stock Umya 3.1.0.
///
/// The extra cold scan is bounded to 256 MiB per XML part and 1 GiB in total.
/// Invalid metadata fails import, rather than returning a partly repaired book.
/// Subsequent edits, evaluation and export use the ordinary Umya document only.
/// Use the paired `write_document` compatibility writer to preserve restored
/// theme/indexed colour identity across export, rather than Umya's raw writer.
pub fn read_document(bytes: &[u8]) -> Result<Workbook, XlsxError> {
    let mut archive = Parts::new(bytes).map_err(XlsxError::Io)?;
    let root = archive.relationships("").map_err(XlsxError::Io)?;
    let workbook = root
        .values()
        .find(|r| r.kind.ends_with("/officeDocument"))
        .ok_or_else(|| invalid("missing workbook relationship"))?
        .target
        .clone();
    let relationships = archive.relationships(&workbook).map_err(XlsxError::Io)?;
    let styles = relationships.values().find(|r| r.kind.ends_with("/styles"));
    let Some(styles) = styles else {
        return umya_spreadsheet3::reader::xlsx::read_reader(Cursor::new(bytes), true);
    };
    let table = read_styles(&mut archive, &styles.target).map_err(XlsxError::Io)?;
    let mut sheets = Vec::new();
    archive
        .scan(&workbook, |path, attrs| {
            if path == ["workbook", "sheets", "sheet"] {
                let name = required(attrs, "name")?.to_owned();
                let rel = relationships
                    .get(required(attrs, "id")?)
                    .ok_or_else(|| invalid("missing worksheet relationship"))?;
                if rel.kind.ends_with("/worksheet") {
                    sheets.push((name, rel.target.clone()));
                }
            }
            Ok(())
        })
        .map_err(XlsxError::Io)?;
    let mut book = umya_spreadsheet3::reader::xlsx::read_reader(Cursor::new(bytes), true)?;
    // Keep theme identity: the paired cold writer corrects colour-blind style
    // deduplication without flattening theme references to RGB.
    for colours in table.borders.iter().chain(&table.dxfs) {
        for colour in colours.values() {
            let mut probe = colour.clone();
            probe.set_theme_index(colour.theme_index());
            if probe == *colour
                && book
                    .theme()
                    .theme_elements()
                    .color_scheme()
                    .color_map()
                    .get(colour.theme_index() as usize)
                    .is_none()
            {
                return Err(invalid("border theme index is outside the workbook theme").into());
            }
        }
    }
    for (name, part) in sheets {
        let sheet = book.sheet_by_name_mut(&name)?;
        let mut conditional = sheet.conditional_formatting_collection().to_vec();
        let mut group = 0usize;
        let mut rule = 0usize;
        archive
            .scan(&part, |path, attrs| {
                match path
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .as_slice()
                {
                    ["worksheet", "sheetData", "row", "c"] => {
                        let index = index(attrs, "s")?.unwrap_or(0);
                        let colours = table.cell(index)?;
                        if !colours.is_empty() {
                            let address = required(attrs, "r")?;
                            // Never create a cell as a side effect of restoring metadata.
                            let (row, col, row_abs, col_abs) =
                                formualizer_common::coord::parse_a1_1based(address).map_err(
                                    |error| invalid(format!("invalid cell address: {error}")),
                                )?;
                            if row_abs || col_abs {
                                return Err(invalid(
                                    "worksheet cell addresses must not contain absolute markers",
                                ));
                            }
                            let cell = sheet
                                .collection_to_hashmap_mut()
                                .get_mut(&(row, col))
                                .ok_or_else(|| invalid("missing imported styled cell"))?;
                            // cell_mut() materializes row/column dimensions. That would
                            // change implicit widths merely by repairing a border.
                            apply(cell.style_mut(), colours);
                        }
                    }
                    ["worksheet", "sheetData", "row"] => {
                        if let Some(style) = index(attrs, "s")? {
                            let row = bounded_index(attrs, "r", 1_048_576)?;
                            apply(sheet.row_dimension_mut(row).style_mut(), table.cell(style)?);
                        }
                    }
                    ["worksheet", "cols", "col"] => {
                        if let Some(style) = index(attrs, "style")? {
                            let min = bounded_index(attrs, "min", 16_384)?;
                            let max = bounded_index(attrs, "max", 16_384)?;
                            if min > max {
                                return Err(invalid("reversed column range"));
                            }
                            for col in min..=max {
                                apply(
                                    sheet.column_dimension_by_number_mut(col).style_mut(),
                                    table.cell(style)?,
                                );
                            }
                        }
                    }
                    ["worksheet", "conditionalFormatting"] => {
                        group += 1;
                        rule = 0;
                    }
                    ["worksheet", "conditionalFormatting", "cfRule"] => {
                        if let Some(dxf) = index(attrs, "dxfId")? {
                            let colours = table
                                .dxfs
                                .get(dxf)
                                .ok_or_else(|| invalid("invalid differential style index"))?;
                            if !colours.is_empty() {
                                let target = conditional
                                    .get_mut(group.saturating_sub(1))
                                    .and_then(|g| g.conditional_collection_mut().get_mut(rule))
                                    .ok_or_else(|| invalid("missing imported conditional rule"))?;
                                let mut style = target
                                    .style()
                                    .cloned()
                                    .ok_or_else(|| invalid("missing differential style"))?;
                                apply(&mut style, colours);
                                target.set_style(style);
                            }
                        }
                        rule += 1;
                    }
                    _ => {}
                }
                Ok(())
            })
            .map_err(XlsxError::Io)?;
        sheet.set_conditional_formatting_collection(conditional);
    }
    Ok(book)
}

fn apply(style: &mut Style, colours: &Colours) {
    // Respect Umya's applyBorder/inheritance decision; only restore lost colour.
    if style.borders().is_none() {
        return;
    }
    let borders = style.borders_mut();
    for (side, colour) in colours {
        let border = match side.as_str() {
            "left" => borders.left_mut(),
            "right" => borders.right_mut(),
            "top" => borders.top_mut(),
            "bottom" => borders.bottom_mut(),
            "diagonal" => borders.diagonal_mut(),
            "vertical" => borders.vertical_mut(),
            "horizontal" => borders.horizontal_mut(),
            _ => continue,
        };
        border.set_color(colour.clone());
    }
}

#[derive(Default)]
struct Styles {
    borders: Vec<Colours>,
    cells: Vec<usize>,
    dxfs: Vec<Colours>,
}
impl Styles {
    fn cell(&self, index: usize) -> io::Result<&Colours> {
        let border = self
            .cells
            .get(index)
            .ok_or_else(|| invalid("invalid cell style index"))?;
        self.borders
            .get(*border)
            .ok_or_else(|| invalid("invalid border index"))
    }
}
fn read_styles(parts: &mut Parts<'_>, part: &str) -> io::Result<Styles> {
    let mut styles = Styles::default();
    parts.scan(part, |path, attrs| {
        match path
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["styleSheet", "borders", "border"] => styles.borders.push(Colours::new()),
            ["styleSheet", "dxfs", "dxf"] => styles.dxfs.push(Colours::new()),
            ["styleSheet", "cellXfs", "xf"] => {
                styles.cells.push(index(attrs, "borderId")?.unwrap_or(0))
            }
            ["styleSheet", "borders", "border", side, "color"] => {
                styles
                    .borders
                    .last_mut()
                    .ok_or_else(|| invalid("colour outside border"))?
                    .insert((*side).to_owned(), colour(attrs)?);
            }
            ["styleSheet", "dxfs", "dxf", "border", side, "color"] => {
                styles
                    .dxfs
                    .last_mut()
                    .ok_or_else(|| invalid("colour outside differential style"))?
                    .insert((*side).to_owned(), colour(attrs)?);
            }
            _ => {}
        }
        if styles.borders.len() > 100_000
            || styles.cells.len() > 100_000
            || styles.dxfs.len() > 100_000
        {
            return Err(invalid("XLSX style count limit exceeded"));
        }
        Ok(())
    })?;
    Ok(styles)
}
fn colour(attrs: &Attributes) -> io::Result<Color> {
    if ["rgb", "theme", "indexed", "auto"]
        .iter()
        .filter(|key| attrs.contains_key(**key))
        .count()
        > 1
    {
        return Err(invalid("ambiguous border colour selectors"));
    }
    // Preserve Umya's existing automatic-colour limitation rather than claiming
    // lossless support: auto is represented by an unset/default colour.
    let mut result = Color::default();
    if let Some(rgb) = attrs.get("rgb") {
        let argb = Color::hex_to_argb8(rgb).ok_or_else(|| invalid("invalid border ARGB"))?;
        // Umya's numeric setter folds palette-equivalent RGB into indexed
        // colours. The validated lowercase string path retains the RGB selector
        // (its palette lookup is case-sensitive, while hex parsing is not).
        result.set_argb_str(Color::argb8_to_hex(argb).to_ascii_lowercase());
    } else if let Some(theme) = index(attrs, "theme")? {
        result.set_theme_index(u32::try_from(theme).map_err(|_| invalid("invalid theme index"))?);
    } else if let Some(indexed) = index(attrs, "indexed")? {
        result.set_indexed(u32::try_from(indexed).map_err(|_| invalid("invalid palette index"))?);
    }
    if let Some(tint) = attrs.get("tint") {
        let value: f64 = tint.parse().map_err(|_| invalid("invalid border tint"))?;
        if !value.is_finite() || !(-1.0..=1.0).contains(&value) {
            return Err(invalid("invalid border tint"));
        }
        result.set_tint(value);
    }
    Ok(result)
}
fn required<'a>(attrs: &'a Attributes, key: &str) -> io::Result<&'a str> {
    attrs
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| invalid(format!("missing {key}")))
}
fn index(attrs: &Attributes, key: &str) -> io::Result<Option<usize>> {
    attrs
        .get(key)
        .map(|v| v.parse().map_err(|_| invalid(format!("invalid {key}"))))
        .transpose()
}
fn bounded_index(attrs: &Attributes, key: &str, max: u32) -> io::Result<u32> {
    let value = required(attrs, key)?
        .parse::<u32>()
        .map_err(|_| invalid(format!("invalid {key}")))?;
    if value == 0 || value > max {
        return Err(invalid(format!("out of range {key}")));
    }
    Ok(value)
}

pub(super) struct Relationship {
    pub(super) kind: String,
    pub(super) target: String,
}
pub(super) struct Parts<'a> {
    archive: zip::ZipArchive<Cursor<&'a [u8]>>,
    total: u64,
}
impl<'a> Parts<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> io::Result<Self> {
        if bytes.len() as u64 > PART_LIMIT {
            return Err(invalid("XLSX input limit exceeded"));
        }
        let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).map_err(io::Error::other)?;
        if archive.len() > 65_536 {
            return Err(invalid("XLSX entry count limit exceeded"));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut total = 0u64;
        for i in 0..archive.len() {
            let entry = archive.by_index_raw(i).map_err(io::Error::other)?;
            total = total
                .checked_add(entry.size())
                .ok_or_else(|| invalid("XLSX size overflow"))?;
            if entry.size() > PART_LIMIT || total > TOTAL_LIMIT {
                return Err(invalid("XLSX expanded size limit exceeded"));
            }
            if resolve("", entry.name())? != entry.name().trim_end_matches('/') {
                return Err(invalid("non-canonical XLSX part name"));
            }
            if !names.insert(entry.name().to_owned()) {
                return Err(invalid("duplicate XLSX part"));
            }
        }
        Ok(Self { archive, total: 0 })
    }
    pub(super) fn relationships(
        &mut self,
        source: &str,
    ) -> io::Result<BTreeMap<String, Relationship>> {
        let (parent, name) = source.rsplit_once('/').unwrap_or(("", source));
        let rels = if source.is_empty() {
            "_rels/.rels".to_owned()
        } else if parent.is_empty() {
            format!("_rels/{name}.rels")
        } else {
            format!("{parent}/_rels/{name}.rels")
        };
        let mut result = BTreeMap::new();
        self.scan(&rels, |path, attrs| {
            if path == ["Relationships", "Relationship"] {
                if attrs.get("TargetMode").is_some_and(|v| v == "External") {
                    return Ok(());
                }
                let target = resolve(source, required(attrs, "Target")?)?;
                let id = required(attrs, "Id")?.to_owned();
                let rel = Relationship {
                    kind: required(attrs, "Type")?.to_owned(),
                    target,
                };
                if result.insert(id, rel).is_some() {
                    return Err(invalid("duplicate relationship ID"));
                }
            }
            Ok(())
        })?;
        Ok(result)
    }
    pub(super) fn scan(
        &mut self,
        name: &str,
        mut visit: impl FnMut(&[String], &Attributes) -> io::Result<()>,
    ) -> io::Result<()> {
        let file = self.archive.by_name(name).map_err(io::Error::other)?;
        self.total = self
            .total
            .checked_add(file.size())
            .ok_or_else(|| invalid("XML size overflow"))?;
        if file.size() > PART_LIMIT || self.total > TOTAL_LIMIT {
            return Err(invalid("border import XML limit exceeded"));
        }
        let mut reader = Reader::from_reader(BufReader::new(file.take(PART_LIMIT + 1)));
        let mut buffer = Vec::new();
        let mut path = Vec::new();
        loop {
            let event = reader
                .read_event_into(&mut buffer)
                .map_err(io::Error::other)?;
            if reader.buffer_position() > PART_LIMIT {
                return Err(invalid("XML part limit exceeded"));
            }
            match event {
                Event::Start(ref e) | Event::Empty(ref e) => {
                    if path.len() >= 128 {
                        return Err(invalid("XML nesting limit exceeded"));
                    }
                    let name = std::str::from_utf8(e.local_name().as_ref())
                        .map_err(io::Error::other)?
                        .to_owned();
                    path.push(name);
                    let mut attrs = Attributes::new();
                    for attr in e.attributes() {
                        let attr = attr.map_err(io::Error::other)?;
                        let key = std::str::from_utf8(attr.key.local_name().as_ref())
                            .map_err(io::Error::other)?
                            .to_owned();
                        let value = attr
                            .decode_and_unescape_value(reader.decoder())
                            .map_err(io::Error::other)?
                            .into_owned();
                        if attrs.insert(key, value).is_some() {
                            return Err(invalid("ambiguous XML attribute"));
                        }
                    }
                    visit(&path, &attrs)?;
                    if matches!(event, Event::Empty(_)) {
                        path.pop();
                    }
                }
                Event::End(ref e) => {
                    let expected = path.pop().ok_or_else(|| invalid("unbalanced XML"))?;
                    if e.local_name().as_ref() != expected.as_bytes() {
                        return Err(invalid("mismatched XML end tag"));
                    }
                }
                Event::DocType(_) => return Err(invalid("DTD not permitted in XLSX metadata")),
                Event::Eof => {
                    if !path.is_empty() {
                        return Err(invalid("truncated XML"));
                    }
                    return Ok(());
                }
                _ => {}
            }
            buffer.clear();
        }
    }
}
use crate::xlsx_path::resolve;
