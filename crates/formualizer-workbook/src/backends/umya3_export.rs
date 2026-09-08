//! Cold XLSX projection repair for Umya 3's colour-blind style hashes.
//! The document is the sole authority. Only emitted colour definitions and
//! their style references are corrected; no export map survives this call.
use super::import::{Parts, invalid};
use quick_xml::{
    Reader, Writer,
    events::{BytesEnd, BytesStart, Event},
};
use std::{
    collections::BTreeMap,
    io::{self, Cursor, Read, Write},
};
use umya_spreadsheet3::{Color, Style, Workbook, Worksheet, XlsxError};

#[derive(Clone)]
struct Element {
    start: BytesStart<'static>,
    children: Vec<Item>,
}
#[derive(Clone)]
enum Item {
    Element(Element),
    Raw(Event<'static>),
}
impl Element {
    fn name(&self) -> &[u8] {
        self.start.local_name().into_inner()
    }
    fn attr(&self, key: &str) -> io::Result<Option<String>> {
        attribute(&self.start, key)
    }
    fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        set_attribute(&mut self.start, key, value)
    }
    fn child(&self, name: &[u8]) -> Option<&Element> {
        self.children.iter().find_map(|c| match c {
            Item::Element(e) if e.name() == name => Some(e),
            _ => None,
        })
    }
    fn child_mut(&mut self, name: &[u8]) -> Option<&mut Element> {
        self.children.iter_mut().find_map(|c| match c {
            Item::Element(e) if e.name() == name => Some(e),
            _ => None,
        })
    }
    fn write(&self, writer: &mut Writer<Vec<u8>>) -> io::Result<()> {
        if self.children.is_empty() {
            writer.write_event(Event::Empty(self.start.clone()))?;
        } else {
            writer.write_event(Event::Start(self.start.clone()))?;
            for child in &self.children {
                match child {
                    Item::Element(e) => e.write(writer)?,
                    Item::Raw(e) => writer.write_event(e.clone())?,
                }
            }
            writer.write_event(Event::End(BytesEnd::new(
                String::from_utf8_lossy(self.start.name().as_ref()).into_owned(),
            )))?;
        }
        Ok(())
    }
    fn bytes(&self) -> io::Result<Vec<u8>> {
        let mut w = Writer::new(Vec::new());
        self.write(&mut w)?;
        Ok(w.into_inner())
    }
}
fn attribute(start: &BytesStart<'_>, key: &str) -> io::Result<Option<String>> {
    for a in start.attributes() {
        let a = a.map_err(io::Error::other)?;
        if a.key.as_ref() == key.as_bytes() {
            return a
                .decode_and_unescape_value(Reader::from_str("").decoder())
                .map(|v| Some(v.into_owned()))
                .map_err(io::Error::other);
        }
    }
    Ok(None)
}
fn set_attribute(start: &mut BytesStart<'static>, key: &str, value: &str) -> io::Result<()> {
    let mut attrs = Vec::new();
    let mut found = false;
    for a in start.attributes() {
        let a = a.map_err(io::Error::other)?;
        if a.key.as_ref() == key.as_bytes() {
            attrs.push((
                a.key.as_ref().to_vec(),
                quick_xml::escape::escape(value).as_bytes().to_vec(),
            ));
            found = true;
        } else {
            attrs.push((a.key.as_ref().to_vec(), a.value.into_owned()));
        }
    }
    if !found {
        attrs.push((
            key.as_bytes().to_vec(),
            quick_xml::escape::escape(value).as_bytes().to_vec(),
        ));
    }
    start.clear_attributes();
    for (k, v) in &attrs {
        start.push_attribute((k.as_slice(), v.as_slice()));
    }
    Ok(())
}
fn parse(xml: &[u8]) -> io::Result<Element> {
    let mut reader = Reader::from_reader(xml);
    let mut stack: Vec<Element> = Vec::new();
    let mut root = None;
    loop {
        match reader.read_event().map_err(io::Error::other)? {
            Event::Start(e) => {
                if stack.len() >= 128 {
                    return Err(invalid("export XML nesting limit"));
                }
                stack.push(Element {
                    start: e.into_owned(),
                    children: Vec::new(),
                });
            }
            Event::Empty(e) => {
                let node = Element {
                    start: e.into_owned(),
                    children: Vec::new(),
                };
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(Item::Element(node));
                } else if root.replace(node).is_some() {
                    return Err(invalid("multiple XML roots"));
                }
            }
            Event::End(_) => {
                let node = stack
                    .pop()
                    .ok_or_else(|| invalid("unbalanced export XML"))?;
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(Item::Element(node));
                } else if root.replace(node).is_some() {
                    return Err(invalid("multiple XML roots"));
                }
            }
            Event::DocType(_) => return Err(invalid("export DTD is not supported")),
            Event::Eof => break,
            e => {
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(Item::Raw(e.into_owned()));
                }
            }
        }
    }
    if !stack.is_empty() {
        return Err(invalid("truncated export XML"));
    }
    root.ok_or_else(|| invalid("missing export XML root"))
}

// Presence probes preserve the actual colour selector, including theme/index 0.
fn colour_element(name: &str, colour: Option<&Color>) -> io::Result<Option<Element>> {
    let Some(c) = colour else {
        return Ok(None);
    };
    let mut attrs = Vec::<(&str, String)>::new();
    let mut probe = c.clone();
    probe.set_theme_index(c.theme_index());
    if probe == *c {
        attrs.push(("theme", c.theme_index().to_string()));
    } else {
        let mut probe = c.clone();
        probe.set_indexed(c.indexed());
        if probe == *c {
            attrs.push(("indexed", c.indexed().to_string()));
        } else {
            let mut probe = c.clone();
            probe.set_argb(c.argb());
            if c.argb() != Default::default() || probe == *c {
                attrs.push(("rgb", Color::argb8_to_hex(c.argb())));
            }
        }
    }
    let mut probe = c.clone();
    probe.set_tint(c.tint());
    if probe == *c {
        attrs.push(("tint", c.tint().to_string()));
    }
    if attrs.is_empty() {
        return Ok(None);
    }
    let mut start = BytesStart::new(name.to_owned());
    for (key, value) in attrs {
        start.push_attribute((key, value.as_str()));
    }
    Ok(Some(Element {
        start,
        children: Vec::new(),
    }))
}
fn replace_colour(parent: &mut Element, name: &str, colour: Option<&Color>) -> io::Result<()> {
    let replacement = colour_element(name, colour)?;
    let at = parent
        .children
        .iter()
        .position(|c| matches!(c, Item::Element(e) if e.name() == name.as_bytes()));
    match (at, replacement) {
        (Some(i), Some(e)) => parent.children[i] = Item::Element(e),
        (Some(i), None) => {
            parent.children.remove(i);
        }
        (None, Some(e)) => parent.children.push(Item::Element(e)),
        (None, None) => {}
    }
    Ok(())
}
fn patch_component(node: &mut Element, component: &str, style: &Style) -> io::Result<()> {
    match component {
        "font" => {
            if let Some(font) = style.font() {
                replace_colour(node, "color", Some(font.color()))?;
            }
        }
        "fill" => {
            if let Some(fill) = style.fill() {
                if let Some(pattern) = fill.get_pattern_fill() {
                    let target = node
                        .child_mut(b"patternFill")
                        .ok_or_else(|| invalid("missing emitted pattern fill"))?;
                    replace_colour(target, "fgColor", pattern.get_foreground_color())?;
                    replace_colour(target, "bgColor", pattern.get_background_color())?;
                }
                if let Some(gradient) = fill.get_gradient_fill() {
                    let target = node
                        .child_mut(b"gradientFill")
                        .ok_or_else(|| invalid("missing emitted gradient fill"))?;
                    let mut stops = target.children.iter_mut().filter_map(|n| match n {
                        Item::Element(e) if e.name() == b"stop" => Some(e),
                        _ => None,
                    });
                    for stop in gradient.get_gradient_stop() {
                        let target = stops
                            .next()
                            .ok_or_else(|| invalid("missing emitted gradient stop"))?;
                        replace_colour(target, "color", Some(stop.get_color()))?;
                    }
                }
            }
        }
        "border" => {
            if let Some(borders) = style.borders() {
                for (name, side) in [
                    ("left", borders.left()),
                    ("right", borders.right()),
                    ("top", borders.top()),
                    ("bottom", borders.bottom()),
                    ("diagonal", borders.diagonal()),
                    ("vertical", borders.vertical()),
                    ("horizontal", borders.horizontal()),
                ] {
                    let colour = side.color();
                    if let Some(target) = node.child_mut(name.as_bytes()) {
                        replace_colour(target, "color", colour.as_ref())?;
                    } else if colour.is_some() {
                        return Err(invalid("missing emitted border side"));
                    }
                }
            }
        }
        _ => return Err(invalid("unknown style component")),
    }
    Ok(())
}
struct Table {
    entries: Vec<Element>,
    interned: BTreeMap<Vec<u8>, usize>,
}
impl Table {
    fn new(section: Option<&Element>) -> io::Result<Self> {
        let entries: Vec<_> = section
            .into_iter()
            .flat_map(|s| &s.children)
            .filter_map(|n| match n {
                Item::Element(e) => Some(e.clone()),
                _ => None,
            })
            .collect();
        if entries.len() > 100_000 {
            return Err(invalid("export style limit exceeded"));
        }
        let mut interned = BTreeMap::new();
        for (i, entry) in entries.iter().enumerate() {
            interned.entry(entry.bytes()?).or_insert(i);
        }
        Ok(Self { entries, interned })
    }
    fn get(&self, index: usize) -> io::Result<Element> {
        self.entries
            .get(index)
            .cloned()
            .ok_or_else(|| invalid("invalid emitted style reference"))
    }
    fn intern(&mut self, entry: Element) -> io::Result<usize> {
        let key = entry.bytes()?;
        if let Some(i) = self.interned.get(&key) {
            return Ok(*i);
        }
        if self.entries.len() >= 100_000 {
            return Err(invalid("export style limit exceeded"));
        }
        let index = self.entries.len();
        self.entries.push(entry);
        self.interned.insert(key, index);
        Ok(index)
    }
}
struct Styles {
    root: Element,
    tables: BTreeMap<&'static str, Table>,
    cache: BTreeMap<usize, Vec<(Style, usize)>>,
}
impl Styles {
    fn new(xml: &[u8]) -> io::Result<Self> {
        let root = parse(xml)?;
        let mut tables = BTreeMap::new();
        for name in ["fonts", "fills", "borders", "cellXfs", "dxfs"] {
            tables.insert(name, Table::new(root.child(name.as_bytes()))?);
        }
        Ok(Self {
            root,
            tables,
            cache: BTreeMap::new(),
        })
    }
    fn xf(&mut self, old: usize, style: &Style) -> io::Result<usize> {
        // Compare actual styles, never Umya's collision-prone hashes. Typical
        // emitted IDs have one source style; collisions add only a few variants.
        if let Some((_, id)) = self
            .cache
            .get(&old)
            .and_then(|entries| entries.iter().find(|(source, _)| source == style))
        {
            return Ok(*id);
        }
        let mut xf = self.tables["cellXfs"].get(old)?;
        for (table, component, attr) in [
            ("fonts", "font", "fontId"),
            ("fills", "fill", "fillId"),
            ("borders", "border", "borderId"),
        ] {
            let id = number(xf.attr(attr)?)?.unwrap_or(0);
            let mut node = self.tables[table].get(id)?;
            patch_component(&mut node, component, style)?;
            let new = self.tables.get_mut(table).unwrap().intern(node)?;
            if new != id {
                xf.set(attr, &new.to_string())?;
            }
        }
        let new = self.tables.get_mut("cellXfs").unwrap().intern(xf)?;
        self.cache
            .entry(old)
            .or_default()
            .push((style.clone(), new));
        Ok(new)
    }
    fn dxf(&mut self, old: usize, style: &Style) -> io::Result<usize> {
        let mut node = self.tables["dxfs"].get(old)?;
        for component in ["font", "fill", "border"] {
            if let Some(target) = node.child_mut(component.as_bytes()) {
                patch_component(target, component, style)?;
            }
        }
        self.tables.get_mut("dxfs").unwrap().intern(node)
    }
    fn finish(mut self) -> io::Result<Vec<u8>> {
        for (name, table) in self.tables {
            if table.entries.is_empty() {
                continue;
            }
            let section = self
                .root
                .child_mut(name.as_bytes())
                .ok_or_else(|| invalid("missing style table"))?;
            section.set("count", &table.entries.len().to_string())?;
            section.children = table.entries.into_iter().map(Item::Element).collect();
        }
        self.root.bytes()
    }
}
fn number(value: Option<String>) -> io::Result<Option<usize>> {
    value
        .map(|v| {
            v.parse()
                .map_err(|_| invalid("invalid emitted numeric attribute"))
        })
        .transpose()
}
fn patch_reference(
    start: &mut BytesStart<'static>,
    attr: &str,
    style: &Style,
    styles: &mut Styles,
) -> io::Result<()> {
    let old = number(attribute(start, attr)?)?.unwrap_or(0);
    let new = styles.xf(old, style)?;
    if old != new {
        set_attribute(start, attr, &new.to_string())?;
    }
    Ok(())
}
fn patch_sheet(xml: &[u8], sheet: &Worksheet, styles: &mut Styles) -> io::Result<Vec<u8>> {
    let mut reader = Reader::from_reader(xml);
    let mut writer = Writer::new(Vec::new());
    let mut path: Vec<String> = Vec::new();
    let mut group = 0usize;
    let mut rule = 0usize;
    loop {
        let event = reader.read_event().map_err(io::Error::other)?;
        match event {
            Event::Start(ref e) | Event::Empty(ref e) => {
                let is_empty = matches!(event, Event::Empty(_));
                let mut start = e.clone().into_owned();
                path.push(String::from_utf8_lossy(start.local_name().as_ref()).into_owned());
                match path
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .as_slice()
                {
                    ["worksheet", "sheetData", "row"] => {
                        if let Some(row) = number(attribute(&start, "r")?)?
                            && let Some(value) =
                                sheet.row_dimensions_to_hashmap().get(&(row as u32))
                        {
                            patch_reference(&mut start, "s", value.style(), styles)?;
                        }
                    }
                    ["worksheet", "sheetData", "row", "c"] => {
                        let address = attribute(&start, "r")?
                            .ok_or_else(|| invalid("missing emitted cell address"))?;
                        let (row, col, _, _) = formualizer_common::coord::parse_a1_1based(&address)
                            .map_err(|e| invalid(e.to_string()))?;
                        if let Some(cell) = sheet.cell((col, row)) {
                            patch_reference(&mut start, "s", cell.style(), styles)?;
                        }
                    }
                    ["worksheet", "cols", "col"] => {
                        let min = number(attribute(&start, "min")?)?
                            .ok_or_else(|| invalid("missing column min"))?;
                        let max = number(attribute(&start, "max")?)?
                            .ok_or_else(|| invalid("missing column max"))?;
                        if min == 0 || min > max || max > 16_384 || !is_empty {
                            return Err(invalid("invalid emitted column range"));
                        }
                        // Split only on actual style-reference differences; widths,
                        // hidden flags and all other attributes remain untouched.
                        let mut runs: Vec<(usize, usize, BytesStart<'static>)> = Vec::new();
                        for col in min..=max {
                            let mut entry = start.clone();
                            if let Some(value) = sheet
                                .column_dimensions()
                                .iter()
                                .find(|v| v.col_num() as usize == col)
                            {
                                patch_reference(&mut entry, "style", value.style(), styles)?;
                            }
                            if let Some((_, end, prior)) = runs.last_mut()
                                && attribute(prior, "style")? == attribute(&entry, "style")?
                            {
                                *end = col;
                                continue;
                            }
                            runs.push((col, col, entry));
                        }
                        for (min, max, mut entry) in runs {
                            set_attribute(&mut entry, "min", &min.to_string())?;
                            set_attribute(&mut entry, "max", &max.to_string())?;
                            writer.write_event(Event::Empty(entry))?;
                        }
                        path.pop();
                        continue;
                    }
                    ["worksheet", "conditionalFormatting"] => {
                        group += 1;
                        rule = 0;
                    }
                    ["worksheet", "conditionalFormatting", "cfRule"] => {
                        if let Some(old) = number(attribute(&start, "dxfId")?)? {
                            let source = sheet
                                .conditional_formatting_collection()
                                .get(group.saturating_sub(1))
                                .and_then(|c| c.conditional_collection().get(rule))
                                .and_then(|r| r.style())
                                .ok_or_else(|| {
                                    invalid("missing authoritative differential style")
                                })?;
                            let new = styles.dxf(old, source)?;
                            if old != new {
                                set_attribute(&mut start, "dxfId", &new.to_string())?;
                            }
                        }
                        rule += 1;
                    }
                    ["worksheet", "sheetFormatPr"] => {
                        if sheet.sheet_format_properties().default_row_height() == 0.0 {
                            // Match the application's existing unspecified-height
                            // interpretation instead of Umya's new 14.25 fallback.
                            set_attribute(&mut start, "defaultRowHeight", "15")?;
                        }
                    }
                    _ => {}
                }
                writer.write_event(if is_empty {
                    Event::Empty(start)
                } else {
                    Event::Start(start)
                })?;
                if is_empty {
                    path.pop();
                }
            }
            Event::End(e) => {
                path.pop();
                writer.write_event(Event::End(e.into_owned()))?;
            }
            Event::Eof => break,
            e => writer.write_event(e.into_owned())?,
        }
    }
    Ok(writer.into_inner())
}

/// Serialize once through Umya, then correct emitted colour/style references
/// from the authoritative document. No workbook reimport or evaluator ingestion.
pub fn write_document(book: &Workbook) -> Result<Vec<u8>, XlsxError> {
    let mut original = Vec::new();
    umya_spreadsheet3::writer::xlsx::write_writer(book, &mut original)?;
    repair_projection(book, &original).map_err(XlsxError::Io)
}
/// Write a completed cold projection to a path. Callers needing atomic replacement
/// must supply their own temporary-file/rename protocol.
pub fn write_document_path(
    book: &Workbook,
    path: impl AsRef<std::path::Path>,
) -> Result<(), XlsxError> {
    std::fs::write(path, write_document(book)?)?;
    Ok(())
}

/// Write a completed cold projection without reparsing a workbook.
pub fn write_document_writer(book: &Workbook, mut writer: impl Write) -> Result<(), XlsxError> {
    writer.write_all(&write_document(book)?)?;
    Ok(())
}

fn repair_projection(book: &Workbook, original: &[u8]) -> io::Result<Vec<u8>> {
    let mut parts = Parts::new(original)?;
    let roots = parts.relationships("")?;
    let workbook = roots
        .values()
        .find(|r| r.kind.ends_with("/officeDocument"))
        .ok_or_else(|| invalid("missing emitted workbook relationship"))?
        .target
        .clone();
    let rels = parts.relationships(&workbook)?;
    let style_path = rels
        .values()
        .find(|r| r.kind.ends_with("/styles"))
        .ok_or_else(|| invalid("missing emitted styles"))?
        .target
        .clone();
    let mut sheets = Vec::new();
    parts.scan(&workbook, |path, attrs| {
        if path == ["workbook", "sheets", "sheet"] {
            let rel = attrs
                .get("id")
                .and_then(|id| rels.get(id))
                .ok_or_else(|| invalid("missing emitted sheet relationship"))?;
            if rel.kind.ends_with("/worksheet") {
                sheets.push((
                    attrs
                        .get("name")
                        .ok_or_else(|| invalid("missing sheet name"))?
                        .clone(),
                    rel.target.clone(),
                ));
            }
        }
        Ok(())
    })?;
    let mut archive = zip::ZipArchive::new(Cursor::new(original)).map_err(io::Error::other)?;
    let mut load = |name: &str| -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        archive
            .by_name(name)
            .map_err(io::Error::other)?
            .read_to_end(&mut bytes)?;
        Ok(bytes)
    };
    let mut styles = Styles::new(&load(&style_path)?)?;
    let mut replacements = BTreeMap::new();
    for (name, path) in sheets {
        let sheet = book.sheet_by_name(&name).map_err(io::Error::other)?;
        replacements.insert(
            path.clone(),
            patch_sheet(&load(&path)?, sheet, &mut styles)?,
        );
    }
    replacements.insert(style_path, styles.finish()?);
    let mut output = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for i in 0..archive.len() {
        let entry = archive.by_index(i).map_err(io::Error::other)?;
        if let Some(bytes) = replacements.get(entry.name()) {
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(entry.compression())
                .last_modified_time(entry.last_modified().unwrap_or_default());
            output
                .start_file(entry.name(), options)
                .map_err(io::Error::other)?;
            output.write_all(bytes)?;
        } else {
            output.raw_copy_file(entry).map_err(io::Error::other)?;
        }
    }
    Ok(output.finish().map_err(io::Error::other)?.into_inner())
}
