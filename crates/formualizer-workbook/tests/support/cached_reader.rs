//! Independent cached-value inspector for tests, not a workbook loader.
//! Reads literal worksheet XML through quick-xml and ZIP, without native ingress.
#![allow(dead_code)]
use quick_xml::{Reader, events::Event};
use std::{
    collections::BTreeMap,
    io::{Read, Seek},
};

#[derive(Debug, Clone, PartialEq)]
pub enum Data {
    Empty,
    Float(f64),
    Bool(bool),
    String(String),
    Error(CellErrorType),
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CellErrorType {
    NA,
    Div0,
    Value,
    Ref,
    Num,
    Null,
    Name,
}
#[derive(Default)]
pub struct Range<T>(BTreeMap<(u32, u32), T>);
impl<T> Range<T> {
    pub fn get_value(&self, cell: (u32, u32)) -> Option<&T> {
        self.0.get(&cell)
    }
}
type CachedCells = BTreeMap<(u32, u32), (String, String, String)>;
pub struct Xlsx {
    parts: BTreeMap<String, String>,
    sheets: Vec<(String, String)>,
}
fn attrs(e: &quick_xml::events::BytesStart<'_>) -> BTreeMap<String, String> {
    e.attributes()
        .map(|a| {
            let a = a.unwrap();
            (
                String::from_utf8_lossy(a.key.local_name().as_ref()).into_owned(),
                quick_xml::escape::unescape(&String::from_utf8_lossy(&a.value))
                    .unwrap()
                    .into_owned(),
            )
        })
        .collect()
}
fn coord(s: &str) -> (u32, u32) {
    let mut col = 0;
    let mut row = 0;
    for b in s.bytes() {
        if b.is_ascii_alphabetic() {
            col = col * 26 + u32::from(b.to_ascii_uppercase() - b'A' + 1)
        } else if b.is_ascii_digit() {
            row = row * 10 + u32::from(b - b'0')
        }
    }
    (row - 1, col - 1)
}
impl Xlsx {
    pub fn sheet_names(&self) -> Vec<String> {
        self.sheets.iter().map(|(n, _)| n.clone()).collect()
    }
    pub fn new<R: Read + Seek>(reader: R) -> Result<Self, String> {
        let mut zip = zip::ZipArchive::new(reader).map_err(|e| e.to_string())?;
        let mut parts = BTreeMap::new();
        for i in 0..zip.len() {
            let mut file = zip.by_index(i).map_err(|e| e.to_string())?;
            let name = file.name().to_owned();
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
            if name.ends_with(".xml") || name.ends_with(".rels") {
                parts.insert(name, String::from_utf8(bytes).map_err(|e| e.to_string())?);
            }
        }
        let mut rels = BTreeMap::new();
        if let Some(xml) = parts.get("xl/_rels/workbook.xml.rels") {
            let mut r = Reader::from_str(xml);
            loop {
                match r.read_event().map_err(|e| e.to_string())? {
                    Event::Start(e) | Event::Empty(e)
                        if e.local_name().as_ref() == b"Relationship" =>
                    {
                        let a = attrs(&e);
                        if let (Some(id), Some(target)) = (a.get("Id"), a.get("Target")) {
                            let p = if target.starts_with('/') {
                                target.trim_start_matches('/').to_owned()
                            } else {
                                format!("xl/{target}")
                            };
                            rels.insert(id.clone(), p);
                        }
                    }
                    Event::Eof => break,
                    _ => {}
                }
            }
        }
        let mut sheets = Vec::new();
        if let Some(xml) = parts.get("xl/workbook.xml") {
            let mut r = Reader::from_str(xml);
            loop {
                match r.read_event().map_err(|e| e.to_string())? {
                    Event::Start(e) | Event::Empty(e) if e.local_name().as_ref() == b"sheet" => {
                        let a = attrs(&e);
                        if let (Some(name), Some(path)) =
                            (a.get("name"), a.get("id").and_then(|id| rels.get(id)))
                        {
                            sheets.push((name.clone(), path.clone()))
                        }
                    }
                    Event::Eof => break,
                    _ => {}
                }
            }
        }
        Ok(Self { parts, sheets })
    }
    fn cells(&self, name: &str) -> Result<CachedCells, String> {
        let path = &self
            .sheets
            .iter()
            .find(|(n, _)| n == name)
            .ok_or("sheet missing")?
            .1;
        let xml = self.parts.get(path).ok_or("worksheet missing")?;
        let mut r = Reader::from_str(xml);
        let mut cells = BTreeMap::new();
        let mut at = None;
        let mut kind = String::new();
        let mut field = Vec::new();
        let mut value = String::new();
        let mut formula = String::new();
        loop {
            match r.read_event().map_err(|e| e.to_string())? {
                Event::Start(e) => {
                    let n = e.local_name();
                    match n.as_ref() {
                        b"c" => {
                            let a = attrs(&e);
                            at = a.get("r").map(|s| coord(s));
                            kind = a.get("t").cloned().unwrap_or_default();
                            value.clear();
                            formula.clear();
                        }
                        b"v" | b"t" | b"f" => field = n.as_ref().to_vec(),
                        _ => {}
                    }
                }
                Event::Text(t) => {
                    let text = t.xml_content().map_err(|e| e.to_string())?;
                    if field == b"f" {
                        formula.push_str(&text)
                    } else if field == b"v" || field == b"t" {
                        value.push_str(&text)
                    }
                }
                Event::GeneralRef(e) => {
                    let text = e.decode().map_err(|e| e.to_string())?;
                    let entity = quick_xml::escape::unescape(&format!("&{text};"))
                        .map_err(|e| e.to_string())?
                        .into_owned();
                    if field == b"f" {
                        formula.push_str(&entity)
                    } else {
                        value.push_str(&entity)
                    }
                }
                Event::End(e) => {
                    if e.local_name().as_ref() == b"c"
                        && let Some(at) = at.take()
                    {
                        cells.insert(at, (kind.clone(), value.clone(), formula.clone()));
                    }
                    field.clear()
                }
                Event::Eof => break,
                _ => {}
            }
        }
        Ok(cells)
    }
    fn shared_strings(&self) -> Result<Vec<String>, String> {
        let Some(xml) = self.parts.get("xl/sharedStrings.xml") else {
            return Ok(Vec::new());
        };
        let mut reader = Reader::from_str(xml);
        let mut strings = Vec::new();
        let mut text = String::new();
        let mut inside_text = false;
        let mut phonetic = false;
        loop {
            match reader.read_event().map_err(|e| e.to_string())? {
                Event::Start(e) => match e.local_name().as_ref() {
                    b"si" => text.clear(),
                    b"rPh" => phonetic = true,
                    b"t" if !phonetic => inside_text = true,
                    _ => {}
                },
                Event::Empty(e) if e.local_name().as_ref() == b"si" => strings.push(String::new()),
                Event::End(e) => match e.local_name().as_ref() {
                    b"si" => strings.push(std::mem::take(&mut text)),
                    b"rPh" => phonetic = false,
                    b"t" => inside_text = false,
                    _ => {}
                },
                Event::Text(e) if inside_text => {
                    text.push_str(&e.xml_content().map_err(|e| e.to_string())?)
                }
                Event::CData(e) if inside_text => {
                    text.push_str(&e.decode().map_err(|e| e.to_string())?)
                }
                Event::GeneralRef(e) if inside_text => {
                    let name = e.decode().map_err(|e| e.to_string())?;
                    text.push_str(
                        &quick_xml::escape::unescape(&format!("&{name};"))
                            .map_err(|e| e.to_string())?,
                    );
                }
                Event::Eof => break,
                _ => {}
            }
        }
        Ok(strings)
    }
    pub fn worksheet_range(&mut self, name: &str) -> Result<Range<Data>, String> {
        let strings = self.shared_strings()?;
        let mut range = Range(BTreeMap::new());
        for (at, (kind, value, _)) in self.cells(name)? {
            let data = match kind.as_str() {
                "str" | "inlineStr" | "d" => Data::String(value),
                "s" if value.is_empty() => Data::Empty,
                "s" => Data::String(
                    strings
                        .get(value.parse::<usize>().map_err(|e| e.to_string())?)
                        .ok_or("missing shared string")?
                        .clone(),
                ),
                "b" => Data::Bool(value == "1"),
                "e" => Data::Error(match value.as_str() {
                    "#N/A" => CellErrorType::NA,
                    "#DIV/0!" => CellErrorType::Div0,
                    "#REF!" => CellErrorType::Ref,
                    "#NUM!" => CellErrorType::Num,
                    "#NULL!" => CellErrorType::Null,
                    "#NAME?" => CellErrorType::Name,
                    "#VALUE!" => CellErrorType::Value,
                    _ => return Err(format!("unsupported cached error: {value}")),
                }),
                "" | "n" if value.is_empty() => Data::Empty,
                "" | "n" => Data::Float(
                    value
                        .parse()
                        .map_err(|e: std::num::ParseFloatError| e.to_string())?,
                ),
                _ => return Err(format!("unsupported cached cell type: {kind}")),
            };
            range.0.insert(at, data);
        }
        Ok(range)
    }
    pub fn worksheet_range_at(&mut self, index: usize) -> Option<Result<Range<Data>, String>> {
        let name = self.sheets.get(index)?.0.clone();
        Some(self.worksheet_range(&name))
    }
    pub fn worksheet_formula(&mut self, name: &str) -> Result<Range<String>, String> {
        Ok(Range(
            self.cells(name)?
                .into_iter()
                .filter(|(_, (_, _, f))| !f.is_empty())
                .map(|(at, (_, _, f))| (at, f))
                .collect(),
        ))
    }
}

impl std::fmt::Display for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => Ok(()),
            Self::Float(n) => write!(f, "{n}"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::String(s) => write!(f, "{s}"),
            Self::Error(e) => write!(f, "{e:?}"),
        }
    }
}
