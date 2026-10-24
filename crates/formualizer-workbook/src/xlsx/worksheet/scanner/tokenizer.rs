//! Namespace/tokenizer state and XML safety checks for the single sheet walk.
//! The hot and cold paths share one cursor and authoritative source buffer.
use super::*;
use crate::xlsx::options::checkpoint;
use crate::xlsx::xml::{
    Element, Ns, Parsed, Scopes, attribute, backend, bind, cdata_end, content, local_part, offset,
    plain_text, qualified_name, valid_text,
};
use quick_xml::{
    Reader,
    events::{
        BytesDecl, BytesEnd, BytesRef, BytesStart, BytesText,
        attributes::{AttrError, Attribute as RawAttribute, Attributes},
    },
    name::QName,
};

pub(super) struct XmlState<'a> {
    pub text: &'a str,
    pub reader: Reader<&'a [u8]>,
    scopes: Scopes<'a>,
    parsed: Vec<Parsed<'a>>,
    pub attributes: Vec<xml::Attribute<'a>>,
    pub hot: crate::xlsx::xml::worksheet::attributes::HotAttributes<'a>,
    pub path: Vec<Element<'a>>,
    pub depth: usize,
    pub roots: usize,
    events: u64,
    declaration: bool,
    pending_pop: bool,
    pending_path_pop: bool,
    scoped_depths: Vec<usize>,
    sheet_data_generation: Option<u64>,
    sheet_data_depth: Option<usize>,
}
impl<'a> XmlState<'a> {
    pub fn new(bytes: &'a [u8]) -> Result<Self, IoError> {
        let text =
            std::str::from_utf8(bytes).map_err(|_| unsupported("non-UTF-8 XML", "XLSX XML"))?;
        valid_text(text)?;
        let mut reader = Reader::from_str(text);
        reader.config_mut().check_end_names = true;
        reader.config_mut().check_comments = true;
        Ok(Self {
            text,
            reader,
            scopes: Scopes::new(),
            parsed: Vec::new(),
            attributes: Vec::new(),
            hot: Default::default(),
            path: Vec::new(),
            depth: 0,
            roots: 0,
            events: 0,
            declaration: false,
            pending_pop: false,
            pending_path_pop: false,
            scoped_depths: Vec::new(),
            sheet_data_generation: None,
            sheet_data_depth: None,
        })
    }
    pub fn advance(&mut self, options: &ReadOptions) -> Result<(), IoError> {
        if self.events & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        self.events += 1;
        if self.pending_path_pop {
            if self.path.len() == self.depth {
                self.path.pop();
            }
            self.depth -= 1;
            self.pending_path_pop = false;
        }
        if self.pending_pop {
            self.scopes.pop();
            self.pending_pop = false;
        }
        Ok(())
    }
    pub fn open<const COUNT_PATHS: bool>(
        &mut self,
        e: &BytesStart<'a>,
        empty: bool,
        start: usize,
        options: &ReadOptions,
        cursor: &mut Cursor,
        path_counts: &mut (usize, usize),
    ) -> Result<(bool, Element<'a>), IoError> {
        let (_, content) = content(self.text, e)?;
        let name_len = e.name().as_ref().len();
        let qualified = &content[..name_len];
        let fast = self.sheet_data_depth.is_some()
            && matches!(
                cursor.position,
                Position::SheetData
                    | Position::Row
                    | Position::Cell
                    | Position::CellFormula
                    | Position::CellValue
                    | Position::CellInline
            )
            && self.sheet_data_generation == Some(self.scopes.generation)
            && matches!(qualified, "row" | "c" | "f" | "v" | "is" | "t")
            && self.hot.parse(content, qualified, start);
        if COUNT_PATHS {
            if fast {
                path_counts.0 += 1;
            } else {
                path_counts.1 += 1;
            }
        }
        let malformed = if fast {
            false
        } else {
            bind(&mut self.scopes, content, name_len, &mut self.parsed)?
        };
        self.pending_pop = empty && !fast;
        if self.depth >= options.limits.max_xml_depth {
            return Err(unsupported("XML depth limit", "XLSX XML"));
        }
        if self.depth == 0 {
            self.roots += 1;
            if self.roots != 1 {
                return Err(unsupported("multiple XML roots", "XLSX XML"));
            }
        }
        if !fast {
            qualified_name(qualified.as_bytes())?;
        }
        let element = Element {
            ns: if fast {
                Ns::Main
            } else {
                self.scopes.element(qualified)?
            },
            local: local_part(qualified),
            qualified,
        };
        self.attributes.clear();
        if !fast {
            self.validate_attributes(content, name_len, start, malformed)?;
        }
        // Re-establish a default-main witness only after full cold
        // binding, name resolution and attribute validation of this tag.
        if !fast && !qualified.contains(':') && element.ns.is_main() {
            self.sheet_data_generation = Some(self.scopes.generation);
        }
        if !fast {
            cursor.materialize(&mut self.path, self.depth)?;
            self.path.push(element.clone());
        }
        self.depth += 1;
        if !empty && !fast {
            self.scoped_depths.push(self.depth);
        }
        if self.depth == 2
            && self.path[0].ns.is_main()
            && self.path[0].local == "worksheet"
            && self.path[1].ns.is_main()
            && self.path[1].local == "sheetData"
            && !empty
        {
            self.sheet_data_depth = Some(self.depth);
        }
        self.pending_path_pop = empty;
        Ok((fast, element))
    }
    fn validate_attributes(
        &mut self,
        content: &'a str,
        name_len: usize,
        start: usize,
        malformed: bool,
    ) -> Result<(), IoError> {
        let decoder = self.reader.decoder();
        let mut seen = None;

        if malformed {
            for a in Attributes::new(content, name_len) {
                attribute(
                    a.map_err(backend)?,
                    content,
                    start,
                    decoder,
                    &mut self.scopes,
                    &mut seen,
                    &mut self.attributes,
                )?;
            }
        } else {
            for (i, p) in self.parsed.iter().enumerate() {
                if let Some(prev) = self.parsed[..i].iter().find(|q| q.key == p.key) {
                    let raw = content.as_bytes();
                    return Err(backend(AttrError::Duplicated(
                        offset(raw, p.key)?,
                        offset(raw, prev.key)?,
                    )));
                }
                attribute(
                    RawAttribute {
                        key: QName(p.key),
                        value: Cow::Borrowed(p.value),
                    },
                    content,
                    start,
                    decoder,
                    &mut self.scopes,
                    &mut seen,
                    &mut self.attributes,
                )?;
            }
        }
        Ok(())
    }
    pub fn close(&mut self, e: BytesEnd<'a>) -> Result<Element<'a>, IoError> {
        if self.sheet_data_depth == Some(self.depth) {
            self.sheet_data_depth = None;
        }
        self.pending_pop = self.scoped_depths.last().copied() == Some(self.depth);
        if self.pending_pop {
            self.scoped_depths.pop();
        }
        if self.depth == 0 {
            return Err(unsupported("unbalanced XML", "XLSX XML"));
        }
        let element = if self.path.len() == self.depth {
            self.path
                .last()
                .ok_or_else(|| unsupported("unbalanced XML", "XLSX XML"))?
                .clone()
        } else {
            let name = e.name();
            let raw = name.as_ref();
            let at = offset(self.text.as_bytes(), raw)?;
            let qualified = self
                .text
                .get(at..at + raw.len())
                .ok_or_else(|| unsupported("unavailable XML name span", "XLSX XML"))?;
            Element {
                ns: Ns::Main,
                local: qualified,
                qualified,
            }
        };
        self.pending_path_pop = true;
        Ok(element)
    }
    pub fn text(&self, t: BytesText<'a>) -> Result<Cow<'a, str>, IoError> {
        let value = match plain_text(self.text, &t) {
            Some(value) => Cow::Borrowed(value),
            None => {
                if cdata_end(&t) {
                    return Err(unsupported("CDATA terminator in text", "XLSX XML"));
                }
                let value = t.xml_content().map_err(backend)?;
                if let Cow::Owned(value) = &value {
                    valid_text(value)?;
                }
                value
            }
        };
        if self.path.is_empty() && !value.trim().is_empty() {
            return Err(unsupported("text outside XML root", "XLSX XML"));
        }
        Ok(value)
    }
    pub fn reference(&self, reference: BytesRef<'a>) -> Result<Cow<'a, str>, IoError> {
        let name = reference.decode().map_err(backend)?;
        let value: Cow<'_, str> = match name.as_ref() {
            "amp" => "&".into(),
            "lt" => "<".into(),
            "gt" => ">".into(),
            "quot" => "\"".into(),
            "apos" => "'".into(),
            _ => reference
                .resolve_char_ref()
                .map_err(backend)?
                .ok_or_else(|| unsupported("unknown XML entity", "XLSX XML"))?
                .to_string()
                .into(),
        };
        if self.path.is_empty() {
            return Err(unsupported("entity outside XML root", "XLSX XML"));
        }
        valid_text(&value)?;
        Ok(value)
    }
    pub fn declaration(&mut self, d: BytesDecl<'a>) -> Result<(), IoError> {
        if self.roots != 0 || self.declaration {
            return Err(unsupported("misplaced XML declaration", "XLSX XML"));
        }
        self.declaration = true;
        if d.version().map_err(backend)?.as_ref() != b"1.0" {
            return Err(unsupported("unsupported XML version", "XLSX XML"));
        }
        if let Some(encoding) = d.encoding() {
            let encoding = encoding.map_err(backend)?;
            if !(encoding.eq_ignore_ascii_case(b"utf-8")
                || encoding.eq_ignore_ascii_case(b"us-ascii") && self.text.as_bytes().is_ascii())
            {
                return Err(unsupported("unsupported XML encoding", "XLSX XML"));
            }
        }
        Ok(())
    }
    pub fn finish(&self) -> Result<(), IoError> {
        if !self.path.is_empty() || self.roots != 1 {
            return Err(unsupported("incomplete XML document", "XLSX XML"));
        }
        Ok(())
    }
}
