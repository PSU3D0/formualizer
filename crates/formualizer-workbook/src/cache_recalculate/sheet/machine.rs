//! Fused worksheet reader and rule loop; the visitor remains the oracle.
use super::*;
pub(in crate::cache_recalculate) mod emission;
#[cfg(test)]
mod path_tests;
mod scalar;
mod shared;
use scalar::{FormulaState, ScalarCell};

/// Semantic position; the tokenizer's namespace path is the only path stack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Position {
    Root,
    Worksheet,
    SheetData,
    Row,
    Cell,
    CellFormula,
    CellValue,
    CellInline,
    CellInlineText,
    Dimension,
    SheetFormatPr,
    Cols,
    AutoFilter,
    MergeCells,
    MergeCell,
    TableParts,
    TablePart,
    ExtLst,
    Other { depth: usize },
}

#[derive(Clone, Copy)]
enum Boundary {
    Open { empty: bool },
    Close,
}

struct AttributeValue<'a> {
    value: Cow<'a, str>,
    span: Range<usize>,
}

/// Typed hot locals or the exact generic cold validation result. No generic
/// attribute record is manufactured for an ignored hot attribute.
enum TagAttributes<'n, 'a> {
    Hot(&'n crate::xlsx_xml::worksheet::attributes::HotAttributes<'a>),
    Cold(&'n [xml::Attribute<'a>]),
}
impl<'n, 'a> TagAttributes<'n, 'a> {
    fn generic(&self) -> &'n [xml::Attribute<'a>] {
        match self {
            Self::Cold(a) => a,
            Self::Hot(_) => &[],
        }
    }
    fn attribute(&self, ns: &str, local: &str) -> Option<AttributeValue<'a>> {
        match self {
            Self::Hot(a) if ns.is_empty() => a.get(local).map(|a| AttributeValue {
                value: Cow::Borrowed(a.value),
                span: a.span.clone(),
            }),
            Self::Hot(_) => None,
            Self::Cold(a) => a
                .iter()
                .find(|a| a.local == local && *a.ns == *ns)
                .map(|a| AttributeValue {
                    value: a.value.clone(),
                    span: a.span.clone(),
                }),
        }
    }
    fn value(&self, name: &str) -> Option<&'n str> {
        match self {
            Self::Hot(a) => a.get(name).map(|a| a.value),
            Self::Cold(a) => a
                .iter()
                .find(|a| a.local == name && a.ns == "")
                .map(|a| a.value.as_ref()),
        }
    }
    fn required(&self, name: &str) -> Result<&'n str, IoError> {
        self.value(name)
            .ok_or_else(|| unsupported(format!("missing {name} attribute"), "XLSX XML"))
    }
    fn required_attribute(&self, name: &str) -> Result<AttributeValue<'a>, IoError> {
        self.attribute("", name)
            .ok_or_else(|| unsupported(format!("missing {name} attribute"), "XLSX XML"))
    }
}

struct Cursor {
    position: Position,
    /// Position immediately outside an opaque subtree. Nested unknown
    /// elements share its depth witness instead of another path stack.
    restore: Position,
}
impl Cursor {
    fn new() -> Self {
        Self {
            position: Position::Root,
            restore: Position::Root,
        }
    }
    fn open(&mut self, tag: Tag, local: &str, depth: usize) {
        if matches!(self.position, Position::Other { .. }) {
            return;
        }
        let next = if tag.main {
            match (self.position, tag.local) {
                (Position::Root, Local::Worksheet) => Position::Worksheet,
                (Position::Worksheet, Local::SheetData) => Position::SheetData,
                (Position::Worksheet, Local::Dimension) => Position::Dimension,
                (Position::Worksheet, Local::SheetFormatPr) => Position::SheetFormatPr,
                (Position::Worksheet, Local::AutoFilter) => Position::AutoFilter,
                (Position::Worksheet, Local::MergeCells) => Position::MergeCells,
                (Position::Worksheet, Local::TableParts) => Position::TableParts,
                (Position::Worksheet, _) if local == "cols" => Position::Cols,
                (Position::Worksheet, _) if local == "extLst" => Position::ExtLst,
                (Position::SheetData, Local::Row) => Position::Row,
                (Position::Row, Local::C) => Position::Cell,
                (Position::Cell, Local::F) => Position::CellFormula,
                (Position::Cell, Local::V) => Position::CellValue,
                (Position::Cell, Local::Is) => Position::CellInline,
                (Position::CellInline, Local::T) => Position::CellInlineText,
                (Position::MergeCells, Local::MergeCell) => Position::MergeCell,
                (Position::TableParts, Local::TablePart) => Position::TablePart,
                _ => Position::Other { depth },
            }
        } else {
            Position::Other { depth }
        };
        if matches!(next, Position::Other { .. }) {
            self.restore = self.position;
        }
        self.position = next;
    }
    /// Expand the ordinary modal spine only when a cold/rich event needs
    /// the original namespace path. Hot events carry no Element stack entry.
    fn materialize<'a>(
        &self,
        path: &mut Vec<xml::Element<'a>>,
        depth: usize,
    ) -> Result<(), IoError> {
        while path.len() < depth {
            let local = match path.len() {
                2 => "row",
                3 => "c",
                4 => match self.position {
                    Position::CellFormula => "f",
                    Position::CellValue => "v",
                    _ => "is",
                },
                5 => "t",
                _ => {
                    return Err(unsupported(
                        "unavailable worksheet namespace spine",
                        "worksheet",
                    ));
                }
            };
            path.push(xml::Element {
                ns: xml::Ns::Main,
                local,
                qualified: local,
            });
        }
        Ok(())
    }
    fn direct(&self, depth: usize) -> bool {
        matches!(
            self.position,
            Position::CellFormula | Position::CellValue | Position::CellInline
        ) || (depth == 5
            && matches!(self.position, Position::Other { depth: 5 })
            && self.restore == Position::Cell)
    }
    fn close(&mut self, depth: usize) {
        self.position = match self.position {
            Position::Other { depth: outer } if outer == depth => self.restore,
            Position::Other { .. } => return,
            Position::Worksheet => Position::Root,
            Position::SheetData => Position::Worksheet,
            Position::Row => Position::SheetData,
            Position::Cell => Position::Row,
            Position::CellFormula | Position::CellValue | Position::CellInline => Position::Cell,
            Position::CellInlineText => Position::CellInline,
            Position::MergeCell => Position::MergeCells,
            Position::TablePart => Position::TableParts,
            Position::Dimension
            | Position::SheetFormatPr
            | Position::Cols
            | Position::AutoFilter
            | Position::MergeCells
            | Position::TableParts
            | Position::ExtLst => Position::Worksheet,
            Position::Root => Position::Root,
        };
    }
}

/// Canonical coordinates in one pass. Uncommon accepted spellings still
/// go through the unchanged `coord` fallback and retain their source spelling.
pub(super) fn plain_coord(value: &str) -> Option<(u32, u32)> {
    let (mut row, mut col) = (0u32, 0u32);
    let (mut letters, mut digits) = (0u8, 0u8);
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' if digits == 0 && letters < 3 => {
                letters += 1;
                col = col * 26 + u32::from(b - b'A') + 1;
            }
            b'0'..=b'9' if letters != 0 && digits < 7 => {
                if digits == 0 && b == b'0' {
                    return None;
                }
                digits += 1;
                row = row * 10 + u32::from(b - b'0');
            }
            _ => return None,
        }
    }
    (digits != 0 && row <= 1_048_576 && col <= 16_384).then_some((row, col))
}

pub(in crate::cache_recalculate) fn scan(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: Mode,
    observed: &mut usize,
    logical_cells: &mut u64,
) -> Result<Scanned, IoError> {
    scan_emitting(bytes, options, mode, observed, logical_cells, |_| Ok(()))
}

pub(in crate::cache_recalculate) fn scan_emitting(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: Mode,
    observed: &mut usize,
    logical_cells: &mut u64,
    mut emit: impl FnMut(emission::Literal<'_>) -> Result<(), IoError>,
) -> Result<Scanned, IoError> {
    let mut needs_index = false;
    match scan_inner::<false>(
        bytes,
        options,
        mode,
        (observed, logical_cells),
        &mut needs_index,
        &mut emit,
        &mut (0, 0),
    ) {
        Err(_) if needs_index => Ok(Scanned::NeedsIndex),
        result => result.map(Scanned::Done),
    }
}

fn scan_inner<const COUNT_PATHS: bool>(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: Mode,
    counts: (&mut usize, &mut u64),
    needs_index: &mut bool,
    emit: &mut impl FnMut(emission::Literal<'_>) -> Result<(), IoError>,
    path_counts: &mut (usize, usize),
) -> Result<Scan, IoError> {
    let (observed, logical_cells) = counts;
    let mut hidden_rows = Vec::new();
    let mut rows_hidden_by_default = false;
    let mut outline_rows = Vec::new();
    let mut outline_collapsed = false;
    let mut active_filters = Vec::new();
    let mut filter_seen = false;
    let mut filter_ref: Option<SourceRect> = None;
    let mut filter_active = false;
    let mut table_merges = Vec::new();
    let mut table_ids = Vec::new();
    let mut table_count = None;
    let mut serialized = 0usize;
    let mut cells = Vec::new();
    let mut current: Option<ScalarCell<'_>> = None;
    let mut formula = FormulaState::default();
    let mut current_style = None;
    let mut literal_text = Cow::Borrowed("");
    let mut inline_text = emission::DisplayString::default();
    let mut row = 0;
    let mut column = 0;
    let mut sheet_data_count = 0;
    let mut dimension = None;
    let mut max_row = 0u32;
    let mut max_col = 0u32;
    let mut index = (mode == Mode::Indexed).then(|| SourceIndex {
        dimension: None,
        sheet_data: 0..0,
        sheet_data_open_end: 0,
        sheet_data_empty: false,
        rows: Vec::new(),
        cells: Vec::new(),
        merges: Vec::new(),
    });
    let mut extent_cells = 0u64;
    let mut families = shared::Families::default();
    let mut cursor = Cursor::new();
    use crate::xlsx_cache_options::checkpoint;
    use crate::xlsx_xml::{
        Element, Kind, Node, Ns, Scopes, attribute, backend, bind, cdata_end, content, local_part,
        offset, plain_text, qualified_name, valid_text,
    };
    use quick_xml::{
        Reader,
        events::{
            Event,
            attributes::{AttrError, Attribute as RawAttribute, Attributes},
        },
        name::QName,
    };
    let text = std::str::from_utf8(bytes).map_err(|_| unsupported("non-UTF-8 XML", "XLSX XML"))?;
    valid_text(text)?;
    let mut reader = Reader::from_str(text);
    reader.config_mut().check_end_names = true;
    reader.config_mut().check_comments = true;
    let mut scopes = Scopes::new();
    let mut parsed = Vec::new();
    let mut attributes = Vec::new();
    let mut hot = crate::xlsx_xml::worksheet::attributes::HotAttributes::default();
    let mut path = Vec::new();
    let mut depth = 0usize;
    let mut roots = 0;
    let mut events = 0u64;
    let mut declaration = false;
    let mut pending_pop = false;
    let mut pending_path_pop = false;
    let mut scoped_depths = Vec::new();
    let mut sheet_data_generation = None;
    let mut sheet_data_depth = None;
    // Text goes straight from the borrowed quick-xml event to cell state.
    // Only the uncommon rich-inline adapter needs a generic XML node.
    macro_rules! accept_text {
        ($value:expr, $start:expr, $end:expr) => {{
            let value = $value;
            if current
                .as_ref()
                .is_some_and(|c| c.inline.is_some() && !c.has_formula)
            {
                cursor.materialize(&mut path, depth)?;
                let node = Node::new(Kind::Text(value.clone()), $start..$end);
                inline_text.event(&path, &node, bytes)?;
            }
            if let Some(cell) = current.as_mut() {
                match cursor.position {
                    Position::CellFormula => append(&mut formula.text, value),
                    Position::CellValue => {
                        if !cell.has_formula {
                            let raw = text.get($start..$end).ok_or_else(|| {
                                unsupported("unavailable XML text span", "worksheet")
                            })?;
                            let literal = if raw.starts_with('&') {
                                value.clone()
                            } else if !raw.contains('\r') {
                                Cow::Borrowed(raw)
                            } else {
                                Cow::Owned(raw.replace("\r\n", "\n").replace('\r', "\n"))
                            };
                            append(&mut literal_text, literal);
                        }
                        append(&mut cell.value.as_mut().expect("opened v").text, value);
                    }
                    _ => {}
                }
            }
        }};
    }
    loop {
        if events & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        events += 1;
        if pending_path_pop {
            if path.len() == depth {
                path.pop();
            }
            depth -= 1;
            pending_path_pop = false;
        }
        if pending_pop {
            scopes.pop();
            pending_pop = false;
        }
        let decoder = reader.decoder();
        let start = reader.buffer_position() as usize;
        let event = reader.read_event().map_err(backend)?;
        let end = reader.buffer_position() as usize;
        let span = start..end;
        let (boundary, attrs, element) = match event {
            Event::Start(ref e) | Event::Empty(ref e) => {
                let empty = matches!(event, Event::Empty(_));
                let (_, content) = content(text, e)?;
                let name_len = e.name().as_ref().len();
                let qualified = &content[..name_len];
                let fast = sheet_data_depth.is_some()
                    && matches!(
                        cursor.position,
                        Position::SheetData
                            | Position::Row
                            | Position::Cell
                            | Position::CellFormula
                            | Position::CellValue
                            | Position::CellInline
                    )
                    && sheet_data_generation == Some(scopes.generation)
                    && matches!(qualified, "row" | "c" | "f" | "v" | "is" | "t")
                    && hot.parse(content, qualified, start);
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
                    bind(&mut scopes, content, name_len, &mut parsed)?
                };
                pending_pop = empty && !fast;
                if depth >= options.limits.max_xml_depth {
                    return Err(unsupported("XML depth limit", "XLSX XML"));
                }
                if depth == 0 {
                    roots += 1;
                    if roots != 1 {
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
                        scopes.element(qualified)?
                    },
                    local: local_part(qualified),
                    qualified,
                };
                attributes.clear();
                let mut seen = None;
                if !fast {
                    if malformed {
                        for a in Attributes::new(content, name_len) {
                            attribute(
                                a.map_err(backend)?,
                                content,
                                start,
                                decoder,
                                &mut scopes,
                                &mut seen,
                                &mut attributes,
                            )?;
                        }
                    } else {
                        for (i, p) in parsed.iter().enumerate() {
                            if let Some(prev) = parsed[..i].iter().find(|q| q.key == p.key) {
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
                                &mut scopes,
                                &mut seen,
                                &mut attributes,
                            )?;
                        }
                    }
                }
                // Re-establish a default-main witness only after full cold
                // binding, name resolution and attribute validation of this tag.
                if !fast && !qualified.contains(':') && element.ns.is_main() {
                    sheet_data_generation = Some(scopes.generation);
                }
                if !fast {
                    cursor.materialize(&mut path, depth)?;
                    path.push(element.clone());
                }
                depth += 1;
                if !empty && !fast {
                    scoped_depths.push(depth);
                }
                if depth == 2
                    && path[0].ns.is_main()
                    && path[0].local == "worksheet"
                    && path[1].ns.is_main()
                    && path[1].local == "sheetData"
                    && !empty
                {
                    sheet_data_depth = Some(depth);
                }
                pending_path_pop = empty;
                (
                    Boundary::Open { empty },
                    if fast {
                        TagAttributes::Hot(&hot)
                    } else {
                        TagAttributes::Cold(&attributes)
                    },
                    element,
                )
            }
            Event::End(e) => {
                if sheet_data_depth == Some(depth) {
                    sheet_data_depth = None;
                }
                pending_pop = scoped_depths.last().copied() == Some(depth);
                if pending_pop {
                    scoped_depths.pop();
                }
                if depth == 0 {
                    return Err(unsupported("unbalanced XML", "XLSX XML"));
                }
                let element = if path.len() == depth {
                    path.last()
                        .ok_or_else(|| unsupported("unbalanced XML", "XLSX XML"))?
                        .clone()
                } else {
                    let name = e.name();
                    let raw = name.as_ref();
                    let at = offset(bytes, raw)?;
                    let qualified = text
                        .get(at..at + raw.len())
                        .ok_or_else(|| unsupported("unavailable XML name span", "XLSX XML"))?;
                    Element {
                        ns: Ns::Main,
                        local: qualified,
                        qualified,
                    }
                };
                pending_path_pop = true;
                (Boundary::Close, TagAttributes::Cold(&[]), element)
            }
            Event::Text(t) => {
                let value = match plain_text(text, &t) {
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
                if path.is_empty() && !value.trim().is_empty() {
                    return Err(unsupported("text outside XML root", "XLSX XML"));
                }
                accept_text!(value, start, end);
                continue;
            }
            Event::CData(_) => {
                return Err(unsupported(
                    "CDATA is not supported by the ingestion view",
                    "XLSX XML",
                ));
            }
            Event::GeneralRef(reference) => {
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
                if path.is_empty() {
                    return Err(unsupported("entity outside XML root", "XLSX XML"));
                }
                valid_text(&value)?;
                accept_text!(value, start, end);
                continue;
            }
            Event::DocType(_) => return Err(unsupported("DTD/entity declarations", "XLSX XML")),
            Event::Decl(d) => {
                if roots != 0 || declaration {
                    return Err(unsupported("misplaced XML declaration", "XLSX XML"));
                }
                declaration = true;
                if d.version().map_err(backend)?.as_ref() != b"1.0" {
                    return Err(unsupported("unsupported XML version", "XLSX XML"));
                }
                if let Some(encoding) = d.encoding() {
                    let encoding = encoding.map_err(backend)?;
                    if !(encoding.eq_ignore_ascii_case(b"utf-8")
                        || encoding.eq_ignore_ascii_case(b"us-ascii") && bytes.is_ascii())
                    {
                        return Err(unsupported("unsupported XML encoding", "XLSX XML"));
                    }
                }
                continue;
            }
            Event::Eof => {
                if !path.is_empty() || roots != 1 {
                    return Err(unsupported("incomplete XML document", "XLSX XML"));
                }
                break;
            }
            Event::Comment(_) | Event::PI(_) => continue,
        };
        if current
            .as_ref()
            .is_some_and(|c| c.inline.is_some() && !c.has_formula)
        {
            if path.len() < depth {
                cursor.materialize(&mut path, depth - 1)?;
                path.push(element.clone());
            }
            let kind = match boundary {
                Boundary::Open { empty } => Kind::Open {
                    empty,
                    attributes: attrs.generic(),
                },
                Boundary::Close => Kind::Close,
            };
            inline_text.event(&path, &Node::new(kind, span.clone()), bytes)?;
        }
        let tag = Tag::of(&element);
        if matches!(boundary, Boundary::Open { .. }) {
            cursor.open(tag, element.local, depth);
        }
        let closes = matches!(boundary, Boundary::Open { empty: true } | Boundary::Close);
        'rules: {
            let position = cursor.position;
            let is_cell = position == Position::Cell;
            let direct = cursor.direct(depth);
            match &boundary {
                Boundary::Open { empty } => {
                    if (position == Position::SheetFormatPr)
                        && matches!(attrs.value("zeroHeight"), Some("1" | "true"))
                    {
                        rows_hidden_by_default = true;
                    }
                    if position == Position::AutoFilter {
                        if filter_seen {
                            return Err(unsupported("duplicate worksheet autoFilter", "worksheet"));
                        }
                        filter_seen = true;
                        filter_ref = Some(SourceRect::parse(attrs.required("ref")?)?);
                    }
                    if path.len() == 4
                        && path[1].ns.is_main()
                        && path[1].local == "autoFilter"
                        && path[2].local == "filterColumn"
                    {
                        filter_active = true;
                    }
                    if depth == 1 && position != Position::Worksheet {
                        return Err(unsupported("worksheet root/namespace", "worksheet"));
                    }
                    // Structural names (and merge metadata) outside the main
                    // namespace.
                    if !tag.main
                        && matches!(
                            tag.local,
                            Local::Worksheet
                                | Local::SheetData
                                | Local::Row
                                | Local::C
                                | Local::F
                                | Local::V
                                | Local::Is
                                | Local::T
                                | Local::R
                                | Local::RPr
                                | Local::Dimension
                                | Local::MergeCells
                                | Local::MergeCell
                        )
                    {
                        if inert_extension(&path) {
                            break 'rules;
                        }
                        return Err(unsupported("foreign worksheet lookalike", element.local));
                    }
                    if matches!(tag.local, Local::F | Local::V | Local::Is) && !direct {
                        return Err(unsupported("misplaced cell payload", "worksheet"));
                    }
                    if matches!(tag.local, Local::TableParts | Local::TablePart) {
                        let valid = if tag.local == Local::TableParts {
                            position == Position::TableParts
                        } else {
                            position == Position::TablePart
                        };
                        if !valid {
                            return Err(unsupported(
                                "misplaced/foreign table metadata",
                                "worksheet",
                            ));
                        }
                        if tag.local == Local::TableParts {
                            if table_count.is_some() {
                                return Err(unsupported("duplicate tableParts", "worksheet"));
                            }
                            let count =
                                attrs.required("count")?.parse::<usize>().map_err(|_| {
                                    unsupported("invalid tableParts count", "worksheet")
                                })?;
                            if count > options.limits.max_entries.min(options.limits.max_cells) {
                                return Err(unsupported("table count limit", "worksheet"));
                            }
                            table_count = Some(count);
                        } else {
                            if table_ids.len() >= table_count.unwrap_or(0) {
                                return Err(unsupported(
                                    "tablePart count disagreement",
                                    "worksheet",
                                ));
                            }
                            table_ids.push(
                                attrs
                                    .attribute(xml::OFFICE, "id")
                                    .ok_or_else(|| {
                                        unsupported("missing table relationship ID", "worksheet")
                                    })?
                                    .value
                                    .clone()
                                    .into_owned(),
                            );
                        }
                    }
                    if tag.local == Local::Dimension {
                        if position != Position::Dimension
                            || dimension.is_some()
                            || sheet_data_count != 0
                        {
                            return Err(unsupported("duplicate/misplaced dimension", "worksheet"));
                        }
                        let range = rect(attrs.required("ref")?)?;
                        let area = u64::from(range.2) * u64::from(range.3);
                        if range.3 > options.limits.max_columns {
                            return Err(unsupported("worksheet width limit", "worksheet"));
                        }
                        if area > options.limits.max_cells as u64 {
                            return Err(unsupported("worksheet dimension cell limit", "worksheet"));
                        }
                        dimension = Some(range);
                        if let Some(index) = index.as_mut() {
                            let span = attrs
                                .attribute("", "ref")
                                .expect("required ref")
                                .span
                                .clone();
                            index.dimension = Some((
                                SourceRect {
                                    first_row: range.0,
                                    first_col: range.1,
                                    last_row: range.2,
                                    last_col: range.3,
                                },
                                span,
                            ));
                        }
                    }
                    if tag.local == Local::SheetData {
                        if position != Position::SheetData {
                            return Err(unsupported("misplaced sheetData", "worksheet"));
                        }
                        sheet_data_count += 1;
                        if sheet_data_count != 1 {
                            return Err(unsupported("duplicate sheetData", "worksheet"));
                        }
                        if let Some(index) = index.as_mut() {
                            index.sheet_data = span.clone();
                            index.sheet_data_open_end = span.end;
                            index.sheet_data_empty = *empty;
                        }
                    }
                    if tag.local == Local::MergeCell {
                        if position != Position::MergeCell {
                            return Err(unsupported("misplaced merge cell", "worksheet"));
                        }
                        let merge = SourceRect::parse(attrs.required("ref")?)?;
                        if mode == Mode::TablePlain {
                            if table_merges.len() >= options.limits.max_cells {
                                return Err(unsupported("merge count limit", "worksheet"));
                            }
                            table_merges.push(merge);
                        }
                        if let Some(index) = index.as_mut() {
                            if index.merges.len() >= options.limits.max_cells {
                                return Err(unsupported("merge count limit", "worksheet"));
                            }
                            index.merges.push(merge);
                        }
                    }
                    if tag.local == Local::Row {
                        if position != Position::Row {
                            return Err(unsupported("misplaced row", "worksheet"));
                        }
                        let next_row = integer(attrs.required("r")?)?;
                        if next_row <= row || next_row > 1_048_576 {
                            return Err(unsupported(
                                "invalid/non-increasing worksheet row",
                                "worksheet",
                            ));
                        }
                        // Excel shows a zero-height row as hidden even without
                        // the `hidden` flag.
                        let zero_height = attrs
                            .value("ht")
                            .is_some_and(|h| h.trim().parse::<f64>().map_or(true, |h| h <= 0.0));
                        if matches!(attrs.value("hidden"), Some("1" | "true")) || zero_height {
                            if hidden_rows.len() >= options.limits.max_cells {
                                return Err(unsupported("hidden row count limit", "worksheet"));
                            }
                            hidden_rows.push(next_row);
                        } else if !matches!(attrs.value("hidden"), None | Some("0" | "false")) {
                            return Err(unsupported("invalid row hidden flag", "worksheet"));
                        }
                        if attrs.value("outlineLevel").is_some_and(|l| l.trim() != "0") {
                            if outline_rows.len() >= options.limits.max_cells {
                                return Err(unsupported("outline row count limit", "worksheet"));
                            }
                            outline_rows.push(next_row);
                        }
                        if !matches!(attrs.value("collapsed"), None | Some("0" | "false")) {
                            outline_collapsed = true;
                        }
                        row = next_row;
                        column = 0;
                        if let Some(index) = index.as_mut() {
                            let at = index.cells.len();
                            index.rows.push(RowEntry {
                                row,
                                span: span.clone(),
                                open_end: span.end,
                                empty: *empty,
                                qualified: element.qualified.to_owned(),
                                spans_attr: attrs.attribute("", "spans").map(|a| a.span.clone()),
                                cells: at..at,
                            });
                        }
                    }
                    if tag.local == Local::C {
                        if !is_cell || current.is_some() {
                            return Err(unsupported("misplaced/nested cell", "worksheet"));
                        }
                        let address = &attrs.required_attribute("r")?.value;
                        let canonical = plain_coord(address);
                        let (r, c) = canonical.map_or_else(|| coord(address), Ok)?;
                        if c > options.limits.max_columns {
                            return Err(unsupported("worksheet width limit", "worksheet"));
                        }
                        max_row = max_row.max(r);
                        max_col = max_col.max(c);
                        if row != r || c <= column {
                            return Err(unsupported(
                                "duplicate/non-increasing cell coordinate",
                                "worksheet",
                            ));
                        }
                        column = c;
                        if let Some((r1, c1, r2, c2)) = dimension
                            && (!(r1..=r2).contains(&r) || !(c1..=c2).contains(&c))
                        {
                            return Err(unsupported(
                                "cell outside worksheet dimension",
                                "worksheet",
                            ));
                        }
                        *observed = observed
                            .checked_add(1)
                            .ok_or_else(|| unsupported("cell count overflow", "worksheet"))?;
                        if *observed > options.limits.max_cells {
                            return Err(unsupported("serialized cell count limit", "worksheet"));
                        }
                        if attrs.value("vm").is_some() {
                            return Err(unsupported("rich value metadata (vm)", "worksheet"));
                        }
                        let cm = attrs
                            .value("cm")
                            .map(|value| {
                                value.parse::<u32>().ok().filter(|n| *n > 0).ok_or_else(|| {
                                    unsupported(
                                        "dangling or invalid dynamic cell metadata index (cm)",
                                        "worksheet",
                                    )
                                })
                            })
                            .transpose()?;
                        if cm.is_some() && index.is_none() {
                            *needs_index = true;
                            return Err(unsupported(NEEDS_INDEX, "worksheet"));
                        }
                        serialized += 1;
                        let kind_attribute = attrs.attribute("", "t");
                        let kind = kind_attribute.as_ref().map(|a| &a.value);
                        if !matches!(
                            kind.map(|k| &**k),
                            None | Some("n" | "b" | "e" | "str" | "s" | "inlineStr" | "d")
                        ) {
                            return Err(unsupported("unknown cell type", "worksheet"));
                        }
                        let kind_span = kind_attribute.as_ref().map(|a| a.span.clone());
                        current_style = attrs.attribute("", "s").map(|a| a.value.clone());
                        literal_text = Cow::Borrowed("");
                        inline_text = emission::DisplayString::default();
                        if !*empty {
                            current = Some(ScalarCell {
                                row: r,
                                col: c,
                                span: span.clone(),
                                open_end: span.end,
                                qualified: element.qualified,
                                kind: kind.and_then(|k| CellKind::parse(k)),
                                kind_span,
                                value: None,
                                inline: None,
                                has_formula: false,
                                dynamic: (cm.is_some() || canonical.is_none()).then(|| {
                                    Box::new(DynamicAttributes {
                                        original_address: canonical
                                            .is_none()
                                            .then(|| address.to_string().into_boxed_str()),
                                        cm,
                                        cm_span: attrs.attribute("", "cm").map(|a| a.span.clone()),
                                        ..Default::default()
                                    })
                                }),
                            });
                        } else if let Some(index) = index.as_mut() {
                            if cm.is_some() {
                                return Err(unsupported(
                                    "cell metadata on a non-dynamic-array cell",
                                    "worksheet",
                                ));
                            }
                            index.cells.push(IndexedCell {
                                row: r,
                                col: c,
                                span: span.clone(),
                                open_end: span.end,
                                empty: true,
                                kind_span,
                                kind: kind.map(|k| k.to_string()),
                                cm: None,
                                value: None,
                                inline: None,
                                formula: None,
                                literal_refusal: None,
                            });
                            index.rows.last_mut().expect("open row").cells.end = index.cells.len();
                        }
                        if *empty {
                            emit(emission::Literal {
                                row: r,
                                col: c,
                                style: current_style.take(),
                                value: emission::Value::Empty,
                            })?;
                        }
                    }
                    if direct {
                        let cell = current
                            .as_mut()
                            .ok_or_else(|| unsupported("payload without cell", "worksheet"))?;
                        match tag.local {
                            Local::F => {
                                if cell.has_formula || cell.value.is_some() || cell.inline.is_some()
                                {
                                    return Err(unsupported(
                                        "duplicate/misordered formula",
                                        "worksheet",
                                    ));
                                }
                                cell.has_formula = true;
                                formula = FormulaState::default();
                                let kind_attribute = attrs.attribute("", "t");
                                formula.kind = match kind_attribute
                                    .as_ref()
                                    .map_or("normal", |a| &*a.value)
                                {
                                    "normal" => "normal",
                                    "shared" => "shared",
                                    "array" => "array",
                                    "dataTable" => {
                                        return Err(unsupported("data-table formula", "worksheet"));
                                    }
                                    _ => {
                                        return Err(unsupported(
                                            "unknown formula kind",
                                            "worksheet",
                                        ));
                                    }
                                };
                                formula.open = span.clone();
                                formula.kind_span = kind_attribute.map(|a| a.span.clone());
                                let extent = attrs.attribute("", "ref");
                                if formula.kind == "array" {
                                    if index.is_none() {
                                        *needs_index = true;
                                        return Err(unsupported(NEEDS_INDEX, "worksheet"));
                                    }
                                    if let Some(extent) = extent.as_ref() {
                                        let rect = SourceRect::parse(&extent.value)?;
                                        // Validate declared geometry before any
                                        // ownership index or child enumeration.
                                        if rect.last_col > options.limits.max_columns {
                                            return Err(unsupported(
                                                "dynamic array extent width limit",
                                                "worksheet",
                                            ));
                                        }
                                        extent_cells = rect
                                            .cell_count()
                                            .and_then(|n| extent_cells.checked_add(n))
                                            .filter(|n| *n <= options.limits.max_cells as u64)
                                            .ok_or_else(|| {
                                                unsupported(
                                                    "dynamic array extent cell limit",
                                                    "worksheet",
                                                )
                                            })?;
                                        max_row = max_row.max(rect.last_row);
                                        max_col = max_col.max(rect.last_col);
                                        let dynamic = cell.dynamic.get_or_insert_default();
                                        dynamic.array_ref = Some(rect);
                                        dynamic.array_ref_span = Some(extent.span.clone());
                                    }
                                } else if extent.is_some() && formula.kind != "shared" {
                                    return Err(unsupported(
                                        "non-shared formula extent",
                                        "worksheet",
                                    ));
                                }
                                if formula.kind == "shared" {
                                    formula.shared_id = Some(integer(attrs.required("si")?)?);
                                    formula.shared_range =
                                        extent.map(|a| rect(&a.value)).transpose()?;
                                }
                                if *empty {
                                    formula.end = span.end;
                                }
                            }
                            Local::V => {
                                if cell.value.is_some() || cell.inline.is_some() {
                                    return Err(unsupported(
                                        "duplicate/ambiguous cell cache",
                                        "worksheet",
                                    ));
                                }
                                cell.value = Some(OpenValue {
                                    span: span.clone(),
                                    open_end: span.end,
                                    close_start: span.end,
                                    empty: *empty,
                                    qualified: element.qualified,
                                    text: Cow::Borrowed(""),
                                });
                            }
                            Local::Is => {
                                if cell.inline.is_some() || cell.value.is_some() {
                                    return Err(unsupported(
                                        "duplicate/ambiguous inline cache",
                                        "worksheet",
                                    ));
                                }
                                cell.inline = Some(span.clone());
                            }
                            _ => {}
                        }
                    }
                    if depth > 5
                        && matches!(cursor.restore, Position::CellFormula | Position::CellValue)
                    {
                        return Err(unsupported("nested formula/cache content", "worksheet"));
                    }
                }
                Boundary::Close => {
                    if (position == Position::AutoFilter) && filter_active {
                        let mut rect = filter_ref.take().expect("validated autoFilter");
                        rect.first_row += 1;
                        if rect.first_row <= rect.last_row {
                            active_filters.push(rect);
                        }
                    }
                    if direct {
                        let cell = current
                            .as_mut()
                            .ok_or_else(|| unsupported("unbalanced cell payload", "worksheet"))?;
                        match tag.local {
                            Local::F => formula.end = span.end,
                            Local::V => {
                                let v = cell.value.as_mut().expect("opened v");
                                v.close_start = span.start;
                                v.span.end = span.end;
                            }
                            Local::Is => cell.inline.as_mut().expect("opened is").end = span.end,
                            _ => {}
                        }
                    }
                    if let Some(index) = index.as_mut() {
                        if position == Position::Row {
                            let entry = index.rows.last_mut().expect("open row");
                            entry.span.end = span.end;
                        }
                        if position == Position::SheetData {
                            index.sheet_data.end = span.end;
                        }
                    }
                    if is_cell {
                        let cell = current
                            .as_mut()
                            .ok_or_else(|| unsupported("unbalanced cell", "worksheet"))?;
                        cell.span.end = span.end;
                        let emitted_value = (!cell.has_formula).then(|| {
                            cell.decode(
                                std::mem::take(&mut literal_text),
                                std::mem::take(&mut inline_text),
                            )
                        });
                        let refusal = emitted_value
                            .as_ref()
                            .and_then(|value| cell.refusal(bytes, value));
                        cell.dynamic_anchor_extent(&formula)?;
                        if let Some(index) = index.as_mut() {
                            // Ownership decides later whether a refused literal
                            // is a masked generated child or a refused input.
                            index.cells.push(IndexedCell {
                                row: cell.row,
                                col: cell.col,
                                span: cell.span.clone(),
                                open_end: cell.open_end,
                                empty: false,
                                kind: cell.kind.as_deref().map(str::to_owned),
                                kind_span: cell.kind_span.clone(),
                                cm: cell.cm(),
                                value: cell.value.as_ref().map(|v| v.span.clone()),
                                inline: cell.inline.clone(),
                                formula: cell.has_formula.then_some(cells.len()),
                                literal_refusal: refusal,
                            });
                            index.rows.last_mut().expect("open row").cells.end = index.cells.len();
                        } else if let Some(refusal) = refusal {
                            return Err(unsupported(refusal, "worksheet"));
                        }
                        if let Some(value) = emitted_value {
                            emit(emission::Literal {
                                row: cell.row,
                                col: cell.col,
                                style: current_style.take(),
                                value,
                            })?;
                            current = None;
                        } else {
                            if formula.kind == "normal" && formula.text.trim().is_empty() {
                                return Err(unsupported("empty ordinary formula", "worksheet"));
                            }
                            if formula.end == 0 {
                                return Err(unsupported("missing formula boundary", "worksheet"));
                            }
                            cells.push(
                                current
                                    .take()
                                    .expect("open cell")
                                    .retain(std::mem::take(&mut formula), bytes),
                            );
                            if cells.len() > options.limits.max_formula_cells {
                                return Err(unsupported("formula cell count limit", "worksheet"));
                            }
                            if let Some(cell) = cells.last() {
                                families.observe(cell, cells.len() - 1, options);
                            }
                        }
                    }
                }
            }
        }
        if closes {
            cursor.close(depth);
        }
    }
    if sheet_data_count != 1 {
        return Err(unsupported("missing sheetData", "worksheet"));
    }
    if let Some((_, _, r, c)) = dimension {
        max_row = max_row.max(r);
        max_col = max_col.max(c);
    }
    *logical_cells = logical_cells
        .checked_add(u64::from(max_row) * u64::from(max_col))
        .ok_or_else(|| unsupported("logical area overflow", "workbook"))?;
    if *logical_cells > options.limits.max_cells as u64 {
        return Err(unsupported("workbook logical cell limit", "workbook"));
    }
    families.finish()?;
    if table_count.is_some_and(|n| n != table_ids.len()) {
        return Err(unsupported("tablePart count disagreement", "worksheet"));
    }
    // A collapsed outline may hide grouped rows that carry no `hidden` flag:
    // treat every grouped row as hidden.
    if outline_collapsed && !outline_rows.is_empty() {
        hidden_rows.extend(outline_rows);
        hidden_rows.sort_unstable();
        hidden_rows.dedup();
    }
    Ok(Scan {
        active_filters,
        hidden_rows,
        rows_hidden_by_default,
        has_dimension: dimension.is_some(),
        table_merges,
        table_ids,
        cells,
        index,
        serialized,
        bounds: (max_row, max_col),
    })
}
