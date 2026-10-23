//! Independent flat worksheet rule loop; the visitor scanner remains the oracle.
use super::*;
pub(in crate::cache_recalculate) mod emission;
mod shared;

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
    match scan_inner(
        bytes,
        options,
        mode,
        (observed, logical_cells),
        &mut needs_index,
        &mut emit,
    ) {
        Err(_) if needs_index => Ok(Scanned::NeedsIndex),
        result => result.map(Scanned::Done),
    }
}

fn scan_inner(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: Mode,
    counts: (&mut usize, &mut u64),
    needs_index: &mut bool,
    emit: &mut impl FnMut(emission::Literal<'_>) -> Result<(), IoError>,
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
    let mut current: Option<OpenCell<'_>> = None;
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
    let mut tokens = crate::xlsx_xml::worksheet::Tokens::new(bytes)?;
    let xml_options = options.into();
    while let Some((path, node)) = tokens.next(&xml_options)? {
        let Some(element) = path.last() else {
            continue;
        };
        if current
            .as_ref()
            .is_some_and(|c| c.inline.is_some() && !c.has_formula)
        {
            inline_text.event(path, &node, bytes)?;
        }
        if cursor.position == Position::CellValue
            && current.as_ref().is_some_and(|c| !c.has_formula)
            && let xml::Kind::Text(decoded) = &node.kind
        {
            append(
                &mut literal_text,
                emission::text(bytes, &node, decoded.clone())?,
            );
        }
        if let xml::Kind::Text(text) = node.kind {
            // Text only matters as the content of a direct `f`/`v` child.
            if let Some(cell) = current.as_mut() {
                match cursor.position {
                    Position::CellFormula => append(&mut cell.formula_text, text),
                    Position::CellValue => {
                        append(&mut cell.value.as_mut().expect("opened v").text, text)
                    }
                    _ => {}
                }
            }
            continue;
        }
        let tag = Tag::of(element);
        if matches!(node.kind, xml::Kind::Open { .. }) {
            cursor.open(tag, element.local, path.len());
        }
        let closes = matches!(
            node.kind,
            xml::Kind::Open { empty: true, .. } | xml::Kind::Close
        );
        'rules: {
            let position = cursor.position;
            let is_cell = position == Position::Cell;
            let direct = cursor.direct(path.len());
            match &node.kind {
                xml::Kind::Open { empty, .. } => {
                    if (position == Position::SheetFormatPr)
                        && matches!(node.value("zeroHeight"), Some("1" | "true"))
                    {
                        rows_hidden_by_default = true;
                    }
                    if position == Position::AutoFilter {
                        if filter_seen {
                            return Err(unsupported("duplicate worksheet autoFilter", "worksheet"));
                        }
                        filter_seen = true;
                        filter_ref = Some(SourceRect::parse(node.required("ref")?)?);
                    }
                    if path.len() == 4
                        && path[1].ns.is_main()
                        && path[1].local == "autoFilter"
                        && path[2].local == "filterColumn"
                    {
                        filter_active = true;
                    }
                    if path.len() == 1 && position != Position::Worksheet {
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
                        if inert_extension(path) {
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
                            let count = node.required("count")?.parse::<usize>().map_err(|_| {
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
                                node.attribute(xml::OFFICE, "id")
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
                        let range = rect(node.required("ref")?)?;
                        let area = u64::from(range.2) * u64::from(range.3);
                        if range.3 > options.limits.max_columns {
                            return Err(unsupported("worksheet width limit", "worksheet"));
                        }
                        if area > options.limits.max_cells as u64 {
                            return Err(unsupported("worksheet dimension cell limit", "worksheet"));
                        }
                        dimension = Some(range);
                        if let Some(index) = index.as_mut() {
                            let span = node
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
                            index.sheet_data = node.span.clone();
                            index.sheet_data_open_end = node.span.end;
                            index.sheet_data_empty = *empty;
                        }
                    }
                    if tag.local == Local::MergeCell {
                        if position != Position::MergeCell {
                            return Err(unsupported("misplaced merge cell", "worksheet"));
                        }
                        let merge = SourceRect::parse(node.required("ref")?)?;
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
                        let next_row = integer(node.required("r")?)?;
                        if next_row <= row || next_row > 1_048_576 {
                            return Err(unsupported(
                                "invalid/non-increasing worksheet row",
                                "worksheet",
                            ));
                        }
                        // Excel shows a zero-height row as hidden even without
                        // the `hidden` flag.
                        let zero_height = node
                            .value("ht")
                            .is_some_and(|h| h.trim().parse::<f64>().map_or(true, |h| h <= 0.0));
                        if matches!(node.value("hidden"), Some("1" | "true")) || zero_height {
                            if hidden_rows.len() >= options.limits.max_cells {
                                return Err(unsupported("hidden row count limit", "worksheet"));
                            }
                            hidden_rows.push(next_row);
                        } else if !matches!(node.value("hidden"), None | Some("0" | "false")) {
                            return Err(unsupported("invalid row hidden flag", "worksheet"));
                        }
                        if node.value("outlineLevel").is_some_and(|l| l.trim() != "0") {
                            if outline_rows.len() >= options.limits.max_cells {
                                return Err(unsupported("outline row count limit", "worksheet"));
                            }
                            outline_rows.push(next_row);
                        }
                        if !matches!(node.value("collapsed"), None | Some("0" | "false")) {
                            outline_collapsed = true;
                        }
                        row = next_row;
                        column = 0;
                        if let Some(index) = index.as_mut() {
                            let at = index.cells.len();
                            index.rows.push(RowEntry {
                                row,
                                span: node.span.clone(),
                                open_end: node.span.end,
                                empty: *empty,
                                qualified: element.qualified.to_owned(),
                                spans_attr: node.attribute("", "spans").map(|a| a.span.clone()),
                                cells: at..at,
                            });
                        }
                    }
                    if tag.local == Local::C {
                        if !is_cell || current.is_some() {
                            return Err(unsupported("misplaced/nested cell", "worksheet"));
                        }
                        let address = &node.required_attribute("r")?.value;
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
                        if node.value("vm").is_some() {
                            return Err(unsupported("rich value metadata (vm)", "worksheet"));
                        }
                        let cm = node
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
                        let kind_attribute = node.attribute("", "t");
                        let kind = kind_attribute.map(|a| &a.value);
                        if !matches!(
                            kind.map(|k| &**k),
                            None | Some("n" | "b" | "e" | "str" | "s" | "inlineStr" | "d")
                        ) {
                            return Err(unsupported("unknown cell type", "worksheet"));
                        }
                        let kind_span = kind_attribute.map(|a| a.span.clone());
                        current_style = node.attribute("", "s").map(|a| a.value.clone());
                        literal_text = Cow::Borrowed("");
                        inline_text = emission::DisplayString::default();
                        if !*empty {
                            current = Some(OpenCell {
                                row: r,
                                col: c,
                                span: node.span.clone(),
                                open_end: node.span.end,
                                qualified: element.qualified,
                                kind: kind.and_then(|k| CellKind::parse(k)),
                                kind_span,
                                formula_end: 0,
                                formula_text: Cow::Borrowed(""),
                                value: None,
                                inline: None,
                                formula_kind: "",
                                shared_id: None,
                                shared_range: None,
                                has_formula: false,
                                formula_open: 0..0,
                                formula_kind_span: None,
                                dynamic: (cm.is_some() || canonical.is_none()).then(|| {
                                    Box::new(DynamicAttributes {
                                        original_address: canonical
                                            .is_none()
                                            .then(|| address.to_string().into_boxed_str()),
                                        cm,
                                        cm_span: node.attribute("", "cm").map(|a| a.span.clone()),
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
                                span: node.span.clone(),
                                open_end: node.span.end,
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
                                let kind_attribute = node.attribute("", "t");
                                cell.formula_kind = match kind_attribute
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
                                cell.formula_open = node.span.clone();
                                cell.formula_kind_span = kind_attribute.map(|a| a.span.clone());
                                let extent = node.attribute("", "ref");
                                if cell.formula_kind == "array" {
                                    if index.is_none() {
                                        *needs_index = true;
                                        return Err(unsupported(NEEDS_INDEX, "worksheet"));
                                    }
                                    if let Some(extent) = extent {
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
                                } else if extent.is_some() && cell.formula_kind != "shared" {
                                    return Err(unsupported(
                                        "non-shared formula extent",
                                        "worksheet",
                                    ));
                                }
                                if cell.formula_kind == "shared" {
                                    cell.shared_id = Some(integer(node.required("si")?)?);
                                    cell.shared_range =
                                        extent.map(|a| rect(&a.value)).transpose()?;
                                }
                                if *empty {
                                    cell.formula_end = node.span.end;
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
                                    span: node.span.clone(),
                                    open_end: node.span.end,
                                    close_start: node.span.end,
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
                                cell.inline = Some(node.span.clone());
                            }
                            _ => {}
                        }
                    }
                    if path.len() > 5
                        && matches!(cursor.restore, Position::CellFormula | Position::CellValue)
                    {
                        return Err(unsupported("nested formula/cache content", "worksheet"));
                    }
                }
                xml::Kind::Text(_) => {}
                xml::Kind::Close => {
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
                            Local::F => cell.formula_end = node.span.end,
                            Local::V => {
                                let v = cell.value.as_mut().expect("opened v");
                                v.close_start = node.span.start;
                                v.span.end = node.span.end;
                            }
                            Local::Is => {
                                cell.inline.as_mut().expect("opened is").end = node.span.end
                            }
                            _ => {}
                        }
                    }
                    if let Some(index) = index.as_mut() {
                        if position == Position::Row {
                            let entry = index.rows.last_mut().expect("open row");
                            entry.span.end = node.span.end;
                        }
                        if position == Position::SheetData {
                            index.sheet_data.end = node.span.end;
                        }
                    }
                    if is_cell {
                        let cell = current
                            .as_mut()
                            .ok_or_else(|| unsupported("unbalanced cell", "worksheet"))?;
                        cell.span.end = node.span.end;
                        let emitted_value = (!cell.has_formula).then(|| {
                            emission::value(
                                cell,
                                std::mem::take(&mut literal_text),
                                std::mem::take(&mut inline_text),
                            )
                        });
                        let refusal = emitted_value
                            .as_ref()
                            .and_then(|value| emission::refusal(cell, bytes, value));
                        cell.dynamic_anchor_extent()?;
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
                            if cell.formula_kind == "normal" && cell.formula_text.trim().is_empty()
                            {
                                return Err(unsupported("empty ordinary formula", "worksheet"));
                            }
                            if cell.formula_end == 0 {
                                return Err(unsupported("missing formula boundary", "worksheet"));
                            }
                            cells.push(current.take().expect("open cell").into_cell(bytes));
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
            cursor.close(path.len());
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
