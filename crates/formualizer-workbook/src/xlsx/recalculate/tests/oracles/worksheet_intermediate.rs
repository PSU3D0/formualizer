//! Retained projected scanner used to compare the flat scanner's transition loop.
//! Its source-mark representation and XML primitives are intentionally shared;
//! the owned-string scanner is the independent end-to-end reference.
//! The test-only worksheet mount preserves private mark-helper access unchanged.
use super::*;

/// [`scan`] without the test-only indexed-scan count.
#[cfg(test)]
pub(in crate::xlsx) fn scan_uncounted(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanStrategy,
    observed: &mut usize,
    logical_cells: &mut u64,
) -> Result<Scanned, IoError> {
    let mut needs_index = false;
    match scan_inner(
        bytes,
        options,
        mode,
        observed,
        logical_cells,
        &mut needs_index,
    ) {
        Err(_) if needs_index => Ok(Scanned::NeedsIndex),
        result => result.map(Scanned::Done),
    }
}
/// [`xml::path_is`] for the main namespace over classified tags.
#[cfg(test)]
fn main_path(tags: &[Tag], names: &[Local]) -> bool {
    tags.len() == names.len() && tags.iter().zip(names).all(|(t, n)| t.main && t.local == *n)
}
#[cfg(test)]
const CELL_PATH: [Local; 4] = [Local::Worksheet, Local::SheetData, Local::Row, Local::C];
#[cfg(test)]
fn scan_inner(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanStrategy,
    observed: &mut usize,
    logical_cells: &mut u64,
    needs_index: &mut bool,
) -> Result<Scan, IoError> {
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
    let mut row = 0;
    let mut column = 0;
    let mut sheet_data_count = 0;
    let mut dimension = None;
    let mut max_row = 0u32;
    let mut max_col = 0u32;
    let mut index = (mode == ScanStrategy::Indexed).then(|| SourceIndex {
        dimension: None,
        sheet_data: 0..0,
        sheet_data_open_end: 0,
        sheet_data_empty: false,
        rows: Vec::new(),
        cells: Vec::new(),
        merges: Vec::new(),
    });
    let mut extent_cells = 0u64;
    // Tags of the open elements, parallel to the walker's path.
    let mut tags: Vec<Tag> = Vec::new();
    xml::walk(bytes, options, |path, node| {
        let Some(element) = path.last() else {
            return Ok(());
        };
        if let xml::Kind::Text(text) = node.kind {
            // Text only matters as the content of a direct `f`/`v` child.
            if tags.len() == 5
                && main_path(&tags[..4], &CELL_PATH)
                && let Some(cell) = current.as_mut()
            {
                match tags[4].local {
                    Local::F => append(&mut cell.formula_text, text),
                    Local::V => append(&mut cell.value.as_mut().expect("opened v").text, text),
                    _ => {}
                }
            }
            return Ok(());
        }
        if matches!(node.kind, xml::Kind::Open { .. }) {
            tags.push(Tag::of(element));
        }
        let closes = matches!(
            node.kind,
            xml::Kind::Open { empty: true, .. } | xml::Kind::Close
        );
        let tags_now = tags.as_slice();
        let result = (|| -> Result<(), IoError> {
            let tags = tags_now;
            let tag = *tags.last().expect("open element tag");
            let is_cell = main_path(tags, &CELL_PATH);
            let direct = tags.len() == 5 && main_path(&tags[..4], &CELL_PATH);
            match &node.kind {
                xml::Kind::Open { empty, .. } => {
                    if main_path(tags, &[Local::Worksheet, Local::SheetFormatPr])
                        && matches!(node.value("zeroHeight"), Some("1" | "true"))
                    {
                        rows_hidden_by_default = true;
                    }
                    if main_path(tags, &[Local::Worksheet, Local::AutoFilter]) {
                        if filter_seen {
                            return Err(unsupported("duplicate worksheet autoFilter", "worksheet"));
                        }
                        filter_seen = true;
                        filter_ref = Some(SourceRect::parse(node.required("ref")?)?);
                    }
                    if tags.len() == 4
                        && tags[1].main
                        && tags[1].local == Local::AutoFilter
                        && tags[2].local == Local::FilterColumn
                    {
                        filter_active = true;
                    }
                    if tags.len() == 1 && !main_path(tags, &[Local::Worksheet]) {
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
                            return Ok(());
                        }
                        return Err(unsupported("foreign worksheet lookalike", element.local));
                    }
                    if matches!(tag.local, Local::F | Local::V | Local::Is) && !direct {
                        return Err(unsupported("misplaced cell payload", "worksheet"));
                    }
                    if matches!(tag.local, Local::TableParts | Local::TablePart) {
                        let valid = if tag.local == Local::TableParts {
                            main_path(tags, &[Local::Worksheet, Local::TableParts])
                        } else {
                            main_path(
                                tags,
                                &[Local::Worksheet, Local::TableParts, Local::TablePart],
                            )
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
                        if !main_path(tags, &[Local::Worksheet, Local::Dimension])
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
                        if !main_path(tags, &[Local::Worksheet, Local::SheetData]) {
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
                        if !main_path(
                            tags,
                            &[Local::Worksheet, Local::MergeCells, Local::MergeCell],
                        ) {
                            return Err(unsupported("misplaced merge cell", "worksheet"));
                        }
                        let merge = SourceRect::parse(node.required("ref")?)?;
                        if mode == ScanStrategy::TablePlain {
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
                        if !main_path(tags, &[Local::Worksheet, Local::SheetData, Local::Row]) {
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
                    if tags.len() > 5 && matches!(tags[4].local, Local::F | Local::V) {
                        return Err(unsupported("nested formula/cache content", "worksheet"));
                    }
                }
                xml::Kind::Text(_) => {}
                xml::Kind::Close => {
                    if main_path(tags, &[Local::Worksheet, Local::AutoFilter]) && filter_active {
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
                        if main_path(tags, &[Local::Worksheet, Local::SheetData, Local::Row]) {
                            let entry = index.rows.last_mut().expect("open row");
                            entry.span.end = node.span.end;
                        }
                        if main_path(tags, &[Local::Worksheet, Local::SheetData]) {
                            index.sheet_data.end = node.span.end;
                        }
                    }
                    if is_cell {
                        let cell = current
                            .as_mut()
                            .ok_or_else(|| unsupported("unbalanced cell", "worksheet"))?;
                        cell.span.end = node.span.end;
                        let refusal = cell.literal_refusal(bytes);
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
                        if !cell.has_formula {
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
                        }
                    }
                }
            }
            Ok(())
        })();
        if closes {
            tags.pop();
        }
        result
    })?;
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
    let mut anchors = HashMap::new();
    for cell in &cells {
        if let Some(id) = cell.shared_id {
            if !cell.formula_text.trim().is_empty() {
                let range = cell
                    .shared_range
                    .ok_or_else(|| unsupported("unbounded shared formula anchor", "worksheet"))?;
                if (cell.row, cell.col) != (range.0, range.1) {
                    return Err(unsupported(
                        "non-top-left shared formula anchor",
                        "worksheet",
                    ));
                }
                let area = u64::from(range.2 - range.0 + 1) * u64::from(range.3 - range.1 + 1);
                if area > options.limits.max_cells as u64 || anchors.insert(id, range).is_some() {
                    return Err(unsupported(
                        "duplicate/oversized shared formula anchor",
                        "worksheet",
                    ));
                }
                if area > 1 {
                    crate::xlsx::recalculate::shared_qualifiers::validate(
                        &cell.formula_text,
                        &cell.address(),
                    )?;
                }
            } else if cell.shared_range.is_some() {
                return Err(unsupported("shared descendant declares range", "worksheet"));
            }
        }
    }
    for cell in &cells {
        if let Some(id) = cell.shared_id {
            let &(r1, c1, r2, c2) = anchors
                .get(&id)
                .ok_or_else(|| unsupported("orphan shared formula descendant", "worksheet"))?;
            if !(r1..=r2).contains(&cell.row) || !(c1..=c2).contains(&cell.col) {
                return Err(unsupported(
                    "shared formula outside declared range",
                    "worksheet",
                ));
            }
        }
    }
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
        raw_dimension_span: None,
        raw_row_refusal_span: None,
        table_merges,
        table_ids,
        cells,
        index,
        serialized,
        bounds: (max_row, max_col),
    })
}
