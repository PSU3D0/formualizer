//! Ordered worksheet admission rules. Tokenization and cursor transitions live
//! in the scanner; this module receives already validated element boundaries.
use super::*;

pub(super) struct RuleContext<'a, 'o, 'c> {
    pub bytes: &'a [u8],
    pub options: &'o ReadOptions,
    pub request: ScanRequest,
    pub observed: &'c mut usize,
    pub needs_index: &'c mut bool,
}

pub(super) struct RuleEvent<'n, 'a> {
    pub attrs: TagAttributes<'n, 'a>,
    pub element: xml::Element<'a>,
    pub path: &'n [xml::Element<'a>],
    pub span: Range<usize>,
    pub tag: Tag,
    pub position: Position,
    pub restore: Position,
    pub depth: usize,
    pub direct: bool,
    pub empty: bool,
}

impl<'a> SheetState<'a> {
    pub fn open(
        &mut self,
        event: &RuleEvent<'_, 'a>,
        context: &mut RuleContext<'a, '_, '_>,
        emit: &mut impl emission::Sink,
    ) -> Result<(), IoError> {
        // Visibility metadata precedes structural admission, preserving malformed filter precedence.
        self.open_visibility(event)?;
        // Refuse wrong roots, foreign lookalikes and misplaced payloads before semantic metadata.
        if !self.open_structure(event)? {
            return Ok(());
        }
        // Refuse ambiguous table declarations before dimensions or cell payloads.
        self.open_tables(event, context)?;
        // Refuse invalid/misplaced dimensions and their resource bounds before sheet data.
        self.open_dimension(event, context)?;
        // Refuse duplicate/misplaced sheetData before rows and cells.
        self.open_sheet_data(event)?;
        // Refuse misplaced merges and bound retained geometry before row admission.
        self.open_merge(event, context)?;
        // Refuse row order and visibility metadata before any enclosed coordinate.
        self.open_row(event, context)?;
        // Refuse coordinate/order/budgets, then metadata/type, before opening a payload.
        self.open_cell(event, context, emit)?;
        // Refuse formula/cache ambiguity before decoding or retaining cell text.
        self.open_payload(event, context)?;
        // Refuse nested formula/cache content only after direct payload admission.
        self.open_nested(event)?;
        Ok(())
    }
    fn open_visibility(&mut self, event: &RuleEvent<'_, 'a>) -> Result<(), IoError> {
        let position = event.position;
        let attrs = &event.attrs;
        let path = event.path;
        if (position == Position::SheetFormatPr)
            && matches!(attrs.value("zeroHeight"), Some("1" | "true"))
        {
            self.rows_hidden_by_default = true;
        }
        if position == Position::AutoFilter {
            if self.filter_seen {
                return Err(unsupported("duplicate worksheet autoFilter", "worksheet"));
            }
            self.filter_seen = true;
            self.filter_ref = Some(SourceRect::parse(attrs.required("ref")?)?);
        }
        if path.len() == 4
            && path[1].ns.is_main()
            && path[1].local == "autoFilter"
            && path[2].local == "filterColumn"
        {
            self.filter_active = true;
        }
        Ok(())
    }

    fn open_structure(&mut self, event: &RuleEvent<'_, 'a>) -> Result<bool, IoError> {
        let position = event.position;
        let tag = event.tag;
        let element = &event.element;
        let depth = event.depth;
        let path = event.path;
        let direct = event.direct;
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
            if inert_extension(path) {
                return Ok(false);
            }
            return Err(unsupported("foreign worksheet lookalike", element.local));
        }
        if matches!(tag.local, Local::F | Local::V | Local::Is) && !direct {
            return Err(unsupported("misplaced cell payload", "worksheet"));
        }
        Ok(true)
    }

    fn open_tables(
        &mut self,
        event: &RuleEvent<'_, 'a>,
        context: &mut RuleContext<'a, '_, '_>,
    ) -> Result<(), IoError> {
        let position = event.position;
        let tag = event.tag;
        let attrs = &event.attrs;
        if matches!(tag.local, Local::TableParts | Local::TablePart) {
            let valid = if tag.local == Local::TableParts {
                position == Position::TableParts
            } else {
                position == Position::TablePart
            };
            if !valid {
                return Err(unsupported("misplaced/foreign table metadata", "worksheet"));
            }
            if tag.local == Local::TableParts {
                if self.table_count.is_some() {
                    return Err(unsupported("duplicate tableParts", "worksheet"));
                }
                let count = attrs
                    .required("count")?
                    .parse::<usize>()
                    .map_err(|_| unsupported("invalid tableParts count", "worksheet"))?;
                if count
                    > context
                        .options
                        .limits
                        .max_entries
                        .min(context.options.limits.max_cells)
                {
                    return Err(unsupported("table count limit", "worksheet"));
                }
                self.table_count = Some(count);
            } else {
                if self.table_ids.len() >= self.table_count.unwrap_or(0) {
                    return Err(unsupported("tablePart count disagreement", "worksheet"));
                }
                self.table_ids.push(
                    attrs
                        .attribute(xml::OFFICE, "id")
                        .ok_or_else(|| unsupported("missing table relationship ID", "worksheet"))?
                        .value
                        .clone()
                        .into_owned(),
                );
            }
        }
        Ok(())
    }

    fn open_dimension(
        &mut self,
        event: &RuleEvent<'_, 'a>,
        context: &mut RuleContext<'a, '_, '_>,
    ) -> Result<(), IoError> {
        let position = event.position;
        let tag = event.tag;
        let attrs = &event.attrs;
        if tag.local == Local::Dimension {
            if position != Position::Dimension
                || self.dimension.is_some()
                || self.sheet_data_count != 0
            {
                return Err(unsupported("duplicate/misplaced dimension", "worksheet"));
            }
            let range = rect(attrs.required("ref")?)?;
            let area = u64::from(range.2) * u64::from(range.3);
            if range.3 > context.options.limits.max_columns {
                return Err(unsupported("worksheet width limit", "worksheet"));
            }
            if area > context.options.limits.max_cells as u64 {
                return Err(unsupported("worksheet dimension cell limit", "worksheet"));
            }
            self.dimension = Some(range);
            self.raw_dimension_span = Some(raw_attribute_span(
                context.bytes,
                &attrs.required_attribute("ref")?.span,
            )?);
            if let Some(index) = self.index.as_mut() {
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
        Ok(())
    }

    fn open_sheet_data(&mut self, event: &RuleEvent<'_, 'a>) -> Result<(), IoError> {
        let position = event.position;
        let tag = event.tag;
        let span = event.span.clone();
        let empty = event.empty;
        if tag.local == Local::SheetData {
            if position != Position::SheetData {
                return Err(unsupported("misplaced sheetData", "worksheet"));
            }
            self.sheet_data_count += 1;
            if self.sheet_data_count != 1 {
                return Err(unsupported("duplicate sheetData", "worksheet"));
            }
            if let Some(index) = self.index.as_mut() {
                index.sheet_data = span.clone();
                index.sheet_data_open_end = span.end;
                index.sheet_data_empty = empty;
            }
        }
        Ok(())
    }

    fn open_merge(
        &mut self,
        event: &RuleEvent<'_, 'a>,
        context: &mut RuleContext<'a, '_, '_>,
    ) -> Result<(), IoError> {
        let position = event.position;
        let tag = event.tag;
        let attrs = &event.attrs;
        if tag.local == Local::MergeCell {
            if position != Position::MergeCell {
                return Err(unsupported("misplaced merge cell", "worksheet"));
            }
            let merge = SourceRect::parse(attrs.required("ref")?)?;
            if context.request.retention.geometry
                && context.request.strategy == ScanStrategy::TablePlain
            {
                if self.table_merges.len() >= context.options.limits.max_cells {
                    return Err(unsupported("merge count limit", "worksheet"));
                }
                self.table_merges.push(merge);
            }
            if let Some(index) = self.index.as_mut() {
                if index.merges.len() >= context.options.limits.max_cells {
                    return Err(unsupported("merge count limit", "worksheet"));
                }
                index.merges.push(merge);
            }
        }
        Ok(())
    }

    fn open_row(
        &mut self,
        event: &RuleEvent<'_, 'a>,
        context: &mut RuleContext<'a, '_, '_>,
    ) -> Result<(), IoError> {
        let position = event.position;
        let tag = event.tag;
        let attrs = &event.attrs;
        let element = &event.element;
        let span = event.span.clone();
        let empty = event.empty;
        if tag.local == Local::Row {
            if position != Position::Row {
                return Err(unsupported("misplaced row", "worksheet"));
            }
            let next_row = integer(attrs.required("r")?)?;
            if self.raw_row_refusal_span.is_none() {
                let raw = raw_attribute_span(context.bytes, &attrs.required_attribute("r")?.span)?;
                if context.bytes[raw.clone()]
                    .iter()
                    .any(|byte| !byte.is_ascii_digit())
                {
                    self.raw_row_refusal_span = Some(raw);
                }
            }
            if next_row <= self.row || next_row > 1_048_576 {
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
                if self.hidden_rows.len() >= context.options.limits.max_cells {
                    return Err(unsupported("hidden row count limit", "worksheet"));
                }
                self.hidden_rows.push(next_row);
            } else if !matches!(attrs.value("hidden"), None | Some("0" | "false")) {
                return Err(unsupported("invalid row hidden flag", "worksheet"));
            }
            if attrs.value("outlineLevel").is_some_and(|l| l.trim() != "0") {
                if self.outline_rows.len() >= context.options.limits.max_cells {
                    return Err(unsupported("outline row count limit", "worksheet"));
                }
                self.outline_rows.push(next_row);
            }
            if !matches!(attrs.value("collapsed"), None | Some("0" | "false")) {
                self.outline_collapsed = true;
            }
            self.row = next_row;
            self.column = 0;
            if let Some(index) = self.index.as_mut() {
                let at = index.cells.len();
                index.rows.push(RowEntry {
                    row: self.row,
                    span: span.clone(),
                    open_end: span.end,
                    empty,
                    qualified: element.qualified.to_owned(),
                    spans_attr: attrs.attribute("", "spans").map(|a| a.span.clone()),
                    cells: at..at,
                });
            }
        }
        Ok(())
    }

    fn open_cell(
        &mut self,
        event: &RuleEvent<'_, 'a>,
        context: &mut RuleContext<'a, '_, '_>,
        emit: &mut impl emission::Sink,
    ) -> Result<(), IoError> {
        let tag = event.tag;
        let attrs = &event.attrs;
        let element = &event.element;
        let span = event.span.clone();
        let empty = event.empty;
        let is_cell = event.position == Position::Cell;
        if tag.local == Local::C {
            if !is_cell || self.open_cell.cell.is_some() {
                return Err(unsupported("misplaced/nested cell", "worksheet"));
            }
            let coordinate_attribute = attrs.required_attribute("r")?;
            self.open_cell.raw_coordinate = Some(raw_attribute_span(
                context.bytes,
                &coordinate_attribute.span,
            )?);
            let address = &coordinate_attribute.value;
            let canonical = plain_coord(address);
            let (r, c) = canonical.map_or_else(|| coord(address), Ok)?;
            if c > context.options.limits.max_columns {
                return Err(unsupported("worksheet width limit", "worksheet"));
            }
            self.max_row = self.max_row.max(r);
            self.max_col = self.max_col.max(c);
            if self.row != r || c <= self.column {
                return Err(unsupported(
                    "duplicate/non-increasing cell coordinate",
                    "worksheet",
                ));
            }
            self.column = c;
            if let Some((r1, c1, r2, c2)) = self.dimension
                && (!(r1..=r2).contains(&r) || !(c1..=c2).contains(&c))
            {
                return Err(unsupported("cell outside worksheet dimension", "worksheet"));
            }
            *context.observed = context
                .observed
                .checked_add(1)
                .ok_or_else(|| unsupported("cell count overflow", "worksheet"))?;
            if *context.observed > context.options.limits.max_cells {
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
            if cm.is_some() && self.index.is_none() {
                *context.needs_index = true;
                return Err(unsupported(NEEDS_INDEX, "worksheet"));
            }
            self.serialized += 1;
            let kind_attribute = attrs.attribute("", "t");
            let kind = kind_attribute.as_ref().map(|a| &a.value);
            if !matches!(
                kind.map(|k| &**k),
                None | Some("n" | "b" | "e" | "str" | "s" | "inlineStr" | "d")
            ) {
                return Err(unsupported("unknown cell type", "worksheet"));
            }
            let kind_span = kind_attribute.as_ref().map(|a| a.span.clone());
            self.open_cell.raw_type = kind_attribute
                .as_ref()
                .map(|a| raw_attribute_span(context.bytes, &a.span))
                .transpose()?;
            let style_attribute = attrs.attribute("", "s");
            self.open_cell.raw_style = style_attribute
                .as_ref()
                .map(|a| raw_attribute_span(context.bytes, &a.span))
                .transpose()?;
            self.open_cell.style = style_attribute.map(|a| a.value);
            self.open_cell.literal_text = Cow::Borrowed("");
            self.open_cell.inline_text = emission::DisplayString::default();
            if !empty {
                self.open_cell.cell = Some(ScalarCell {
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
            } else if let Some(index) = self.index.as_mut() {
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
            if empty && context.request.retention.values {
                emit.literal(emission::Literal {
                    row: r,
                    col: c,
                    generated_child_hint: self.generated_children.contains(r, c),
                    style: self.open_cell.style.take(),
                    raw_coordinate_span: self.open_cell.raw_coordinate.take(),
                    raw_type_span: self.open_cell.raw_type.take(),
                    raw_style_span: self.open_cell.raw_style.take(),
                    raw_value_span: None,
                    value: emission::Value::Empty,
                })?;
            }
        }
        Ok(())
    }

    fn open_payload(
        &mut self,
        event: &RuleEvent<'_, 'a>,
        context: &mut RuleContext<'a, '_, '_>,
    ) -> Result<(), IoError> {
        let tag = event.tag;
        let attrs = &event.attrs;
        let element = &event.element;
        let span = event.span.clone();
        let empty = event.empty;
        let direct = event.direct;
        if direct {
            let cell = self
                .open_cell
                .cell
                .as_mut()
                .ok_or_else(|| unsupported("payload without cell", "worksheet"))?;
            match tag.local {
                Local::F => {
                    if cell.has_formula || cell.value.is_some() || cell.inline.is_some() {
                        return Err(unsupported("duplicate/misordered formula", "worksheet"));
                    }
                    cell.has_formula = true;
                    self.open_cell.formula = FormulaState::default();
                    let kind_attribute = attrs.attribute("", "t");
                    self.open_cell.formula.kind =
                        match kind_attribute.as_ref().map_or("normal", |a| &*a.value) {
                            "normal" => "normal",
                            "shared" => "shared",
                            "array" => "array",
                            "dataTable" => {
                                return Err(unsupported("data-table formula", "worksheet"));
                            }
                            _ => {
                                return Err(unsupported("unknown formula kind", "worksheet"));
                            }
                        };
                    self.open_cell.formula.open = span.clone();
                    self.open_cell.formula.raw_kind_span = kind_attribute
                        .as_ref()
                        .map(|a| raw_attribute_span(context.bytes, &a.span))
                        .transpose()?;
                    self.open_cell.formula.raw_shared_index_span = attrs
                        .attribute("", "si")
                        .map(|a| raw_attribute_span(context.bytes, &a.span))
                        .transpose()?;
                    self.open_cell.formula.raw_range_span = attrs
                        .attribute("", "ref")
                        .map(|a| raw_attribute_span(context.bytes, &a.span))
                        .transpose()?;
                    self.open_cell.formula.kind_span = kind_attribute.map(|a| a.span.clone());
                    let extent = attrs.attribute("", "ref");
                    if self.open_cell.formula.kind == "array" {
                        if self.index.is_none() {
                            *context.needs_index = true;
                            return Err(unsupported(NEEDS_INDEX, "worksheet"));
                        }
                        if let Some(extent) = extent.as_ref() {
                            let rect = SourceRect::parse(&extent.value)?;
                            // Validate declared geometry before any
                            // ownership index or child enumeration.
                            if rect.last_col > context.options.limits.max_columns {
                                return Err(unsupported(
                                    "dynamic array extent width limit",
                                    "worksheet",
                                ));
                            }
                            self.extent_cells = rect
                                .cell_count()
                                .and_then(|n| self.extent_cells.checked_add(n))
                                .filter(|n| *n <= context.options.limits.max_cells as u64)
                                .ok_or_else(|| {
                                    unsupported("dynamic array extent cell limit", "worksheet")
                                })?;
                            self.max_row = self.max_row.max(rect.last_row);
                            self.max_col = self.max_col.max(rect.last_col);
                            let dynamic = cell.dynamic.get_or_insert_default();
                            dynamic.array_ref = Some(rect);
                            dynamic.array_ref_span = Some(extent.span.clone());
                        }
                    } else if extent.is_some() && self.open_cell.formula.kind != "shared" {
                        return Err(unsupported("non-shared formula extent", "worksheet"));
                    }
                    if self.open_cell.formula.kind == "shared" {
                        self.open_cell.formula.shared_id = Some(integer(attrs.required("si")?)?);
                        self.open_cell.formula.shared_range =
                            extent.map(|a| rect(&a.value)).transpose()?;
                    }
                    if empty {
                        self.open_cell.formula.end = span.end;
                    }
                }
                Local::V => {
                    if cell.value.is_some() || cell.inline.is_some() {
                        return Err(unsupported("duplicate/ambiguous cell cache", "worksheet"));
                    }
                    cell.value = Some(OpenValue {
                        span: span.clone(),
                        open_end: span.end,
                        close_start: span.end,
                        empty,
                        qualified: element.qualified,
                        text: Cow::Borrowed(""),
                    });
                }
                Local::Is => {
                    if cell.inline.is_some() || cell.value.is_some() {
                        return Err(unsupported("duplicate/ambiguous inline cache", "worksheet"));
                    }
                    cell.inline = Some(span.clone());
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn open_nested(&mut self, event: &RuleEvent<'_, 'a>) -> Result<(), IoError> {
        let depth = event.depth;
        let restore = event.restore;
        if depth > 5 && matches!(restore, Position::CellFormula | Position::CellValue) {
            return Err(unsupported("nested formula/cache content", "worksheet"));
        }
        Ok(())
    }

    pub fn close(
        &mut self,
        event: &RuleEvent<'_, 'a>,
        context: &mut RuleContext<'a, '_, '_>,
        emit: &mut impl emission::Sink,
    ) -> Result<(), IoError> {
        let position = event.position;
        let tag = event.tag;
        let span = event.span.clone();
        let direct = event.direct;
        let is_cell = event.position == Position::Cell;
        if (position == Position::AutoFilter) && self.filter_active {
            let mut rect = self.filter_ref.take().expect("validated autoFilter");
            rect.first_row += 1;
            if rect.first_row <= rect.last_row {
                self.active_filters.push(rect);
            }
        }
        if direct {
            self.open_cell.close_payload(tag.local, &span)?;
        }
        if let Some(index) = self.index.as_mut() {
            if position == Position::Row {
                let entry = index.rows.last_mut().expect("open row");
                entry.span.end = span.end;
            }
            if position == Position::SheetData {
                index.sheet_data.end = span.end;
            }
        }
        if is_cell {
            let cell = self
                .open_cell
                .cell
                .as_mut()
                .ok_or_else(|| unsupported("unbalanced cell", "worksheet"))?;
            cell.span.end = span.end;
            let emitted_value = (!cell.has_formula).then(|| {
                cell.decode(
                    std::mem::take(&mut self.open_cell.literal_text),
                    std::mem::take(&mut self.open_cell.inline_text),
                )
            });
            let refusal = emitted_value
                .as_ref()
                .and_then(|value| cell.refusal(context.bytes, value));
            cell.dynamic_anchor_extent(&self.open_cell.formula)?;
            if cell.has_formula
                && self.open_cell.formula.kind == "array"
                && let Some(extent) = cell.dynamic.as_ref().and_then(|d| d.array_ref)
            {
                // cm/XLDAPR and legacy array anchors share the same
                // validated top-left source extent. Do not resolve
                // metadata or check overlap/ownership here.
                self.generated_children.observe(extent);
            }
            if let Some(index) = self.index.as_mut() {
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
                    formula: cell.has_formula.then_some(self.formulas),
                    literal_refusal: refusal,
                });
                index.rows.last_mut().expect("open row").cells.end = index.cells.len();
            } else if let Some(refusal) = refusal {
                return Err(unsupported(refusal, "worksheet"));
            }
            if let Some(value) = emitted_value {
                if context.request.retention.values {
                    emit.literal(emission::Literal {
                        row: cell.row,
                        col: cell.col,
                        generated_child_hint: self.generated_children.contains(cell.row, cell.col),
                        style: self.open_cell.style.take(),
                        raw_coordinate_span: self.open_cell.raw_coordinate.take(),
                        raw_type_span: self.open_cell.raw_type.take(),
                        raw_style_span: self.open_cell.raw_style.take(),
                        raw_value_span: cell.value.as_ref().map(|v| v.open_end..v.close_start),
                        value,
                    })?;
                }
                self.open_cell.cell = None;
            } else {
                if self.open_cell.formula.kind == "normal"
                    && self.open_cell.formula.text.trim().is_empty()
                {
                    return Err(unsupported("empty ordinary formula", "worksheet"));
                }
                if self.open_cell.formula.end == 0 {
                    return Err(unsupported("missing formula boundary", "worksheet"));
                }
                let envelope = self.open_cell.source(self.formulas as u64)?.envelope;
                let mut mark = self
                    .open_cell
                    .cell
                    .take()
                    .expect("open cell")
                    .retain(std::mem::take(&mut self.open_cell.formula), context.bytes);
                let mut source = mark.source_event(self.formulas as u64);
                source.envelope.raw_coordinate = envelope.raw_coordinate;
                source.envelope.raw_cell_type = envelope.raw_cell_type;
                source.envelope.raw_formula_type = envelope.raw_formula_type;
                source.envelope.raw_shared_index = envelope.raw_shared_index;
                source.envelope.raw_declared_range = envelope.raw_declared_range;
                let evidence = super::super::compatibility::inspect(&source, context.bytes);
                if context.request.retention.formula_events {
                    emit.formula(source)?;
                }
                mark.compatibility = evidence;
                self.formulas += 1;
                if self.formulas > context.options.limits.max_formula_cells {
                    return Err(unsupported("formula cell count limit", "worksheet"));
                }
                self.families
                    .observe(&mark, self.formulas - 1, context.options);
                if context.request.retention.marks {
                    self.cells.push(mark);
                }
            }
        }
        Ok(())
    }
}
