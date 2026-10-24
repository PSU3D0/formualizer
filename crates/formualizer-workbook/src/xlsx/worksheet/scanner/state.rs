//! Typed per-sheet admission/geometry state and transient open-cell value/formula
//! state. Raw source spellings stay separate from decoded text and retained marks.
use super::*;

#[derive(Default)]
pub(super) struct OpenCellState<'a> {
    pub cell: Option<ScalarCell<'a>>,
    pub formula: FormulaState<'a>,
    pub style: Option<Cow<'a, str>>,
    pub raw_coordinate: Option<Range<usize>>,
    pub raw_type: Option<Range<usize>>,
    pub raw_style: Option<Range<usize>>,
    pub literal_text: Cow<'a, str>,
    pub inline_text: emission::DisplayString<'a>,
}

impl<'a> OpenCellState<'a> {
    pub fn is_inline(&self) -> bool {
        self.cell
            .as_ref()
            .is_some_and(|c| c.inline.is_some() && !c.has_formula)
    }

    pub fn text(
        &mut self,
        value: Cow<'a, str>,
        span: Range<usize>,
        text: &'a str,
        path: &[xml::Element<'a>],
        position: Position,
    ) -> Result<(), IoError> {
        if self.is_inline() {
            let node = xml::Node::new(xml::Kind::Text(value.clone()), span.clone());
            self.inline_text.event(path, &node, text.as_bytes())?;
        }
        if let Some(cell) = self.cell.as_mut() {
            match position {
                Position::CellFormula => append(&mut self.formula.text, value),
                Position::CellValue => {
                    if !cell.has_formula {
                        let raw = text
                            .get(span)
                            .ok_or_else(|| unsupported("unavailable XML text span", "worksheet"))?;
                        let literal = if raw.starts_with('&') {
                            value.clone()
                        } else if !raw.contains('\r') {
                            Cow::Borrowed(raw)
                        } else {
                            Cow::Owned(raw.replace("\r\n", "\n").replace('\r', "\n"))
                        };
                        append(&mut self.literal_text, literal);
                    }
                    append(&mut cell.value.as_mut().expect("opened v").text, value);
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn source(&self, sequence: u64) -> Result<emission::Formula<'_>, IoError> {
        let cell = self
            .cell
            .as_ref()
            .ok_or_else(|| unsupported("formula without cell", "worksheet"))?;
        let formula = &self.formula;
        let kind = match formula.kind {
            "normal" => emission::FormulaKind::Ordinary {
                text: &formula.text,
            },
            "shared" => match formula.shared_range {
                Some((first_row, first_col, last_row, last_col)) => {
                    emission::FormulaKind::SharedAnchor {
                        shared_index: formula.shared_id,
                        declared_range: SourceRect {
                            first_row,
                            first_col,
                            last_row,
                            last_col,
                        },
                        text: &formula.text,
                    }
                }
                None => emission::FormulaKind::SharedDescendant {
                    shared_index: formula.shared_id,
                },
            },
            kind => emission::FormulaKind::Unsupported {
                kind,
                text: &formula.text,
            },
        };
        Ok(emission::Formula {
            row: cell.row,
            col: cell.col,
            sequence,
            offset: cell.span.start,
            kind,
            envelope: emission::FormulaEnvelope {
                cell: cell.span.clone(),
                formula: formula.open.start..formula.end,
                raw_coordinate: self.raw_coordinate.clone(),
                raw_cell_type: self.raw_type.clone(),
                raw_formula_type: formula.raw_kind_span.clone(),
                raw_shared_index: formula.raw_shared_index_span.clone(),
                raw_declared_range: formula.raw_range_span.clone(),
                raw_cache: cell.value.as_ref().map(|v| v.open_end..v.close_start),
            },
        })
    }

    pub fn close_payload(&mut self, local: Local, span: &Range<usize>) -> Result<(), IoError> {
        let cell = self
            .cell
            .as_mut()
            .ok_or_else(|| unsupported("unbalanced cell payload", "worksheet"))?;
        match local {
            Local::F => self.formula.end = span.end,
            Local::V => {
                let value = cell.value.as_mut().expect("opened v");
                value.close_start = span.start;
                value.span.end = span.end;
            }
            Local::Is => cell.inline.as_mut().expect("opened is").end = span.end,
            _ => {}
        }
        Ok(())
    }
}

/// Semantic sheet state, independent of XML namespace/tokenizer bookkeeping.
pub(super) struct SheetState<'a, const STRICT: bool> {
    pub open_cell: OpenCellState<'a>,
    pub hidden_rows: Vec<u32>,
    pub rows_hidden_by_default: bool,
    pub outline_rows: Vec<u32>,
    pub outline_collapsed: bool,
    pub active_filters: Vec<SourceRect>,
    pub filter_seen: bool,
    pub filter_ref: Option<SourceRect>,
    pub filter_active: bool,
    pub table_merges: Vec<SourceRect>,
    pub table_ids: Vec<String>,
    pub table_count: Option<usize>,
    pub serialized: usize,
    pub formulas: usize,
    pub cells: Vec<FormulaMark>,
    pub row: u32,
    pub column: u32,
    pub sheet_data_count: usize,
    pub dimension: Option<(u32, u32, u32, u32)>,
    pub raw_dimension_span: Option<Range<usize>>,
    pub raw_row_refusal_span: Option<Range<usize>>,
    pub max_row: u32,
    pub max_col: u32,
    pub index: Option<SourceIndex>,
    pub extent_cells: u64,
    pub families: shared::Families,
    pub generated_children: generated::GeneratedChildren,
}

impl<'a, const STRICT: bool> SheetState<'a, STRICT> {
    pub fn new(request: ScanRequest) -> Self {
        Self {
            open_cell: OpenCellState::default(),
            hidden_rows: Vec::new(),
            rows_hidden_by_default: false,
            outline_rows: Vec::new(),
            outline_collapsed: false,
            active_filters: Vec::new(),
            filter_seen: false,
            filter_ref: None,
            filter_active: false,
            table_merges: Vec::new(),
            table_ids: Vec::new(),
            table_count: None,
            serialized: 0,
            formulas: 0,
            cells: Vec::new(),
            row: 0,
            column: 0,
            sheet_data_count: 0,
            dimension: None,
            raw_dimension_span: None,
            raw_row_refusal_span: None,
            max_row: 0,
            max_col: 0,
            index: (request.retention.geometry && request.strategy == ScanStrategy::Indexed).then(
                || SourceIndex {
                    dimension: None,
                    sheet_data: 0..0,
                    sheet_data_open_end: 0,
                    sheet_data_empty: false,
                    rows: Vec::new(),
                    cells: Vec::new(),
                    merges: Vec::new(),
                },
            ),
            extent_cells: 0,
            families: shared::Families::default(),
            generated_children: generated::GeneratedChildren::default(),
        }
    }

    pub fn finish(
        mut self,
        options: &ReadOptions,
        logical_cells: &mut u64,
    ) -> Result<Scan, IoError> {
        if STRICT && self.sheet_data_count != 1 {
            return Err(unsupported("missing sheetData", "worksheet"));
        }
        if let Some((_, _, r, c)) = self.dimension {
            self.max_row = self.max_row.max(r);
            self.max_col = self.max_col.max(c);
        }
        *logical_cells = logical_cells
            .checked_add(u64::from(self.max_row) * u64::from(self.max_col))
            .ok_or_else(|| unsupported("logical area overflow", "workbook"))?;
        if *logical_cells > options.limits.max_cells as u64 {
            return Err(unsupported("workbook logical cell limit", "workbook"));
        }
        if STRICT {
            self.families.finish()?;
        }
        if STRICT && self.table_count.is_some_and(|n| n != self.table_ids.len()) {
            return Err(unsupported("tablePart count disagreement", "worksheet"));
        }
        // A collapsed outline may hide grouped rows that carry no `hidden` flag:
        // treat every grouped row as hidden.
        if self.outline_collapsed && !self.outline_rows.is_empty() {
            self.hidden_rows.extend(self.outline_rows);
            self.hidden_rows.sort_unstable();
            self.hidden_rows.dedup();
        }
        Ok(Scan {
            active_filters: self.active_filters,
            hidden_rows: self.hidden_rows,
            rows_hidden_by_default: self.rows_hidden_by_default,
            has_dimension: self.dimension.is_some(),
            raw_dimension_span: self.raw_dimension_span,
            raw_row_refusal_span: self.raw_row_refusal_span,
            table_merges: self.table_merges,
            table_ids: self.table_ids,
            cells: self.cells,
            index: self.index,
            serialized: self.serialized,
            bounds: (self.max_row, self.max_col),
        })
    }
}
