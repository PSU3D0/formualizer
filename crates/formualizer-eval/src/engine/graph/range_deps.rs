use super::*;
use crate::engine::used_extent::{ExtentPolicy, OpenRangeBounds, resolve_used_extent};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

#[derive(Clone, Copy, PartialEq, Eq)]
enum RangeSelfUse {
    NoMatch,
    Excluded,
    IncludedOrUnknown,
}

impl RangeSelfUse {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::IncludedOrUnknown, _) | (_, Self::IncludedOrUnknown) => Self::IncludedOrUnknown,
            (Self::Excluded, _) | (_, Self::Excluded) => Self::Excluded,
            _ => Self::NoMatch,
        }
    }
}

impl DependencyGraph {
    pub(crate) fn compressed_range_dependents_intersecting_deleted_rows(
        &self,
        sheet_id: SheetId,
        start_row: u32,
        end_row: u32,
    ) -> Vec<VertexId> {
        self.formula_to_range_deps
            .iter()
            .filter_map(|(&dependent, ranges)| {
                ranges
                    .iter()
                    .any(|range| {
                        // `Current` is the dependent formula's own sheet. An
                        // unresolvable sheet name keeps the dependent in the
                        // candidate set (over-approximating is safe here;
                        // dropping it would silently lose a dependent).
                        let range_sheet_id = self
                            .sheet_reg
                            .resolve_locator(&range.sheet, self.get_vertex_sheet_id(dependent))
                            .unwrap_or(sheet_id);
                        let range_start = range.start_row.map(|bound| bound.index).unwrap_or(0);
                        let range_end = range.end_row.map(|bound| bound.index).unwrap_or(u32::MAX);
                        range_sheet_id == sheet_id
                            && range_start <= end_row
                            && range_end >= start_row
                    })
                    .then_some(dependent)
            })
            .collect()
    }

    pub(crate) fn compressed_range_dependents_intersecting_deleted_columns(
        &self,
        sheet_id: SheetId,
        start_col: u32,
        end_col: u32,
    ) -> Vec<VertexId> {
        self.formula_to_range_deps
            .iter()
            .filter_map(|(&dependent, ranges)| {
                ranges
                    .iter()
                    .any(|range| {
                        // See the row query above.
                        let range_sheet_id = self
                            .sheet_reg
                            .resolve_locator(&range.sheet, self.get_vertex_sheet_id(dependent))
                            .unwrap_or(sheet_id);
                        let range_start = range.start_col.map(|bound| bound.index).unwrap_or(0);
                        let range_end = range.end_col.map(|bound| bound.index).unwrap_or(u32::MAX);
                        range_sheet_id == sheet_id
                            && range_start <= end_col
                            && range_end >= start_col
                    })
                    .then_some(dependent)
            })
            .collect()
    }

    /// Visit compressed-range formula dependents covering one cell without
    /// materializing the stripe union used by dirty propagation.
    ///
    /// This path is intentionally parallel to
    /// `collect_range_dependents_for_rect`: scheduling keeps its existing
    /// behavior, while inspection can stop before a pathological stripe has
    /// been copied into an unbounded candidate set. Work is charged for every
    /// stripe candidate and every compressed range exact-check.
    pub(crate) fn visit_range_dependents_covering_bounded(
        &self,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        remaining_work: &mut u64,
        visitor: &mut dyn FnMut(VertexId) -> bool,
    ) -> bool {
        if self.stripe_to_dependents.is_empty() {
            return true;
        }

        let mut seen = FxHashSet::default();
        let keys = [
            StripeKey {
                sheet_id,
                stripe_type: StripeType::Column,
                index: col0,
            },
            StripeKey {
                sheet_id,
                stripe_type: StripeType::Row,
                index: row0,
            },
            StripeKey {
                sheet_id,
                stripe_type: StripeType::Block,
                index: block_index(row0, col0),
            },
        ];

        for key in keys {
            if key.stripe_type == StripeType::Block && !self.config.enable_block_stripes {
                continue;
            }
            let Some(candidates) = self.stripe_to_dependents.get(&key) else {
                continue;
            };
            for &dependent in candidates {
                if *remaining_work == 0 {
                    return false;
                }
                *remaining_work -= 1;
                if !seen.insert(dependent) {
                    continue;
                }
                let Some(ranges) = self.formula_to_range_deps.get(&dependent) else {
                    continue;
                };
                let mut covered = false;
                for range in ranges {
                    if *remaining_work == 0 {
                        return false;
                    }
                    *remaining_work -= 1;
                    // `Current` is the dependent formula's own sheet; an
                    // unresolvable sheet name is interpreted on the query sheet
                    // so the dependent is not silently dropped.
                    let range_sheet = self
                        .sheet_reg
                        .resolve_locator(&range.sheet, self.get_vertex_sheet_id(dependent))
                        .unwrap_or(sheet_id);
                    if range_sheet != sheet_id {
                        continue;
                    }
                    let start_row = range.start_row.map(|bound| bound.index).unwrap_or(0);
                    let end_row = range.end_row.map(|bound| bound.index).unwrap_or(u32::MAX);
                    let start_col = range.start_col.map(|bound| bound.index).unwrap_or(0);
                    let end_col = range.end_col.map(|bound| bound.index).unwrap_or(u32::MAX);
                    if start_row <= row0 && row0 <= end_row && start_col <= col0 && col0 <= end_col
                    {
                        covered = true;
                        break;
                    }
                }
                if covered && !visitor(dependent) {
                    return false;
                }
            }
        }
        true
    }

    /// Public wrapper to add range-dependent edges.
    pub fn add_range_edges(
        &mut self,
        dependent: VertexId,
        ranges: &[SharedRangeRef<'static>],
        current_sheet_id: SheetId,
    ) {
        self.add_range_dependent_edges(dependent, ranges, current_sheet_id);
    }

    /// Return the compressed range dependencies recorded for a formula vertex, if any.
    /// These are `SharedRangeRef` entries that were not expanded into explicit
    /// cell edges due to `range_expansion_limit` or due to infinite/partial bounds.
    pub fn get_range_dependencies(
        &self,
        vertex: VertexId,
    ) -> Option<&Vec<SharedRangeRef<'static>>> {
        self.formula_to_range_deps.get(&vertex)
    }

    #[cfg(test)]
    pub(crate) fn formula_to_range_deps(
        &self,
    ) -> &FxHashMap<VertexId, Vec<SharedRangeRef<'static>>> {
        &self.formula_to_range_deps
    }

    #[cfg(test)]
    pub(crate) fn stripe_to_dependents(&self) -> &FxHashMap<StripeKey, FxHashSet<VertexId>> {
        &self.stripe_to_dependents
    }

    /// True when a (possibly open-ended) range region on `sheet_id` covers
    /// the formula vertex's own cell. Used to record a self-loop for
    /// stripe-compressed / whole-axis self-inclusion (#120): such references
    /// never produce explicit cell edges, so the ingest self-reference check
    /// (which scans expanded cell deps) misses them. `None` bounds mean the
    /// axis is unbounded (whole column/row), which always covers the cell.
    fn range_region_contains_self(
        &self,
        dependent: VertexId,
        sheet_id: SheetId,
        s_row: Option<u32>,
        e_row: Option<u32>,
        s_col: Option<u32>,
        e_col: Option<u32>,
    ) -> bool {
        if self.store.sheet_id(dependent) != sheet_id {
            return false;
        }
        let coord = self.store.coord(dependent);
        let r0 = coord.row();
        let c0 = coord.col();
        s_row.is_none_or(|s| r0 >= s)
            && e_row.is_none_or(|e| r0 <= e)
            && s_col.is_none_or(|s| c0 >= s)
            && e_col.is_none_or(|e| c0 <= e)
    }

    /// Record a self-loop edge (vertex → itself). The edge store and Tarjan
    /// both treat self-loops as cycles (`separate_cycles` via `has_self_loop`).
    fn record_self_loop(&mut self, vertex: VertexId) {
        if !self.has_self_loop(vertex) {
            self.edges.add_edge(vertex, vertex);
        }
    }

    pub(crate) fn compressed_range_resolved_bounds(
        &self,
        sheet: SheetId,
        range: (Option<u32>, Option<u32>, Option<u32>, Option<u32>),
    ) -> Option<(u32, u32, u32, u32)> {
        let (start_row, end_row, start_col, end_col) = range;
        let extent = resolve_used_extent(
            OpenRangeBounds {
                start_row,
                start_column: start_col,
                end_row,
                end_column: end_col,
            },
            ExtentPolicy::GraphCompat {
                fallback_row: self.config.max_open_ended_rows.saturating_sub(1),
                fallback_column: self.config.max_open_ended_cols.saturating_sub(1),
            },
            |first, last| self.used_row_bounds_for_columns(sheet, first, last),
            |first, last| self.used_col_bounds_for_rows(sheet, first, last),
        )?;
        Some((
            extent.start_row,
            extent.end_row,
            extent.start_column,
            extent.end_column,
        ))
    }

    /// Classify whether every occurrence of one compressed range that covers
    /// the formula cell is narrowed away from that cell by a statically
    /// resolvable `INDEX`. The range dependency itself remains conservative so
    /// used-bound growth still invalidates the formula; only the synthetic #120
    /// self-loop is omitted when the selected reference cannot contain the
    /// formula cell.
    fn compressed_range_self_use(
        &self,
        dependent: VertexId,
        range_sheet: SheetId,
        range: (Option<u32>, Option<u32>, Option<u32>, Option<u32>),
    ) -> RangeSelfUse {
        let Some(ast) = self.get_formula(dependent) else {
            return RangeSelfUse::IncludedOrUnknown;
        };

        fn static_index(node: &ASTNode) -> Option<i64> {
            match &node.node_type {
                ASTNodeType::Literal(LiteralValue::Int(value)) => Some(*value),
                ASTNodeType::Literal(LiteralValue::Number(value)) if value.is_finite() => {
                    Some(*value as i64)
                }
                ASTNodeType::UnaryOp { op, expr } if op == "+" => static_index(expr),
                ASTNodeType::UnaryOp { op, expr } if op == "-" => static_index(expr)?.checked_neg(),
                _ => None,
            }
        }

        fn matching_range(
            graph: &DependencyGraph,
            node: &ASTNode,
            dependent: VertexId,
            range_sheet: SheetId,
            range: (Option<u32>, Option<u32>, Option<u32>, Option<u32>),
        ) -> bool {
            let ASTNodeType::Reference {
                reference:
                    ReferenceType::Range {
                        sheet,
                        start_row,
                        start_col,
                        end_row,
                        end_col,
                        ..
                    },
                ..
            } = &node.node_type
            else {
                return false;
            };
            let sheet_id = match sheet.as_deref() {
                Some(name) => match graph.sheet_id(name) {
                    Some(id) => id,
                    None => return false,
                },
                None => graph.get_vertex_sheet_id(dependent),
            };
            sheet_id == range_sheet
                && start_row.map(|index| index.saturating_sub(1)) == range.0
                && end_row.map(|index| index.saturating_sub(1)) == range.1
                && start_col.map(|index| index.saturating_sub(1)) == range.2
                && end_col.map(|index| index.saturating_sub(1)) == range.3
        }

        fn selected_region_contains_self(
            graph: &DependencyGraph,
            dependent: VertexId,
            range_sheet: SheetId,
            range: (Option<u32>, Option<u32>, Option<u32>, Option<u32>),
            position: i64,
            explicit_col: Option<i64>,
        ) -> Option<bool> {
            let (sr, er, sc, ec) = graph.compressed_range_resolved_bounds(range_sheet, range)?;
            let (row, col) = match explicit_col {
                Some(col) => (position, col),
                None if sr == er => (1, position),
                None => (position, 1),
            };
            if row < 0 || col < 0 {
                return Some(false);
            }
            let coord = graph.store.coord(dependent);
            let contains = if row == 0 && col == 0 {
                coord.row() >= sr && coord.row() <= er && coord.col() >= sc && coord.col() <= ec
            } else if col == 0 {
                let selected_row = sr.checked_add(u32::try_from(row).ok()?.saturating_sub(1))?;
                selected_row <= er
                    && coord.row() == selected_row
                    && coord.col() >= sc
                    && coord.col() <= ec
            } else if row == 0 {
                let selected_col = sc.checked_add(u32::try_from(col).ok()?.saturating_sub(1))?;
                selected_col <= ec
                    && coord.col() == selected_col
                    && coord.row() >= sr
                    && coord.row() <= er
            } else {
                let selected_row = sr.checked_add(u32::try_from(row).ok()?.saturating_sub(1))?;
                let selected_col = sc.checked_add(u32::try_from(col).ok()?.saturating_sub(1))?;
                selected_row <= er
                    && selected_col <= ec
                    && coord.row() == selected_row
                    && coord.col() == selected_col
            };
            Some(contains)
        }

        fn visit(
            graph: &DependencyGraph,
            node: &ASTNode,
            dependent: VertexId,
            range_sheet: SheetId,
            range: (Option<u32>, Option<u32>, Option<u32>, Option<u32>),
            index: Option<(i64, Option<i64>)>,
        ) -> RangeSelfUse {
            if matching_range(graph, node, dependent, range_sheet, range) {
                return match index.and_then(|(row, col)| {
                    selected_region_contains_self(graph, dependent, range_sheet, range, row, col)
                }) {
                    Some(false) => RangeSelfUse::Excluded,
                    Some(true) | None => RangeSelfUse::IncludedOrUnknown,
                };
            }
            match &node.node_type {
                ASTNodeType::Function { name, args }
                    if name.eq_ignore_ascii_case("INDEX") && (2..=3).contains(&args.len()) =>
                {
                    let row = static_index(&args[1]);
                    let col = args.get(2).and_then(static_index);
                    let selection = row.and_then(|row| {
                        if args.len() == 2 || col.is_some() {
                            Some((row, col))
                        } else {
                            None
                        }
                    });
                    let mut use_kind =
                        visit(graph, &args[0], dependent, range_sheet, range, selection);
                    for arg in &args[1..] {
                        use_kind =
                            use_kind.merge(visit(graph, arg, dependent, range_sheet, range, None));
                    }
                    use_kind
                }
                ASTNodeType::Function { args, .. } => {
                    args.iter().fold(RangeSelfUse::NoMatch, |kind, arg| {
                        kind.merge(visit(graph, arg, dependent, range_sheet, range, None))
                    })
                }
                ASTNodeType::UnaryOp { expr, .. } => {
                    visit(graph, expr, dependent, range_sheet, range, None)
                }
                ASTNodeType::BinaryOp { left, right, .. } => visit(
                    graph,
                    left,
                    dependent,
                    range_sheet,
                    range,
                    None,
                )
                .merge(visit(graph, right, dependent, range_sheet, range, None)),
                ASTNodeType::Call { callee, args } => {
                    let mut kind = visit(graph, callee, dependent, range_sheet, range, None);
                    for arg in args {
                        kind = kind.merge(visit(graph, arg, dependent, range_sheet, range, None));
                    }
                    kind
                }
                ASTNodeType::Array(rows) => {
                    rows.iter()
                        .flatten()
                        .fold(RangeSelfUse::NoMatch, |kind, item| {
                            kind.merge(visit(graph, item, dependent, range_sheet, range, None))
                        })
                }
                ASTNodeType::Literal(_) | ASTNodeType::Omitted | ASTNodeType::Reference { .. } => {
                    RangeSelfUse::NoMatch
                }
            }
        }

        visit(self, &ast, dependent, range_sheet, range, None)
    }

    pub(super) fn add_range_dependent_edges(
        &mut self,
        dependent: VertexId,
        ranges: &[SharedRangeRef<'static>],
        current_sheet_id: SheetId,
    ) {
        if ranges.is_empty() {
            return;
        }

        self.formula_to_range_deps
            .insert(dependent, ranges.to_vec());

        for range in ranges {
            // `current_sheet_id` is the dependent formula's sheet, which is what
            // `Current` means. An unresolvable sheet name falls back to it so a
            // stripe is still registered rather than the edge being dropped.
            let sheet_id = self
                .sheet_reg
                .resolve_locator(&range.sheet, current_sheet_id)
                .unwrap_or(current_sheet_id);

            let s_row = range.start_row.map(|b| b.index);
            let e_row = range.end_row.map(|b| b.index);
            let s_col = range.start_col.map(|b| b.index);
            let e_col = range.end_col.map(|b| b.index);

            // #120: a compressed range whose region covers this formula's own
            // cell is a self-reference. Record a self-loop so SCC detection
            // flags the cycle (the ingest self-ref check only sees expanded
            // cell edges, which compressed ranges do not produce).
            if self.range_region_contains_self(dependent, sheet_id, s_row, e_row, s_col, e_col)
                && self.compressed_range_self_use(dependent, sheet_id, (s_row, e_row, s_col, e_col))
                    != RangeSelfUse::Excluded
            {
                self.record_self_loop(dependent);
            }

            let col_stripes = (s_row.is_none() && e_row.is_none())
                || (s_col.is_some() && e_col.is_some() && (s_row.is_none() || e_row.is_none()));
            let row_stripes = (s_col.is_none() && e_col.is_none())
                || (s_row.is_some() && e_row.is_some() && (s_col.is_none() || e_col.is_none()));

            if col_stripes && !row_stripes {
                let sc = s_col.unwrap_or(0);
                let ec = e_col.unwrap_or(sc);
                for col in sc..=ec {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Column,
                        index: col,
                    };
                    self.stripe_to_dependents
                        .entry(key.clone())
                        .or_default()
                        .insert(dependent);
                    #[cfg(test)]
                    {
                        if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1)
                            && let Ok(mut g) = self.instr.lock()
                        {
                            g.stripe_inserts += 1;
                        }
                    }
                }
                continue;
            }

            if row_stripes && !col_stripes {
                let sr = s_row.unwrap_or(0);
                let er = e_row.unwrap_or(sr);
                for row in sr..=er {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Row,
                        index: row,
                    };
                    self.stripe_to_dependents
                        .entry(key.clone())
                        .or_default()
                        .insert(dependent);
                    #[cfg(test)]
                    {
                        if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1)
                            && let Ok(mut g) = self.instr.lock()
                        {
                            g.stripe_inserts += 1;
                        }
                    }
                }
                continue;
            }

            let start_row = s_row.unwrap_or(0);
            let start_col = s_col.unwrap_or(0);
            let end_row = e_row.unwrap_or(start_row);
            let end_col = e_col.unwrap_or(start_col);

            let height = end_row.saturating_sub(start_row) + 1;
            let width = end_col.saturating_sub(start_col) + 1;

            if self.config.enable_block_stripes && height > 1 && width > 1 {
                let start_block_row = start_row / BLOCK_H;
                let end_block_row = end_row / BLOCK_H;
                let start_block_col = start_col / BLOCK_W;
                let end_block_col = end_col / BLOCK_W;

                for block_row in start_block_row..=end_block_row {
                    for block_col in start_block_col..=end_block_col {
                        let key = StripeKey {
                            sheet_id,
                            stripe_type: StripeType::Block,
                            index: block_index(block_row * BLOCK_H, block_col * BLOCK_W),
                        };
                        self.stripe_to_dependents
                            .entry(key.clone())
                            .or_default()
                            .insert(dependent);
                        #[cfg(test)]
                        {
                            if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1)
                                && let Ok(mut g) = self.instr.lock()
                            {
                                g.stripe_inserts += 1;
                            }
                        }
                    }
                }
            } else if height > width {
                for col in start_col..=end_col {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Column,
                        index: col,
                    };
                    self.stripe_to_dependents
                        .entry(key.clone())
                        .or_default()
                        .insert(dependent);
                    #[cfg(test)]
                    {
                        if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1)
                            && let Ok(mut g) = self.instr.lock()
                        {
                            g.stripe_inserts += 1;
                        }
                    }
                }
            } else {
                for row in start_row..=end_row {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Row,
                        index: row,
                    };
                    self.stripe_to_dependents
                        .entry(key.clone())
                        .or_default()
                        .insert(dependent);
                    #[cfg(test)]
                    {
                        if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1)
                            && let Ok(mut g) = self.instr.lock()
                        {
                            g.stripe_inserts += 1;
                        }
                    }
                }
            }
        }
    }

    /// Fast-path: add range dependencies using compact RangeKey.
    pub fn add_range_deps_from_keys(
        &mut self,
        dependent: VertexId,
        keys: &[crate::engine::plan::RangeKey],
        current_sheet_id: SheetId,
    ) {
        use crate::engine::plan::RangeKey as RK;
        if keys.is_empty() {
            return;
        }

        let mut shared_ranges: Vec<SharedRangeRef<'static>> = Vec::with_capacity(keys.len());
        for k in keys {
            let sheet_loc = SharedSheetLocator::Id(match k {
                RK::Rect { sheet, .. }
                | RK::WholeRow { sheet, .. }
                | RK::WholeCol { sheet, .. }
                | RK::OpenRect { sheet, .. } => *sheet,
            });

            let mk_axis = |idx0: u32| formualizer_common::AxisBound::new(idx0, false);

            let built = match k {
                RK::Rect { start, end, .. } => {
                    let sr = mk_axis(start.row());
                    let sc = mk_axis(start.col());
                    let er = mk_axis(end.row());
                    let ec = mk_axis(end.col());
                    SharedRangeRef::from_parts(sheet_loc, Some(sr), Some(sc), Some(er), Some(ec))
                        .ok()
                }
                RK::WholeRow { row, .. } => {
                    let r0 = row.saturating_sub(1);
                    let b = mk_axis(r0);
                    SharedRangeRef::from_parts(sheet_loc, Some(b), None, Some(b), None).ok()
                }
                RK::WholeCol { col, .. } => {
                    let c0 = col.saturating_sub(1);
                    let b = mk_axis(c0);
                    SharedRangeRef::from_parts(sheet_loc, None, Some(b), None, Some(b)).ok()
                }
                RK::OpenRect { start, end, .. } => {
                    let (sr, sc) = match start {
                        Some(p) => (Some(mk_axis(p.row())), Some(mk_axis(p.col()))),
                        None => (None, None),
                    };
                    let (er, ec) = match end {
                        Some(p) => (Some(mk_axis(p.row())), Some(mk_axis(p.col()))),
                        None => (None, None),
                    };
                    SharedRangeRef::from_parts(sheet_loc, sr, sc, er, ec).ok()
                }
            };

            if let Some(r) = built {
                shared_ranges.push(r.into_owned());
            }
        }

        if shared_ranges.is_empty() {
            return;
        }

        self.formula_to_range_deps
            .insert(dependent, shared_ranges.clone());

        for range in &shared_ranges {
            // See add_range_dependent_edges.
            let sheet_id = self
                .sheet_reg
                .resolve_locator(&range.sheet, current_sheet_id)
                .unwrap_or(current_sheet_id);

            let s_row = range.start_row.map(|b| b.index);
            let e_row = range.end_row.map(|b| b.index);
            let s_col = range.start_col.map(|b| b.index);
            let e_col = range.end_col.map(|b| b.index);

            // #120: see add_range_dependent_edges — compressed range covering
            // the formula's own cell records a self-loop for SCC detection.
            if self.range_region_contains_self(dependent, sheet_id, s_row, e_row, s_col, e_col)
                && self.compressed_range_self_use(dependent, sheet_id, (s_row, e_row, s_col, e_col))
                    != RangeSelfUse::Excluded
            {
                self.record_self_loop(dependent);
            }

            let col_stripes = (s_row.is_none() && e_row.is_none())
                || (s_col.is_some() && e_col.is_some() && (s_row.is_none() || e_row.is_none()));
            let row_stripes = (s_col.is_none() && e_col.is_none())
                || (s_row.is_some() && e_row.is_some() && (s_col.is_none() || e_col.is_none()));

            if col_stripes && !row_stripes {
                let sc = s_col.unwrap_or(0);
                let ec = e_col.unwrap_or(sc);
                for col in sc..=ec {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Column,
                        index: col,
                    };
                    self.stripe_to_dependents
                        .entry(key)
                        .or_default()
                        .insert(dependent);
                }
                continue;
            }

            if row_stripes && !col_stripes {
                let sr = s_row.unwrap_or(0);
                let er = e_row.unwrap_or(sr);
                for row in sr..=er {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Row,
                        index: row,
                    };
                    self.stripe_to_dependents
                        .entry(key)
                        .or_default()
                        .insert(dependent);
                }
                continue;
            }

            let start_row = s_row.unwrap_or(0);
            let start_col = s_col.unwrap_or(0);
            let end_row = e_row.unwrap_or(start_row);
            let end_col = e_col.unwrap_or(start_col);

            let height = end_row.saturating_sub(start_row) + 1;
            let width = end_col.saturating_sub(start_col) + 1;

            if self.config.enable_block_stripes && height > 1 && width > 1 {
                let start_block_row = start_row / BLOCK_H;
                let end_block_row = end_row / BLOCK_H;
                let start_block_col = start_col / BLOCK_W;
                let end_block_col = end_col / BLOCK_W;

                for block_row in start_block_row..=end_block_row {
                    for block_col in start_block_col..=end_block_col {
                        let key = StripeKey {
                            sheet_id,
                            stripe_type: StripeType::Block,
                            index: block_index(block_row * BLOCK_H, block_col * BLOCK_W),
                        };
                        self.stripe_to_dependents
                            .entry(key)
                            .or_default()
                            .insert(dependent);
                    }
                }
            } else if height > width {
                for col in start_col..=end_col {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Column,
                        index: col,
                    };
                    self.stripe_to_dependents
                        .entry(key)
                        .or_default()
                        .insert(dependent);
                }
            } else {
                for row in start_row..=end_row {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Row,
                        index: row,
                    };
                    self.stripe_to_dependents
                        .entry(key)
                        .or_default()
                        .insert(dependent);
                }
            }
        }
    }
}
