//! Program 2 (contract decision 20.5): row/column inserts and deletes move
//! and adjust virtual family member runs as blocks.
//!
//! The per-cell editor moves every vertex at or past the edit and rewrites
//! every formula the edit changes, one at a time. For a virtual member run
//! (consecutive members down one column, one template) that is
//! O(members) of per-cell map and AST work for what is one fact about the
//! run: its rows (or column) shifted, and its template changed once.
//!
//! [`DependencyGraph::shift_virtual_runs`] does that for every run it can:
//! it splits a run at the edit's boundary, and for each part compares the
//! reference adjuster's output on the part's first and last members. A
//! member's references are affine in its row (relative references move
//! with the member, absolute ones are shared), and the adjuster maps each
//! reference endpoint by a non-decreasing function whose steps are 0 or 1
//! (unchanged, clamped into a deleted band, or shifted by the count).
//! So if the first and last members' adjusted formulas are the same
//! template relocated (the same relative offsets), every member between
//! them is too: the part becomes one run with the adjusted template.
//! Parts that fail (a reference crossing the edit inside the run, a
//! reference turned into `#REF!`, anything but cell and range references)
//! are materialized back to the per-cell maps and take the per-vertex path.
//! Debug builds check every member of every part against the per-cell
//! result.

use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use rustc_hash::FxHashMap;

use super::DependencyGraph;
use super::editor::reference_adjuster::{ReferenceAdjuster, ReferenceContext, ShiftOperation};
use super::virtual_members::MemberRun;
use crate::SheetId;
use crate::engine::arena::AstNodeId;
use crate::engine::vertex::VertexId;

/// A run the block transform moved or adjusted (positions after the edit).
#[derive(Debug, Clone)]
pub(crate) struct ShiftedRun {
    pub(crate) first: VertexId,
    pub(crate) len: u32,
    pub(crate) sheet: SheetId,
    pub(crate) col: u32,
    pub(crate) row0: u32,
    /// The members moved.
    pub(crate) moved: bool,
    /// The members' formulas changed: the first member's formula before
    /// and after (each member's is these relocated down by its index).
    pub(crate) adjusted: Option<(ASTNode, ASTNode)>,
}

/// What happens to one part of a run.
enum PartPlan {
    /// Neither moved nor changed.
    Unchanged(MemberRun),
    /// Becomes `new` (moved by `(dr, dc)` and/or with a new template).
    Shifted {
        new: MemberRun,
        dr: i64,
        dc: i64,
        adjusted: Option<Box<(ASTNode, ASTNode)>>,
    },
    /// Back to the per-cell maps (at its current, pre-edit position).
    Materialize(MemberRun),
}

/// Cell and range references of a formula, or `None` when it has any
/// other kind (3-D, external, table): those take the per-vertex path.
fn reference_count(ast: &ASTNode) -> Option<usize> {
    fn walk(n: &ASTNode, acc: &mut usize) -> bool {
        match &n.node_type {
            ASTNodeType::Reference { reference, .. } => match reference {
                ReferenceType::Cell { .. } | ReferenceType::Range { .. } => {
                    *acc += 1;
                    true
                }
                ReferenceType::NamedRange(_) => true,
                _ => false,
            },
            ASTNodeType::Literal(_) | ASTNodeType::Omitted => true,
            ASTNodeType::UnaryOp { expr, .. } => walk(expr, acc),
            ASTNodeType::BinaryOp { left, right, .. } => walk(left, acc) && walk(right, acc),
            ASTNodeType::Function { args, .. } => args.iter().all(|a| walk(a, acc)),
            ASTNodeType::Call { callee, args } => {
                walk(callee, acc) && args.iter().all(|a| walk(a, acc))
            }
            ASTNodeType::Array(rows) => rows.iter().flatten().all(|a| walk(a, acc)),
        }
    }
    let mut n = 0;
    walk(ast, &mut n).then_some(n)
}

/// The cell and range references of two formulas of the same shape, in
/// order (`None` when the shapes differ).
fn paired_references<'a>(
    a: &'a ASTNode,
    b: &'a ASTNode,
    out: &mut Vec<(&'a ReferenceType, &'a ReferenceType)>,
) -> bool {
    match (&a.node_type, &b.node_type) {
        (
            ASTNodeType::Reference { reference: x, .. },
            ASTNodeType::Reference { reference: y, .. },
        ) => {
            out.push((x, y));
            true
        }
        (ASTNodeType::Literal(_), ASTNodeType::Literal(_))
        | (ASTNodeType::Omitted, ASTNodeType::Omitted) => true,
        (ASTNodeType::UnaryOp { expr: x, .. }, ASTNodeType::UnaryOp { expr: y, .. }) => {
            paired_references(x, y, out)
        }
        (
            ASTNodeType::BinaryOp {
                left: xl,
                right: xr,
                ..
            },
            ASTNodeType::BinaryOp {
                left: yl,
                right: yr,
                ..
            },
        ) => paired_references(xl, yl, out) && paired_references(xr, yr, out),
        (ASTNodeType::Function { args: x, .. }, ASTNodeType::Function { args: y, .. }) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| paired_references(p, q, out))
        }
        (
            ASTNodeType::Call {
                callee: xc,
                args: x,
            },
            ASTNodeType::Call {
                callee: yc,
                args: y,
            },
        ) => {
            paired_references(xc, yc, out)
                && x.len() == y.len()
                && x.iter().zip(y).all(|(p, q)| paired_references(p, q, out))
        }
        (ASTNodeType::Array(x), ASTNodeType::Array(y)) => {
            x.len() == y.len()
                && x.iter().zip(y).all(|(p, q)| {
                    p.len() == q.len() && p.iter().zip(q).all(|(u, v)| paired_references(u, v, out))
                })
        }
        _ => false,
    }
}

/// The formula a member of `template` (valid at `anchor`) has at
/// `(row, col)`, exactly as `DependencyGraph::get_formula` renders it.
fn member_ast(template: &ASTNode, anchor: (u32, u32), row: u32, col: u32) -> Option<ASTNode> {
    let dr = i64::from(row) - i64::from(anchor.0);
    let dc = i64::from(col) - i64::from(anchor.1);
    if dr == 0 && dc == 0 {
        return Some(template.clone());
    }
    crate::engine::template::relocate::instantiate_member_ast(template, dr, dc).ok()
}

impl DependencyGraph {
    /// Move and adjust every virtual member run for a row/column insert or
    /// delete `op` as blocks (see the module docs). Called after the
    /// edit's deletions and before its per-vertex moves. Runs it cannot
    /// express are materialized first; the returned runs are the ones it
    /// moved or rewrote. Moved members' readers are queued for the dirty
    /// flag (`mark_dependents_dirty`), and rewritten members are marked
    /// dirty, as the per-vertex path does.
    pub(crate) fn shift_virtual_runs(&mut self, op: &ShiftOperation) -> Vec<ShiftedRun> {
        if self.vertex_formulas.virtual_members().is_empty() {
            return Vec::new();
        }
        let runs: Vec<MemberRun> = self
            .vertex_formulas
            .virtual_members()
            .runs()
            .copied()
            .collect();
        let adjuster = ReferenceAdjuster::new();
        let mut templates: FxHashMap<AstNodeId, Option<ASTNode>> = FxHashMap::default();

        // Phase 1 (reads the pre-edit graph only): plan every part.
        let mut plans: Vec<(u32, Vec<PartPlan>)> = Vec::new();
        for run in &runs {
            // A run on another sheet does not move but can read the
            // edited sheet: every run is planned.
            let parts = split_for(op, run);
            let mut out = Vec::with_capacity(parts.len());
            for (part, shift) in parts {
                out.push(match shift {
                    Some((dr, dc)) => self.plan_part(op, &adjuster, &mut templates, part, dr, dc),
                    None => PartPlan::Materialize(part),
                });
            }
            if out.iter().all(|p| matches!(p, PartPlan::Unchanged(_))) && out.len() == 1 {
                continue;
            }
            plans.push((run.first, out));
        }

        // Phase 2: apply.
        let mut shifted = Vec::new();
        let mut materialize: Vec<MemberRun> = Vec::new();
        let mut insert: Vec<MemberRun> = Vec::new();
        for (first, parts) in plans {
            self.vertex_formulas
                .virtual_members_mut()
                .remove_run(first)
                .expect("a planned run is live");
            for plan in parts {
                match plan {
                    PartPlan::Unchanged(r) => insert.push(r),
                    PartPlan::Materialize(r) => materialize.push(r),
                    PartPlan::Shifted {
                        new,
                        dr,
                        dc,
                        adjusted,
                    } => {
                        insert.push(new);
                        shifted.push(ShiftedRun {
                            first: VertexId(new.first),
                            len: new.len,
                            sheet: new.sheet,
                            col: new.col,
                            row0: new.row0,
                            moved: dr != 0 || dc != 0,
                            adjusted: adjusted.map(|b| *b),
                        });
                        if dr != 0 || dc != 0 {
                            self.store
                                .shift_member_addrs(VertexId(new.first), new.len, dr, dc);
                        }
                    }
                }
            }
        }
        // Materialized parts keep their pre-edit cells (the per-vertex
        // path moves them next); no inserted run occupies those cells
        // until the moves are done, as the shift is a bijection.
        self.restore_runs(materialize);
        for r in insert {
            self.vertex_formulas.virtual_members_mut().insert(r);
        }

        for run in &shifted {
            let ids: Vec<VertexId> = (0..run.len).map(|i| VertexId(run.first.0 + i)).collect();
            if run.moved {
                #[cfg(any(test, feature = "legacy_oracle"))]
                for (i, &v) in ids.iter().enumerate() {
                    let addr = crate::engine::addr::GridAddr::new(run.row0 + i as u32, run.col);
                    self.edges.update_addr(v, super::VertexAddr::grid(addr));
                }
                self.authority_queue_direct_dirty_run(
                    run.sheet,
                    run.col,
                    run.row0,
                    run.row0 + run.len - 1,
                );
            }
            if run.adjusted.is_some() {
                if !self.ref_error_vertices.is_empty() || !self.vertex_values.is_empty() {
                    for v in &ids {
                        self.ref_error_vertices.remove(v);
                        self.vertex_values.remove(v);
                    }
                }
                self.mark_vertices_dirty_batch(&ids);
            }
        }
        shifted
    }

    /// After the edit's per-vertex moves: legacy's per-cell rewrite
    /// re-derives each rewritten member's oracle edges (test/oracle builds
    /// only; the direct edge count and range flag are unchanged, the
    /// references being the same).
    pub(crate) fn after_shift_moves(&mut self, runs: &[ShiftedRun]) {
        #[cfg(any(test, feature = "legacy_oracle"))]
        for run in runs.iter().filter(|r| r.adjusted.is_some()) {
            for i in 0..run.len {
                let v = VertexId(run.first.0 + i);
                if let Some(ast) = self.get_formula(v) {
                    self.refresh_oracle_edges(v, &ast);
                }
            }
        }
        #[cfg(not(any(test, feature = "legacy_oracle")))]
        let _ = runs;
    }

    /// Plan one part of a run (pre-edit graph; see the module docs).
    fn plan_part(
        &mut self,
        op: &ShiftOperation,
        adjuster: &ReferenceAdjuster,
        templates: &mut FxHashMap<AstNodeId, Option<ASTNode>>,
        part: MemberRun,
        dr: i64,
        dc: i64,
    ) -> PartPlan {
        let template = templates
            .entry(part.template)
            .or_insert_with(|| self.data_store.retrieve_ast(part.template, &self.sheet_reg))
            .clone();
        let Some(template) = template else {
            return PartPlan::Materialize(part);
        };
        let last_row = part.row0 + part.len - 1;
        let (Some(f_first), Some(f_last)) = (
            member_ast(&template, part.anchor, part.row0, part.col),
            member_ast(&template, part.anchor, last_row, part.col),
        ) else {
            return PartPlan::Materialize(part);
        };
        let ctx = ReferenceContext::new(part.sheet, &self.sheet_reg);
        let a_first = adjuster.adjust_ast_if_changed_in_context(&f_first, op, &ctx);
        let a_last = adjuster.adjust_ast_if_changed_in_context(&f_last, op, &ctx);
        if a_first.is_some() != a_last.is_some() {
            return PartPlan::Materialize(part);
        }
        if a_first.is_none() && dr == 0 && dc == 0 {
            return PartPlan::Unchanged(part);
        }
        let new_row0 = i64::from(part.row0) + dr;
        let new_col = i64::from(part.col) + dc;
        let (Ok(new_row0), Ok(new_col)) = (u32::try_from(new_row0), u32::try_from(new_col)) else {
            return PartPlan::Materialize(part);
        };
        let new_last = new_row0 + part.len - 1;
        let (new_first_ast, new_last_ast) = match (&a_first, &a_last) {
            (Some(a), Some(b)) => (a.clone(), b.clone()),
            _ => (f_first.clone(), f_last.clone()),
        };
        if a_first.is_some() {
            // A rewrite keeps every reference (none became `#REF!`: the
            // direct edge count and range flag stay).
            let before = reference_count(&f_first);
            if before.is_none() || before != reference_count(&new_first_ast) {
                return PartPlan::Materialize(part);
            }
            if !self.member_deps_preserved(&f_first, &new_first_ast, &part) {
                return PartPlan::Materialize(part);
            }
        } else if reference_count(&f_first).is_none() {
            return PartPlan::Materialize(part);
        }
        // Same template at the same anchor (a move whose references move
        // with the members), else the adjusted first member anchored at
        // its new cell.
        let (template_id, anchor) = if member_ast(&template, part.anchor, new_row0, new_col)
            .as_ref()
            == Some(&new_first_ast)
            && member_ast(&template, part.anchor, new_last, new_col).as_ref() == Some(&new_last_ast)
        {
            (part.template, part.anchor)
        } else {
            let id = self.data_store.store_ast(&new_first_ast, &self.sheet_reg);
            let Some(stored) = self.data_store.retrieve_ast(id, &self.sheet_reg) else {
                return PartPlan::Materialize(part);
            };
            let anchor = (new_row0, new_col);
            if stored != new_first_ast
                || member_ast(&stored, anchor, new_last, new_col).as_ref() != Some(&new_last_ast)
            {
                return PartPlan::Materialize(part);
            }
            templates.insert(id, Some(stored));
            (id, anchor)
        };
        let new = MemberRun {
            sheet: part.sheet,
            col: new_col,
            row0: new_row0,
            len: part.len,
            first: part.first,
            template: template_id,
            anchor,
        };
        if cfg!(debug_assertions) {
            self.debug_check_shifted_part(op, adjuster, &template, &part, &new, templates);
        }
        PartPlan::Shifted {
            new,
            dr,
            dc,
            adjusted: a_first.map(|a| Box::new((f_first, a))),
        }
    }

    /// A per-cell rewrite of every member from `old` (the first member's
    /// formula before the edit) to `new` (after) would leave its graph
    /// state as the block shift does: the same direct dependency count and
    /// range flag. That holds when every small range (expanded into cell
    /// dependencies, up to the range expansion limit) keeps its area for
    /// every member: direct dependencies are counted per distinct cell,
    /// with or without a vertex (references create none, decision 27).
    fn member_deps_preserved(&self, old: &ASTNode, new: &ASTNode, part: &MemberRun) -> bool {
        let mut pairs = Vec::new();
        if !paired_references(old, new, &mut pairs) {
            return false;
        }
        let limit = self.config.range_expansion_limit as u64;
        let sheet_of = |sheet: &Option<String>| match sheet {
            None => Some(part.sheet),
            Some(name) => self.sheet_reg.get_id(name),
        };
        // Row of an endpoint for member `i`.
        let at = |row: u32, abs: bool, i: u32| if abs { row } else { row + i };
        for (x, y) in pairs {
            match (x, y) {
                (
                    ReferenceType::Cell {
                        sheet,
                        row,
                        col,
                        row_abs,
                        ..
                    },
                    ReferenceType::Cell { .. },
                ) => {
                    let _ = (row, col, row_abs);
                    if sheet_of(sheet).is_none() {
                        return false;
                    }
                }
                (
                    ReferenceType::Range {
                        sheet,
                        start_row,
                        start_col,
                        end_row,
                        end_col,
                        start_row_abs,
                        end_row_abs,
                        ..
                    },
                    ReferenceType::Range {
                        start_row: nsr,
                        start_col: nsc,
                        end_row: ner,
                        end_col: nec,
                        start_row_abs: nsra,
                        end_row_abs: nera,
                        ..
                    },
                ) => {
                    let (
                        Some(sr),
                        Some(sc),
                        Some(er),
                        Some(ec),
                        Some(nsr),
                        Some(nsc),
                        Some(ner),
                        Some(nec),
                    ) = (
                        *start_row, *start_col, *end_row, *end_col, *nsr, *nsc, *ner, *nec,
                    )
                    else {
                        // Open ranges are never expanded; an open range
                        // that became finite (or back) is not a shift.
                        if start_row.is_some() != nsr.is_some()
                            || end_row.is_some() != ner.is_some()
                            || start_col.is_some() != nsc.is_some()
                            || end_col.is_some() != nec.is_some()
                        {
                            return false;
                        }
                        continue;
                    };
                    if sheet_of(sheet).is_none() {
                        return false;
                    }
                    let width = u64::from(ec.max(sc) - ec.min(sc) + 1);
                    let nwidth = u64::from(nec.max(nsc) - nec.min(nsc) + 1);
                    for i in 0..part.len {
                        let (a, b) = (at(sr, *start_row_abs, i), at(er, *end_row_abs, i));
                        let (na, nb) = (at(nsr, *nsra, i), at(ner, *nera, i));
                        let area = u64::from(a.max(b) - a.min(b) + 1) * width;
                        let narea = u64::from(na.max(nb) - na.min(nb) + 1) * nwidth;
                        if area > limit && narea > limit {
                            continue;
                        }
                        if area != narea {
                            return false;
                        }
                    }
                }
                (ReferenceType::NamedRange(_), ReferenceType::NamedRange(_)) => {}
                _ => return false,
            }
        }
        true
    }

    /// Debug builds: every member of a shifted part has exactly the formula
    /// the per-cell rewrite would give it.
    fn debug_check_shifted_part(
        &self,
        op: &ShiftOperation,
        adjuster: &ReferenceAdjuster,
        template: &ASTNode,
        old: &MemberRun,
        new: &MemberRun,
        templates: &FxHashMap<AstNodeId, Option<ASTNode>>,
    ) {
        let new_template = templates
            .get(&new.template)
            .cloned()
            .flatten()
            .or_else(|| self.data_store.retrieve_ast(new.template, &self.sheet_reg))
            .expect("new template");
        let ctx = ReferenceContext::new(old.sheet, &self.sheet_reg);
        let first_changed = {
            let f = member_ast(template, old.anchor, old.row0, old.col).expect("first");
            adjuster
                .adjust_ast_if_changed_in_context(&f, op, &ctx)
                .is_some()
        };
        for i in 0..old.len {
            let before =
                member_ast(template, old.anchor, old.row0 + i, old.col).expect("member formula");
            let adjusted = adjuster.adjust_ast_if_changed_in_context(&before, op, &ctx);
            assert_eq!(
                adjusted.is_some(),
                first_changed,
                "member {i} of run {old:?}: changed status differs from the first member's"
            );
            let expected = adjusted.unwrap_or(before);
            let got = member_ast(&new_template, new.anchor, new.row0 + i, new.col)
                .expect("new member formula");
            assert_eq!(
                got, expected,
                "member {i} of run {old:?} -> {new:?}: block shift differs from the per-cell rewrite"
            );
        }
    }

    /// Re-derive a formula vertex's oracle edges for `ast` (test/oracle
    /// builds), leaving the release state alone: checks that the per-cell
    /// rewrite would have left it as the block shift does (same direct
    /// dependency count and range flag, no placeholder).
    #[cfg(any(test, feature = "legacy_oracle"))]
    fn refresh_oracle_edges(&mut self, v: VertexId, ast: &ASTNode) {
        let sheet_id = self.store.sheet_id(v);
        self.oracle_remove_dependent_edges(v);
        let Ok((deps, ranges, vertexless, _, _)) =
            self.extract_dependencies_with_pending_names(ast, sheet_id)
        else {
            panic!("a block-shifted member's formula resolves: {v:?} {ast}");
        };
        assert_eq!(
            self.store.edge_offset(v) as usize,
            deps.len() + vertexless.len(),
            "block shift keeps {v:?}'s direct dependency count ({ast})"
        );
        assert_eq!(
            self.store.reads_range(v),
            !ranges.is_empty(),
            "block shift keeps {v:?}'s range flag ({ast})"
        );
        self.oracle_add_dependent_edges(v, &deps);
        self.oracle_add_range_dependent_edges(v, &ranges, sheet_id);
    }
}

/// Whether `op` edits the run's own sheet.
fn touches_sheet(op: &ShiftOperation, run: &MemberRun) -> bool {
    op_sheet(op) == run.sheet
}

fn op_sheet(op: &ShiftOperation) -> SheetId {
    match *op {
        ShiftOperation::InsertRows { sheet_id, .. }
        | ShiftOperation::DeleteRows { sheet_id, .. }
        | ShiftOperation::InsertColumns { sheet_id, .. }
        | ShiftOperation::DeleteColumns { sheet_id, .. } => sheet_id,
    }
}

/// The parts of `run` and how far each moves (`None`: back to the
/// per-vertex path). A row insert splits the run at the insertion row.
/// Members in a deleted band (rows, or the run's column) cannot be here
/// (the edit removed them first); such a run takes the per-vertex path.
#[allow(clippy::type_complexity)]
fn split_for(op: &ShiftOperation, run: &MemberRun) -> Vec<(MemberRun, Option<(i64, i64)>)> {
    if !touches_sheet(op, run) {
        return vec![(*run, Some((0, 0)))];
    }
    let end = run.row0 + run.len - 1;
    let part = |row0: u32, row1: u32| MemberRun {
        row0,
        len: row1 - row0 + 1,
        first: run.first + (row0 - run.row0),
        ..*run
    };
    match *op {
        ShiftOperation::InsertRows { before, count, .. } => {
            let k = i64::from(count);
            if end < before {
                vec![(*run, Some((0, 0)))]
            } else if run.row0 >= before {
                vec![(*run, Some((k, 0)))]
            } else {
                vec![
                    (part(run.row0, before - 1), Some((0, 0))),
                    (part(before, end), Some((k, 0))),
                ]
            }
        }
        ShiftOperation::DeleteRows { start, count, .. } => {
            if end < start {
                vec![(*run, Some((0, 0)))]
            } else if run.row0 >= start + count {
                vec![(*run, Some((-i64::from(count), 0)))]
            } else {
                vec![(*run, None)]
            }
        }
        ShiftOperation::InsertColumns { before, count, .. } => {
            if run.col >= before {
                vec![(*run, Some((0, i64::from(count))))]
            } else {
                vec![(*run, Some((0, 0)))]
            }
        }
        ShiftOperation::DeleteColumns { start, count, .. } => {
            if run.col < start {
                vec![(*run, Some((0, 0)))]
            } else if run.col >= start + count {
                vec![(*run, Some((0, -i64::from(count))))]
            } else {
                vec![(*run, None)]
            }
        }
    }
}
