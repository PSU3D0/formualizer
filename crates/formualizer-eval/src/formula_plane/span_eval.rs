//! Internal scalar FormulaPlane span evaluator for FP6.4.
//!
//! This is direct FormulaPlane substrate only. It evaluates accepted spans with
//! existing scalar interpreter semantics and stages results into
//! `ComputedWriteBuffer`; it does not integrate with normal engine scheduling.

use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

use crate::arrow_store::{OverlayValue, map_error_code};
use crate::engine::eval::ComputedWriteBuffer;
use crate::interpreter::Interpreter;
use crate::reference::CellRef;
use crate::traits::EvaluationContext;

use super::region_index::{DirtyDomain, RegionKey};
use super::runtime::{FormulaPlane, FormulaSpan, FormulaSpanRef, PlacementCoord, PlacementDomain};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanEvalTask {
    pub(crate) span: FormulaSpanRef,
    pub(crate) dirty: DirtyDomain,
    pub(crate) plane_epoch: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SpanEvalReport {
    pub(crate) span_eval_task_count: u64,
    pub(crate) span_eval_placement_count: u64,
    pub(crate) skipped_overlay_punchout_count: u64,
    pub(crate) computed_write_buffer_push_count: u64,
    pub(crate) transient_ast_relocation_count: u64,
    pub(crate) fallback_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpanEvalError {
    StalePlaneEpoch,
    StaleSpan,
    MissingTemplate,
    UnsupportedDirtyDomain,
    UnsupportedReferenceRelocation,
}

pub(crate) struct SpanComputedWriteSink<'a> {
    buffer: &'a mut ComputedWriteBuffer,
    push_count: u64,
}

impl<'a> SpanComputedWriteSink<'a> {
    pub(crate) fn new(buffer: &'a mut ComputedWriteBuffer) -> Self {
        Self {
            buffer,
            push_count: 0,
        }
    }

    pub(crate) fn push_cell(&mut self, placement: PlacementCoord, value: OverlayValue) {
        self.buffer
            .push_cell(placement.sheet_id, placement.row, placement.col, value);
        self.push_count = self.push_count.saturating_add(1);
    }

    pub(crate) fn push_count(&self) -> u64 {
        self.push_count
    }
}

pub(crate) struct SpanEvaluator<'a> {
    plane: &'a FormulaPlane,
    context: &'a dyn EvaluationContext,
    current_sheet: &'a str,
}

impl<'a> SpanEvaluator<'a> {
    pub(crate) fn new(
        plane: &'a FormulaPlane,
        context: &'a dyn EvaluationContext,
        current_sheet: &'a str,
    ) -> Self {
        Self {
            plane,
            context,
            current_sheet,
        }
    }

    pub(crate) fn evaluate_task(
        &self,
        task: &SpanEvalTask,
        sink: &mut SpanComputedWriteSink<'_>,
    ) -> Result<SpanEvalReport, SpanEvalError> {
        if self.plane.epoch().0 != task.plane_epoch {
            return Err(SpanEvalError::StalePlaneEpoch);
        }

        let span = self
            .plane
            .spans
            .get(task.span)
            .ok_or(SpanEvalError::StaleSpan)?;
        let template = self
            .plane
            .templates
            .get(span.template_id)
            .ok_or(SpanEvalError::MissingTemplate)?;
        let origin = domain_origin(&span.domain);
        let placements = placements_for_dirty(span, &task.dirty)?;
        let push_count_before = sink.push_count();

        let mut report = SpanEvalReport {
            span_eval_task_count: 1,
            ..SpanEvalReport::default()
        };
        for placement in placements {
            if self.plane.formula_overlay.find_at(placement).is_some() {
                report.skipped_overlay_punchout_count =
                    report.skipped_overlay_punchout_count.saturating_add(1);
                continue;
            }

            let relocated = relocate_ast_for_placement(&template.ast, origin, placement)?;
            report.transient_ast_relocation_count =
                report.transient_ast_relocation_count.saturating_add(1);
            let interpreter = Interpreter::new_with_cell(
                self.context,
                self.current_sheet,
                CellRef::new_absolute(placement.sheet_id, placement.row, placement.col),
            );
            let value = match interpreter.evaluate_ast(&relocated) {
                Ok(calc) => literal_to_overlay(calc.into_literal()),
                Err(err) => OverlayValue::Error(map_error_code(err.kind)),
            };
            sink.push_cell(placement, value);
            report.span_eval_placement_count = report.span_eval_placement_count.saturating_add(1);
        }
        report.computed_write_buffer_push_count =
            sink.push_count().saturating_sub(push_count_before);
        Ok(report)
    }
}

fn placements_for_dirty(
    span: &FormulaSpan,
    dirty: &DirtyDomain,
) -> Result<Vec<PlacementCoord>, SpanEvalError> {
    match dirty {
        DirtyDomain::WholeSpan(span_ref) => {
            if span_ref.id != span.id || span_ref.generation != span.generation {
                return Err(SpanEvalError::StaleSpan);
            }
            Ok(span.domain.iter().collect())
        }
        DirtyDomain::Cells(keys) => Ok(keys
            .iter()
            .copied()
            .map(PlacementCoord::from)
            .filter(|coord| span.domain.contains(*coord))
            .collect()),
        DirtyDomain::Regions(regions) => Ok(span
            .domain
            .iter()
            .filter(|coord| {
                let key = RegionKey::from(*coord);
                regions.iter().any(|region| region.contains_key(key))
            })
            .collect()),
    }
}

impl From<RegionKey> for PlacementCoord {
    fn from(key: RegionKey) -> Self {
        PlacementCoord::new(key.sheet_id, key.row, key.col)
    }
}

fn domain_origin(domain: &PlacementDomain) -> PlacementCoord {
    match *domain {
        PlacementDomain::RowRun {
            sheet_id,
            row_start,
            col,
            ..
        } => PlacementCoord::new(sheet_id, row_start, col),
        PlacementDomain::ColRun {
            sheet_id,
            row,
            col_start,
            ..
        } => PlacementCoord::new(sheet_id, row, col_start),
        PlacementDomain::Rect {
            sheet_id,
            row_start,
            col_start,
            ..
        } => PlacementCoord::new(sheet_id, row_start, col_start),
    }
}

fn relocate_ast_for_placement(
    ast: &ASTNode,
    origin: PlacementCoord,
    target: PlacementCoord,
) -> Result<ASTNode, SpanEvalError> {
    let node_type = match &ast.node_type {
        ASTNodeType::Literal(value) => ASTNodeType::Literal(value.clone()),
        ASTNodeType::Reference {
            original,
            reference,
        } => ASTNodeType::Reference {
            original: original.clone(),
            reference: relocate_reference(reference, origin, target)?,
        },
        ASTNodeType::UnaryOp { op, expr } => ASTNodeType::UnaryOp {
            op: op.clone(),
            expr: Box::new(relocate_ast_for_placement(expr, origin, target)?),
        },
        ASTNodeType::BinaryOp { op, left, right } => ASTNodeType::BinaryOp {
            op: op.clone(),
            left: Box::new(relocate_ast_for_placement(left, origin, target)?),
            right: Box::new(relocate_ast_for_placement(right, origin, target)?),
        },
        ASTNodeType::Function { name, args } => ASTNodeType::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| relocate_ast_for_placement(arg, origin, target))
                .collect::<Result<Vec<_>, _>>()?,
        },
        ASTNodeType::Call { callee, args } => ASTNodeType::Call {
            callee: Box::new(relocate_ast_for_placement(callee, origin, target)?),
            args: args
                .iter()
                .map(|arg| relocate_ast_for_placement(arg, origin, target))
                .collect::<Result<Vec<_>, _>>()?,
        },
        ASTNodeType::Array(rows) => ASTNodeType::Array(
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| relocate_ast_for_placement(cell, origin, target))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
    };
    Ok(ASTNode {
        node_type,
        source_token: ast.source_token.clone(),
        contains_volatile: ast.contains_volatile,
    })
}

fn relocate_reference(
    reference: &ReferenceType,
    origin: PlacementCoord,
    target: PlacementCoord,
) -> Result<ReferenceType, SpanEvalError> {
    let row_delta = i64::from(target.row) - i64::from(origin.row);
    let col_delta = i64::from(target.col) - i64::from(origin.col);
    match reference {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => Ok(ReferenceType::Cell {
            sheet: sheet.clone(),
            row: shift_axis(*row, row_delta, *row_abs)?,
            col: shift_axis(*col, col_delta, *col_abs)?,
            row_abs: *row_abs,
            col_abs: *col_abs,
        }),
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => Ok(ReferenceType::Range {
            sheet: sheet.clone(),
            start_row: shift_optional_axis(*start_row, row_delta, *start_row_abs)?,
            start_col: shift_optional_axis(*start_col, col_delta, *start_col_abs)?,
            end_row: shift_optional_axis(*end_row, row_delta, *end_row_abs)?,
            end_col: shift_optional_axis(*end_col, col_delta, *end_col_abs)?,
            start_row_abs: *start_row_abs,
            start_col_abs: *start_col_abs,
            end_row_abs: *end_row_abs,
            end_col_abs: *end_col_abs,
        }),
        ReferenceType::NamedRange(_)
        | ReferenceType::Table(_)
        | ReferenceType::Cell3D { .. }
        | ReferenceType::Range3D { .. }
        | ReferenceType::External(_) => Err(SpanEvalError::UnsupportedReferenceRelocation),
    }
}

fn shift_optional_axis(
    value: Option<u32>,
    delta: i64,
    is_absolute: bool,
) -> Result<Option<u32>, SpanEvalError> {
    value
        .map(|value| shift_axis(value, delta, is_absolute))
        .transpose()
}

fn shift_axis(value: u32, delta: i64, is_absolute: bool) -> Result<u32, SpanEvalError> {
    if is_absolute {
        return Ok(value);
    }
    let shifted = i64::from(value) + delta;
    if shifted < 1 || shifted > i64::from(u32::MAX) {
        return Err(SpanEvalError::UnsupportedReferenceRelocation);
    }
    Ok(shifted as u32)
}

fn literal_to_overlay(value: LiteralValue) -> OverlayValue {
    match value {
        LiteralValue::Int(i) => OverlayValue::Number(i as f64),
        LiteralValue::Number(n) => OverlayValue::Number(n),
        LiteralValue::Text(s) => OverlayValue::Text(Arc::from(s)),
        LiteralValue::Boolean(b) => OverlayValue::Boolean(b),
        LiteralValue::Array(mut rows) => rows
            .get_mut(0)
            .and_then(|row| row.get_mut(0))
            .cloned()
            .map(literal_to_overlay)
            .unwrap_or(OverlayValue::Empty),
        LiteralValue::Date(_) | LiteralValue::DateTime(_) | LiteralValue::Time(_) => value
            .as_serial_number()
            .map(OverlayValue::DateTime)
            .unwrap_or(OverlayValue::Empty),
        LiteralValue::Duration(_) => value
            .as_serial_number()
            .map(OverlayValue::Duration)
            .unwrap_or(OverlayValue::Empty),
        LiteralValue::Empty => OverlayValue::Empty,
        LiteralValue::Pending => OverlayValue::Pending,
        LiteralValue::Error(err) => OverlayValue::Error(map_error_code(err.kind)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::parse;

    use crate::engine::EvalConfig;
    use crate::engine::eval::{ComputedWrite, Engine};
    use crate::test_workbook::TestWorkbook;

    use super::super::placement::{FormulaPlacementCandidate, place_candidate_family};
    use super::super::region_index::RegionPattern;
    use super::super::runtime::{FormulaOverlayEntryKind, NewFormulaSpan, ResultRegion};
    use super::*;

    fn candidate(sheet_id: u16, row: u32, col: u32, formula: &str) -> FormulaPlacementCandidate {
        FormulaPlacementCandidate::new(
            sheet_id,
            row,
            col,
            Arc::new(parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"))),
            Some(Arc::<str>::from(formula)),
        )
    }

    fn whole_span_task(plane: &FormulaPlane, span: FormulaSpanRef) -> SpanEvalTask {
        SpanEvalTask {
            span,
            dirty: DirtyDomain::WholeSpan(span),
            plane_epoch: plane.epoch().0,
        }
    }

    fn cells_task(
        plane: &FormulaPlane,
        span: FormulaSpanRef,
        cells: Vec<RegionKey>,
    ) -> SpanEvalTask {
        SpanEvalTask {
            span,
            dirty: DirtyDomain::Cells(cells),
            plane_epoch: plane.epoch().0,
        }
    }

    fn regions_task(
        plane: &FormulaPlane,
        span: FormulaSpanRef,
        regions: Vec<RegionPattern>,
    ) -> SpanEvalTask {
        SpanEvalTask {
            span,
            dirty: DirtyDomain::Regions(regions),
            plane_epoch: plane.epoch().0,
        }
    }

    fn eval_task(
        plane: &FormulaPlane,
        workbook: &TestWorkbook,
        task: &SpanEvalTask,
        buffer: &mut ComputedWriteBuffer,
    ) -> SpanEvalReport {
        let evaluator = SpanEvaluator::new(plane, workbook, "Sheet1");
        let mut sink = SpanComputedWriteSink::new(buffer);
        evaluator.evaluate_task(task, &mut sink).unwrap()
    }

    fn arrow_eval_config() -> EvalConfig {
        EvalConfig {
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            ..Default::default()
        }
    }

    fn span_from_report(
        report: &super::super::placement::FormulaPlacementReport,
    ) -> FormulaSpanRef {
        match report.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        }
    }

    fn cell_values(buffer: &ComputedWriteBuffer) -> Vec<(u32, u32, OverlayValue)> {
        buffer
            .writes()
            .iter()
            .map(|write| match write {
                ComputedWrite::Cell {
                    row0, col0, value, ..
                } => (*row0, *col0, value.clone()),
                ComputedWrite::Rect { .. } => panic!("span evaluator should push cells"),
            })
            .collect()
    }

    fn computed_overlay_stats(
        engine: &Engine<TestWorkbook>,
        sheet: &str,
        col0: usize,
        row0: usize,
    ) -> crate::arrow_store::OverlayDebugStats {
        let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
        let (chunk_idx, _) = asheet.chunk_of_row(row0).expect("row chunk");
        asheet.columns[col0]
            .chunk(chunk_idx)
            .expect("column chunk")
            .computed_overlay
            .debug_stats()
    }

    fn range_number_values(
        engine: &Engine<TestWorkbook>,
        sheet: &str,
        sr: u32,
        sc: u32,
        er: u32,
        ec: u32,
    ) -> Vec<f64> {
        let view = engine.read_range_values(sheet, sr, sc, er, ec);
        let rows = er.saturating_sub(sr).saturating_add(1) as usize;
        let cols = view.slice_numbers(0, rows);
        assert_eq!(cols.len(), ec.saturating_sub(sc).saturating_add(1) as usize);
        let arr = cols[0].as_ref().expect("numeric column");
        (0..arr.len()).map(|idx| arr.value(idx)).collect()
    }

    #[test]
    fn span_eval_row_run_matches_legacy_outputs() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 1, 2, LiteralValue::Number(10.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 2, 2, LiteralValue::Number(20.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 3, 2, LiteralValue::Number(30.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 0, 2, "=A1+B1"),
                candidate(0, 1, 2, "=A2+B2"),
                candidate(0, 2, 2, "=A3+B3"),
            ],
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );

        assert_eq!(report.span_eval_placement_count, 3);
        assert_eq!(report.computed_write_buffer_push_count, 3);
        assert_eq!(
            cell_values(&buffer),
            vec![
                (0, 2, OverlayValue::Number(11.0)),
                (1, 2, OverlayValue::Number(22.0)),
                (2, 2, OverlayValue::Number(33.0)),
            ]
        );
    }

    #[test]
    fn span_eval_col_run_matches_legacy_outputs() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(10.0))
            .with_cell("Sheet1", 1, 2, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 2, 2, LiteralValue::Number(20.0))
            .with_cell("Sheet1", 1, 3, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 2, 3, LiteralValue::Number(30.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 2, 0, "=A1+A2"),
                candidate(0, 2, 1, "=B1+B2"),
                candidate(0, 2, 2, "=C1+C2"),
            ],
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );

        assert_eq!(
            cell_values(&buffer),
            vec![
                (2, 0, OverlayValue::Number(11.0)),
                (2, 1, OverlayValue::Number(22.0)),
                (2, 2, OverlayValue::Number(33.0)),
            ]
        );
    }

    #[test]
    fn span_eval_rect_matches_legacy_outputs() {
        // Use externally-anchored reads so the rect family has no internal
        // dependency: every cell reads $A$1, none of which is in the rect.
        let workbook = TestWorkbook::new().with_cell("Sheet1", 1, 1, LiteralValue::Number(10.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 1, 1, "=$A$1+1"),
                candidate(0, 1, 2, "=$A$1+1"),
                candidate(0, 2, 1, "=$A$1+1"),
                candidate(0, 2, 2, "=$A$1+1"),
            ],
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );

        assert_eq!(
            cell_values(&buffer),
            vec![
                (1, 1, OverlayValue::Number(11.0)),
                (1, 2, OverlayValue::Number(11.0)),
                (2, 1, OverlayValue::Number(11.0)),
                (2, 2, OverlayValue::Number(11.0)),
            ]
        );
    }

    #[test]
    fn span_eval_preserves_explicit_empty_outputs() {
        let mut plane = FormulaPlane::default();
        let template_id = plane.intern_template(
            Arc::<str>::from("empty-template"),
            Arc::new(ASTNode::new(
                ASTNodeType::Literal(LiteralValue::Empty),
                None,
            )),
            Some(Arc::<str>::from("=")),
        );
        let domain = PlacementDomain::row_run(0, 0, 1, 0);
        let span = plane.insert_span(NewFormulaSpan {
            sheet_id: 0,
            template_id,
            result_region: ResultRegion::scalar_cells(domain.clone()),
            domain,
            intrinsic_mask_id: None,
            read_summary_id: None,
        });
        let workbook = TestWorkbook::new();
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );

        assert_eq!(
            cell_values(&buffer),
            vec![(0, 0, OverlayValue::Empty), (1, 0, OverlayValue::Empty)]
        );
    }

    #[test]
    fn span_eval_effective_domain_skips_overlay_punchouts() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(2.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![candidate(0, 0, 1, "=A1+1"), candidate(0, 1, 1, "=A2+1")],
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 1, 1, 1),
            FormulaOverlayEntryKind::ValueOverride,
            Some(span),
        );
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );

        assert_eq!(report.span_eval_placement_count, 1);
        assert_eq!(report.skipped_overlay_punchout_count, 1);
        assert_eq!(
            cell_values(&buffer),
            vec![(0, 1, OverlayValue::Number(2.0))]
        );
    }

    #[test]
    fn span_eval_writes_through_computed_write_buffer_not_direct_overlay() {
        let workbook = TestWorkbook::new().with_cell("Sheet1", 1, 1, LiteralValue::Number(9.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![candidate(0, 0, 1, "=A1+1"), candidate(0, 1, 1, "=A2+1")],
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );

        assert_eq!(report.computed_write_buffer_push_count, buffer.len() as u64);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn span_eval_cells_dirty_domain_evaluates_only_matching_cells() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(3.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 0, 1, "=A1+1"),
                candidate(0, 1, 1, "=A2+1"),
                candidate(0, 2, 1, "=A3+1"),
            ],
        );
        let span = span_from_report(&placement);
        let mut buffer = ComputedWriteBuffer::default();
        let task = cells_task(
            &plane,
            span,
            vec![RegionKey::new(0, 1, 1), RegionKey::new(0, 99, 1)],
        );

        let report = eval_task(&plane, &workbook, &task, &mut buffer);

        assert_eq!(report.span_eval_placement_count, 1);
        assert_eq!(report.transient_ast_relocation_count, 1);
        assert_eq!(
            cell_values(&buffer),
            vec![(1, 1, OverlayValue::Number(3.0))]
        );
    }

    #[test]
    fn span_eval_regions_dirty_domain_intersects_span_domain() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(3.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 0, 1, "=A1+1"),
                candidate(0, 1, 1, "=A2+1"),
                candidate(0, 2, 1, "=A3+1"),
            ],
        );
        let span = span_from_report(&placement);
        let mut buffer = ComputedWriteBuffer::default();
        let task = regions_task(&plane, span, vec![RegionPattern::rect(0, 1, 2, 1, 1)]);

        let report = eval_task(&plane, &workbook, &task, &mut buffer);

        assert_eq!(report.span_eval_placement_count, 2);
        assert_eq!(report.transient_ast_relocation_count, 2);
        assert_eq!(
            cell_values(&buffer),
            vec![
                (1, 1, OverlayValue::Number(3.0)),
                (2, 1, OverlayValue::Number(4.0))
            ]
        );
    }

    #[test]
    fn span_eval_stale_span_generation_fails_closed() {
        let workbook = TestWorkbook::new();
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![candidate(0, 0, 1, "=1"), candidate(0, 1, 1, "=1")],
        );
        let span = span_from_report(&placement);
        let task = whole_span_task(&plane, span);
        assert!(plane.remove_span(span));
        let stale_generation_task = SpanEvalTask {
            plane_epoch: plane.epoch().0,
            ..task
        };
        let mut buffer = ComputedWriteBuffer::default();
        let evaluator = SpanEvaluator::new(&plane, &workbook, "Sheet1");
        let mut sink = SpanComputedWriteSink::new(&mut buffer);

        let err = evaluator
            .evaluate_task(&stale_generation_task, &mut sink)
            .unwrap_err();

        assert_eq!(err, SpanEvalError::StaleSpan);
        assert!(buffer.is_empty());
    }

    #[test]
    fn span_eval_stale_plane_epoch_fails_closed() {
        let workbook = TestWorkbook::new();
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![candidate(0, 0, 1, "=1"), candidate(0, 1, 1, "=1")],
        );
        let span = span_from_report(&placement);
        let task = whole_span_task(&plane, span);
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 0, 0, 1),
            FormulaOverlayEntryKind::ValueOverride,
            Some(span),
        );
        let mut buffer = ComputedWriteBuffer::default();
        let evaluator = SpanEvaluator::new(&plane, &workbook, "Sheet1");
        let mut sink = SpanComputedWriteSink::new(&mut buffer);

        let err = evaluator.evaluate_task(&task, &mut sink).unwrap_err();

        assert_eq!(err, SpanEvalError::StalePlaneEpoch);
        assert!(buffer.is_empty());
    }

    #[test]
    fn span_eval_absolute_refs_match_legacy_engine_outputs() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(4.0))
            .with_cell("Sheet1", 1, 6, LiteralValue::Number(10.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 0, 2, "=A1*$F$1"),
                candidate(0, 1, 2, "=A2*$F$1"),
                candidate(0, 2, 2, "=A3*$F$1"),
            ],
        );
        let span = span_from_report(&placement);
        let mut formula_plane_engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(formula_plane_engine.graph.sheet_id_mut("Sheet1"), 0);
        let mut buffer = ComputedWriteBuffer::default();
        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );
        formula_plane_engine
            .flush_computed_write_buffer(&mut buffer)
            .unwrap();

        let mut legacy = Engine::new(TestWorkbook::new(), arrow_eval_config());
        legacy
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(4.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(10.0))
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 1, 3, parse("=A1*$F$1").unwrap())
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 2, 3, parse("=A2*$F$1").unwrap())
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 3, 3, parse("=A3*$F$1").unwrap())
            .unwrap();
        legacy.evaluate_all().unwrap();

        for row in 1..=3 {
            assert_eq!(
                formula_plane_engine.get_cell_value("Sheet1", row, 3),
                legacy.get_cell_value("Sheet1", row, 3)
            );
        }
    }

    #[test]
    fn span_eval_div_zero_error_matches_legacy_engine_outputs() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 1, 2, LiteralValue::Number(0.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 2, 2, LiteralValue::Number(0.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![candidate(0, 0, 2, "=A1/B1"), candidate(0, 1, 2, "=A2/B2")],
        );
        let span = span_from_report(&placement);
        let mut formula_plane_engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(formula_plane_engine.graph.sheet_id_mut("Sheet1"), 0);
        let mut buffer = ComputedWriteBuffer::default();
        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );
        formula_plane_engine
            .flush_computed_write_buffer(&mut buffer)
            .unwrap();

        let mut legacy = Engine::new(TestWorkbook::new(), arrow_eval_config());
        legacy
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(0.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(0.0))
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 1, 3, parse("=A1/B1").unwrap())
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 2, 3, parse("=A2/B2").unwrap())
            .unwrap();
        legacy.evaluate_all().unwrap();

        for row in 1..=2 {
            assert_eq!(
                formula_plane_engine.get_cell_value("Sheet1", row, 3),
                legacy.get_cell_value("Sheet1", row, 3)
            );
        }
    }

    #[test]
    fn span_eval_varying_outputs_emit_dense_fragment_and_rangeview_reads_results() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 4, 1, LiteralValue::Number(4.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 0, 2, "=A1+1"),
                candidate(0, 1, 2, "=A2+1"),
                candidate(0, 2, 2, "=A3+1"),
                candidate(0, 3, 2, "=A4+1"),
            ],
        );
        let span = span_from_report(&placement);
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(engine.graph.sheet_id_mut("Sheet1"), 0);
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );
        engine.flush_computed_write_buffer(&mut buffer).unwrap();

        assert_eq!(report.transient_ast_relocation_count, 4);
        let stats = computed_overlay_stats(&engine, "Sheet1", 2, 0);
        assert_eq!(stats.dense_fragments, 1);
        assert_eq!(stats.run_fragments, 0);
        assert_eq!(
            range_number_values(&engine, "Sheet1", 1, 3, 4, 3),
            vec![2.0, 3.0, 4.0, 5.0]
        );
    }

    #[test]
    fn span_eval_constant_outputs_emit_run_fragment() {
        let workbook = TestWorkbook::new();
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            (0..8).map(|row| candidate(0, row, 0, "=7")).collect(),
        );
        let span = span_from_report(&placement);
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(engine.graph.sheet_id_mut("Sheet1"), 0);
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );
        engine.flush_computed_write_buffer(&mut buffer).unwrap();

        let stats = computed_overlay_stats(&engine, "Sheet1", 0, 0);
        assert_eq!(stats.run_fragments, 1);
        assert_eq!(stats.dense_fragments, 0);
        assert_eq!(
            range_number_values(&engine, "Sheet1", 1, 1, 8, 1),
            vec![7.0; 8]
        );
    }

    #[test]
    fn span_eval_sparse_dirty_domain_emits_sparse_fragment() {
        let workbook = TestWorkbook::new();
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            (0..128).map(|row| candidate(0, row, 0, "=1")).collect(),
        );
        let span = span_from_report(&placement);
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(engine.graph.sheet_id_mut("Sheet1"), 0);
        let dirty_cells = (0..128)
            .step_by(2)
            .map(|row| RegionKey::new(0, row, 0))
            .collect();
        let task = cells_task(&plane, span, dirty_cells);
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(&plane, &workbook, &task, &mut buffer);
        engine.flush_computed_write_buffer(&mut buffer).unwrap();

        assert_eq!(report.span_eval_placement_count, 64);
        let stats = computed_overlay_stats(&engine, "Sheet1", 0, 0);
        assert_eq!(stats.sparse_fragments, 1);
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(1.0))
        );
        assert_eq!(engine.get_cell_value("Sheet1", 2, 1), None);
        assert_eq!(
            engine.get_cell_value("Sheet1", 127, 1),
            Some(LiteralValue::Number(1.0))
        );
        assert_eq!(engine.get_cell_value("Sheet1", 128, 1), None);
    }

    #[test]
    fn span_eval_user_overlay_masks_computed_span_result_after_flush() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(3.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 0, 2, "=A1+1"),
                candidate(0, 1, 2, "=A2+1"),
                candidate(0, 2, 2, "=A3+1"),
            ],
        );
        let span = span_from_report(&placement);
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(engine.graph.sheet_id_mut("Sheet1"), 0);
        {
            let mut ingest = engine.begin_bulk_ingest_arrow();
            ingest.add_sheet("Sheet1", 3, 8);
            for _ in 0..3 {
                ingest
                    .append_row(
                        "Sheet1",
                        &[
                            LiteralValue::Empty,
                            LiteralValue::Empty,
                            LiteralValue::Empty,
                        ],
                    )
                    .unwrap();
            }
            ingest.finish().unwrap();
        }
        {
            let asheet = engine.sheet_store_mut().sheet_mut("Sheet1").unwrap();
            let (chunk_idx, offset) = asheet.chunk_of_row(1).unwrap();
            asheet.columns[2].chunks[chunk_idx]
                .overlay
                .set_scalar(offset, OverlayValue::Text("user".into()));
        }
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
        );
        engine.flush_computed_write_buffer(&mut buffer).unwrap();

        let stats = computed_overlay_stats(&engine, "Sheet1", 2, 0);
        assert_eq!(stats.dense_fragments, 1);
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 3),
            Some(LiteralValue::Number(2.0))
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", 2, 3),
            Some(LiteralValue::Text("user".into()))
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", 3, 3),
            Some(LiteralValue::Number(4.0))
        );
    }

    #[test]
    fn span_eval_fallback_for_unsupported_template_matches_legacy() {
        let mut plane = FormulaPlane::default();
        let template_id = plane.intern_template(
            Arc::<str>::from("external-ref"),
            Arc::new(parse("='[book.xlsx]Sheet1'!A1").unwrap()),
            Some(Arc::<str>::from("='[book.xlsx]Sheet1'!A1")),
        );
        let domain = PlacementDomain::row_run(0, 0, 1, 0);
        let span = plane.insert_span(NewFormulaSpan {
            sheet_id: 0,
            template_id,
            result_region: ResultRegion::scalar_cells(domain.clone()),
            domain,
            intrinsic_mask_id: None,
            read_summary_id: None,
        });
        let workbook = TestWorkbook::new();
        let mut buffer = ComputedWriteBuffer::default();
        let evaluator = SpanEvaluator::new(&plane, &workbook, "Sheet1");
        let mut sink = SpanComputedWriteSink::new(&mut buffer);

        let err = evaluator
            .evaluate_task(&whole_span_task(&plane, span), &mut sink)
            .unwrap_err();

        assert_eq!(err, SpanEvalError::UnsupportedReferenceRelocation);
        assert!(buffer.is_empty());
    }
}
