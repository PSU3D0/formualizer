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
    pub(crate) fallback_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpanEvalError {
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
        report.computed_write_buffer_push_count = sink.push_count();
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

    use crate::engine::eval::ComputedWrite;
    use crate::test_workbook::TestWorkbook;

    use super::super::placement::{FormulaPlacementCandidate, place_candidate_family};
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

    fn whole_span_task(span: FormulaSpanRef) -> SpanEvalTask {
        SpanEvalTask {
            span,
            dirty: DirtyDomain::WholeSpan(span),
            plane_epoch: 0,
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

        let report = eval_task(&plane, &workbook, &whole_span_task(span), &mut buffer);

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

        eval_task(&plane, &workbook, &whole_span_task(span), &mut buffer);

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
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 1, 2, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 2, 2, LiteralValue::Number(4.0));
        let mut plane = FormulaPlane::default();
        let placement = place_candidate_family(
            &mut plane,
            vec![
                candidate(0, 1, 1, "=A1+1"),
                candidate(0, 1, 2, "=B1+1"),
                candidate(0, 2, 1, "=A2+1"),
                candidate(0, 2, 2, "=B2+1"),
            ],
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(&plane, &workbook, &whole_span_task(span), &mut buffer);

        assert_eq!(
            cell_values(&buffer),
            vec![
                (1, 1, OverlayValue::Number(2.0)),
                (1, 2, OverlayValue::Number(3.0)),
                (2, 1, OverlayValue::Number(4.0)),
                (2, 2, OverlayValue::Number(5.0)),
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
        });
        let workbook = TestWorkbook::new();
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(&plane, &workbook, &whole_span_task(span), &mut buffer);

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

        let report = eval_task(&plane, &workbook, &whole_span_task(span), &mut buffer);

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

        let report = eval_task(&plane, &workbook, &whole_span_task(span), &mut buffer);

        assert_eq!(report.computed_write_buffer_push_count, buffer.len() as u64);
        assert!(!buffer.is_empty());
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
        });
        let workbook = TestWorkbook::new();
        let mut buffer = ComputedWriteBuffer::default();
        let evaluator = SpanEvaluator::new(&plane, &workbook, "Sheet1");
        let mut sink = SpanComputedWriteSink::new(&mut buffer);

        let err = evaluator
            .evaluate_task(&whole_span_task(span), &mut sink)
            .unwrap_err();

        assert_eq!(err, SpanEvalError::UnsupportedReferenceRelocation);
        assert!(buffer.is_empty());
    }
}
