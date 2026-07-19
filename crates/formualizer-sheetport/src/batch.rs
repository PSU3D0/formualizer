use crate::SheetPortError;
use crate::runtime::{EvalOptions, ResolvedEvaluationRequest, SheetPort};
use crate::value::{InputUpdate, OutputSnapshot};
use formualizer_common::ExcelErrorExtra;
use formualizer_eval::engine::RecalcPlan;

type ProgressCallback<'a> = Box<dyn FnMut(BatchProgress<'_>) + Send + 'a>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BatchPlanStalePolicy {
    #[default]
    Error,
    RebuildOnStale,
}

/// Execution options for batch runs.
#[derive(Default)]
pub struct BatchOptions<'a> {
    pub eval: EvalOptions,
    pub concurrency: Option<usize>,
    pub progress: Option<ProgressCallback<'a>>,
    pub stale_policy: BatchPlanStalePolicy,
}

#[derive(Debug, Clone)]
pub struct BatchProgress<'a> {
    pub completed: usize,
    pub total: usize,
    pub scenario_id: &'a str,
}

#[derive(Debug, Clone)]
pub struct BatchInput {
    pub id: String,
    pub update: InputUpdate,
}

impl BatchInput {
    pub fn new(id: impl Into<String>, update: InputUpdate) -> Self {
        Self {
            id: id.into(),
            update,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InputUpdate, PortValue, SheetPort};
    use formualizer_common::{ExcelErrorKind, LiteralValue};
    use formualizer_parse::parser::parse;
    use formualizer_workbook::Workbook;
    use sheetport_spec::Manifest;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn manifest() -> Manifest {
        Manifest::from_yaml_str(
            r#"
spec: fio
spec_version: "0.3.0"
manifest: { id: batch-plan, name: Batch Plan }
ports:
  - id: input
    dir: in
    shape: scalar
    location: { a1: Sheet!A1 }
    schema: { type: number }
  - id: output
    dir: out
    shape: scalar
    location: { a1: Sheet!B1 }
    schema: { type: number }
"#,
        )
        .unwrap()
    }

    fn workbook() -> Workbook {
        let mut workbook = Workbook::new();
        workbook.add_sheet("Sheet").unwrap();
        workbook
            .set_value("Sheet", 1, 1, LiteralValue::Number(2.0))
            .unwrap();
        workbook.set_formula("Sheet", 1, 2, "=A1*2").unwrap();
        workbook
    }

    fn scenario(value: f64) -> BatchInput {
        let mut update = InputUpdate::new();
        update.insert("input", PortValue::Scalar(LiteralValue::Number(value)));
        BatchInput::new("case", update)
    }

    #[test]
    fn stale_policy_error_and_rebuild_are_bounded_and_deterministic() {
        let mut workbook_one = workbook();
        let mut port = SheetPort::new(&mut workbook_one, manifest()).unwrap();
        let mut executor = port.batch(BatchOptions::default()).unwrap();
        executor
            .sheetport
            .workbook_mut()
            .engine_mut()
            .set_cell_formula("Sheet", 1, 2, parse("=A1*3").unwrap())
            .unwrap();
        let error = executor.run(Vec::<BatchInput>::new()).unwrap_err();
        assert!(is_plan_stale_for_test(&error));
        assert_eq!(executor.rebuild_attempts_for_test, 0);

        let mut workbook = workbook();
        let mut port = SheetPort::new(&mut workbook, manifest()).unwrap();
        let mut executor = port
            .batch(BatchOptions {
                stale_policy: BatchPlanStalePolicy::RebuildOnStale,
                ..BatchOptions::default()
            })
            .unwrap();
        executor
            .sheetport
            .workbook_mut()
            .engine_mut()
            .set_cell_formula("Sheet", 1, 2, parse("=A1*3").unwrap())
            .unwrap();
        let results = executor.run([scenario(3.0)]).unwrap();
        assert_eq!(executor.rebuild_attempts_for_test, 1);
        assert_eq!(
            results[0].outputs.get("output"),
            Some(&PortValue::Scalar(LiteralValue::Number(9.0)))
        );
        assert_eq!(
            executor.sheetport.workbook().get_value("Sheet", 1, 1),
            Some(LiteralValue::Number(2.0))
        );
    }

    #[test]
    fn stale_primary_and_restoration_preserve_both_errors_in_priority_order() {
        let mut workbook = workbook();
        let mut port = SheetPort::new(&mut workbook, manifest()).unwrap();
        let mut executor = port.batch(BatchOptions::default()).unwrap();
        executor
            .sheetport
            .workbook_mut()
            .engine_mut()
            .set_cell_formula("Sheet", 1, 2, parse("=A1*3").unwrap())
            .unwrap();
        let mut invalid = InputUpdate::new();
        invalid.insert(
            "input",
            PortValue::Scalar(LiteralValue::Text("not a number".to_string())),
        );
        let error = executor
            .run([BatchInput::new("invalid", invalid)])
            .unwrap_err();
        let SheetPortError::BatchRestoration {
            primary,
            restoration,
        } = error
        else {
            panic!("expected both failures")
        };
        assert!(matches!(
            *primary,
            SheetPortError::ConstraintViolation { .. }
        ));
        assert!(is_plan_stale_for_test(&restoration));
    }

    #[test]
    fn cancelled_batch_restores_baseline_without_reusing_cancelled_token() {
        let cancel = Arc::new(AtomicBool::new(false));
        let mut workbook = workbook();
        let mut port = SheetPort::new(&mut workbook, manifest()).unwrap();
        let mut executor = port
            .batch(BatchOptions {
                eval: EvalOptions {
                    cancel: Some(Arc::clone(&cancel)),
                    ..EvalOptions::default()
                },
                ..BatchOptions::default()
            })
            .unwrap();
        cancel.store(true, Ordering::Relaxed);
        let error = executor.run([scenario(7.0)]).unwrap_err();
        assert_eq!(excel_kind_for_test(&error), Some(ExcelErrorKind::Cancelled));
        assert_eq!(
            executor.sheetport.workbook().get_value("Sheet", 1, 1),
            Some(LiteralValue::Number(2.0))
        );
        assert_eq!(
            executor.sheetport.workbook().get_value("Sheet", 1, 2),
            Some(LiteralValue::Number(4.0))
        );
    }

    fn excel_kind_for_test(error: &SheetPortError) -> Option<ExcelErrorKind> {
        match error {
            SheetPortError::Engine { source } => Some(source.kind),
            SheetPortError::Workbook {
                source: formualizer_workbook::error::IoError::Engine(source),
            } => Some(source.kind),
            _ => None,
        }
    }

    fn is_plan_stale_for_test(error: &SheetPortError) -> bool {
        match error {
            SheetPortError::Engine { source } => matches!(
                source.extra,
                formualizer_common::ExcelErrorExtra::PlanStale { .. }
            ),
            SheetPortError::Workbook {
                source: formualizer_workbook::error::IoError::Engine(source),
            } => matches!(
                source.extra,
                formualizer_common::ExcelErrorExtra::PlanStale { .. }
            ),
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchResult {
    pub id: String,
    pub outputs: OutputSnapshot,
}

pub struct BatchExecutor<'a> {
    sheetport: &'a mut SheetPort<'a>,
    baseline_update: InputUpdate,
    options: BatchOptions<'a>,
    plan: RecalcPlan,
    request: ResolvedEvaluationRequest,
    #[cfg(test)]
    rebuild_attempts_for_test: usize,
}

impl<'a> BatchExecutor<'a> {
    pub(crate) fn new(
        sheetport: &'a mut SheetPort<'a>,
        baseline_update: InputUpdate,
        options: BatchOptions<'a>,
        plan: RecalcPlan,
        request: ResolvedEvaluationRequest,
    ) -> Self {
        Self {
            sheetport,
            baseline_update,
            options,
            plan,
            request,
            #[cfg(test)]
            rebuild_attempts_for_test: 0,
        }
    }

    #[cfg(feature = "benchmark_internal")]
    #[doc(hidden)]
    /// Read-only access used to snapshot engine telemetry without affecting batch execution.
    pub fn workbook_for_benchmark(&self) -> &formualizer_workbook::Workbook {
        self.sheetport.workbook()
    }

    fn is_plan_stale(error: &SheetPortError) -> bool {
        let excel = match error {
            SheetPortError::Engine { source } => Some(source),
            SheetPortError::Workbook {
                source: formualizer_workbook::error::IoError::Engine(source),
            } => Some(source),
            _ => None,
        };
        excel.is_some_and(|error| matches!(error.extra, ExcelErrorExtra::PlanStale { .. }))
    }

    fn evaluate_current(&mut self, allow_rebuild: bool) -> Result<OutputSnapshot, SheetPortError> {
        match self
            .sheetport
            .evaluate_with_plan(&self.plan, self.options.eval.clone())
        {
            Err(error)
                if allow_rebuild
                    && self.options.stale_policy == BatchPlanStalePolicy::RebuildOnStale
                    && Self::is_plan_stale(&error) =>
            {
                #[cfg(test)]
                {
                    self.rebuild_attempts_for_test += 1;
                }
                self.plan = self
                    .sheetport
                    .rebuild_target_plan(&self.request, &self.options.eval)?;
                self.sheetport
                    .evaluate_with_plan(&self.plan, self.options.eval.clone())
            }
            result => result,
        }
    }

    fn restore_baseline(&mut self) -> Result<(), SheetPortError> {
        self.sheetport
            .write_inputs_raw(self.baseline_update.clone())?;
        let mut restore_options = self.options.eval.clone();
        restore_options.cancel = None;
        match self
            .sheetport
            .evaluate_with_plan(&self.plan, restore_options.clone())
        {
            Err(error)
                if self.options.stale_policy == BatchPlanStalePolicy::RebuildOnStale
                    && Self::is_plan_stale(&error) =>
            {
                #[cfg(test)]
                {
                    self.rebuild_attempts_for_test += 1;
                }
                self.plan = self
                    .sheetport
                    .rebuild_target_plan(&self.request, &restore_options)?;
                self.sheetport
                    .evaluate_with_plan(&self.plan, restore_options)?;
                Ok(())
            }
            result => result.map(|_| ()),
        }
    }

    pub fn run<I>(&mut self, scenarios: I) -> Result<Vec<BatchResult>, SheetPortError>
    where
        I: IntoIterator<Item = BatchInput>,
    {
        let cases = scenarios.into_iter().collect::<Vec<_>>();
        let total = cases.len();
        let mut results = Vec::with_capacity(total);
        let primary = (|| {
            for (index, case) in cases.into_iter().enumerate() {
                self.sheetport
                    .write_inputs_raw(self.baseline_update.clone())?;
                if !case.update.is_empty() {
                    self.sheetport.write_inputs(case.update)?;
                }
                let outputs = self.evaluate_current(true)?;
                if let Some(callback) = self.options.progress.as_mut() {
                    callback(BatchProgress {
                        completed: index + 1,
                        total,
                        scenario_id: &case.id,
                    });
                }
                results.push(BatchResult {
                    id: case.id,
                    outputs,
                });
            }
            Ok(())
        })();
        let restoration = self.restore_baseline();
        match (primary, restoration) {
            (Ok(()), Ok(())) => Ok(results),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(restoration)) => Err(restoration),
            (Err(primary), Err(restoration)) => Err(SheetPortError::BatchRestoration {
                primary: Box::new(primary),
                restoration: Box::new(restoration),
            }),
        }
    }
}
