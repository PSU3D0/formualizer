use crate::SheetPortError;
use crate::runtime::{EvalOptions, SheetPort};
use crate::value::{InputUpdate, OutputSnapshot};
use formualizer_eval::engine::RecalcPlan;

type ProgressCallback<'a> = Box<dyn FnMut(BatchProgress<'_>) + Send + 'a>;

/// Execution options for batch runs.
#[derive(Default)]
pub struct BatchOptions<'a> {
    pub eval: EvalOptions,
    pub concurrency: Option<usize>,
    pub progress: Option<ProgressCallback<'a>>,
}

/// Progress information emitted during batch execution.
#[derive(Debug, Clone)]
pub struct BatchProgress<'a> {
    pub completed: usize,
    pub total: usize,
    pub scenario_id: &'a str,
}

/// Input payload for a single batch scenario.
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

/// Result for a single batch scenario.
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
}

impl<'a> BatchExecutor<'a> {
    pub(crate) fn new(
        sheetport: &'a mut SheetPort<'a>,
        baseline_update: InputUpdate,
        options: BatchOptions<'a>,
        plan: RecalcPlan,
    ) -> Self {
        Self {
            sheetport,
            baseline_update,
            options,
            plan,
        }
    }

    pub fn run<I>(&mut self, scenarios: I) -> Result<Vec<BatchResult>, SheetPortError>
    where
        I: IntoIterator<Item = BatchInput>,
    {
        let cases: Vec<BatchInput> = scenarios.into_iter().collect();
        let total = cases.len();
        let mut results = Vec::with_capacity(total);

        for (idx, case) in cases.into_iter().enumerate() {
            let BatchInput { id, update } = case;
            self.sheetport.write_inputs(self.baseline_update.clone())?;
            if !update.is_empty() {
                self.sheetport.write_inputs(update)?;
            }
            let outputs = self
                .sheetport
                .evaluate_with_plan(&self.plan, self.options.eval.clone())?;
            if let Some(callback) = self.options.progress.as_mut() {
                callback(BatchProgress {
                    completed: idx + 1,
                    total,
                    scenario_id: &id,
                });
            }
            results.push(BatchResult { id, outputs });
        }

        // Restore baseline after all scenarios.
        self.sheetport.write_inputs(self.baseline_update.clone())?;

        Ok(results)
    }
}
