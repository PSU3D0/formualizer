//! Request-local planner control. No thread-local state or retained callback.
//! Every tick denotes one actually executed, counted operation. Callers flush
//! before publishing a result; dropping an interrupted plan publishes nothing.

use formualizer_common::ExcelError;

pub(crate) struct PlanControl<F> {
    total: u64,
    pending: u64,
    checkpoint: F,
}

impl<F: FnMut(u64) -> Result<(), ExcelError>> PlanControl<F> {
    /// Check cancellation before any planning allocation.
    pub fn new(checkpoint: F) -> Result<Self, ExcelError> {
        let mut control = Self {
            total: 0,
            pending: 0,
            checkpoint,
        };
        control.flush()?;
        Ok(control)
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn tick(&mut self) -> Result<(), ExcelError> {
        self.total += 1;
        self.pending += 1;
        if self.pending == 4096 {
            self.flush()?;
        }
        Ok(())
    }

    /// For bounded fixed-size operations such as histogram initialization.
    /// Split even a crossing batch so no callback delta can exceed 4096.
    pub fn charge(&mut self, mut units: u64) -> Result<(), ExcelError> {
        while units != 0 {
            let batch = units.min(4096 - self.pending);
            self.total += batch;
            self.pending += batch;
            units -= batch;
            if self.pending == 4096 {
                self.flush()?;
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), ExcelError> {
        (self.checkpoint)(self.pending)?;
        self.pending = 0;
        Ok(())
    }
}
