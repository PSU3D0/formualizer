//! `FZ_DEBUG_RECALC=1` prints each recalculation phase's wall time to stderr,
//! like `FZ_DEBUG_LOAD` for loading. A development aid, not a receipt field.
use formualizer_eval::instant::FzInstant as Instant;

pub(super) struct PhaseClock {
    /// `(run start, last lap)`; `None` when disabled, so no clock is read.
    marks: Option<(Instant, Instant)>,
}

impl PhaseClock {
    pub(super) fn from_env() -> Self {
        let enabled = std::env::var("FZ_DEBUG_RECALC")
            .ok()
            .is_some_and(|v| v != "0");
        Self {
            marks: enabled.then(|| {
                let now = Instant::now();
                (now, now)
            }),
        }
    }

    /// Print the time since the previous lap, attributed to `phase`.
    pub(super) fn lap(&mut self, phase: &str) {
        if let Some((_, last)) = &mut self.marks {
            let now = Instant::now();
            eprintln!(
                "[fz][recalc] {phase}: {:.1} ms",
                now.duration_since(*last).as_secs_f64() * 1e3
            );
            *last = now;
        }
    }

    /// Print the whole run's time.
    pub(super) fn total(&self, formulas: usize) {
        if let Some((start, _)) = &self.marks {
            eprintln!(
                "[fz][recalc] total: {:.1} ms ({formulas} formula cells)",
                start.elapsed().as_secs_f64() * 1e3
            );
        }
    }
}
