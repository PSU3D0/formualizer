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

    /// Print the load-time formula staging counters (no timing: the
    /// line is not a phase).
    pub(super) fn families<R: formualizer_eval::traits::EvaluationContext>(
        &self,
        engine: &formualizer_eval::engine::Engine<R>,
    ) {
        if self.marks.is_some() {
            eprintln!(
                "[fz][families] {}",
                engine.source_family_counters().debug_line()
            );
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

use super::{DateTime, DeterministicMode, Utc, XlsxRecalculateOptions};

/// The instant this run's `NOW()`/`TODAY()` observe: the fixed timestamp in
/// deterministic mode, otherwise one system-clock sample (when the build has a
/// system clock).
pub(super) fn clock_instant(options: &XlsxRecalculateOptions) -> Option<DateTime<Utc>> {
    match &options.eval_config.deterministic_mode {
        DeterministicMode::Enabled { timestamp_utc, .. } => Some(*timestamp_utc),
        // NOW() has one-second resolution. A whole-second sample is exactly
        // what the caches record and round-trips through every binding.
        #[cfg(feature = "system-clock")]
        DeterministicMode::Disabled { .. } => {
            Some(chrono::SubsecRound::trunc_subsecs(Utc::now(), 0))
        }
        #[cfg(not(feature = "system-clock"))]
        DeterministicMode::Disabled { .. } => None,
    }
}
