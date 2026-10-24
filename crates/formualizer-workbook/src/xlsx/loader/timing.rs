//! Opt-in, phase-level load diagnostics; never time individual cell events.

pub(super) struct Timer {
    #[cfg(not(target_arch = "wasm32"))]
    started: Option<std::time::Instant>,
}

impl Timer {
    pub(super) fn start(enabled: bool) -> Self {
        #[cfg(target_arch = "wasm32")]
        let _ = enabled;
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            started: enabled.then(std::time::Instant::now),
        }
    }

    pub(super) fn elapsed_ms(&self) -> f64 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started
                .map_or(0.0, |start| start.elapsed().as_secs_f64() * 1_000.0)
        }
        #[cfg(target_arch = "wasm32")]
        {
            0.0
        }
    }
}

#[derive(Default)]
pub(super) struct Phases {
    pub package_ms: f64,
    pub scan_ms: f64,
    pub sinks_ms: f64,
    pub formulas_ms: f64,
    pub install_ms: f64,
}

impl Phases {
    pub(super) fn report(&self, adapter_ms: f64) {
        let accounted =
            self.package_ms + self.scan_ms + self.sinks_ms + self.formulas_ms + self.install_ms;
        eprintln!(
            "FZ_NATIVE_LOAD package_ms={:.3} scan_ms={:.3} sinks_ms={:.3} formulas_ms={:.3} install_ms={:.3} other_ms={:.3} adapter_ms={:.3}",
            self.package_ms,
            self.scan_ms,
            self.sinks_ms,
            self.formulas_ms,
            self.install_ms,
            (adapter_ms - accounted).max(0.0),
            adapter_ms,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_timer_has_no_clock_sample() {
        let timer = Timer::start(false);
        assert_eq!(timer.elapsed_ms(), 0.0);
        #[cfg(not(target_arch = "wasm32"))]
        assert!(timer.started.is_none());
    }
}
