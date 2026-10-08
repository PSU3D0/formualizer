//! Load-time formula families from source text: the selection switch and
//! the counters loaders report under `FZ_DEBUG_LOAD` / `FZ_DEBUG_RECALC`.

use std::sync::Mutex;

/// How load-time staging treats formula text that may be a relocated copy
/// of a formula already parsed on the same sheet. Selected per engine from
/// `FZ_SOURCE_FAMILIES` (`off`/`0`: `Off`; `oracle`: `Oracle`; otherwise
/// `On`) and overridable with `Engine::set_source_family_mode`.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SourceFamilyMode {
    /// Parse every formula (the previous behaviour).
    Off,
    /// Stage proven copies without parsing them.
    #[default]
    On,
    /// As `On`, and also parse every proven copy and check it against its
    /// template; a mismatch is counted, reported and parsed instead.
    Oracle,
}

impl SourceFamilyMode {
    pub fn from_env() -> Self {
        match std::env::var("FZ_SOURCE_FAMILIES") {
            Ok(v) if v == "0" || v.eq_ignore_ascii_case("off") => Self::Off,
            Ok(v) if v.eq_ignore_ascii_case("oracle") => Self::Oracle,
            _ => Self::On,
        }
    }
}

/// Load-time formula staging counters.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceFamilyCounters {
    /// Formula texts staged.
    pub formulas: u64,
    /// Formula texts parsed, and their bytes.
    pub parse_calls: u64,
    pub parse_bytes: u64,
    /// Formula texts answered by the per-load parse cache.
    pub parse_cache_hits: u64,
    /// Parsed formulas that became members of an adjacent family.
    pub parsed_members: u64,
    /// Parsed templates certified for relocation, and those that were not.
    pub templates_certified: u64,
    pub templates_uncertified: u64,
    /// Proven members staged without parsing: through shared-formula
    /// identity, and through adjacency.
    pub shared_members: u64,
    pub adjacent_members: u64,
    /// Unparsed candidates that fell back to parsing, by reason.
    pub fallback_no_template: u64,
    pub fallback_mismatch: u64,
    pub fallback_off_grid: u64,
    /// Oracle mode: proven members checked by parsing, and mismatches.
    pub oracle_checked: u64,
    pub oracle_mismatches: u64,
}

static PROCESS_TOTALS: Mutex<SourceFamilyCounters> = Mutex::new(SourceFamilyCounters {
    formulas: 0,
    parse_calls: 0,
    parse_bytes: 0,
    parse_cache_hits: 0,
    parsed_members: 0,
    templates_certified: 0,
    templates_uncertified: 0,
    shared_members: 0,
    adjacent_members: 0,
    fallback_no_template: 0,
    fallback_mismatch: 0,
    fallback_off_grid: 0,
    oracle_checked: 0,
    oracle_mismatches: 0,
});

/// Totals over every engine of this process (development tools).
#[doc(hidden)]
pub fn source_family_process_totals() -> SourceFamilyCounters {
    *PROCESS_TOTALS.lock().unwrap_or_else(|e| e.into_inner())
}

impl SourceFamilyCounters {
    pub fn accumulate(&mut self, o: &Self) {
        self.formulas += o.formulas;
        self.parse_calls += o.parse_calls;
        self.parse_bytes += o.parse_bytes;
        self.parse_cache_hits += o.parse_cache_hits;
        self.parsed_members += o.parsed_members;
        self.templates_certified += o.templates_certified;
        self.templates_uncertified += o.templates_uncertified;
        self.shared_members += o.shared_members;
        self.adjacent_members += o.adjacent_members;
        self.fallback_no_template += o.fallback_no_template;
        self.fallback_mismatch += o.fallback_mismatch;
        self.fallback_off_grid += o.fallback_off_grid;
        self.oracle_checked += o.oracle_checked;
        self.oracle_mismatches += o.oracle_mismatches;
    }

    pub(crate) fn publish(&self) {
        PROCESS_TOTALS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .accumulate(self);
    }

    /// One `key=value` line for debug output.
    pub fn debug_line(&self) -> String {
        format!(
            "formulas={} parse_calls={} parse_bytes={} cache_hits={} parsed_members={} \
             templates_certified={} templates_uncertified={} shared_members={} \
             adjacent_members={} fallback_no_template={} fallback_mismatch={} \
             fallback_off_grid={} oracle_checked={} oracle_mismatches={}",
            self.formulas,
            self.parse_calls,
            self.parse_bytes,
            self.parse_cache_hits,
            self.parsed_members,
            self.templates_certified,
            self.templates_uncertified,
            self.shared_members,
            self.adjacent_members,
            self.fallback_no_template,
            self.fallback_mismatch,
            self.fallback_off_grid,
            self.oracle_checked,
            self.oracle_mismatches,
        )
    }
}
