//! Engine wiring (feature `unified_authority`, M1a).
//!
//! The authority lives inside `DependencyGraph` beside the legacy graph,
//! which stays the runtime evaluation path until M1b. The graph's formula
//! map records every vertex whose formula changes; `authority_sync` turns
//! those into authority mutations (set formula / clear) and rebuilds from
//! scratch at load and when a symbol (name, table, sheet) changes, because
//! symbol rebinding is M4. Structural edits, moves and sheet operations (M3)
//! capture the formula identities, and the next sync rebuilds from the
//! already-transformed formulas keeping them (`history`). FormulaPlane spans
//! are M2: while spans exist the host is in a typed "unsupported under
//! unified_authority" state, and every query answers that error instead of
//! a stale relation.

use super::dirty::DirtyStore;
use super::history::{Carried, IdJournal};
use super::store::{AuthorityError, Store};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum HostState {
    /// Not built yet (a load is in progress or nothing was queried).
    #[default]
    Unbuilt,
    Ready,
    /// A mutation outside M1a scope happened; queries fail with this error.
    Failed(AuthorityError),
}

/// Counters for the differential self-check (gates).
#[derive(Clone, Debug, Default)]
pub struct DiffCounters {
    pub propagations: u64,
    pub checked_seeds: u64,
    /// Dirty-propagation mismatches against legacy's actual dirty set
    /// (Δ(a)).
    pub closure_mismatches: u64,
    /// Propagations where legacy's bounding-rectangle value path dirtied
    /// more than the authority (a strict superset; not a mismatch).
    pub closure_conservative: u64,
    pub direct_mismatches: u64,
    pub skipped: u64,
}

#[derive(Debug, Default)]
pub struct AuthorityHost {
    pub(crate) store: Store,
    pub(crate) state: HostState,
    /// Symbol revision the store was built against.
    pub(crate) symbol_rev: u64,
    /// Dirty cover (design §4.4): marked by every legacy propagation with
    /// the authority's propagation of the same seeds.
    pub(crate) dirty: DirtyStore,
    pub(crate) builds: u64,
    pub(crate) incremental_mutations: u64,
    pub(crate) diff: DiffCounters,
    /// Identities captured before pending structural mutations; `Some`
    /// makes the next sync a rebuild that keeps them (M3).
    pub(crate) carried: Option<Carried>,
    /// Retired ids that undo/redo may restore (M3, §6).
    pub(crate) journal: IdJournal,
    pub(crate) structural_rebuilds: u64,
}

impl AuthorityHost {
    pub fn state(&self) -> &HostState {
        &self.state
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn dirty(&self) -> &DirtyStore {
        &self.dirty
    }

    pub fn builds(&self) -> u64 {
        self.builds
    }

    pub fn incremental_mutations(&self) -> u64 {
        self.incremental_mutations
    }

    pub fn diff_counters(&self) -> &DiffCounters {
        &self.diff
    }

    pub fn structural_rebuilds(&self) -> u64 {
        self.structural_rebuilds
    }

    pub fn journal(&self) -> &IdJournal {
        &self.journal
    }
}
