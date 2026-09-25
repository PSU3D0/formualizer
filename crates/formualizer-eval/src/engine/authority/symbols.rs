//! Stable, non-grid symbol identities (design §4.1).
//!
//! The Store's symbol-revision rebuild path maintains this table alongside
//! cell identities. Symbol relation and scheduling are separate. SymbolAddr is a binding
//! identity, never a fabricated cell or the executor's VertexId.

use super::identity::{IdError, IdentityTable, Vid};
use super::store::{AuthorityError, Budget};
use crate::engine::addr::SymbolAddr;

pub(crate) type SymbolId = SymbolAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    symbol: SymbolId,
    vid: Vid,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SymbolTable {
    entries: Vec<Entry>,
    /// Also retained by an empty table: deletions never reset identity history.
    next_id: Vid,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SymbolWork {
    /// Validation, merge comparisons/advances, and emitted entries.
    pub visits: u64,
    pub created: u64,
}

impl SymbolTable {
    pub(crate) fn heap_bytes(&self) -> u64 {
        (self.entries.capacity() * size_of::<Entry>()) as u64
    }

    /// Diagnostic invariant check; never run on a mutation's hot path.
    pub(crate) fn check(&self, ids: &IdentityTable) -> Result<(), String> {
        if self.next_id > ids.next_id() {
            return Err("symbol high-water mark exceeds shared counter".into());
        }
        let mut previous = None;
        let mut seen = std::collections::BTreeSet::new();
        for entry in &self.entries {
            if previous.is_some_and(|symbol| symbol >= entry.symbol) {
                return Err("symbol keys are not strictly increasing".into());
            }
            if entry.vid >= self.next_id || ids.locate(entry.vid).is_some() {
                return Err("symbol ID is outside its counter or aliases a cell".into());
            }
            if !seen.insert(entry.vid) {
                return Err("two symbols share an ID".into());
            }
            previous = Some(entry.symbol);
        }
        Ok(())
    }

    pub(crate) fn lookup(&self, symbol: SymbolId) -> Option<Vid> {
        self.entries
            .binary_search_by_key(&symbol, |e| e.symbol)
            .ok()
            .map(|i| self.entries[i].vid)
    }

    /// Rebuild from strictly increasing live binding identities. Surviving
    /// symbols keep their IDs; omitted symbols are retired. Reintroducing a
    /// retired symbol allocates a fresh ID, even if its binding key is reused.
    ///
    /// `ids` must be the current cell counter, or a rebuild counter continued
    /// from the prior store. No cell run is inserted for a symbol. All possible
    /// failures precede the counter commit; prior and counter survive rejection.
    /// Budget is table-local: retained limits the output; scratch limits the
    /// coexisting prior table (above the retained output). Callers must subtract
    /// other live storage, including their borrowed sorted input, beforehand.
    pub(crate) fn rebuild(
        live: &[SymbolId],
        prior: &Self,
        ids: &mut IdentityTable,
        budget: Budget,
    ) -> Result<(Self, SymbolWork), AuthorityError> {
        if ids.next_id() < prior.next_id {
            return Err(AuthorityError::Identity(IdError::Conflict(
                "symbol rebuild counter precedes prior high-water mark".into(),
            )));
        }
        let mut work = SymbolWork::default();
        let mut previous = None;
        for &symbol in live {
            work.visits += 1;
            if previous.is_some_and(|p| p >= symbol) {
                return Err(AuthorityError::Identity(IdError::Conflict(
                    "symbol rebuild input is not strictly increasing".into(),
                )));
            }
            previous = Some(symbol);
        }
        let bytes = live
            .len()
            .checked_mul(size_of::<Entry>())
            .ok_or(AuthorityError::Alloc)? as u64;
        for (resource, needed, limit) in [
            ("symbol retained", bytes, budget.retained),
            ("symbol scratch", prior.heap_bytes(), budget.scratch),
        ] {
            if let Some(limit) = limit.filter(|&limit| needed > limit) {
                return Err(AuthorityError::Admission {
                    resource,
                    needed,
                    limit,
                });
            }
        }
        // A merge rather than one binary search per symbol makes whole-table
        // rebuild work linear in live plus prior, including all deleted keys.
        let mut old = 0;
        for &symbol in live {
            work.visits += 1;
            while old < prior.entries.len() && prior.entries[old].symbol < symbol {
                work.visits += 1;
                old += 1;
            }
            work.visits += 1;
            if old < prior.entries.len() && prior.entries[old].symbol == symbol {
                if prior.entries[old].vid >= ids.next_id() {
                    return Err(AuthorityError::Identity(IdError::Conflict(
                        "symbol rebuild counter does not continue prior identities".into(),
                    )));
                }
            } else {
                work.created += 1;
            }
        }
        ids.check_alloc(work.created)
            .map_err(AuthorityError::Identity)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(live.len())
            .map_err(|_| AuthorityError::Alloc)?;
        let first = ids.next_id();
        let mut next = first;
        old = 0;
        for &symbol in live {
            work.visits += 1;
            while old < prior.entries.len() && prior.entries[old].symbol < symbol {
                work.visits += 1;
                old += 1;
            }
            work.visits += 1;
            let vid = if old < prior.entries.len() && prior.entries[old].symbol == symbol {
                prior.entries[old].vid
            } else {
                let vid = next;
                next += 1;
                vid
            };
            entries.push(Entry { symbol, vid });
            work.visits += 1;
        }
        // The earlier check proves this conversion and commit cannot fail.
        ids.allocate_symbol_ids(next - first)
            .map_err(AuthorityError::Identity)?;
        Ok((
            Self {
                entries,
                next_id: ids.next_id(),
            },
            work,
        ))
    }
}
