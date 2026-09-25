//! Identity carry across structural edits and history replay (M3, design
//! §6, decision 9).
//!
//! Structural edits, moves and sheet operations resync the store from the
//! graph's already-transformed formulas (a rebuild, which stays in legacy's
//! O(F·|AST|) class for the same edit). Identities follow the legacy
//! vertex, which keeps its `VertexId` when legacy moves it: [`Carried`] is
//! captured from the synced store before a structural operation and maps
//! each formula vertex to its id and pre-edit cell, so a moved formula
//! keeps its id and a destroyed one retires it (the counter continues, so
//! a fresh formula never reuses it).
//!
//! History replay may bring a retired id back (ID4). Legacy undo/redo
//! replays graph mutations without ids, and re-creates removed cells on new
//! vertices (or none: a value cell may have no vertex), so the host keeps
//! an [`IdJournal`] keyed by **cell**. History is LIFO, so every replayed
//! step runs in exactly the coordinate frame its forward step left: an id
//! retired at cell c (in the frame before the retiring step) is wanted
//! back at c when that step is undone. Per cell, `past` holds ids retired
//! on the current timeline (most recent last) and `future` the ids an undo
//! retired, for redo. A creation during undo resurrects the top of `past`,
//! during redo the top of `future`, and a fresh creation outside replay
//! starts a new timeline.

use super::geom::Cell;
use super::identity::Vid;
use crate::engine::vertex::VertexId;
use rustc_hash::FxHashMap;

/// Direction of the mutations the host is about to sync.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Replay {
    #[default]
    Forward,
    Undo,
    Redo,
}

/// Formula vertex → (id, cell) before the pending structural mutations.
pub type Carried = FxHashMap<VertexId, (Vid, Cell)>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct IdHistory {
    past: Vec<Vid>,
    future: Vec<Vid>,
}

#[derive(Clone, Debug, Default)]
pub struct IdJournal {
    mode: Replay,
    hist: FxHashMap<Cell, IdHistory>,
}

impl IdJournal {
    pub fn mode(&self) -> Replay {
        self.mode
    }

    pub fn set_mode(&mut self, mode: Replay) {
        self.mode = mode;
    }

    /// The formula at `cell` (pre-mutation frame) left, retiring `id`.
    pub fn retired(&mut self, cell: Cell, id: Vid) {
        let h = self.hist.entry(cell).or_default();
        match self.mode {
            Replay::Forward | Replay::Redo => h.past.push(id),
            Replay::Undo => h.future.push(id),
        }
    }

    /// A formula appeared at `cell` (post-mutation frame): the id replay
    /// restores, if any. `None` means a fresh id.
    pub fn created(&mut self, cell: Cell) -> Option<Vid> {
        let h = self.hist.get_mut(&cell)?;
        let id = match self.mode {
            Replay::Forward => {
                h.future.clear();
                None
            }
            Replay::Undo => h.past.pop(),
            Replay::Redo => h.future.pop(),
        };
        if h.past.is_empty() && h.future.is_empty() {
            self.hist.remove(&cell);
        }
        id
    }

    /// The id an undo would restore at `cell` (tests).
    #[cfg(test)]
    pub fn undo_target(&self, cell: Cell) -> Option<Vid> {
        self.hist.get(&cell)?.past.last().copied()
    }

    /// Ids waiting for replay (tests, accounting).
    pub fn pending(&self) -> usize {
        self.hist
            .values()
            .map(|h| h.past.len() + h.future.len())
            .sum()
    }

    pub fn heap_bytes(&self) -> usize {
        use super::dir::hash_table_bytes;
        hash_table_bytes::<(Cell, IdHistory)>(self.hist.capacity())
            + self
                .hist
                .values()
                .map(|h| (h.past.capacity() + h.future.capacity()) * size_of::<Vid>())
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn undo_redo_chain_restores_ids_per_timeline() {
        let c = (0, 4, 2);
        let mut j = IdJournal::default();
        // F(1) -> value -> F'(2) forward.
        j.retired(c, 1);
        assert_eq!(j.created(c), None);
        // undo F'->value, undo value->F.
        j.set_mode(Replay::Undo);
        j.retired(c, 2);
        assert_eq!(j.created(c), Some(1));
        // redo value, redo F'.
        j.set_mode(Replay::Redo);
        j.retired(c, 1);
        assert_eq!(j.created(c), Some(2));
        // A fresh edit forks the timeline: the redo branch is gone.
        j.set_mode(Replay::Undo);
        j.retired(c, 2);
        j.set_mode(Replay::Forward);
        assert_eq!(j.created(c), None);
        j.set_mode(Replay::Redo);
        assert_eq!(j.created(c), None);
        assert_eq!(j.pending(), 1);
    }
}
