//! History replay direction (M3, design §6, decision 9 as amended in
//! Program 2).
//!
//! A formula cell's id is its executor `VertexId` (P2-M2: one id space).
//! Identities therefore follow the graph's vertices: a moved cell keeps its
//! vertex, a formula -> value -> formula edit keeps the cell's vertex (value
//! cells still have vertices), and undo/redo of a structural delete revives
//! the deleted vertex itself (`VertexEditor::apply_inverse` of
//! `RemoveVertex`). No id is renumbered or given to another cell. The
//! Program 1 host kept its own id counter and a structural capture; both
//! went away with the unification.
//!
//! Legacy replay also removes a cell's vertex where it has no prior state
//! to restore (undo of a formula typed over a value: Arrow holds the value,
//! so the logged event carries none) and re-creates the cell later on a
//! new vertex. The graph keeps an [`IdJournal`] keyed by **cell** for
//! those: history is LIFO, so every replayed step runs in exactly the
//! coordinate frame its forward step left, and a vertex removed at cell c
//! is wanted back at c when that step is undone. Per cell, `past` holds
//! ids removed on the current timeline (most recent last) and `future` the
//! ids an undo removed, for redo. A creation during undo revives the top of
//! `past`, during redo the top of `future`, and a creation outside replay
//! starts a new timeline.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Direction of the mutations the host is about to sync.
pub enum Replay {
    #[default]
    Forward,
    Undo,
    Redo,
}

use super::geom::Cell;
use super::identity::Vid;
use rustc_hash::FxHashMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct IdHistory {
    past: Vec<Vid>,
    future: Vec<Vid>,
}

/// Vertices removed per cell, for replay (see the module doc).
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

    /// The vertex `id` at `cell` (pre-mutation frame) was removed.
    pub fn retired(&mut self, cell: Cell, id: Vid) {
        let h = self.hist.entry(cell).or_default();
        match self.mode {
            Replay::Forward | Replay::Redo => h.past.push(id),
            Replay::Undo => h.future.push(id),
        }
    }

    /// A vertex is needed at `cell` (post-mutation frame): the id replay
    /// revives, if any. `None` means a new vertex.
    pub fn created(&mut self, cell: Cell) -> Option<Vid> {
        if self.hist.is_empty() {
            return None;
        }
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

    /// Drop `id` from the top of `cell`'s stack for the current mode (the
    /// vertex was revived by other means: a `RemoveVertex` undo names it).
    pub fn forget(&mut self, cell: Cell, id: Vid) {
        let Some(h) = self.hist.get_mut(&cell) else {
            return;
        };
        let stack = match self.mode {
            Replay::Undo => &mut h.past,
            Replay::Forward | Replay::Redo => &mut h.future,
        };
        if stack.last() == Some(&id) {
            stack.pop();
        }
        if h.past.is_empty() && h.future.is_empty() {
            self.hist.remove(&cell);
        }
    }

    /// Ids waiting for replay (tests, accounting).
    pub fn pending(&self) -> usize {
        self.hist
            .values()
            .map(|h| h.past.len() + h.future.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn undo_redo_chain_restores_ids_per_timeline() {
        let c = (0, 4, 2);
        let mut j = IdJournal::default();
        // Vertex 1 removed forward, a new one created.
        j.retired(c, 1);
        assert_eq!(j.created(c), None);
        // undo: 2 removed, 1 revived.
        j.set_mode(Replay::Undo);
        j.retired(c, 2);
        assert_eq!(j.created(c), Some(1));
        // redo: 1 removed, 2 revived.
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
