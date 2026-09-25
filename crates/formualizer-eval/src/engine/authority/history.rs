//! Identity carry across structural edits and history replay (M3, design
//! §6, decision 9).
//!
//! Structural edits, moves and sheet operations resync the store from the
//! graph's already-transformed formulas (a rebuild, which stays in legacy's
//! O(F·|AST|) class for the same edit). Identities follow the legacy
//! vertex, which keeps its `VertexId` when legacy moves it:
//! [`Carried`] is captured from the synced store before the first
//! structural mutation and maps each formula vertex to its id, so a moved
//! formula keeps its id and a destroyed one retires it (the counter
//! continues, so it is never reused by a fresh formula).
//!
//! History replay may bring a retired id back (ID4). Legacy undo/redo
//! replays graph mutations without ids, so the host keeps a per-vertex
//! [`IdJournal`]: `past` holds ids retired on the current timeline (most
//! recent last), `future` the ids an undo retired, for redo. A creation
//! during undo resurrects the top of `past`, during redo the top of
//! `future`, and a fresh creation outside replay starts a new timeline.
//! Legacy's `RemoveVertex` inverse re-creates the cell on a new vertex;
//! [`IdJournal::revived`] aliases the new vertex to the old one's history.

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

/// Formula vertex → id before the pending structural mutations.
pub type Carried = FxHashMap<VertexId, Vid>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct IdHistory {
    past: Vec<Vid>,
    future: Vec<Vid>,
}

#[derive(Clone, Debug, Default)]
pub struct IdJournal {
    mode: Replay,
    hist: FxHashMap<VertexId, IdHistory>,
    /// Revived vertex → the vertex whose history it continues.
    alias: FxHashMap<VertexId, VertexId>,
}

impl IdJournal {
    pub fn mode(&self) -> Replay {
        self.mode
    }

    pub fn set_mode(&mut self, mode: Replay) {
        self.mode = mode;
    }

    fn key(&self, v: VertexId) -> VertexId {
        self.alias.get(&v).copied().unwrap_or(v)
    }

    /// The formula at `v` left its cell, retiring `id`.
    pub fn retired(&mut self, v: VertexId, id: Vid) {
        let k = self.key(v);
        let h = self.hist.entry(k).or_default();
        match self.mode {
            Replay::Forward | Replay::Redo => h.past.push(id),
            Replay::Undo => h.future.push(id),
        }
    }

    /// A formula appeared at `v`: the id replay restores, if any. `None`
    /// means a fresh id.
    pub fn created(&mut self, v: VertexId) -> Option<Vid> {
        let k = self.key(v);
        let h = self.hist.get_mut(&k)?;
        let id = match self.mode {
            Replay::Forward => {
                h.future.clear();
                None
            }
            Replay::Undo => h.past.pop(),
            Replay::Redo => h.future.pop(),
        };
        if h.past.is_empty() && h.future.is_empty() {
            self.hist.remove(&k);
        }
        id
    }

    /// Replay re-created `old`'s cell on vertex `new`.
    pub fn revived(&mut self, old: VertexId, new: VertexId) {
        let k = self.key(old);
        if k != new {
            self.alias.insert(new, k);
        }
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
        hash_table_bytes::<(VertexId, IdHistory)>(self.hist.capacity())
            + self
                .hist
                .values()
                .map(|h| (h.past.capacity() + h.future.capacity()) * size_of::<Vid>())
                .sum::<usize>()
            + hash_table_bytes::<(VertexId, VertexId)>(self.alias.capacity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn undo_redo_chain_restores_ids_per_timeline() {
        let v = VertexId(7);
        let mut j = IdJournal::default();
        // F(1) -> value -> F'(2) forward.
        j.retired(v, 1);
        assert_eq!(j.created(v), None);
        // undo F'->value, undo value->F.
        j.set_mode(Replay::Undo);
        j.retired(v, 2);
        assert_eq!(j.created(v), Some(1));
        // redo value, redo F'.
        j.set_mode(Replay::Redo);
        j.retired(v, 1);
        assert_eq!(j.created(v), Some(2));
        // A fresh edit forks the timeline: the redo branch is gone.
        j.set_mode(Replay::Undo);
        j.retired(v, 2);
        j.set_mode(Replay::Forward);
        assert_eq!(j.created(v), None);
        j.set_mode(Replay::Redo);
        assert_eq!(j.created(v), None);
    }

    #[test]
    fn revived_vertex_continues_the_old_history_across_redo() {
        let (old, new1, new2) = (VertexId(1), VertexId(2), VertexId(3));
        let mut j = IdJournal::default();
        j.retired(old, 9); // delete
        j.set_mode(Replay::Undo);
        j.revived(old, new1);
        assert_eq!(j.created(new1), Some(9));
        j.set_mode(Replay::Redo);
        j.retired(new1, 9); // redo deletes the revived vertex
        j.set_mode(Replay::Undo);
        j.revived(old, new2); // the logged event still names `old`
        assert_eq!(j.created(new2), Some(9));
        assert_eq!(j.pending(), 0);
    }
}
