use super::Mode;
use crate::{CellRef, engine::VertexId};
use rustc_hash::FxHashMap as HashMap;
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

#[derive(Clone, Copy, Default)]
pub(super) struct Origin {
    pub named_vertex: Option<VertexId>,
    pub reference_cell: Option<CellRef>,
}
#[derive(Clone, Copy)]
struct Frame {
    key: usize,
    previous: Option<usize>,
    mode: Mode,
    origin: Origin,
}
#[derive(Default)]
struct Slot {
    head: Option<usize>,
    events: u64,
    taints: u64,
}
#[derive(Default)]
struct ThreadState {
    frames: Vec<Frame>,
    slots: HashMap<usize, Slot>,
}
thread_local! {
    static SLOTS: RefCell<ThreadState> = RefCell::new(ThreadState::default());
}
pub(super) struct Guard {
    key: usize,
    index: usize,
    events: u64,
    taints: u64,
    _not_send: PhantomData<Rc<()>>,
}
pub(super) fn enter(key: usize, requested: Mode, override_origin: Origin) -> Guard {
    SLOTS.with(|state| {
        let mut state = state.borrow_mut();
        let ThreadState { frames, slots } = &mut *state;
        let slot = slots.entry(key).or_default();
        let parent = slot.head.map(|index| frames[index]);
        let parent_mode = parent.map_or(Mode::Inherit, |frame| frame.mode);
        let parent_origin = parent.map_or(Origin::default(), |frame| frame.origin);
        let mode = if parent_mode == Mode::Suppressed || requested == Mode::Suppressed {
            Mode::Suppressed
        } else if requested == Mode::Inherit {
            parent_mode
        } else {
            requested
        };
        let origin = Origin {
            named_vertex: override_origin.named_vertex.or(parent_origin.named_vertex),
            reference_cell: override_origin
                .reference_cell
                .or(parent_origin.reference_cell),
        };
        let index = frames.len();
        let guard = Guard {
            key,
            index,
            events: slot.events,
            taints: slot.taints,
            _not_send: PhantomData,
        };
        frames.push(Frame {
            key,
            previous: slot.head,
            mode,
            origin,
        });
        slot.head = Some(index);
        guard
    })
}
pub(super) fn current(key: usize) -> (Mode, Origin) {
    SLOTS.with(|state| {
        let state = state.borrow();
        state
            .slots
            .get(&key)
            .and_then(|slot| slot.head)
            .map(|index| state.frames[index])
            .map_or((Mode::Inherit, Origin::default()), |frame| {
                (frame.mode, frame.origin)
            })
    })
}
pub(super) fn event(key: usize) -> Option<(Mode, Origin)> {
    SLOTS.with(|state| {
        let mut state = state.borrow_mut();
        let ThreadState { frames, slots } = &mut *state;
        let Some(slot) = slots.get_mut(&key) else {
            return Some((Mode::Inherit, Origin::default()));
        };
        let frame = frames[slot.head.expect("active slot has a head")];
        if frame.mode == Mode::Suppressed {
            return None;
        }
        slot.events = slot.events.wrapping_add(1);
        Some((frame.mode, frame.origin))
    })
}
pub(super) fn taint(key: usize) {
    SLOTS.with(|state| {
        let mut state = state.borrow_mut();
        let ThreadState { frames, slots } = &mut *state;
        if let Some(slot) = slots.get_mut(&key)
            && slot
                .head
                .is_some_and(|index| frames[index].mode != Mode::Suppressed)
        {
            slot.taints = slot.taints.wrapping_add(1);
        }
    });
}
impl Guard {
    pub(super) fn encountered(&self) -> bool {
        SLOTS.with(|state| state.borrow().slots[&self.key].events != self.events)
    }
    pub(super) fn tainted(&self) -> bool {
        SLOTS.with(|state| {
            let state = state.borrow();
            let slot = &state.slots[&self.key];
            slot.events != self.events || slot.taints != self.taints
        })
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        SLOTS.with(|state| {
            let mut state = state.borrow_mut();
            debug_assert_eq!(state.frames.len(), self.index + 1);
            let frame = state.frames.pop().expect("scope frame exists");
            debug_assert_eq!(frame.key, self.key);
            let slot = state
                .slots
                .get_mut(&self.key)
                .expect("collector slot exists");
            slot.head = frame.previous;
            if slot.head.is_none() {
                state.slots.remove(&self.key);
            }
            // Keep the one frames Vec and HashMap backing allocation for reuse.
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_and_taint_reach_ancestors_without_frame_walks() {
        let outer = enter(101, Mode::Inherit, Origin::default());
        let inner = enter(101, Mode::Inherit, Origin::default());
        assert!(!outer.encountered());
        taint(101);
        assert!(outer.tainted());
        assert!(inner.tainted());
        assert!(event(101).is_some());
        assert!(outer.encountered());
        assert!(inner.encountered());
        drop(inner);
        drop(outer);
        SLOTS.with(|state| assert!(!state.borrow().slots.contains_key(&101)));
    }

    #[test]
    fn collectors_do_not_share_generations() {
        let a = enter(201, Mode::Inherit, Origin::default());
        let b = enter(202, Mode::Inherit, Origin::default());
        event(202);
        assert!(!a.encountered());
        assert!(b.encountered());
    }

    #[test]
    fn suppression_dominates_nested_speculation() {
        let outer = enter(301, Mode::Suppressed, Origin::default());
        let inner = enter(301, Mode::Speculative, Origin::default());
        assert!(event(301).is_none());
        taint(301);
        assert!(!outer.encountered());
        assert!(!inner.tainted());
    }

    #[test]
    fn dropping_last_scope_cannot_leak_state_to_reused_key() {
        {
            let first = enter(401, Mode::Speculative, Origin::default());
            event(401);
            assert!(first.encountered());
        }
        let next = enter(401, Mode::Inherit, Origin::default());
        assert!(!next.encountered());
        assert!(!next.tainted());
        assert!(current(401).0 == Mode::Inherit);
    }
}
