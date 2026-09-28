//! Test-only counting allocator for the feature build's test binary (M1a
//! correction B5): per-thread allocation counts, live bytes and peak, and
//! failure injection at the Nth allocation. Only an armed thread is
//! observed, so the rest of the suite runs unchanged. A reallocation counts
//! as one allocation whose old buffer coexists with the new one.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Probe;

#[global_allocator]
static GLOBAL: Probe = Probe;

#[derive(Clone, Copy)]
struct State {
    armed: bool,
    count: u64,
    fail_at: u64,
    failed: bool,
    live: i64,
    peak: i64,
}

const IDLE: State = State {
    armed: false,
    count: 0,
    fail_at: u64::MAX,
    failed: false,
    live: 0,
    peak: 0,
};

thread_local! {
    static STATE: Cell<State> = const { Cell::new(IDLE) };
}

/// Account an allocation of `new` bytes replacing `old` (0 for a fresh
/// one); false = inject a failure.
fn on_alloc(old: usize, new: usize) -> bool {
    STATE
        .try_with(|st| {
            let mut s = st.get();
            if !s.armed {
                return true;
            }
            let n = s.count;
            s.count += 1;
            if n == s.fail_at {
                s.failed = true;
                st.set(s);
                return false;
            }
            s.peak = s.peak.max(s.live + new as i64);
            s.live += new as i64 - old as i64;
            st.set(s);
            true
        })
        .unwrap_or(true)
}

fn on_dealloc(size: usize) {
    let _ = STATE.try_with(|st| {
        let mut s = st.get();
        if s.armed {
            s.live -= size as i64;
            st.set(s);
        }
    });
}

unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if !on_alloc(0, l.size()) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(l) }
    }

    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if !on_alloc(0, l.size()) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        on_dealloc(l.size());
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if !on_alloc(l.size(), new) {
            return std::ptr::null_mut();
        }
        unsafe { System.realloc(p, l, new) }
    }
}

/// What the allocator saw on this thread while `f` ran.
#[derive(Clone, Copy, Debug)]
pub struct Measure {
    /// Allocations (and reallocations) attempted.
    pub allocs: u64,
    /// Peak live bytes above the start.
    pub peak: i64,
    /// Live bytes at the end minus the start.
    pub net: i64,
    /// The injected failure fired.
    pub failed: bool,
}

/// Run `f` with this thread armed, failing allocation number `fail_at`
/// (0-based) if given.
pub fn measure<R>(fail_at: Option<u64>, f: impl FnOnce() -> R) -> (R, Measure) {
    STATE.with(|st| {
        st.set(State {
            armed: true,
            fail_at: fail_at.unwrap_or(u64::MAX),
            ..IDLE
        })
    });
    let r = f();
    let s = STATE.with(|st| {
        let s = st.get();
        st.set(IDLE);
        s
    });
    (
        r,
        Measure {
            allocs: s.count,
            peak: s.peak,
            net: s.live,
            failed: s.failed,
        },
    )
}
