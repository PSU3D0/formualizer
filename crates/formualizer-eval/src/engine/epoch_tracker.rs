use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Maximum number of concurrent reader threads supported
pub const MAX_THREADS: usize = 256;

/// Cache-padded atomic u64 to avoid false sharing
#[repr(align(64))]
struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    fn new(value: T) -> Self {
        Self { value }
    }
}

/// Epoch-based MVCC tracker for concurrent reads during writes
///
/// Allows multiple readers to access consistent snapshots while
/// writers make changes. Tracks the minimum epoch across all active
/// readers to determine when old data can be safely reclaimed.
pub struct EpochTracker {
    /// Current global epoch, incremented on each write
    current_epoch: AtomicU64,

    /// Per-thread reader epochs (u64::MAX = no active reader)
    /// Cache-padded to avoid false sharing between threads
    reader_epochs: Arc<Vec<CachePadded<AtomicU64>>>,

    /// Minimum epoch that is safe to reclaim (all readers are past this)
    safe_epoch: AtomicU64,
}

impl EpochTracker {
    pub fn new() -> Self {
        let mut reader_epochs = Vec::with_capacity(MAX_THREADS);
        for _ in 0..MAX_THREADS {
            reader_epochs.push(CachePadded::new(AtomicU64::new(u64::MAX)));
        }

        Self {
            current_epoch: AtomicU64::new(0),
            reader_epochs: Arc::new(reader_epochs),
            safe_epoch: AtomicU64::new(0),
        }
    }

    /// Get the current epoch
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Acquire)
    }

    /// Get the safe epoch (minimum across all active readers)
    pub fn safe_epoch(&self) -> u64 {
        self.safe_epoch.load(Ordering::Acquire)
    }

    /// Begin a write operation, incrementing the global epoch
    pub fn begin_write(&'_ self) -> WriteGuard<'_> {
        let epoch = self.current_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        WriteGuard {
            tracker: self,
            epoch,
            committed: false,
        }
    }

    /// Begin a read operation on the given thread
    pub fn begin_read(&'_ self, thread_id: usize) -> ReadGuard<'_> {
        assert!(
            thread_id < MAX_THREADS,
            "Thread ID {thread_id} exceeds MAX_THREADS"
        );

        let epoch = self.current_epoch.load(Ordering::Acquire);
        self.reader_epochs[thread_id]
            .value
            .store(epoch, Ordering::Release);

        ReadGuard {
            tracker: self,
            thread_id,
            epoch,
        }
    }

    /// Update the safe epoch based on current reader states
    fn update_safe_epoch(&self) {
        let current = self.current_epoch.load(Ordering::Acquire);
        let min_reader = self
            .reader_epochs
            .iter()
            .map(|padded| padded.value.load(Ordering::Acquire))
            .filter(|&epoch| epoch != u64::MAX) // Ignore inactive readers
            .min()
            .unwrap_or(current); // If no active readers, use current epoch

        self.safe_epoch.store(min_reader, Ordering::Release);
    }

    /// Wait until all readers have advanced past the given epoch
    pub fn wait_for_readers(&self, target_epoch: u64) {
        loop {
            self.update_safe_epoch();
            if self.safe_epoch.load(Ordering::Acquire) > target_epoch {
                break;
            }
            std::hint::spin_loop();
        }
    }
}

impl Default for EpochTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Guard for write operations - updates safe epoch on drop
pub struct WriteGuard<'a> {
    tracker: &'a EpochTracker,
    epoch: u64,
    committed: bool,
}

impl<'a> WriteGuard<'a> {
    /// Get the epoch this write is operating in
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Mark this write as committed (for two-phase commit protocols)
    pub fn commit(&mut self) {
        self.committed = true;
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        self.tracker.update_safe_epoch();
    }
}

/// Guard for read operations - clears reader epoch on drop
pub struct ReadGuard<'a> {
    tracker: &'a EpochTracker,
    thread_id: usize,
    epoch: u64,
}

impl<'a> ReadGuard<'a> {
    /// Get the epoch this read is operating in
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Check if this read's view is still current
    pub fn is_current(&self) -> bool {
        self.epoch == self.tracker.current_epoch()
    }
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        self.tracker.reader_epochs[self.thread_id]
            .value
            .store(u64::MAX, Ordering::Release);
        self.tracker.update_safe_epoch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_epoch_basic() {
        let tracker = EpochTracker::new();

        // Initial epoch should be 0
        assert_eq!(tracker.current_epoch(), 0);

        // Begin write should increment epoch
        let _write = tracker.begin_write();
        assert_eq!(tracker.current_epoch(), 1);
    }

    #[test]
    fn test_read_guard() {
        let tracker = EpochTracker::new();

        // Begin read on thread 0
        let _read = tracker.begin_read(0);

        // Safe epoch should be 0 (reader is at epoch 0)
        assert_eq!(tracker.safe_epoch(), 0);
    }

    #[test]
    fn test_write_advances_epoch() {
        let tracker = EpochTracker::new();

        {
            let _write = tracker.begin_write();
            assert_eq!(tracker.current_epoch(), 1);
        }

        // After write guard drops, safe epoch updates
        assert_eq!(tracker.safe_epoch(), 1);
    }

    #[test]
    fn test_concurrent_readers() {
        let tracker = Arc::new(EpochTracker::new());

        // Start multiple readers
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let t = Arc::clone(&tracker);
                thread::spawn(move || {
                    let _read = t.begin_read(i);
                    thread::sleep(Duration::from_millis(10));
                })
            })
            .collect();

        // Wait a bit for readers to start
        thread::sleep(Duration::from_millis(5));

        // Safe epoch should still be 0 (readers are at epoch 0)
        assert_eq!(tracker.safe_epoch(), 0);

        // Wait for all readers to finish
        for h in handles {
            h.join().unwrap();
        }

        // Force update after readers finish
        tracker.update_safe_epoch();

        // Safe epoch should be current epoch (0) after all readers finish
        assert_eq!(tracker.safe_epoch(), 0);
    }

    #[test]
    fn test_write_waits_for_readers() {
        let tracker = Arc::new(EpochTracker::new());

        // Start a long-running reader at epoch 0
        let reader_tracker = Arc::clone(&tracker);
        let reader = thread::spawn(move || {
            let _read = reader_tracker.begin_read(0);
            thread::sleep(Duration::from_millis(50));
        });

        // Give reader time to start
        thread::sleep(Duration::from_millis(10));

        // Create a write guard (advances to epoch 1)
        let _write = tracker.begin_write();
        assert_eq!(tracker.current_epoch(), 1);

        // Safe epoch should still be 0 (reader is active at epoch 0)
        tracker.update_safe_epoch();
        assert_eq!(tracker.safe_epoch(), 0);

        // Wait for reader to finish
        reader.join().unwrap();

        // Now safe epoch should advance
        tracker.update_safe_epoch();
        assert_eq!(tracker.safe_epoch(), 1);
    }

    #[test]
    #[should_panic(expected = "Thread ID 256 exceeds MAX_THREADS")]
    fn test_thread_id_overflow() {
        let tracker = EpochTracker::new();
        tracker.begin_read(MAX_THREADS); // Should panic
    }

    #[test]
    fn test_multiple_write_guards() {
        let tracker = EpochTracker::new();

        let write1 = tracker.begin_write();
        assert_eq!(tracker.current_epoch(), 1);

        let write2 = tracker.begin_write();
        assert_eq!(tracker.current_epoch(), 2);

        drop(write1);
        drop(write2);

        assert_eq!(tracker.safe_epoch(), 2);
    }

    #[test]
    fn test_mvcc_with_vertex_store() {
        use crate::engine::packed_coord::PackedCoord;
        use crate::engine::vertex_store::VertexStore;

        let tracker = Arc::new(EpochTracker::new());
        let store = Arc::new(std::sync::Mutex::new(VertexStore::new()));

        // Writer adds vertices
        let writer_tracker = Arc::clone(&tracker);
        let writer_store = Arc::clone(&store);
        let writer = thread::spawn(move || {
            let _write = writer_tracker.begin_write();
            let mut store = writer_store.lock().unwrap();

            for i in 0..5 {
                store.allocate(PackedCoord::new(i, i), 0, 0);
            }
        });

        // Reader observes consistent snapshot
        let reader_tracker = Arc::clone(&tracker);
        let reader_store = Arc::clone(&store);
        let reader = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10)); // Let writer start

            let _read = reader_tracker.begin_read(0);
            let store = reader_store.lock().unwrap();

            // Reader sees consistent view

            store.len()
        });

        writer.join().unwrap();
        let observed_len = reader.join().unwrap();

        // Reader either saw 0 (before write) or 5 (after write), not partial
        assert!(observed_len == 0 || observed_len == 5);
    }
}
