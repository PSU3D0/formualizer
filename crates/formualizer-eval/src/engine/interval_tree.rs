use std::collections::HashSet;

/// Custom interval tree optimized for spreadsheet cell indexing.
///
/// ## Design decisions:
///
/// 1. **Point intervals are the common case** - Most cells are single points [r,r] or [c,c]
/// 2. **Sparse data** - Even million-row sheets typically have <10K cells
/// 3. **Batch updates** - During shifts, we update many intervals at once
/// 4. **Small value sets** - Each interval maps to a small set of VertexIds
///
/// ## Implementation:
///
/// Uses an augmented BST where each node stores:
/// - Interval [low, high]
/// - Max endpoint in subtree (for efficient pruning)
/// - Value set (HashSet<VertexId>)
///
/// This is simpler than generic interval trees because we optimize for our specific use case.
#[derive(Debug, Clone)]
pub struct IntervalTree<T: Clone + Eq + std::hash::Hash> {
    root: Option<Box<Node<T>>>,
    size: usize,
}

#[derive(Debug, Clone)]
struct Node<T: Clone + Eq + std::hash::Hash> {
    /// The interval [low, high]
    low: u32,
    high: u32,
    /// Maximum high value in this subtree (for query pruning)
    max_high: u32,
    /// Values associated with this interval
    values: HashSet<T>,
    /// Left child (intervals with smaller low value)
    left: Option<Box<Node<T>>>,
    /// Right child (intervals with larger low value)
    right: Option<Box<Node<T>>>,
}

impl<T: Clone + Eq + std::hash::Hash> Node<T> {
    /// Creates a new leaf node for the interval tree.
    fn new(low: u32, high: u32, values: HashSet<T>) -> Self {
        Self {
            low,
            high,
            max_high: high,
            values,
            left: None,
            right: None,
        }
    }
}

impl<T: Clone + Eq + std::hash::Hash> IntervalTree<T> {
    /// Create a new empty interval tree
    pub fn new() -> Self {
        Self {
            root: None,
            size: 0,
        }
    }

    /// Insert a value for the given interval [low, high]
    pub fn insert(&mut self, low: u32, high: u32, value: T) {
        if let Some(root) = &mut self.root {
            if Self::insert_into_node(root, low, high, value) {
                self.size += 1;
            }
        } else {
            let mut values = HashSet::new();
            values.insert(value);
            self.root = Some(Box::new(Node {
                low,
                high,
                max_high: high,
                values,
                left: None,
                right: None,
            }));
            self.size = 1;
        }
    }

    /// Insert into a node, returns true if a new interval was created
    fn insert_into_node(mut node: &mut Box<Node<T>>, low: u32, high: u32, value: T) -> bool {
        loop {
            // Update max_high as we traverse down
            if high > node.max_high {
                node.max_high = high;
            }

            // Exact match found
            if node.low == low && node.high == high {
                node.values.insert(value);
                return false;
            }

            if low < node.low {
                if node.left.is_none() {
                    let mut values = HashSet::new();
                    values.insert(value);
                    node.left = Some(Box::new(Node::new(low, high, values)));
                    return true;
                }
                // Move reference to the left child and continue loop
                node = node.left.as_mut().unwrap();
            } else {
                if node.right.is_none() {
                    let mut values = HashSet::new();
                    values.insert(value);
                    node.right = Some(Box::new(Node::new(low, high, values)));
                    return true;
                }
                // Move reference to the right child and continue loop
                node = node.right.as_mut().unwrap();
            }
        }
    }

    /// Remove a value from the interval [low, high]
    pub fn remove(&mut self, low: u32, high: u32, value: &T) -> bool {
        if let Some(root) = &mut self.root {
            Self::remove_from_node(root, low, high, value)
        } else {
            false
        }
    }

    fn remove_from_node(node: &mut Box<Node<T>>, low: u32, high: u32, value: &T) -> bool {
        if low == node.low && high == node.high {
            return node.values.remove(value);
        }

        if low < node.low {
            if let Some(left) = &mut node.left {
                return Self::remove_from_node(left, low, high, value);
            }
        } else if let Some(right) = &mut node.right {
            return Self::remove_from_node(right, low, high, value);
        }

        false
    }

    /// Query all intervals that overlap with [query_low, query_high]
    pub fn query(&self, q_low: u32, q_high: u32) -> Vec<(u32, u32, HashSet<T>)> {
        let mut results = Vec::new();
        let mut stack = Vec::new();

        if let Some(root) = &self.root {
            stack.push(root.as_ref());
        }

        while let Some(node) = stack.pop() {
            // 1. Check if current node's interval overlaps [q_low, q_high]
            if node.low <= q_high && node.high >= q_low {
                results.push((node.low, node.high, node.values.clone()));
            }

            // 2. Search left child?
            // Only if the subtree's maximum endpoint could reach our query range
            if let Some(left) = &node.left
                && left.max_high >= q_low
            {
                stack.push(left.as_ref());
            }

            // 3. Search right child?
            // Since right.low >= node.low, we only search if the query high
            // could possibly overlap with any node starting at node.low or higher.
            if let Some(right) = &node.right
                && q_high >= node.low
            {
                stack.push(right.as_ref());
            }
        }
        results
    }

    /// Get mutable reference to values for an exact interval match
    /// non recursive implementation.
    pub fn get_mut(&mut self, low: u32, high: u32) -> Option<&mut HashSet<T>> {
        let mut current = self.root.as_mut();

        while let Some(node) = current {
            if node.low == low && node.high == high {
                return Some(&mut node.values);
            }

            if low < node.low {
                current = node.left.as_mut();
            } else {
                current = node.right.as_mut();
            }
        }
        None
    }

    fn get_mut_in_node(node: &mut Box<Node<T>>, low: u32, high: u32) -> Option<&mut HashSet<T>> {
        if low == node.low && high == node.high {
            return Some(&mut node.values);
        }

        if low < node.low {
            if let Some(left) = &mut node.left {
                return Self::get_mut_in_node(left, low, high);
            }
        } else if let Some(right) = &mut node.right {
            return Self::get_mut_in_node(right, low, high);
        }

        None
    }

    /// Check if the tree is empty
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Get the number of intervals in the tree
    pub fn len(&self) -> usize {
        self.size
    }

    /// Clear all intervals from the tree
    pub fn clear(&mut self) {
        self.root = None;
        self.size = 0;
    }

    /// Entry API for convenient insert-or-update operations
    pub fn entry(&mut self, low: u32, high: u32) -> Entry<'_, T> {
        Entry {
            tree: self,
            low,
            high,
        }
    }

    /// Bulk build optimization for a collection of point intervals [x,x].
    /// Expects (low == high) for all items. Existing content is discarded if tree is empty; if not empty, falls back to incremental inserts.
    pub fn bulk_build_points(&mut self, mut items: Vec<(u32, std::collections::HashSet<T>)>) {
        if self.root.is_some() {
            // Fallback: incremental insert to preserve existing nodes
            for (k, set) in items.into_iter() {
                for v in set {
                    self.insert(k, k, v);
                }
            }
            return;
        }
        if items.is_empty() {
            return;
        }
        // Sort by coordinate to build balanced tree
        items.sort_by_key(|(k, _)| *k);
        // Deduplicate keys by merging sets
        let mut dedup: Vec<(u32, std::collections::HashSet<T>)> = Vec::with_capacity(items.len());
        for (k, set) in items.into_iter() {
            if let Some(last) = dedup.last_mut()
                && last.0 == k
            {
                last.1.extend(set);
                continue;
            }
            dedup.push((k, set));
        }
        fn build_balanced<T: Clone + Eq + std::hash::Hash>(
            slice: &[(u32, std::collections::HashSet<T>)],
        ) -> Option<Box<Node<T>>> {
            if slice.is_empty() {
                return None;
            }
            let mid = slice.len() / 2;
            let (low, values) = (&slice[mid].0, &slice[mid].1);
            let left = build_balanced(&slice[..mid]);
            let right = build_balanced(&slice[mid + 1..]);
            // max_high is same as low (point interval); but need subtree max
            let mut max_high = *low;
            if let Some(ref l) = left
                && l.max_high > max_high
            {
                max_high = l.max_high;
            }
            if let Some(ref r) = right
                && r.max_high > max_high
            {
                max_high = r.max_high;
            }
            Some(Box::new(Node {
                low: *low,
                high: *low,
                max_high,
                values: values.clone(),
                left,
                right,
            }))
        }
        self.size = dedup.len();
        self.root = build_balanced(&dedup);
    }
}

impl<T: Clone + Eq + std::hash::Hash> Default for IntervalTree<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Entry API for interval tree
pub struct Entry<'a, T: Clone + Eq + std::hash::Hash> {
    tree: &'a mut IntervalTree<T>,
    low: u32,
    high: u32,
}

impl<'a, T: Clone + Eq + std::hash::Hash> Entry<'a, T> {
    /// Get or insert an empty HashSet for this interval
    pub fn or_insert_with<F>(self, f: F) -> &'a mut HashSet<T>
    where
        F: FnOnce() -> HashSet<T>,
    {
        // Check if interval exists
        if self.tree.get_mut(self.low, self.high).is_none() {
            let initial_values = f();
            // Create new node with empty set
            if let Some(ref mut root) = self.tree.root {
                // Iterative creation
                if Self::ensure_interval_exists(root, self.low, self.high, initial_values) {
                    self.tree.size += 1;
                }
            } else {
                self.tree.root = Some(Box::new(Node::new(self.low, self.high, initial_values)));
                self.tree.size = 1;
            }
        }

        self.tree.get_mut(self.low, self.high).unwrap()
    }

    // returns true, if a new node was inserted
    // returns false, if the node already existed.
    fn ensure_interval_exists(
        mut node: &mut Box<Node<T>>,
        low: u32,
        high: u32,
        values: HashSet<T>,
    ) -> bool {
        loop {
            if high > node.max_high {
                node.max_high = high;
            }

            if low == node.low && high == node.high {
                return false; // Already exists
            }

            if low < node.low {
                if node.left.is_none() {
                    node.left = Some(Box::new(Node {
                        low,
                        high,
                        max_high: high,
                        values,
                        left: None,
                        right: None,
                    }));
                    return true;
                }
                node = node.left.as_mut().unwrap();
            } else {
                if node.right.is_none() {
                    node.right = Some(Box::new(Node {
                        low,
                        high,
                        max_high: high,
                        values,
                        left: None,
                        right: None,
                    }));

                    return true;
                }
                node = node.right.as_mut().unwrap();
            }
        }
    }
}
impl<T: Clone + Eq + std::hash::Hash> Drop for IntervalTree<T> {
    /// Manually handles the deallocation of the tree structure.
    ///
    /// **Why it's needed:** The default Rust destructor is recursive. If the tree
    /// is deep (e.g., 10,000+ nodes in a line), the default drop will exceed
    /// the stack limit. This implementation moves the cleanup to the heap.
    ///
    /// **Callers:** Automatically called by the Rust compiler when an `IntervalTree`
    /// goes out of scope, or when `drop(tree)` is called manually.
    fn drop(&mut self) {
        let mut stack = Vec::new();

        // Take the root out of the tree, replacing it with None.
        if let Some(node) = self.root.take() {
            stack.push(node);
        }

        while let Some(mut node) = stack.pop() {
            // Use .take() to move the children into the heap-allocated stack.
            // This prevents the compiler from trying to drop them recursively
            // when 'node' itself is dropped at the end of this loop iteration.
            if let Some(left) = node.left.take() {
                stack.push(left);
            }
            if let Some(right) = node.right.take() {
                stack.push(right);
            }
            // 'node' goes out of scope here and is deallocated, but its
            // children are safe on our manual stack.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_query_point_interval() {
        let mut tree = IntervalTree::new();
        tree.insert(5, 5, 100);

        let results = tree.query(5, 5);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 5);
        assert_eq!(results[0].1, 5);
        assert!(results[0].2.contains(&100));
    }

    #[test]
    fn test_insert_and_query_range() {
        let mut tree = IntervalTree::new();
        tree.insert(10, 20, 1);
        tree.insert(15, 25, 2);
        tree.insert(30, 40, 3);

        // Query overlapping with first two intervals
        let results = tree.query(12, 22);
        assert_eq!(results.len(), 2);

        // Query overlapping with only the third interval
        let results = tree.query(35, 45);
        assert_eq!(results.len(), 1);
        assert!(results[0].2.contains(&3));
    }

    #[test]
    fn test_remove_value() {
        let mut tree = IntervalTree::new();
        tree.insert(5, 5, 100);
        tree.insert(5, 5, 200);

        assert_eq!(tree.query(5, 5).len(), 1);
        assert_eq!(tree.query(5, 5)[0].2.len(), 2);

        tree.remove(5, 5, &100);

        let results = tree.query(5, 5);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2.len(), 1);
        assert!(results[0].2.contains(&200));
    }

    #[test]
    fn test_entry_api() {
        let mut tree: IntervalTree<i32> = IntervalTree::new();

        tree.entry(10, 10).or_insert_with(HashSet::new).insert(42);

        tree.entry(10, 10).or_insert_with(HashSet::new).insert(43);

        let results = tree.query(10, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2.len(), 2);
        assert!(results[0].2.contains(&42));
        assert!(results[0].2.contains(&43));
    }

    #[test]
    fn test_large_sparse_tree() {
        let mut tree = IntervalTree::new();

        // Simulate sparse spreadsheet
        for i in (0..1_000_000).step_by(10000) {
            tree.insert(i, i, i as i32);
        }

        assert_eq!(tree.len(), 100);

        // Query for high rows
        let results = tree.query(500_000, u32::MAX);
        assert_eq!(results.len(), 50);
    }

    #[test]
    fn test_entry_recursion_bug() {
        let mut tree: IntervalTree<u32> = IntervalTree::new();

        // The bug happens when we insert a value, then use entry()
        // on a coordinate that would be a child of that value.
        let count: u32 = 5000;
        for i in 0..count {
            tree.entry(i, i).or_insert_with(HashSet::new);
        }

        assert_eq!(tree.len(), count as usize);
    }

    #[test]
    fn test_complex_overlaps() {
        let mut tree = IntervalTree::new();
        // Nested intervals
        tree.insert(10, 100, "A");
        tree.insert(20, 50, "B");
        tree.insert(30, 40, "C");

        // Partially overlapping
        tree.insert(5, 15, "D");
        tree.insert(95, 105, "E");

        // Query for the very middle
        let results = tree.query(35, 35);
        assert_eq!(results.len(), 3); // Should hit A, B, and C

        // Query for a range that only hits the "tail" of the large interval and the "head" of the end interval
        let results = tree.query(98, 102);
        assert_eq!(results.len(), 2); // Should hit A and E
    }

    #[test]
    fn test_multiple_values_and_size() {
        let mut tree = IntervalTree::new();

        // Insert same interval twice with different values
        tree.insert(10, 10, "val1");
        tree.insert(10, 10, "val2");
        assert_eq!(tree.len(), 1); // Size should only count unique intervals

        // Insert same value twice
        tree.insert(10, 10, "val1");
        assert_eq!(tree.len(), 1);
        let results = tree.query(10, 10);
        assert_eq!(results[0].2.len(), 2); // HashSet handles the duplicate value "val1"
    }

    #[test]
    fn test_remove_edge_cases() {
        let mut tree = IntervalTree::new();
        tree.insert(10, 20, "A");

        // Try to remove a value that isn't there
        let removed = tree.remove(10, 20, &"B");
        assert!(!removed);
        assert_eq!(tree.query(10, 20)[0].2.len(), 1);

        // Try to remove from an interval that doesn't exist
        let removed = tree.remove(99, 100, &"A");
        assert!(!removed);
    }

    #[test]
    fn test_bulk_build_consistency() {
        let mut incremental_tree = IntervalTree::new();
        let mut bulk_tree = IntervalTree::new();

        let data: Vec<(u32, HashSet<&str>)> = vec![
            (10, vec!["A", "B"].into_iter().collect()),
            (20, vec!["C"].into_iter().collect()),
            (5, vec!["D"].into_iter().collect()),
        ];

        // Build incrementally
        for (coord, values) in &data {
            for val in values {
                incremental_tree.insert(*coord, *coord, *val);
            }
        }

        // Build using bulk
        bulk_tree.bulk_build_points(data.clone());

        // Compare results
        assert_eq!(incremental_tree.len(), bulk_tree.len());
        assert_eq!(incremental_tree.query(0, 100), bulk_tree.query(0, 100));
    }

    #[test]
    fn test_query_stack_safety() {
        let mut tree = IntervalTree::new();
        let count = 10_000;

        // Create a deep right-leaning tree
        for i in 0..count {
            tree.insert(i, i, i);
        }

        // Query the very end of the tree
        // If this causes a SIGABRT, it means query_node() must be made iterative
        let results = tree.query(count - 1, count - 1);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_empty_and_boundaries() {
        let mut tree: IntervalTree<i32> = IntervalTree::new();

        assert!(tree.is_empty());
        assert_eq!(tree.query(0, 100).len(), 0);
        assert!(!tree.remove(0, 0, &1));

        // Test a query that "misses" everything
        tree.insert(50, 60, 1);
        assert_eq!(tree.query(0, 49).len(), 0);
        assert_eq!(tree.query(61, 100).len(), 0);
    }
}
