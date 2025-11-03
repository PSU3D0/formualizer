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
    fn insert_into_node(node: &mut Box<Node<T>>, low: u32, high: u32, value: T) -> bool {
        // Update max_high if needed
        if high > node.max_high {
            node.max_high = high;
        }

        // Check if this is the same interval
        if low == node.low && high == node.high {
            // Add value to existing interval
            node.values.insert(value);
            return false; // No new interval created
        }

        // Decide which subtree to insert into based on low value
        if low < node.low {
            if let Some(left) = &mut node.left {
                Self::insert_into_node(left, low, high, value)
            } else {
                let mut values = HashSet::new();
                values.insert(value);
                node.left = Some(Box::new(Node {
                    low,
                    high,
                    max_high: high,
                    values,
                    left: None,
                    right: None,
                }));
                true
            }
        } else if let Some(right) = &mut node.right {
            Self::insert_into_node(right, low, high, value)
        } else {
            let mut values = HashSet::new();
            values.insert(value);
            node.right = Some(Box::new(Node {
                low,
                high,
                max_high: high,
                values,
                left: None,
                right: None,
            }));
            true
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
    pub fn query(&self, query_low: u32, query_high: u32) -> Vec<(u32, u32, HashSet<T>)> {
        let mut results = Vec::new();
        if let Some(root) = &self.root {
            Self::query_node(root, query_low, query_high, &mut results);
        }
        results
    }

    fn query_node(
        node: &Node<T>,
        query_low: u32,
        query_high: u32,
        results: &mut Vec<(u32, u32, HashSet<T>)>,
    ) {
        // Check if this node's interval overlaps with query
        if node.low <= query_high && node.high >= query_low {
            results.push((node.low, node.high, node.values.clone()));
        }

        // Check left subtree if it might contain overlapping intervals
        if let Some(left) = &node.left {
            // Only traverse left if its max_high could overlap
            if left.max_high >= query_low {
                Self::query_node(left, query_low, query_high, results);
            }
        }

        // Check right subtree if it might contain overlapping intervals
        if let Some(right) = &node.right {
            // Only traverse right if the query extends beyond this node's low
            if query_high >= node.low {
                Self::query_node(right, query_low, query_high, results);
            }
        }
    }

    /// Get mutable reference to values for an exact interval match
    pub fn get_mut(&mut self, low: u32, high: u32) -> Option<&mut HashSet<T>> {
        if let Some(root) = &mut self.root {
            Self::get_mut_in_node(root, low, high)
        } else {
            None
        }
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
            // Create new node with empty set
            if let Some(root) = &mut self.tree.root {
                Self::ensure_interval_exists(root, self.low, self.high);
            } else {
                self.tree.root = Some(Box::new(Node {
                    low: self.low,
                    high: self.high,
                    max_high: self.high,
                    values: f(),
                    left: None,
                    right: None,
                }));
                self.tree.size = 1;
            }
        }

        self.tree.get_mut(self.low, self.high).unwrap()
    }

    fn ensure_interval_exists(node: &mut Box<Node<T>>, low: u32, high: u32) {
        if high > node.max_high {
            node.max_high = high;
        }

        if low == node.low && high == node.high {
            return;
        }

        if low < node.low {
            if let Some(left) = &mut node.left {
                Self::ensure_interval_exists(left, low, high);
            } else {
                node.left = Some(Box::new(Node {
                    low,
                    high,
                    max_high: high,
                    values: HashSet::new(),
                    left: None,
                    right: None,
                }));
            }
        } else if let Some(right) = &mut node.right {
            Self::ensure_interval_exists(right, low, high);
        } else {
            node.right = Some(Box::new(Node {
                low,
                high,
                max_high: high,
                values: HashSet::new(),
                left: None,
                right: None,
            }));
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
}
