use std::collections::{BTreeMap, HashSet};

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
struct IntervalNode<T: Clone + Eq + std::hash::Hash> {
    high: u32,
    values: HashSet<T>,
}

/// B-Tree based implementation of the interval index.
#[derive(Debug, Clone)]
pub struct IntervalTree<T: Clone + Eq + std::hash::Hash> {
    /// Maps low coordinate to a set of intervals/values starting there.
    /// Internal storage uses IntervalNode, NOT Entry.
    map: BTreeMap<u32, Vec<IntervalNode<T>>>,
    size: usize,
}

impl<T: Clone + Eq + std::hash::Hash> Default for IntervalTree<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Eq + std::hash::Hash> IntervalTree<T> {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Get a mutable reference to the values for an exact interval match.
    /// Required by the Entry API.
    pub fn get_mut(&mut self, low: u32, high: u32) -> Option<&mut HashSet<T>> {
        self.map.get_mut(&low).and_then(|nodes| {
            nodes
                .iter_mut()
                .find(|n| n.high == high)
                .map(|n| &mut n.values)
        })
    }

    /// Insert a value for the given interval [low, high]
    pub fn insert(&mut self, low: u32, high: u32, value: T) {
        let entries = self.map.entry(low).or_default();

        if let Some(node) = entries.iter_mut().find(|n| n.high == high) {
            node.values.insert(value);
        } else {
            let mut values = HashSet::new();
            values.insert(value);
            entries.push(IntervalNode { high, values });
            self.size += 1;
        }
    }

    pub fn query(&self, q_low: u32, q_high: u32) -> Vec<(u32, u32, HashSet<T>)> {
        let mut results = Vec::new();
        for (&low, nodes) in self.map.range(..=q_high) {
            for node in nodes {
                if node.high >= q_low {
                    results.push((low, node.high, node.values.clone()));
                }
            }
        }
        results
    }

    pub fn remove(&mut self, low: u32, high: u32, value: &T) -> bool {
        if let Some(nodes) = self.map.get_mut(&low)
            && let Some(node) = nodes.iter_mut().find(|n| n.high == high)
        {
            let removed = node.values.remove(value);
            if node.values.is_empty() {
                nodes.retain(|n| n.high != high);
            }
            if nodes.is_empty() {
                self.map.remove(&low);
            }
            if removed {
                self.size -= 1;
            }
            return removed;
        }
        false
    }

    pub fn entry(&mut self, low: u32, high: u32) -> BTreeEntry<'_, T> {
        BTreeEntry {
            tree: self,
            low,
            high,
        }
    }

    /// Bulk build optimization for a collection of point intervals [x,x].
    pub fn bulk_build_points(&mut self, mut items: Vec<(u32, HashSet<T>)>) {
        if !self.is_empty() {
            // Fallback: incremental insert to preserve existing nodes
            for (coord, set) in items {
                for val in set {
                    self.insert(coord, coord, val);
                }
            }
            return;
        }

        if items.is_empty() {
            return;
        }

        // 1. Sort by coordinate
        items.sort_by_key(|(k, _)| *k);

        // 2. Process items. BTreeMap handles the balancing (O(log N)).
        for (coord, set) in items {
            let entries = self.map.entry(coord).or_default();

            // Since this is specifically for point intervals, check if [coord, coord] exists
            if let Some(node) = entries.iter_mut().find(|n| n.high == coord) {
                node.values.extend(set);
            } else {
                entries.push(IntervalNode {
                    high: coord,
                    values: set,
                });
                self.size += 1;
            }
        }
    }
}

pub struct BTreeEntry<'a, T: Clone + Eq + std::hash::Hash> {
    tree: &'a mut IntervalTree<T>,
    low: u32,
    high: u32,
}

impl<'a, T: Clone + Eq + std::hash::Hash> BTreeEntry<'a, T> {
    pub fn or_insert_with<F>(self, f: F) -> &'a mut HashSet<T>
    where
        F: FnOnce() -> HashSet<T>,
    {
        if self.tree.get_mut(self.low, self.high).is_none() {
            let values = f();
            let entries = self.tree.map.entry(self.low).or_default();
            entries.push(IntervalNode {
                high: self.high,
                values,
            });
            self.tree.size += 1;
        }
        self.tree.get_mut(self.low, self.high).unwrap()
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
