/// String interning for deduplication of text values and identifiers
/// Uses FxHashMap for fast lookups and Box<str> to minimize allocations
use rustc_hash::FxHashMap;
use std::{fmt, sync::Arc};

/// Reference to an interned string
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct StringId(u32);

impl StringId {
    /// Maximum number of strings that can be interned (2^32 - 1)
    pub const MAX: u32 = u32::MAX - 1;

    /// Invalid/null string ID
    pub const INVALID: StringId = StringId(u32::MAX);

    pub fn as_u32(self) -> u32 {
        self.0
    }

    pub fn from_raw(raw: u32) -> Self {
        StringId(raw)
    }

    pub fn is_valid(self) -> bool {
        self.0 != u32::MAX
    }
}

impl fmt::Display for StringId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StringId({})", self.0)
    }
}

/// String interner for deduplicating strings
#[derive(Debug)]
pub struct StringInterner {
    /// Storage for interned strings
    strings: Vec<Arc<str>>,
    /// Map from string content to ID for deduplication
    lookup: FxHashMap<Arc<str>, StringId>,
}

impl StringInterner {
    pub fn new() -> Self {
        Self {
            strings: Vec::new(),
            lookup: FxHashMap::default(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            strings: Vec::with_capacity(cap),
            lookup: FxHashMap::with_capacity_and_hasher(cap, Default::default()),
        }
    }

    /// Intern a string, returning its ID
    /// If the string is already interned, returns the existing ID
    pub fn intern(&mut self, s: &str) -> StringId {
        // Check if already interned
        if let Some(&id) = self.lookup.get(s) {
            return id;
        }

        // Check for overflow
        let index = self.strings.len() as u32;
        if index > StringId::MAX {
            panic!("String interner overflow: too many strings");
        }

        // Add new string
        let id = StringId(index);
        let boxed: Arc<str> = s.into();

        // We need to clone the boxed string for the lookup key
        // This is safe because Box<str> is cheap to clone (just pointer copy)
        self.lookup.insert(boxed.clone(), id);
        self.strings.push(boxed);

        id
    }

    /// Get a string by its ID
    #[inline]
    pub fn get(&self, id: StringId) -> Option<&str> {
        if id.is_valid() {
            self.strings.get(id.0 as usize).map(|s| &**s)
        } else {
            None
        }
    }

    /// Get a string by its ID, panicking if invalid
    #[inline]
    pub fn resolve(&self, id: StringId) -> &str {
        self.get(id)
            .unwrap_or_else(|| panic!("Invalid string ID: {id:?}"))
    }

    /// Check if a string is already interned
    pub fn contains(&self, s: &str) -> bool {
        self.lookup.contains_key(s)
    }

    /// Get the ID of an already-interned string without interning
    pub fn get_id(&self, s: &str) -> Option<StringId> {
        self.lookup.get(s).copied()
    }

    /// Returns the number of interned strings
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Returns true if no strings are interned
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    /// Returns memory usage in bytes (approximate)
    pub fn memory_usage(&self) -> usize {
        // Vector overhead
        let vec_overhead = self.strings.capacity() * std::mem::size_of::<Box<str>>();

        // String data
        let string_data: usize = self
            .strings
            .iter()
            .map(|s| s.len() + std::mem::size_of::<Box<str>>())
            .sum();

        // HashMap overhead (approximate)
        let map_overhead = self.lookup.capacity()
            * (std::mem::size_of::<Box<str>>() + std::mem::size_of::<StringId>());

        vec_overhead + string_data + map_overhead
    }

    /// Clear all interned strings
    pub fn clear(&mut self) {
        self.strings.clear();
        self.lookup.clear();
    }

    /// Iterate over all interned strings with their IDs
    pub fn iter(&self) -> impl Iterator<Item = (StringId, &str)> + '_ {
        self.strings
            .iter()
            .enumerate()
            .map(|(i, s)| (StringId(i as u32), &**s))
    }

    /// Get statistics about the interner
    pub fn stats(&self) -> InternerStats {
        let total_bytes: usize = self.strings.iter().map(|s| s.len()).sum();
        let unique_count = self.strings.len();

        InternerStats {
            unique_count,
            total_bytes,
            average_length: if unique_count > 0 {
                total_bytes as f64 / unique_count as f64
            } else {
                0.0
            },
        }
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the string interner
#[derive(Debug, Clone)]
pub struct InternerStats {
    pub unique_count: usize,
    pub total_bytes: usize,
    pub average_length: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_interning() {
        let mut interner = StringInterner::new();

        let s1 = interner.intern("Hello");
        let s2 = interner.intern("Hello");
        let s3 = interner.intern("World");

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
        assert_eq!(interner.get(s1), Some("Hello"));
        assert_eq!(interner.get(s3), Some("World"));
    }

    #[test]
    fn test_string_memory_efficiency() {
        let mut interner = StringInterner::new();

        // Intern same string 1000 times
        let refs: Vec<_> = (0..1000).map(|_| interner.intern("repeated")).collect();

        // Should only store string once
        assert_eq!(interner.len(), 1);

        // All refs should be the same
        for r in &refs[1..] {
            assert_eq!(*r, refs[0]);
        }

        // Memory usage should be much less than 9000 bytes
        assert!(interner.memory_usage() < 1000);
    }

    #[test]
    fn test_string_interner_capacity() {
        let mut interner = StringInterner::with_capacity(100);

        // Intern many different strings
        for i in 0..1000 {
            let s = format!("string_{i}");
            let id = interner.intern(&s);
            assert_eq!(interner.get(id), Some(s.as_str()));
        }

        assert_eq!(interner.len(), 1000);
    }

    #[test]
    fn test_string_interner_contains() {
        let mut interner = StringInterner::new();

        interner.intern("exists");

        assert!(interner.contains("exists"));
        assert!(!interner.contains("not_exists"));
    }

    #[test]
    fn test_string_interner_get_id() {
        let mut interner = StringInterner::new();

        let id = interner.intern("test");

        assert_eq!(interner.get_id("test"), Some(id));
        assert_eq!(interner.get_id("not_interned"), None);
    }

    #[test]
    fn test_string_interner_clear() {
        let mut interner = StringInterner::new();

        interner.intern("one");
        interner.intern("two");
        interner.intern("three");

        assert_eq!(interner.len(), 3);

        interner.clear();

        assert_eq!(interner.len(), 0);
        assert!(interner.is_empty());
    }

    #[test]
    fn test_string_interner_iter() {
        let mut interner = StringInterner::new();

        let id1 = interner.intern("first");
        let id2 = interner.intern("second");
        let id3 = interner.intern("third");

        let items: Vec<_> = interner.iter().collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0], (id1, "first"));
        assert_eq!(items[1], (id2, "second"));
        assert_eq!(items[2], (id3, "third"));
    }

    #[test]
    fn test_string_interner_stats() {
        let mut interner = StringInterner::new();

        interner.intern("short");
        interner.intern("medium_length");
        interner.intern("this_is_a_longer_string");

        let stats = interner.stats();

        assert_eq!(stats.unique_count, 3);
        assert_eq!(stats.total_bytes, 5 + 13 + 23);
        assert!((stats.average_length - 13.67).abs() < 0.01);
    }

    #[test]
    fn test_invalid_string_id() {
        let interner = StringInterner::new();

        let invalid = StringId::INVALID;
        assert_eq!(invalid.0, u32::MAX);
        assert!(!StringId::INVALID.is_valid());
        assert_eq!(interner.get(StringId::INVALID), None);
    }

    #[test]
    #[should_panic(expected = "Invalid string ID")]
    fn test_resolve_invalid_id() {
        let interner = StringInterner::new();
        interner.resolve(StringId::INVALID);
    }

    #[test]
    fn test_string_id_ordering() {
        let mut interner = StringInterner::new();

        let id1 = interner.intern("a");
        let id2 = interner.intern("b");
        let id3 = interner.intern("c");

        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }

    #[test]
    fn test_empty_string() {
        let mut interner = StringInterner::new();

        let id = interner.intern("");
        assert_eq!(interner.get(id), Some(""));

        // Empty string should also be deduplicated
        let id2 = interner.intern("");
        assert_eq!(id, id2);
    }

    #[test]
    fn test_unicode_strings() {
        let mut interner = StringInterner::new();

        let id1 = interner.intern("Hello 世界");
        let id2 = interner.intern("🦀 Rust");
        let id3 = interner.intern("Hello 世界");

        assert_eq!(id1, id3);
        assert_ne!(id1, id2);

        assert_eq!(interner.get(id1), Some("Hello 世界"));
        assert_eq!(interner.get(id2), Some("🦀 Rust"));
    }
}
