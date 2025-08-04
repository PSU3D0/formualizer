/// Efficient storage for Excel errors with message preservation
use super::string_interner::{StringId, StringInterner};
use formualizer_common::{ExcelError, ExcelErrorKind};
use rustc_hash::FxHashMap;

/// Reference to an error in the arena
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ErrorRef(u32);

impl ErrorRef {
    pub fn as_u32(self) -> u32 {
        self.0
    }

    pub fn from_raw(raw: u32) -> Self {
        Self(raw)
    }
}

/// Stored error data
#[derive(Debug, Clone)]
struct ErrorData {
    kind: ExcelErrorKind,
    message_id: Option<StringId>,
}

/// Arena for efficient error storage with deduplication
#[derive(Debug)]
pub struct ErrorArena {
    /// All stored errors
    errors: Vec<ErrorData>,

    /// Deduplication cache by (kind, message_id)
    dedup_cache: FxHashMap<(ExcelErrorKind, Option<StringId>), ErrorRef>,

    /// String interner for error messages
    strings: StringInterner,
}

impl ErrorArena {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            dedup_cache: FxHashMap::default(),
            strings: StringInterner::new(),
        }
    }

    pub fn with_capacity(estimated_errors: usize) -> Self {
        Self {
            errors: Vec::with_capacity(estimated_errors),
            dedup_cache: FxHashMap::default(),
            strings: StringInterner::with_capacity(estimated_errors / 2),
        }
    }

    /// Store an error and return a reference
    pub fn insert(&mut self, error: &ExcelError) -> ErrorRef {
        // Intern the message if present
        let message_id = error.message.as_ref().map(|msg| self.strings.intern(msg));

        // Check deduplication cache
        let cache_key = (error.kind, message_id);
        if let Some(&error_ref) = self.dedup_cache.get(&cache_key) {
            return error_ref;
        }

        // Create new error entry
        let error_data = ErrorData {
            kind: error.kind,
            message_id,
        };

        let idx = self.errors.len() as u32;
        self.errors.push(error_data);

        let error_ref = ErrorRef(idx);
        self.dedup_cache.insert(cache_key, error_ref);

        error_ref
    }

    /// Retrieve an error by reference
    pub fn get(&self, error_ref: ErrorRef) -> Option<ExcelError> {
        let error_data = self.errors.get(error_ref.0 as usize)?;

        let message = error_data
            .message_id
            .map(|id| self.strings.resolve(id).to_string());

        let mut error = ExcelError::new(error_data.kind);
        if let Some(msg) = message {
            error = error.with_message(msg);
        }

        Some(error)
    }

    /// Get memory usage statistics
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<ErrorData>() * self.errors.capacity()
            + self.strings.memory_usage()
            + self.dedup_cache.len()
                * std::mem::size_of::<((ExcelErrorKind, Option<StringId>), ErrorRef)>()
    }

    /// Get number of stored errors
    pub fn len(&self) -> usize {
        self.errors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Clear all errors
    pub fn clear(&mut self) {
        self.errors.clear();
        self.dedup_cache.clear();
        self.strings.clear();
    }
}

impl Default for ErrorArena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_storage_and_retrieval() {
        let mut arena = ErrorArena::new();

        let error = ExcelError::new(ExcelErrorKind::Div).with_message("Cannot divide by zero");

        let error_ref = arena.insert(&error);
        let retrieved = arena.get(error_ref).unwrap();

        assert_eq!(retrieved.kind, ExcelErrorKind::Div);
        assert_eq!(retrieved.message, Some("Cannot divide by zero".to_string()));
    }

    #[test]
    fn test_error_deduplication() {
        let mut arena = ErrorArena::new();

        let error1 = ExcelError::new(ExcelErrorKind::Value).with_message("Invalid type");
        let error2 = ExcelError::new(ExcelErrorKind::Value).with_message("Invalid type");

        let ref1 = arena.insert(&error1);
        let ref2 = arena.insert(&error2);

        assert_eq!(ref1, ref2);
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn test_different_messages_not_deduplicated() {
        let mut arena = ErrorArena::new();

        let error1 = ExcelError::new(ExcelErrorKind::Value).with_message("Message 1");
        let error2 = ExcelError::new(ExcelErrorKind::Value).with_message("Message 2");

        let ref1 = arena.insert(&error1);
        let ref2 = arena.insert(&error2);

        assert_ne!(ref1, ref2);
        assert_eq!(arena.len(), 2);
    }

    #[test]
    fn test_error_without_message() {
        let mut arena = ErrorArena::new();

        let error = ExcelError::new(ExcelErrorKind::Na);
        let error_ref = arena.insert(&error);
        let retrieved = arena.get(error_ref).unwrap();

        assert_eq!(retrieved.kind, ExcelErrorKind::Na);
        assert_eq!(retrieved.message, None);
    }
}
