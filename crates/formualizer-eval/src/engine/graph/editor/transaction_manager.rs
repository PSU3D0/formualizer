//! Transaction state management for dependency graph mutations
//!
//! This module provides:
//! - TransactionManager: Manages transaction lifecycle and state
//! - TransactionId: Unique identifier for transactions
//! - Transaction: Internal state for active transactions

use std::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(u64);

impl Default for TransactionId {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionId {
    /// Create a new unique transaction ID
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        TransactionId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tx:{}", self.0)
    }
}

/// Internal state for an active transaction
#[derive(Debug)]
struct Transaction {
    id: TransactionId,
    /// Index in change log where this transaction started
    start_index: usize,
    /// Named savepoints for partial rollback
    savepoints: Vec<(String, usize)>,
}

/// Errors that can occur during transaction operations
#[derive(Debug, Clone)]
pub enum TransactionError {
    /// A transaction is already active
    AlreadyActive,
    /// No transaction is currently active
    NoActiveTransaction,
    /// Transaction has grown too large
    TransactionTooLarge { size: usize, max: usize },
    /// Rollback operation failed
    RollbackFailed(String),
    /// Savepoint not found
    SavepointNotFound(String),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyActive => write!(f, "Transaction already active"),
            Self::NoActiveTransaction => write!(f, "No active transaction"),
            Self::TransactionTooLarge { size, max } => {
                write!(f, "Transaction too large: {size} > {max}")
            }
            Self::RollbackFailed(msg) => write!(f, "Rollback failed: {msg}"),
            Self::SavepointNotFound(name) => write!(f, "Savepoint not found: {name}"),
        }
    }
}

impl std::error::Error for TransactionError {}

/// Manages transaction state independently of graph mutations
#[derive(Debug)]
pub struct TransactionManager {
    active_transaction: Option<Transaction>,
    /// Maximum number of changes allowed in a single transaction
    max_transaction_size: usize,
}

impl TransactionManager {
    /// Create a new transaction manager with default settings
    pub fn new() -> Self {
        Self {
            active_transaction: None,
            max_transaction_size: 10_000, // Configurable limit
        }
    }

    /// Create a transaction manager with custom size limit
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            active_transaction: None,
            max_transaction_size: max_size,
        }
    }

    /// Begin a new transaction
    ///
    /// # Arguments
    /// * `change_log_size` - Current size of the change log
    ///
    /// # Returns
    /// The ID of the newly created transaction
    ///
    /// # Errors
    /// Returns `AlreadyActive` if a transaction is already in progress
    pub fn begin(&mut self, change_log_size: usize) -> Result<TransactionId, TransactionError> {
        if self.active_transaction.is_some() {
            return Err(TransactionError::AlreadyActive);
        }

        let id = TransactionId::new();
        self.active_transaction = Some(Transaction {
            id,
            start_index: change_log_size,
            savepoints: Vec::new(),
        });
        Ok(id)
    }

    /// Commit the current transaction
    ///
    /// # Returns
    /// The ID of the committed transaction
    ///
    /// # Errors
    /// Returns `NoActiveTransaction` if no transaction is active
    pub fn commit(&mut self) -> Result<TransactionId, TransactionError> {
        self.active_transaction
            .take()
            .map(|tx| tx.id)
            .ok_or(TransactionError::NoActiveTransaction)
    }

    /// Get information needed for rollback and clear the transaction
    ///
    /// # Returns
    /// A tuple of (transaction_id, start_index) for the transaction
    ///
    /// # Errors
    /// Returns `NoActiveTransaction` if no transaction is active
    pub fn rollback_info(&mut self) -> Result<(TransactionId, usize), TransactionError> {
        self.active_transaction
            .take()
            .map(|tx| (tx.id, tx.start_index))
            .ok_or(TransactionError::NoActiveTransaction)
    }

    /// Add a named savepoint to the current transaction
    ///
    /// # Arguments
    /// * `name` - Name for the savepoint
    /// * `change_log_size` - Current size of the change log
    ///
    /// # Errors
    /// Returns `NoActiveTransaction` if no transaction is active
    pub fn add_savepoint(
        &mut self,
        name: String,
        change_log_size: usize,
    ) -> Result<(), TransactionError> {
        if let Some(tx) = &mut self.active_transaction {
            tx.savepoints.push((name, change_log_size));
            Ok(())
        } else {
            Err(TransactionError::NoActiveTransaction)
        }
    }

    /// Get the index for a named savepoint
    ///
    /// # Arguments
    /// * `name` - Name of the savepoint to find
    ///
    /// # Returns
    /// The change log index where the savepoint was created
    ///
    /// # Errors
    /// Returns `NoActiveTransaction` if no transaction is active
    /// Returns `SavepointNotFound` if the named savepoint doesn't exist
    pub fn get_savepoint(&self, name: &str) -> Result<usize, TransactionError> {
        if let Some(tx) = &self.active_transaction {
            tx.savepoints
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, idx)| *idx)
                .ok_or_else(|| TransactionError::SavepointNotFound(name.to_string()))
        } else {
            Err(TransactionError::NoActiveTransaction)
        }
    }

    /// Remove savepoints after a given index (for partial rollback)
    ///
    /// # Arguments
    /// * `index` - Remove all savepoints with index >= this value
    pub fn truncate_savepoints(&mut self, index: usize) {
        if let Some(tx) = &mut self.active_transaction {
            tx.savepoints.retain(|(_, idx)| *idx < index);
        }
    }

    /// Check if a transaction is currently active
    pub fn is_active(&self) -> bool {
        self.active_transaction.is_some()
    }

    /// Get the ID of the active transaction if any
    pub fn active_id(&self) -> Option<TransactionId> {
        self.active_transaction.as_ref().map(|tx| tx.id)
    }

    /// Check if transaction size is within limits
    ///
    /// # Arguments
    /// * `change_log_size` - Current size of the change log
    ///
    /// # Errors
    /// Returns `TransactionTooLarge` if size exceeds maximum
    pub fn check_size(&self, change_log_size: usize) -> Result<(), TransactionError> {
        if let Some(tx) = &self.active_transaction {
            let tx_size = change_log_size - tx.start_index;
            if tx_size > self.max_transaction_size {
                return Err(TransactionError::TransactionTooLarge {
                    size: tx_size,
                    max: self.max_transaction_size,
                });
            }
        }
        Ok(())
    }

    /// Get the maximum transaction size limit
    pub fn max_size(&self) -> usize {
        self.max_transaction_size
    }

    /// Set the maximum transaction size limit
    pub fn set_max_size(&mut self, max_size: usize) {
        self.max_transaction_size = max_size;
    }

    /// Get the start index of the active transaction
    pub fn start_index(&self) -> Option<usize> {
        self.active_transaction.as_ref().map(|tx| tx.start_index)
    }

    /// Get all savepoints in the current transaction
    pub fn savepoints(&self) -> Vec<(String, usize)> {
        self.active_transaction
            .as_ref()
            .map(|tx| tx.savepoints.clone())
            .unwrap_or_default()
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_id_uniqueness() {
        let id1 = TransactionId::new();
        let id2 = TransactionId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_transaction_manager_lifecycle() {
        let mut tm = TransactionManager::new();

        // No transaction initially
        assert!(!tm.is_active());
        assert!(tm.commit().is_err());
        assert!(tm.rollback_info().is_err());

        // Begin transaction
        let tx_id = tm.begin(0).unwrap();
        assert!(tm.is_active());
        assert_eq!(tm.active_id(), Some(tx_id));

        // Cannot begin nested transaction
        assert!(matches!(tm.begin(0), Err(TransactionError::AlreadyActive)));

        // Commit transaction
        let committed_id = tm.commit().unwrap();
        assert_eq!(tx_id, committed_id);
        assert!(!tm.is_active());

        // No transaction after commit
        assert!(tm.commit().is_err());
    }

    #[test]
    fn test_transaction_rollback_info() {
        let mut tm = TransactionManager::new();

        // Begin transaction at index 42
        let tx_id = tm.begin(42).unwrap();

        // Get rollback info
        let (rollback_id, start_index) = tm.rollback_info().unwrap();
        assert_eq!(rollback_id, tx_id);
        assert_eq!(start_index, 42);

        // Transaction cleared after rollback_info
        assert!(!tm.is_active());
    }

    #[test]
    fn test_transaction_size_limits() {
        let mut tm = TransactionManager::with_max_size(100);

        tm.begin(0).unwrap();

        // Within limit
        assert!(tm.check_size(50).is_ok());
        assert!(tm.check_size(100).is_ok());

        // Exceeds limit
        match tm.check_size(101) {
            Err(TransactionError::TransactionTooLarge { size, max }) => {
                assert_eq!(size, 101);
                assert_eq!(max, 100);
            }
            _ => panic!("Expected TransactionTooLarge error"),
        }
    }

    #[test]
    fn test_transaction_savepoints() {
        let mut tm = TransactionManager::new();

        // Cannot add savepoint without transaction
        assert!(tm.add_savepoint("test".to_string(), 10).is_err());

        tm.begin(0).unwrap();

        // Add savepoints
        tm.add_savepoint("before_risky_op".to_string(), 10).unwrap();
        tm.add_savepoint("after_risky_op".to_string(), 20).unwrap();
        tm.add_savepoint("final".to_string(), 30).unwrap();

        // Get savepoint
        assert_eq!(tm.get_savepoint("before_risky_op").unwrap(), 10);
        assert_eq!(tm.get_savepoint("after_risky_op").unwrap(), 20);
        assert_eq!(tm.get_savepoint("final").unwrap(), 30);

        // Non-existent savepoint
        assert!(matches!(
            tm.get_savepoint("missing"),
            Err(TransactionError::SavepointNotFound(_))
        ));

        // List all savepoints
        let savepoints = tm.savepoints();
        assert_eq!(savepoints.len(), 3);
        assert_eq!(savepoints[0].0, "before_risky_op");
        assert_eq!(savepoints[0].1, 10);
    }

    #[test]
    fn test_truncate_savepoints() {
        let mut tm = TransactionManager::new();
        tm.begin(0).unwrap();

        tm.add_savepoint("sp1".to_string(), 10).unwrap();
        tm.add_savepoint("sp2".to_string(), 20).unwrap();
        tm.add_savepoint("sp3".to_string(), 30).unwrap();

        // Truncate savepoints >= index 20
        tm.truncate_savepoints(20);

        // Only first savepoint remains
        let savepoints = tm.savepoints();
        assert_eq!(savepoints.len(), 1);
        assert_eq!(savepoints[0].0, "sp1");

        // sp2 and sp3 were removed
        assert!(tm.get_savepoint("sp2").is_err());
        assert!(tm.get_savepoint("sp3").is_err());
    }

    #[test]
    fn test_max_size_configuration() {
        let mut tm = TransactionManager::new();
        assert_eq!(tm.max_size(), 10_000);

        tm.set_max_size(500);
        assert_eq!(tm.max_size(), 500);

        // New limit applies to size checks
        tm.begin(0).unwrap();
        assert!(tm.check_size(499).is_ok());
        assert!(tm.check_size(501).is_err());
    }

    #[test]
    fn test_start_index_tracking() {
        let mut tm = TransactionManager::new();

        // No start index without transaction
        assert_eq!(tm.start_index(), None);

        // Start index tracked during transaction
        tm.begin(123).unwrap();
        assert_eq!(tm.start_index(), Some(123));

        // Cleared after commit
        tm.commit().unwrap();
        assert_eq!(tm.start_index(), None);
    }

    #[test]
    fn test_error_display() {
        let err = TransactionError::AlreadyActive;
        assert_eq!(format!("{err}"), "Transaction already active");

        let err = TransactionError::NoActiveTransaction;
        assert_eq!(format!("{err}"), "No active transaction");

        let err = TransactionError::TransactionTooLarge {
            size: 150,
            max: 100,
        };
        assert_eq!(format!("{err}"), "Transaction too large: 150 > 100");

        let err = TransactionError::RollbackFailed("test error".to_string());
        assert_eq!(format!("{err}"), "Rollback failed: test error");

        let err = TransactionError::SavepointNotFound("missing".to_string());
        assert_eq!(format!("{err}"), "Savepoint not found: missing");
    }

    #[test]
    fn test_transaction_id_display() {
        let id = TransactionId(42);
        assert_eq!(format!("{id}"), "tx:42");
    }
}
