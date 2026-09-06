use super::lock_manager::LockManager;
use super::{Transaction, TransactionId};
use crate::{BustubxError, BustubxResult, Database};
use std::sync::{Arc, Mutex};

pub(super) struct ManagerState {
    pub database: Database,
    pub locks: LockManager,
    pub next_id: TransactionId,
}

/// Shareable transaction entry point. Use one manager for all related transactions.
#[derive(Clone)]
pub struct TransactionManager {
    pub(super) state: Arc<Mutex<ManagerState>>,
}
impl TransactionManager {
    pub fn new_temp() -> BustubxResult<Self> {
        Ok(Self {
            state: Arc::new(Mutex::new(ManagerState {
                database: Database::new_temp()?,
                locks: LockManager::default(),
                next_id: 1,
            })),
        })
    }
    /// Begin a strict two-phase-locking transaction. Lock conflicts do not wait.
    pub fn begin(&self) -> BustubxResult<Transaction> {
        let mut state = self.state.lock().unwrap();
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| BustubxError::Transaction("Transaction IDs exhausted".into()))?;
        if !state.locks.shared(id) {
            return Err(BustubxError::Transaction(
                "Database is locked by a writer".into(),
            ));
        }
        let snapshot = state
            .database
            .snapshot_bytes()
            .and_then(|bytes| Database::from_snapshot(&bytes));
        match snapshot {
            Ok(database) => Ok(Transaction {
                manager: self.clone(),
                id,
                database: Some(database),
                dirty: false,
            }),
            Err(error) => {
                state.locks.release(id);
                Err(error)
            }
        }
    }
}
