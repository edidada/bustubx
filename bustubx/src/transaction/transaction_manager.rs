use super::lock_manager::LockManager;
use super::recovery::Journal;
use super::{Transaction, TransactionId};
use crate::{BustubxError, BustubxResult, Database};
use std::sync::{Arc, Mutex};

pub(super) struct ManagerState {
    pub database: Database,
    pub locks: LockManager,
    pub next_id: TransactionId,
    pub version: u64,
    pub journal: Option<Journal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    Serializable,
    SnapshotIsolation,
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
                version: 0,
                journal: None,
            })),
        })
    }

    /// Open a durable full-image transaction journal (not a raw Database page file).
    pub fn new_on_disk(path: impl AsRef<std::path::Path>) -> BustubxResult<Self> {
        if cfg!(target_family = "wasm") {
            return Err(BustubxError::NotSupport(
                "Durable transaction journals require native file locking".into(),
            ));
        }
        let (mut journal, snapshot) = Journal::open(path)?;
        let (version, database) = match snapshot {
            Some((version, bytes)) => (version, Database::from_snapshot(&bytes)?),
            None => {
                let database = Database::new_temp()?;
                journal.append(0, &database.snapshot_bytes()?)?;
                (0, database)
            }
        };
        Ok(Self {
            state: Arc::new(Mutex::new(ManagerState {
                database,
                locks: LockManager::default(),
                next_id: 1,
                version,
                journal: Some(journal),
            })),
        })
    }
    /// Begin a strict two-phase-locking transaction. Lock conflicts do not wait.
    pub fn begin(&self) -> BustubxResult<Transaction> {
        self.begin_with_isolation(IsolationLevel::Serializable)
    }

    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> BustubxResult<Transaction> {
        let mut state = self.state.lock().unwrap();
        if let Some(journal) = &state.journal {
            journal.ensure_usable()?;
        }
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| BustubxError::Transaction("Transaction IDs exhausted".into()))?;
        if isolation == IsolationLevel::Serializable && !state.locks.shared(id) {
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
                isolation,
                base_version: state.version,
            }),
            Err(error) => {
                state.locks.release(id);
                Err(error)
            }
        }
    }
}
