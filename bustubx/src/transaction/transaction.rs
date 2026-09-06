use super::{IsolationLevel, TransactionId, TransactionManager};
use crate::planner::logical_plan::LogicalPlan;
use crate::{BustubxError, BustubxResult, Database, Tuple};

/// A private SQL workspace. Dropping an active transaction aborts it.
pub struct Transaction {
    pub(super) manager: TransactionManager,
    pub(super) id: TransactionId,
    pub(super) database: Option<Database>,
    pub(super) dirty: bool,
    pub(super) isolation: IsolationLevel,
    pub(super) base_version: u64,
}
impl Transaction {
    pub fn id(&self) -> TransactionId {
        self.id
    }
    pub fn set_parallelism(&mut self, workers: usize) -> BustubxResult<()> {
        self.database
            .as_mut()
            .ok_or_else(|| BustubxError::Transaction("Transaction is no longer active".into()))?
            .set_parallelism(workers)
    }
    pub fn run(&mut self, sql: &str) -> BustubxResult<Vec<Tuple>> {
        let result = self.run_active(sql);
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn run_active(&mut self, sql: &str) -> BustubxResult<Vec<Tuple>> {
        let db = self
            .database
            .as_mut()
            .ok_or_else(|| BustubxError::Transaction("Transaction is no longer active".into()))?;
        let plan = db.create_logical_plan(sql)?;
        let write = matches!(
            plan,
            LogicalPlan::Insert(_)
                | LogicalPlan::Update(_)
                | LogicalPlan::CreateTable(_)
                | LogicalPlan::CreateIndex(_)
        );
        if write && !self.dirty {
            if self.isolation == IsolationLevel::Serializable
                && !self.manager.state.lock().unwrap().locks.exclusive(self.id)
            {
                return Err(BustubxError::Transaction(
                    "Lock upgrade conflict; transaction aborted".into(),
                ));
            }
            self.dirty = true;
        }
        db.run(sql)
    }
    /// Publish all writes atomically within this process, then release all locks.
    pub fn commit(&mut self) -> BustubxResult<()> {
        let database = self
            .database
            .take()
            .ok_or_else(|| BustubxError::Transaction("Transaction is no longer active".into()))?;
        let mut state = self.manager.state.lock().unwrap();
        if self.dirty {
            if state.version != self.base_version || !state.locks.exclusive(self.id) {
                state.locks.release(self.id);
                return Err(BustubxError::Transaction(
                    "Write conflict; transaction aborted".into(),
                ));
            }
            let Some(version) = state.version.checked_add(1) else {
                state.locks.release(self.id);
                return Err(BustubxError::Transaction(
                    "Commit versions exhausted".into(),
                ));
            };
            state.database = database;
            state.version = version;
        }
        state.locks.release(self.id);
        Ok(())
    }
    pub fn abort(&mut self) {
        if self.database.take().is_some() {
            self.manager.state.lock().unwrap().locks.release(self.id);
        }
    }
}
impl Drop for Transaction {
    fn drop(&mut self) {
        self.abort();
    }
}
