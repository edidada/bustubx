use super::{TransactionId, TransactionManager};
use crate::planner::logical_plan::LogicalPlan;
use crate::{BustubxError, BustubxResult, Database, Tuple};

/// A private SQL workspace. Dropping an active transaction aborts it.
pub struct Transaction {
    pub(super) manager: TransactionManager,
    pub(super) id: TransactionId,
    pub(super) database: Option<Database>,
    pub(super) dirty: bool,
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
            if !self.manager.state.lock().unwrap().locks.exclusive(self.id) {
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
            state.database = database;
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
