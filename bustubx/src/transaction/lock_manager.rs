use super::TransactionId;
use std::collections::HashSet;

/// Database-level strict S/X locking. Protected by the manager mutex.
#[derive(Default)]
pub(super) struct LockManager {
    readers: HashSet<TransactionId>,
    writer: Option<TransactionId>,
}
impl LockManager {
    pub fn shared(&mut self, id: TransactionId) -> bool {
        if self.writer.is_some() && self.writer != Some(id) {
            return false;
        }
        self.readers.insert(id);
        true
    }
    pub fn exclusive(&mut self, id: TransactionId) -> bool {
        if self.writer.is_some() && self.writer != Some(id) {
            return false;
        }
        if self.readers.iter().any(|reader| *reader != id) {
            return false;
        }
        self.writer = Some(id);
        true
    }
    pub fn release(&mut self, id: TransactionId) {
        self.readers.remove(&id);
        if self.writer == Some(id) {
            self.writer = None;
        }
    }
}
