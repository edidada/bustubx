use crate::buffer::{PageId, INVALID_PAGE_ID};
use crate::execution::parallel::ordered_map;
use crate::storage::TableHeap;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::catalog::SchemaRef;
use crate::common::TableReference;
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::{TableIterator, Tuple},
    BustubxError, BustubxResult,
};

#[derive(Debug)]
struct ScanState {
    heap: Arc<TableHeap>,
    next_page: PageId,
    visited: HashSet<PageId>,
    pending: VecDeque<BustubxResult<Tuple>>,
}

#[derive(Debug)]
pub struct PhysicalSeqScan {
    pub table: TableReference,
    pub table_schema: SchemaRef,

    iterator: Mutex<Option<TableIterator>>,
    parallelism: usize,
    scan: Mutex<Option<ScanState>>,
}

impl PhysicalSeqScan {
    pub fn new(table: TableReference, table_schema: SchemaRef) -> Self {
        PhysicalSeqScan {
            table,
            table_schema,
            iterator: Mutex::new(None),
            parallelism: 1,
            scan: Mutex::new(None),
        }
    }

    pub fn with_parallelism(mut self, workers: usize) -> Self {
        self.parallelism = workers.clamp(1, 64);
        self
    }

    fn next_parallel(&self) -> BustubxResult<Option<Tuple>> {
        let mut guard = self.scan.lock().unwrap();
        let state = guard
            .as_mut()
            .ok_or_else(|| BustubxError::Execution("scan not initialized".into()))?;
        loop {
            if let Some(row) = state.pending.pop_front() {
                return row.map(Some);
            }
            if state.next_page == INVALID_PAGE_ID {
                return Ok(None);
            }
            let mut pages = Vec::with_capacity(16);
            let mut failure = None;
            for _ in 0..16 {
                if state.next_page == INVALID_PAGE_ID {
                    break;
                }
                let page_id = state.next_page;
                if !state.visited.insert(page_id) {
                    failure = Some(BustubxError::Storage("Cycle in table page chain".into()));
                    state.next_page = INVALID_PAGE_ID;
                    break;
                }
                match state
                    .heap
                    .buffer_pool
                    .fetch_table_page(page_id, state.heap.schema.clone())
                {
                    Ok((_pin, page)) => {
                        state.next_page = page.header.next_page_id;
                        pages.push(page);
                    }
                    Err(error) => {
                        failure = Some(error);
                        state.next_page = INVALID_PAGE_ID;
                        break;
                    }
                }
            }
            let decoded = ordered_map(&pages, self.parallelism, |page| {
                let mut rows = Vec::with_capacity(page.header.num_tuples as usize);
                for slot in 0..page.header.num_tuples {
                    let row = page.tuple(slot).map(|(_, tuple)| tuple);
                    let failed = row.is_err();
                    rows.push(row);
                    if failed {
                        break;
                    }
                }
                rows
            });
            match decoded {
                Ok(pages) => state.pending.extend(pages.into_iter().flatten()),
                Err(error) => {
                    state.next_page = INVALID_PAGE_ID;
                    return Err(error);
                }
            }
            if let Some(error) = failure {
                state.pending.push_back(Err(error));
            }
        }
    }
}

impl VolcanoExecutor for PhysicalSeqScan {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        let table_heap = context.catalog.table_heap(&self.table)?;
        *self.iterator.lock().unwrap() = None;
        *self.scan.lock().unwrap() = None;
        if self.parallelism > 1 {
            *self.scan.lock().unwrap() = Some(ScanState {
                next_page: table_heap.first_page_id.load(Ordering::SeqCst),
                heap: table_heap,
                visited: HashSet::new(),
                pending: VecDeque::new(),
            });
        } else {
            *self.iterator.lock().unwrap() = Some(TableIterator::new(table_heap, ..));
        }
        Ok(())
    }

    fn next(&self, _context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        if self.parallelism > 1 {
            return self.next_parallel();
        }
        let Some(iterator) = &mut *self.iterator.lock().unwrap() else {
            return Err(BustubxError::Execution(
                "table iterator not created".to_string(),
            ));
        };
        Ok(iterator.next()?.map(|full| full.1))
    }

    fn output_schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }
}

impl std::fmt::Display for PhysicalSeqScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.parallelism > 1 {
            write!(f, "ParallelSeqScan: workers={}", self.parallelism)
        } else {
            write!(f, "SeqScan")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::util::page_bytes_to_array;
    use crate::storage::codec::TablePageCodec;
    use crate::storage::{TablePage, EMPTY_TUPLE_META};
    use crate::Database;

    #[test]
    fn scan_skips_empty_pages_releases_pins_and_detects_cycles() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t (a int)").unwrap();
        let table = TableReference::Bare { table: "t".into() };
        let heap = db.catalog.table_heap(&table).unwrap();
        let first = heap.first_page_id.load(Ordering::SeqCst);
        // Allocate a gap: page IDs must follow links rather than arithmetic.
        let gap = heap.buffer_pool.new_page().unwrap();
        drop(gap);
        let next = heap.buffer_pool.new_page().unwrap();
        let next_id = next.read().unwrap().page_id;
        assert!(next_id > first + 1);
        let mut page = TablePage::new(heap.schema.clone(), INVALID_PAGE_ID);
        page.insert_tuple(
            &EMPTY_TUPLE_META,
            &Tuple::new(heap.schema.clone(), vec![42i32.into()]),
        )
        .unwrap();
        next.write()
            .unwrap()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&page)));
        drop(next);
        let (pin, mut first_page) = heap
            .buffer_pool
            .fetch_table_page(first, heap.schema.clone())
            .unwrap();
        first_page.header.next_page_id = next_id;
        pin.write()
            .unwrap()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&first_page)));
        drop(pin);
        let scan = PhysicalSeqScan::new(table, heap.schema.clone()).with_parallelism(4);
        let mut context = ExecutionContext::new(&mut db.catalog);
        for _ in 0..2 {
            scan.init(&mut context).unwrap();
            assert_eq!(
                scan.next(&mut context).unwrap().unwrap().data,
                vec![42i32.into()]
            );
            assert!(scan.next(&mut context).unwrap().is_none());
            assert!(scan.next(&mut context).unwrap().is_none());
        }
        for id in [first, next_id] {
            let pin = heap.buffer_pool.fetch_page(id).unwrap();
            assert_eq!(pin.read().unwrap().pin_count, 1);
        }
        let (pin, mut last_page) = heap
            .buffer_pool
            .fetch_table_page(next_id, heap.schema.clone())
            .unwrap();
        last_page.header.next_page_id = first;
        pin.write()
            .unwrap()
            .set_data(page_bytes_to_array(&TablePageCodec::encode(&last_page)));
        drop(pin);
        scan.init(&mut context).unwrap();
        assert!(scan.next(&mut context).unwrap().is_some());
        assert!(matches!(
            scan.next(&mut context),
            Err(BustubxError::Storage(_))
        ));
        assert!(scan.next(&mut context).unwrap().is_none());
    }
}
