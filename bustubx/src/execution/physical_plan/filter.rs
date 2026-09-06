use log::debug;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::catalog::SchemaRef;
use crate::execution::parallel::{ordered_map, BATCH_SIZE};
use crate::expression::{Expr, ExprTrait};
use crate::{
    common::ScalarValue,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxError, BustubxResult,
};

use super::PhysicalPlan;

#[derive(Debug, Default)]
struct FilterState {
    pending: VecDeque<BustubxResult<Tuple>>,
    exhausted: bool,
}

#[derive(Debug)]
pub struct PhysicalFilter {
    pub predicate: Expr,
    pub input: Arc<PhysicalPlan>,
    parallelism: usize,
    state: Mutex<FilterState>,
}

impl PhysicalFilter {
    pub fn new(predicate: Expr, input: Arc<PhysicalPlan>) -> Self {
        Self {
            predicate,
            input,
            parallelism: 1,
            state: Mutex::new(FilterState::default()),
        }
    }

    pub fn with_parallelism(mut self, workers: usize) -> Self {
        self.parallelism = workers.clamp(1, 64);
        self
    }

    fn matches(&self, tuple: &Tuple) -> BustubxResult<bool> {
        match self.predicate.evaluate(tuple)? {
            ScalarValue::Boolean(Some(value)) => Ok(value),
            _ => Err(BustubxError::Execution(
                "filter predicate value should be boolean".to_string(),
            )),
        }
    }
}

impl VolcanoExecutor for PhysicalFilter {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        debug!("init filter executor");
        *self.state.lock().unwrap() = FilterState::default();
        self.input.init(context)
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        if self.parallelism == 1 {
            loop {
                if let Some(tuple) = self.input.next(context)? {
                    if self.matches(&tuple)? {
                        return Ok(Some(tuple));
                    }
                } else {
                    return Ok(None);
                }
            }
        }
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(result) = state.pending.pop_front() {
                return result.map(Some);
            }
            if state.exhausted {
                return Ok(None);
            }
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            let mut input_error = None;
            for _ in 0..BATCH_SIZE {
                match self.input.next(context) {
                    Ok(Some(tuple)) => batch.push(tuple),
                    Ok(None) => {
                        state.exhausted = true;
                        break;
                    }
                    Err(error) => {
                        state.exhausted = true;
                        input_error = Some(error);
                        break;
                    }
                }
            }
            let decisions = match ordered_map(&batch, self.parallelism, |tuple| self.matches(tuple))
            {
                Ok(decisions) => decisions,
                Err(error) => {
                    state.exhausted = true;
                    return Err(error);
                }
            };
            for (tuple, decision) in batch.into_iter().zip(decisions) {
                match decision {
                    Ok(true) => state.pending.push_back(Ok(tuple)),
                    Ok(false) => {}
                    Err(error) => state.pending.push_back(Err(error)),
                }
            }
            if let Some(error) = input_error {
                state.pending.push_back(Err(error));
            }
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.parallelism > 1 {
            write!(
                f,
                "ParallelFilter: workers={}, {}",
                self.parallelism, self.predicate
            )
        } else {
            write!(f, "Filter: {}", self.predicate)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, Schema};
    use crate::common::TableReference;
    use crate::execution::physical_plan::{PhysicalLimit, PhysicalSeqScan, PhysicalValues};
    use crate::expression::{Cast, ColumnExpr, Literal};
    use crate::Database;

    fn literal(value: ScalarValue) -> Expr {
        Expr::Literal(Literal { value })
    }

    fn column() -> Expr {
        Expr::Column(ColumnExpr {
            relation: None,
            name: "a".into(),
        })
    }

    fn filter(rows: Vec<Expr>, workers: usize) -> PhysicalFilter {
        let schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Boolean, true)]));
        PhysicalFilter::new(
            column(),
            Arc::new(PhysicalPlan::Values(PhysicalValues::new(
                schema,
                rows.into_iter().map(|row| vec![row]).collect(),
            ))),
        )
        .with_parallelism(workers)
    }

    #[test]
    fn predicates_and_errors_follow_input_order() {
        let mut db = Database::new_temp().unwrap();
        let mut context = ExecutionContext::new(&mut db.catalog);
        for workers in [1, 4] {
            // Keep the existing NULL predicate error semantics in both modes.
            let op = filter(
                vec![
                    literal(false.into()),
                    literal(true.into()),
                    literal(ScalarValue::Boolean(None)),
                ],
                workers,
            );
            op.init(&mut context).unwrap();
            assert_eq!(
                op.next(&mut context).unwrap().unwrap().data,
                vec![true.into()]
            );
            assert!(op.next(&mut context).is_err());
            for _ in 0..2 {
                assert!(op.next(&mut context).unwrap().is_none());
            }
            let op = filter(
                vec![
                    literal(true.into()),
                    Expr::Cast(Cast {
                        expr: Box::new(literal(ScalarValue::Varchar(Some("bad".into())))),
                        data_type: DataType::Boolean,
                    }),
                ],
                workers,
            );
            op.init(&mut context).unwrap();
            assert!(op.next(&mut context).unwrap().is_some());
            assert!(op.next(&mut context).is_err());

            let limit = PhysicalLimit::new(
                Some(1),
                0,
                Arc::new(PhysicalPlan::Filter(filter(
                    vec![
                        literal(false.into()),
                        literal(true.into()),
                        literal(ScalarValue::Boolean(None)),
                    ],
                    workers,
                ))),
            );
            limit.init(&mut context).unwrap();
            assert!(limit.next(&mut context).unwrap().is_some());
            assert!(limit.next(&mut context).unwrap().is_none());
        }
    }

    #[test]
    fn non_boolean_predicates_are_errors() {
        let mut db = Database::new_temp().unwrap();
        let mut context = ExecutionContext::new(&mut db.catalog);
        for workers in [1, 4] {
            let mut op = filter(vec![literal(true.into()), literal(false.into())], workers);
            op.predicate = literal(7i32.into());
            op.init(&mut context).unwrap();
            assert!(matches!(
                op.next(&mut context),
                Err(BustubxError::Execution(_))
            ));
        }
    }

    #[test]
    fn init_discards_pending_rows_and_resets_end_of_input() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t (a boolean)").unwrap();
        db.run("insert into t values (true), (false), (true)")
            .unwrap();
        let table = TableReference::Bare { table: "t".into() };
        let schema = db.catalog.table_heap(&table).unwrap().schema.clone();
        let op = PhysicalFilter::new(
            column(),
            Arc::new(PhysicalPlan::SeqScan(PhysicalSeqScan::new(table, schema))),
        )
        .with_parallelism(4);
        let mut context = ExecutionContext::new(&mut db.catalog);
        op.init(&mut context).unwrap();
        assert!(op.next(&mut context).unwrap().is_some());
        for _ in 0..2 {
            op.init(&mut context).unwrap();
            assert!(op.next(&mut context).unwrap().is_some());
            assert!(op.next(&mut context).unwrap().is_some());
            assert!(op.next(&mut context).unwrap().is_none());
        }
    }
}
