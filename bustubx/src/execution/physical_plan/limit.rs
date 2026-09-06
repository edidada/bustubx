use std::sync::{Arc, Mutex};

use crate::catalog::SchemaRef;
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};

use super::PhysicalPlan;

#[derive(Debug)]
struct LimitState {
    to_skip: usize,
    remaining: Option<usize>,
    exhausted: bool,
}

#[derive(Debug)]
pub struct PhysicalLimit {
    pub limit: Option<usize>,
    pub offset: usize,
    pub input: Arc<PhysicalPlan>,

    state: Mutex<LimitState>,
}
impl PhysicalLimit {
    pub fn new(limit: Option<usize>, offset: usize, input: Arc<PhysicalPlan>) -> Self {
        PhysicalLimit {
            limit,
            offset,
            input,
            state: Mutex::new(LimitState {
                to_skip: offset,
                remaining: limit,
                exhausted: false,
            }),
        }
    }
}
impl VolcanoExecutor for PhysicalLimit {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        *self.state.lock().unwrap() = LimitState {
            to_skip: self.offset,
            remaining: self.limit,
            exhausted: false,
        };
        self.input.init(context)
    }
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let mut state = self.state.lock().unwrap();
        if state.exhausted || state.remaining == Some(0) {
            return Ok(None);
        }
        loop {
            let Some(tuple) = self.input.next(context)? else {
                state.exhausted = true;
                return Ok(None);
            };
            if state.to_skip > 0 {
                state.to_skip -= 1;
                continue;
            }
            if let Some(remaining) = &mut state.remaining {
                *remaining -= 1;
            }
            return Ok(Some(tuple));
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limit")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, Schema};
    use crate::common::{ScalarValue, TableReference};
    use crate::execution::physical_plan::{PhysicalProject, PhysicalSeqScan, PhysicalValues};
    use crate::expression::{Cast, ColumnExpr, Expr, Literal};
    use crate::Database;

    fn values(rows: Vec<Expr>, workers: usize) -> Arc<PhysicalPlan> {
        let schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int32, false)]));
        Arc::new(PhysicalPlan::Project(
            PhysicalProject::new(
                vec![Expr::Column(ColumnExpr {
                    relation: None,
                    name: "a".into(),
                })],
                schema.clone(),
                Arc::new(PhysicalPlan::Values(PhysicalValues::new(
                    schema,
                    rows.into_iter().map(|row| vec![row]).collect(),
                ))),
            )
            .with_parallelism(workers),
        ))
    }

    fn good() -> Expr {
        Expr::Literal(Literal { value: 7i32.into() })
    }
    fn bad() -> Expr {
        Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(Literal {
                value: ScalarValue::Varchar(Some("bad".into())),
            })),
            data_type: DataType::Int32,
        })
    }

    #[test]
    fn limit_never_requests_rows_beyond_its_quota() {
        let mut db = Database::new_temp().unwrap();
        let mut context = ExecutionContext::new(&mut db.catalog);
        for workers in [1, 4] {
            let zero = PhysicalLimit::new(Some(0), 5, values(vec![bad()], workers));
            zero.init(&mut context).unwrap();
            assert!(zero.next(&mut context).unwrap().is_none());
            let one = PhysicalLimit::new(Some(1), 1, values(vec![good(), good(), bad()], workers));
            one.init(&mut context).unwrap();
            assert_eq!(
                one.next(&mut context).unwrap().unwrap().data,
                vec![7i32.into()]
            );
            for _ in 0..3 {
                assert!(one.next(&mut context).unwrap().is_none());
            }
            let error = PhysicalLimit::new(Some(2), 0, values(vec![good(), bad()], workers));
            error.init(&mut context).unwrap();
            assert!(error.next(&mut context).unwrap().is_some());
            assert!(error.next(&mut context).is_err());
        }
    }

    #[test]
    fn large_limit_and_offset_do_not_overflow() {
        let mut db = Database::new_temp().unwrap();
        let mut context = ExecutionContext::new(&mut db.catalog);
        for limit in [Some(usize::MAX), None] {
            let op = PhysicalLimit::new(limit, 1, values(vec![good(), good()], 4));
            op.init(&mut context).unwrap();
            assert!(op.next(&mut context).unwrap().is_some());
            assert!(op.next(&mut context).unwrap().is_none());
        }
        let op = PhysicalLimit::new(None, usize::MAX, values(vec![good()], 4));
        op.init(&mut context).unwrap();
        assert!(op.next(&mut context).unwrap().is_none());
    }

    #[test]
    fn init_resets_offset_quota_and_exhaustion() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t (a int)").unwrap();
        db.run("insert into t values (1), (2), (3)").unwrap();
        let table = TableReference::Bare { table: "t".into() };
        let schema = db.catalog.table_heap(&table).unwrap().schema.clone();
        let op = PhysicalLimit::new(
            Some(1),
            1,
            Arc::new(PhysicalPlan::SeqScan(PhysicalSeqScan::new(table, schema))),
        );
        let mut context = ExecutionContext::new(&mut db.catalog);
        for _ in 0..2 {
            op.init(&mut context).unwrap();
            assert_eq!(
                op.next(&mut context).unwrap().unwrap().data,
                vec![2i32.into()]
            );
            assert!(op.next(&mut context).unwrap().is_none());
        }
    }
}
