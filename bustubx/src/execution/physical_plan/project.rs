use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::catalog::SchemaRef;
use crate::execution::parallel::{ordered_map, BATCH_SIZE};
use crate::expression::{Expr, ExprTrait};
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::Tuple,
    BustubxResult,
};

use super::PhysicalPlan;

#[derive(Debug, Default)]
struct ProjectState {
    pending: VecDeque<BustubxResult<Tuple>>,
    exhausted: bool,
}

#[derive(Debug)]
pub struct PhysicalProject {
    pub exprs: Vec<Expr>,
    pub schema: SchemaRef,
    pub input: Arc<PhysicalPlan>,
    parallelism: usize,
    state: Mutex<ProjectState>,
}

impl PhysicalProject {
    pub fn new(exprs: Vec<Expr>, schema: SchemaRef, input: Arc<PhysicalPlan>) -> Self {
        Self {
            exprs,
            schema,
            input,
            parallelism: 1,
            state: Mutex::new(ProjectState::default()),
        }
    }

    pub fn with_parallelism(mut self, workers: usize) -> Self {
        self.parallelism = workers.clamp(1, 64);
        self
    }

    fn project(&self, tuple: &Tuple) -> BustubxResult<Tuple> {
        let values = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(tuple))
            .collect::<BustubxResult<Vec<_>>>()?;
        Ok(Tuple::new(self.output_schema(), values))
    }
}

impl VolcanoExecutor for PhysicalProject {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        *self.state.lock().unwrap() = ProjectState::default();
        self.input.init(context)
    }

    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        if self.parallelism == 1 {
            return self
                .input
                .next(context)?
                .as_ref()
                .map(|tuple| self.project(tuple))
                .transpose();
        }
        let mut state = self.state.lock().unwrap();
        if state.pending.is_empty() && !state.exhausted {
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
            match ordered_map(&batch, self.parallelism, |tuple| self.project(tuple)) {
                Ok(rows) => state.pending.extend(rows),
                Err(error) => {
                    state.exhausted = true;
                    return Err(error);
                }
            }
            if let Some(error) = input_error {
                state.pending.push_back(Err(error));
            }
        }
        state.pending.pop_front().transpose()
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalProject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.parallelism > 1 {
            write!(f, "ParallelProject: workers={}", self.parallelism)
        } else {
            write!(f, "Project")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, Schema};
    use crate::execution::physical_plan::PhysicalValues;
    use crate::expression::{Cast, ColumnExpr, Literal};
    use crate::Database;

    #[test]
    fn errors_are_returned_in_row_order() {
        let mut db = Database::new_temp().unwrap();
        let schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int32, false)]));
        let bad_value = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(Literal {
                value: crate::common::ScalarValue::Varchar(Some("bad".into())),
            })),
            data_type: DataType::Int32,
        });
        let values = PhysicalPlan::Values(PhysicalValues::new(
            schema.clone(),
            vec![
                vec![Expr::Literal(Literal { value: 7i32.into() })],
                vec![bad_value],
            ],
        ));
        let project = PhysicalProject::new(
            vec![Expr::Column(ColumnExpr {
                relation: None,
                name: "a".into(),
            })],
            schema,
            Arc::new(values),
        )
        .with_parallelism(4);
        let mut context = ExecutionContext::new(&mut db.catalog);
        project.init(&mut context).unwrap();
        assert_eq!(
            project.next(&mut context).unwrap().unwrap().data,
            vec![7i32.into()]
        );
        assert!(project.next(&mut context).is_err());
        assert!(project.next(&mut context).unwrap().is_none());
        assert!(project.next(&mut context).unwrap().is_none());
    }

    #[test]
    fn projection_errors_remain_lazy_and_init_discards_pending_rows() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t (a varchar)").unwrap();
        db.run("insert into t values ('7'), ('bad')").unwrap();
        let table = crate::common::TableReference::Bare { table: "t".into() };
        let input_schema = db.catalog.table_heap(&table).unwrap().schema.clone();
        let input = Arc::new(PhysicalPlan::SeqScan(super::super::PhysicalSeqScan::new(
            table,
            input_schema.clone(),
        )));
        let column = Expr::Column(ColumnExpr {
            relation: None,
            name: "a".into(),
        });
        let valid_project = PhysicalProject::new(vec![column.clone()], input_schema, input.clone())
            .with_parallelism(4);
        let schema = Arc::new(Schema::new(vec![Column::new("a", DataType::Int32, false)]));
        let project = PhysicalProject::new(
            vec![Expr::Cast(Cast {
                expr: Box::new(Expr::Column(ColumnExpr {
                    relation: None,
                    name: "a".into(),
                })),
                data_type: DataType::Int32,
            })],
            schema,
            input,
        )
        .with_parallelism(4);
        let mut context = ExecutionContext::new(&mut db.catalog);
        valid_project.init(&mut context).unwrap();
        let first = valid_project.next(&mut context).unwrap();
        valid_project.init(&mut context).unwrap();
        assert_eq!(valid_project.next(&mut context).unwrap(), first);
        assert_ne!(valid_project.next(&mut context).unwrap(), first);
        project.init(&mut context).unwrap();
        assert!(project.next(&mut context).is_err());
        project.init(&mut context).unwrap();
        assert!(project.next(&mut context).is_err());
        assert!(project.next(&mut context).is_err());
        assert!(project.next(&mut context).unwrap().is_none());
        assert_eq!(db.run("select 1").unwrap().len(), 1);
    }
}
