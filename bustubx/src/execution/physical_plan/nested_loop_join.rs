use super::PhysicalPlan;
use crate::catalog::SchemaRef;
use crate::common::ScalarValue;
use crate::execution::parallel::{ordered_map, BATCH_SIZE};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::{Expr, ExprTrait};
use crate::planner::logical_plan::JoinType;
use crate::{BustubxError, BustubxResult, Tuple};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct JoinState {
    left: Option<Tuple>,
    right_finished: bool,
    exhausted: bool,
    pending: VecDeque<BustubxResult<Tuple>>,
}

#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    pub join_type: JoinType,
    pub condition: Option<Expr>,
    pub left_input: Arc<PhysicalPlan>,
    pub right_input: Arc<PhysicalPlan>,
    pub schema: SchemaRef,
    parallelism: usize,
    state: Mutex<JoinState>,
}

impl PhysicalNestedLoopJoin {
    pub fn new(
        join_type: JoinType,
        condition: Option<Expr>,
        left_input: Arc<PhysicalPlan>,
        right_input: Arc<PhysicalPlan>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            join_type,
            condition,
            left_input,
            right_input,
            schema,
            parallelism: 1,
            state: Mutex::new(JoinState::default()),
        }
    }
    pub fn with_parallelism(mut self, workers: usize) -> Self {
        self.parallelism = workers.clamp(1, 64);
        self
    }
    fn join(&self, left: &Tuple, right: &Tuple) -> BustubxResult<Option<Tuple>> {
        let tuple = Tuple::try_merge([left.clone(), right.clone()])?;
        if let Some(condition) = &self.condition {
            match condition.evaluate(&tuple)? {
                ScalarValue::Boolean(Some(true)) => {}
                ScalarValue::Boolean(_) => return Ok(None),
                _ => {
                    return Err(BustubxError::Execution(
                        "join condition should be boolean".into(),
                    ))
                }
            }
        }
        Ok(Some(tuple))
    }
}
impl VolcanoExecutor for PhysicalNestedLoopJoin {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        if !matches!(self.join_type, JoinType::Inner | JoinType::Cross) {
            return Err(BustubxError::NotSupport(
                "Outer join execution is not implemented".into(),
            ));
        }
        *self.state.lock().unwrap() = JoinState::default();
        self.left_input.init(context)?;
        self.right_input.init(context)
    }
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(row) = state.pending.pop_front() {
                return row.map(Some);
            }
            if state.exhausted {
                return Ok(None);
            }
            if state.left.is_none() || state.right_finished {
                state.left = self.left_input.next(context)?;
                if state.left.is_none() {
                    state.exhausted = true;
                    return Ok(None);
                }
                if state.right_finished {
                    self.right_input.init(context)?;
                }
                state.right_finished = false;
            }
            let mut batch = Vec::new();
            let mut failure = None;
            for _ in 0..if self.parallelism > 1 { BATCH_SIZE } else { 1 } {
                match self.right_input.next(context) {
                    Ok(Some(row)) => batch.push(row),
                    Ok(None) => {
                        state.right_finished = true;
                        break;
                    }
                    Err(error) => {
                        state.exhausted = true;
                        failure = Some(error);
                        break;
                    }
                }
            }
            let left = state.left.as_ref().unwrap();
            match ordered_map(&batch, self.parallelism, |right| self.join(left, right)) {
                Ok(rows) => {
                    for row in rows {
                        match row {
                            Ok(Some(tuple)) => state.pending.push_back(Ok(tuple)),
                            Ok(None) => {}
                            Err(error) => state.pending.push_back(Err(error)),
                        }
                    }
                }
                Err(error) => {
                    state.exhausted = true;
                    return Err(error);
                }
            }
            if let Some(error) = failure {
                state.pending.push_back(Err(error));
            }
        }
    }
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl std::fmt::Display for PhysicalNestedLoopJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.parallelism > 1 {
            write!(f, "ParallelNestedLoopJoin: workers={}", self.parallelism)
        } else {
            write!(f, "NestedLoopJoin")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, Schema};
    use crate::execution::physical_plan::PhysicalValues;
    use crate::expression::Literal;
    use crate::Database;

    fn values(name: &str) -> Arc<PhysicalPlan> {
        let schema = Arc::new(Schema::new(vec![Column::new(name, DataType::Int32, false)]));
        Arc::new(PhysicalPlan::Values(PhysicalValues::new(
            schema,
            vec![
                vec![Expr::Literal(Literal { value: 1i32.into() })],
                vec![Expr::Literal(Literal { value: 2i32.into() })],
            ],
        )))
    }

    #[test]
    fn values_rescan_eof_and_join_conditions() {
        let mut db = Database::new_temp().unwrap();
        let mut context = ExecutionContext::new(&mut db.catalog);
        for workers in [1, 4] {
            let left = values("a");
            let right = values("b");
            let schema = Arc::new(
                Schema::try_merge(vec![
                    left.output_schema().as_ref().clone(),
                    right.output_schema().as_ref().clone(),
                ])
                .unwrap(),
            );
            let mut join = PhysicalNestedLoopJoin::new(JoinType::Cross, None, left, right, schema)
                .with_parallelism(workers);
            for _ in 0..2 {
                join.init(&mut context).unwrap();
                for (a, b) in [(1, 1), (1, 2), (2, 1), (2, 2)] {
                    assert_eq!(
                        join.next(&mut context).unwrap().unwrap().data,
                        vec![ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))]
                    );
                }
                for _ in 0..2 {
                    assert!(join.next(&mut context).unwrap().is_none());
                }
            }
            join.condition = Some(Expr::Literal(Literal {
                value: ScalarValue::Boolean(None),
            }));
            join.init(&mut context).unwrap();
            assert!(join.next(&mut context).unwrap().is_none());
            join.condition = Some(Expr::Literal(Literal { value: 7i32.into() }));
            join.init(&mut context).unwrap();
            assert!(matches!(
                join.next(&mut context),
                Err(BustubxError::Execution(_))
            ));
            join.join_type = JoinType::LeftOuter;
            assert!(matches!(
                join.init(&mut context),
                Err(BustubxError::NotSupport(_))
            ));
        }
    }
}
