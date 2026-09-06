use crate::catalog::SchemaRef;
use crate::common::ScalarValue;
use crate::execution::parallel::{ordered_map, BATCH_SIZE};
use crate::execution::physical_plan::PhysicalPlan;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::{Expr, ExprTrait};
use crate::function::Accumulator;
use crate::{BustubxError, BustubxResult, Tuple};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

type Groups = HashMap<Vec<ScalarValue>, Vec<Box<dyn Accumulator>>>;

#[derive(Debug, Default)]
struct AggregateState {
    rows: VecDeque<Tuple>,
    built: bool,
}

#[derive(Debug)]
pub struct PhysicalAggregate {
    pub input: Arc<PhysicalPlan>,
    pub group_exprs: Vec<Expr>,
    pub aggr_exprs: Vec<Expr>,
    pub schema: SchemaRef,
    parallelism: usize,
    state: Mutex<AggregateState>,
}

impl PhysicalAggregate {
    pub fn new(
        input: Arc<PhysicalPlan>,
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            group_exprs,
            aggr_exprs,
            schema,
            parallelism: 1,
            state: Mutex::new(AggregateState::default()),
        }
    }
    pub fn with_parallelism(mut self, workers: usize) -> Self {
        self.parallelism = workers.clamp(1, 64);
        self
    }
    fn build_accumulators(&self) -> BustubxResult<Vec<Box<dyn Accumulator>>> {
        self.aggr_exprs
            .iter()
            .map(|expr| {
                if let Expr::AggregateFunction(aggr) = expr {
                    if aggr.distinct {
                        return Err(BustubxError::NotSupport(
                            "DISTINCT aggregate is not implemented".into(),
                        ));
                    }
                    Ok(aggr.func_kind.create_accumulator())
                } else {
                    Err(BustubxError::Execution(format!(
                        "Invalid aggregate expression: {expr}"
                    )))
                }
            })
            .collect()
    }
    fn accumulate(&self, rows: &[Tuple]) -> BustubxResult<Groups> {
        let mut groups = Groups::new();
        for tuple in rows {
            let key = self
                .group_exprs
                .iter()
                .map(|expr| expr.evaluate(tuple))
                .collect::<BustubxResult<Vec<_>>>()?;
            if !groups.contains_key(&key) {
                groups.insert(key.clone(), self.build_accumulators()?);
            }
            for (acc, expr) in groups
                .get_mut(&key)
                .unwrap()
                .iter_mut()
                .zip(&self.aggr_exprs)
            {
                acc.update_value(&expr.evaluate(tuple)?)?;
            }
        }
        Ok(groups)
    }
    fn aggregate(&self, context: &mut ExecutionContext) -> BustubxResult<VecDeque<Tuple>> {
        let mut groups = Groups::new();
        if self.group_exprs.is_empty() {
            groups.insert(Vec::new(), self.build_accumulators()?);
        }
        loop {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                match self.input.next(context)? {
                    Some(row) => batch.push(row),
                    None => break,
                }
            }
            if batch.is_empty() {
                break;
            }
            let chunk_size = batch.len().div_ceil(self.parallelism);
            let chunks = batch.chunks(chunk_size).collect::<Vec<_>>();
            let partials = ordered_map(&chunks, self.parallelism, |rows| self.accumulate(rows))?;
            for partial in partials {
                for (key, accumulators) in partial? {
                    if let Some(target) = groups.get_mut(&key) {
                        for (dest, source) in target.iter_mut().zip(accumulators) {
                            dest.merge(source.state())?;
                        }
                    } else {
                        groups.insert(key, accumulators);
                    }
                }
            }
        }
        groups
            .into_iter()
            .map(|(key, accumulators)| {
                let mut values = accumulators
                    .iter()
                    .map(|acc| acc.evaluate())
                    .collect::<BustubxResult<Vec<_>>>()?;
                values.extend(key);
                Ok(Tuple::new(self.schema.clone(), values))
            })
            .collect()
    }
}
impl VolcanoExecutor for PhysicalAggregate {
    fn init(&self, context: &mut ExecutionContext) -> BustubxResult<()> {
        *self.state.lock().unwrap() = AggregateState::default();
        self.input.init(context)
    }
    fn next(&self, context: &mut ExecutionContext) -> BustubxResult<Option<Tuple>> {
        let mut state = self.state.lock().unwrap();
        if !state.built {
            state.built = true;
            state.rows = self.aggregate(context)?;
        }
        Ok(state.rows.pop_front())
    }
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl std::fmt::Display for PhysicalAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.parallelism > 1 {
            write!(f, "ParallelAggregate: workers={}", self.parallelism)
        } else {
            write!(f, "Aggregate")
        }
    }
}
