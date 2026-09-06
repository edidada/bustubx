mod avg;
mod count;

pub use avg::AvgAccumulator;
pub use count::CountAccumulator;
use std::fmt::Debug;

use crate::common::ScalarValue;
use crate::BustubxResult;
use strum::{EnumIter, IntoEnumIterator};

#[derive(Clone, PartialEq, Eq, Debug, EnumIter)]
pub enum AggregateFunctionKind {
    Count,
    Avg,
}

impl AggregateFunctionKind {
    pub fn create_accumulator(&self) -> Box<dyn Accumulator> {
        match self {
            AggregateFunctionKind::Count => Box::new(CountAccumulator::new()),
            AggregateFunctionKind::Avg => Box::new(AvgAccumulator::new()),
        }
    }

    pub fn find(name: &str) -> Option<Self> {
        AggregateFunctionKind::iter().find(|kind| kind.to_string().eq_ignore_ascii_case(name))
    }
}

impl std::fmt::Display for AggregateFunctionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub trait Accumulator: Send + Sync + Debug {
    fn state(&self) -> AccumulatorState;

    fn merge(&mut self, state: AccumulatorState) -> BustubxResult<()>;

    /// Updates the accumulator's state from its input.
    fn update_value(&mut self, value: &ScalarValue) -> BustubxResult<()>;

    /// Returns the final aggregate value, consuming the internal state.
    fn evaluate(&self) -> BustubxResult<ScalarValue>;
}

#[derive(Debug, Clone)]
pub enum AccumulatorState {
    Count(i64),
    Avg { sum: Option<f64>, count: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_states_merge_counts_and_weighted_average() {
        let mut avg = AvgAccumulator::new();
        avg.merge(AccumulatorState::Avg {
            sum: Some(10.0),
            count: 1,
        })
        .unwrap();
        avg.merge(AccumulatorState::Avg {
            sum: Some(90.0),
            count: 9,
        })
        .unwrap();
        avg.merge(AccumulatorState::Avg {
            sum: None,
            count: 0,
        })
        .unwrap();
        assert_eq!(avg.evaluate().unwrap(), ScalarValue::Float64(Some(10.0)));
        assert!(avg.merge(AccumulatorState::Count(1)).is_err());
        let mut count = CountAccumulator::new();
        count.merge(AccumulatorState::Count(i64::MAX)).unwrap();
        assert!(count.update_value(&1i32.into()).is_err());
        assert_eq!(
            count.evaluate().unwrap(),
            ScalarValue::Int64(Some(i64::MAX))
        );
        assert!(count.merge(avg.state()).is_err());
        let mut huge = AvgAccumulator::new();
        huge.merge(AccumulatorState::Avg {
            sum: Some(1.0),
            count: u64::MAX,
        })
        .unwrap();
        assert!(huge.update_value(&1i32.into()).is_err());
    }
}
