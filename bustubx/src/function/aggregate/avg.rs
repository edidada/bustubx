use crate::catalog::DataType;
use crate::common::ScalarValue;
use crate::function::{Accumulator, AccumulatorState};
use crate::{BustubxError, BustubxResult};

#[derive(Debug, Clone)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self {
            sum: None,
            count: 0,
        }
    }
}

impl Accumulator for AvgAccumulator {
    fn state(&self) -> AccumulatorState {
        AccumulatorState::Avg {
            sum: self.sum,
            count: self.count,
        }
    }

    fn merge(&mut self, state: AccumulatorState) -> BustubxResult<()> {
        let AccumulatorState::Avg { sum, count } = state else {
            return Err(BustubxError::Execution("Invalid AVG partial state".into()));
        };
        let new_count = self
            .count
            .checked_add(count)
            .ok_or_else(|| BustubxError::Execution("AVG count overflow".into()))?;
        if let Some(value) = sum {
            self.sum = Some(self.sum.unwrap_or(0.0) + value);
        }
        self.count = new_count;
        Ok(())
    }

    fn update_value(&mut self, value: &ScalarValue) -> BustubxResult<()> {
        if !value.is_null() {
            let value = match value.cast_to(&DataType::Float64)? {
                ScalarValue::Float64(Some(v)) => v,
                _ => {
                    return Err(BustubxError::Internal(format!(
                        "Failed to cast value {} to float64",
                        value
                    )))
                }
            };

            self.merge(AccumulatorState::Avg {
                sum: Some(value),
                count: 1,
            })?;
        }
        Ok(())
    }

    fn evaluate(&self) -> BustubxResult<ScalarValue> {
        Ok(ScalarValue::Float64(
            self.sum.map(|f| f / self.count as f64),
        ))
    }
}
