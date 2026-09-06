use crate::common::ScalarValue;
use crate::function::{Accumulator, AccumulatorState};
use crate::{BustubxError, BustubxResult};

#[derive(Debug, Clone)]
pub struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn state(&self) -> AccumulatorState {
        AccumulatorState::Count(self.count)
    }

    fn merge(&mut self, state: AccumulatorState) -> BustubxResult<()> {
        let AccumulatorState::Count(count) = state else {
            return Err(BustubxError::Execution(
                "Invalid COUNT partial state".into(),
            ));
        };
        self.count = self
            .count
            .checked_add(count)
            .ok_or_else(|| BustubxError::Execution("COUNT overflow".into()))?;
        Ok(())
    }

    fn update_value(&mut self, value: &ScalarValue) -> BustubxResult<()> {
        if !value.is_null() {
            self.merge(AccumulatorState::Count(1))?;
        }
        Ok(())
    }

    fn evaluate(&self) -> BustubxResult<ScalarValue> {
        Ok(self.count.into())
    }
}
