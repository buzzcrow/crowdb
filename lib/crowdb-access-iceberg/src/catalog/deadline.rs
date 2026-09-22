use crate::error::ValidationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClearBounds {
    pub root_lease_ms: u64,
    pub request_ms: u64,
    pub delegated_access_ms: u64,
    pub clock_skew_ms: u64,
}

impl ClearBounds {
    /// # Errors
    /// Rejects an unbounded request lifetime or overflowing deadline.
    pub fn completion_deadline(self, maintenance_observed_ms: u64) -> Result<u64, ValidationError> {
        if self.request_ms == 0 {
            return Err(ValidationError::Deadline);
        }
        [
            self.root_lease_ms,
            self.request_ms,
            self.delegated_access_ms,
            self.clock_skew_ms,
        ]
        .into_iter()
        .try_fold(maintenance_observed_ms, |deadline, duration| {
            deadline.checked_add(duration).ok_or(ValidationError::Deadline)
        })
    }
}
