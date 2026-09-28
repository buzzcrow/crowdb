use crate::error::ValidationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClearBounds {
    pub root_lease_ms: u64,
    pub request_ms: u64,
    pub delegated_access_ms: u64,
    pub clock_skew_ms: u64,
}

impl Default for ClearBounds {
    fn default() -> Self {
        Self {
            root_lease_ms: 0,
            request_ms: 10_000,
            delegated_access_ms: 0,
            clock_skew_ms: 1_000,
        }
    }
}

impl ClearBounds {
    pub(crate) fn cover(self, other: Self) -> Self {
        Self {
            root_lease_ms: self.root_lease_ms.max(other.root_lease_ms),
            request_ms: self.request_ms.max(other.request_ms),
            delegated_access_ms: self.delegated_access_ms.max(other.delegated_access_ms),
            clock_skew_ms: self.clock_skew_ms.max(other.clock_skew_ms),
        }
    }

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
