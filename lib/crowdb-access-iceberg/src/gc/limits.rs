use crate::error::ValidationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcLimits {
    pub page_items: u16,
    pub page_bytes: u32,
    pub step_bytes: u32,
    pub step_ms: u32,
    pub concurrency: u16,
    pub minimum_retention_ms: u64,
    pub retry_base_ms: u32,
    pub retry_max_ms: u32,
    pub corruption_attempts: u16,
}

impl Default for GcLimits {
    fn default() -> Self {
        Self {
            page_items: 64,
            page_bytes: 48 * 1024,
            step_bytes: 8 * 1024 * 1024,
            step_ms: 1000,
            concurrency: 1,
            minimum_retention_ms: 7 * 24 * 60 * 60 * 1000,
            retry_base_ms: 1000,
            retry_max_ms: 60_000,
            corruption_attempts: 3,
        }
    }
}

impl GcLimits {
    /// # Errors
    /// Rejects zero, excessive or inconsistent independent worker budgets.
    pub fn validate(self) -> Result<(), ValidationError> {
        if self.page_items == 0
            || self.page_items > 256
            || self.page_bytes < 4096
            || self.page_bytes > 48 * 1024
            || self.step_bytes < 32 * 1024
            || self.step_bytes > 64 * 1024 * 1024
            || self.step_ms == 0
            || self.step_ms > 60_000
            || self.concurrency == 0
            || self.concurrency > 16
            || self.minimum_retention_ms == 0
            || self.retry_base_ms == 0
            || self.retry_max_ms < self.retry_base_ms
            || self.retry_max_ms > 24 * 60 * 60 * 1000
            || self.corruption_attempts == 0
            || self.corruption_attempts > 100
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    #[must_use]
    pub fn retry_delay_ms(self, attempts: u32) -> u64 {
        u64::from(self.retry_base_ms)
            .saturating_mul(1_u64 << attempts.saturating_sub(1).min(63))
            .min(u64::from(self.retry_max_ms))
    }
}
