//! Legacy paged HTTP response format; new requests do not create these records.

use crate::error::ValidationError;

use super::{PayloadReference, RetryRecord};

pub const INLINE_RETRY_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryResult {
    pub binding: RetryRecord,
    pub body: PayloadReference,
}

impl RetryResult {
    /// # Errors
    /// Rejects nonterminal bindings, inline bodies and cross-domain references.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.binding.validate()?;
        self.body.validate()?;
        if self.binding.status == 0
            || !self.binding.body.is_empty()
            || self.body.catalog != self.binding.context.catalog
            || self.body.operation != self.binding.identity.operation
            || self.body.length <= INLINE_RETRY_BYTES
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}
