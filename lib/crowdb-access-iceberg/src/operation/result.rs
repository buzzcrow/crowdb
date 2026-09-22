use crate::catalog::CatalogError;
use crate::error::ValidationError;
use crate::record::StorageRecord;

use super::{PayloadReference, PayloadStore, RetryRecord};

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

pub(super) async fn encode_result(
    payloads: &PayloadStore,
    request: &RetryRecord,
) -> Result<Vec<u8>, CatalogError> {
    if request.body.len() <= INLINE_RETRY_BYTES {
        return Ok(StorageRecord::Retry(Box::new(request.clone())).encode()?);
    }
    let body = payloads
        .put(request.context.catalog, request.identity.operation, &request.body)
        .await?;
    let mut binding = request.clone();
    binding.body.clear();
    Ok(StorageRecord::RetryResult(Box::new(RetryResult { binding, body })).encode()?)
}

pub(super) async fn decode_result(
    payloads: &PayloadStore,
    record: StorageRecord,
) -> Result<RetryRecord, CatalogError> {
    match record {
        StorageRecord::Retry(record) => Ok(*record),
        StorageRecord::RetryResult(result) => {
            let mut record = result.binding;
            record.body = payloads.get(&result.body).await?;
            record.validate()?;
            Ok(record)
        }
        _ => Err(ValidationError::Record.into()),
    }
}
