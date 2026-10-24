//! Legacy HTTP response records retained for decoding and garbage collection.

use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey};

use super::{RequestIdentity, MAX_PAYLOAD_BYTES};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryRecord {
    pub identity: RequestIdentity,
    pub principal: String,
    pub route: String,
    pub digest: [u8; 32],
    pub context: CatalogContext,
    pub retained_until_ms: u64,
    pub status: u16,
    pub body: Vec<u8>,
}

impl RetryRecord {
    /// # Errors
    /// Rejects invalid contexts, unbounded fields and non-final result statuses.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.context.validate()?;
        if self.principal.is_empty()
            || self.principal.len() > 256
            || self.principal.contains('\0')
            || self.route.is_empty()
            || self.route.len() > 1024
            || self.route.contains('\0')
            || self.body.len() > MAX_PAYLOAD_BYTES
            || (self.status == 0 && !self.body.is_empty())
            || !(self.status == 0 || terminal_status(self.status))
            || self.retained_until_ms <= self.identity.issued_ms
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    #[must_use]
    pub fn result_key(&self) -> IcebergKey {
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::Operation,
            suffix: self.identity.operation.as_bytes().to_vec(),
        }
    }
}

fn terminal_status(status: u16) -> bool {
    matches!(status, 200 | 201 | 204 | 400 | 403 | 404 | 406 | 409 | 422)
}
