//! Legacy global multipart credit formats, retained only for metadata decoding.

use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, OperationId};
use crate::operation::PayloadReference;
use crate::record::MAX_RECORD_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MultipartCredit {
    pub policy: OperationId,
    pub sequence: u64,
    pub released: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MultipartAdmissionLimits {
    pub max_sessions: u32,
    pub max_reserved_bytes: u64,
}

impl MultipartAdmissionLimits {
    /// # Errors
    /// Rejects missing or excessive independent session and byte ceilings.
    pub fn validate(self) -> Result<(), ValidationError> {
        if self.max_sessions == 0
            || self.max_sessions > 65_536
            || self.max_reserved_bytes == 0
            || self.max_reserved_bytes > u64::MAX / 8
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultipartCreditAction {
    Reserve,
    Release,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartCreditMutation {
    pub action: MultipartCreditAction,
    pub upload: OperationId,
    pub reservation_bytes: u64,
    pub before: Option<PayloadReference>,
    pub after: PayloadReference,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartAdmissionRecord {
    pub context: CatalogContext,
    pub policy: OperationId,
    pub revision: u64,
    pub limits: MultipartAdmissionLimits,
    pub sessions: u32,
    pub reserved_bytes: u64,
    pub pending: Option<MultipartCreditMutation>,
}

impl MultipartAdmissionRecord {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::MultipartAdmission,
            suffix: Vec::new(),
        }
    }

    /// # Errors
    /// Rejects incoherent counters, foreign payloads and invalid credit mutations.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.context.validate()?;
        self.limits.validate()?;
        if self.revision == 0
            || self.sessions > self.limits.max_sessions
            || self.reserved_bytes > self.limits.max_reserved_bytes
            || self.reserved_bytes < u64::from(self.sessions)
            || (self.sessions == 0 && self.reserved_bytes != 0)
        {
            return Err(ValidationError::Record);
        }
        if let Some(pending) = &self.pending {
            if self.revision < 2 || self.revision == u64::MAX {
                return Err(ValidationError::Record);
            }
            pending.validate(self)?;
        }
        Ok(())
    }
}

impl MultipartCreditMutation {
    fn validate(&self, record: &MultipartAdmissionRecord) -> Result<(), ValidationError> {
        if self.reservation_bytes == 0 || self.reservation_bytes > record.limits.max_reserved_bytes {
            return Err(ValidationError::Record);
        }
        for reference in self.before.iter().chain(std::iter::once(&self.after)) {
            reference.validate()?;
            if reference.catalog != record.context.catalog
                || reference.operation != self.upload
                || reference.length == 0
                || reference.length > MAX_RECORD_BYTES
            {
                return Err(ValidationError::Record);
            }
        }
        match self.action {
            MultipartCreditAction::Reserve
                if self.before.is_none()
                    && record.sessions > 0
                    && record.reserved_bytes >= self.reservation_bytes =>
            {
                Ok(())
            }
            MultipartCreditAction::Release
                if self.before.is_some()
                    && self.before.as_ref() != Some(&self.after)
                    && record.sessions < record.limits.max_sessions
                    && record
                        .reserved_bytes
                        .checked_add(self.reservation_bytes)
                        .is_some_and(|bytes| bytes <= record.limits.max_reserved_bytes) =>
            {
                Ok(())
            }
            _ => Err(ValidationError::Record),
        }
    }
}
