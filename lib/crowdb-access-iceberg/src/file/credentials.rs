use crate::catalog::CatalogContext;
use crate::key::{OperationId, TableId};

use super::FileLocation;

mod token;
pub use token::FileGrantIssuer;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FileOperation {
    Head = 0,
    Get = 1,
    Put = 2,
    CreateMultipart = 3,
    UploadPart = 4,
    ListParts = 5,
    CompleteMultipart = 6,
    AbortMultipart = 7,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileOperations(u16);

impl FileOperations {
    /// # Errors
    /// Rejects empty and unknown operation sets; file DELETE has no representation.
    pub fn from_bits(bits: u16) -> Result<Self, FileGrantError> {
        if bits == 0 || bits & !0xff != 0 {
            return Err(FileGrantError::Invalid);
        }
        Ok(Self(bits))
    }

    /// # Errors
    /// Rejects empty operation sets.
    pub fn new(operations: &[FileOperation]) -> Result<Self, FileGrantError> {
        Self::from_bits(
            operations
                .iter()
                .fold(0, |bits, operation| bits | (1 << *operation as u8)),
        )
    }

    #[must_use]
    pub const fn allows(self, operation: FileOperation) -> bool {
        self.0 & (1 << operation as u8) != 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileGrant {
    pub context: CatalogContext,
    pub table: TableId,
    pub principal: [u8; 32],
    pub nonce: OperationId,
    pub issued_ms: u64,
    pub expires_ms: u64,
    pub operations: FileOperations,
    pub max_request_bytes: u64,
    pub max_file_bytes: u64,
}

impl FileGrant {
    /// # Errors
    /// Rejects requests outside the exact table prefix, operation or byte budgets.
    pub fn authorize(
        &self,
        operation: FileOperation,
        location: &FileLocation,
        request_bytes: u64,
        file_bytes: u64,
    ) -> Result<(), FileGrantError> {
        if location.table().catalog != self.context.catalog
            || location.table().table != self.table
            || !self.operations.allows(operation)
        {
            return Err(FileGrantError::Forbidden);
        }
        if request_bytes > self.max_request_bytes || file_bytes > self.max_file_bytes {
            return Err(FileGrantError::Bounds);
        }
        Ok(())
    }

    fn validate(&self, max_ttl_ms: u64) -> Result<(), FileGrantError> {
        self.context.validate().map_err(|_| FileGrantError::Invalid)?;
        let ttl = self
            .expires_ms
            .checked_sub(self.issued_ms)
            .ok_or(FileGrantError::Invalid)?;
        if ttl == 0
            || ttl > max_ttl_ms
            || self.max_request_bytes == 0
            || self.max_file_bytes == 0
            || self.max_request_bytes > self.max_file_bytes
        {
            return Err(FileGrantError::Invalid);
        }
        FileOperations::from_bits(self.operations.0)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FileGrantError {
    #[error("invalid delegated file credential")]
    Invalid,
    #[error("delegated file credential is not currently valid")]
    Expired,
    #[error("file request is outside its delegated scope")]
    Forbidden,
    #[error("file request exceeds its delegated byte limits")]
    Bounds,
}

pub struct FileCredentials {
    grant: FileGrant,
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
}

impl FileCredentials {
    #[must_use]
    pub const fn grant(&self) -> &FileGrant {
        &self.grant
    }
    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }
    #[must_use]
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
    #[must_use]
    pub fn session_token(&self) -> &str {
        &self.session_token
    }
}
