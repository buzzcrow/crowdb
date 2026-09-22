use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::catalog::{CasOutcome, CatalogError, CatalogStore};
use crate::error::ValidationError;
use crate::key::{CatalogId, CatalogScope, IcebergKey, OperationId};
use crate::record::StorageRecord;

use super::mutation_identity;

pub const PAYLOAD_PAGE_BYTES: usize = 32 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadReference {
    pub catalog: CatalogId,
    pub operation: OperationId,
    pub digest: [u8; 32],
    pub length: usize,
}

impl PayloadReference {
    /// # Errors
    /// Rejects an oversized payload before reading or allocating pages.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.length > MAX_PAYLOAD_BYTES {
            return Err(ValidationError::RecordTooLarge);
        }
        Ok(())
    }

    #[must_use]
    pub fn page_count(&self) -> usize {
        self.length.div_ceil(PAYLOAD_PAGE_BYTES).max(1)
    }

    /// # Errors
    /// Rejects invalid page numbers and oversized payloads.
    pub fn page_key(&self, index: u16) -> Result<IcebergKey, ValidationError> {
        self.validate()?;
        if usize::from(index) >= self.page_count() {
            return Err(ValidationError::Key);
        }
        let mut suffix = self.operation.as_bytes().to_vec();
        suffix.extend_from_slice(&self.digest);
        suffix.extend_from_slice(&index.to_be_bytes());
        Ok(IcebergKey::Catalog {
            catalog: self.catalog,
            scope: CatalogScope::OperationPayload,
            suffix,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadPage {
    pub reference: PayloadReference,
    pub index: u16,
    pub bytes: Vec<u8>,
}

impl PayloadPage {
    /// # Errors
    /// Rejects noncanonical chunk lengths and out-of-range indexes.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.reference.page_key(self.index)?;
        let start = usize::from(self.index) * PAYLOAD_PAGE_BYTES;
        let expected = self
            .reference
            .length
            .saturating_sub(start)
            .min(PAYLOAD_PAGE_BYTES);
        if self.bytes.len() != expected {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

pub struct PayloadStore {
    store: Arc<dyn CatalogStore>,
}

impl PayloadStore {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// # Errors
    /// Rejects excessive payloads, conflicting immutable pages and storage failures.
    pub async fn put(
        &self,
        catalog: CatalogId,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<PayloadReference, CatalogError> {
        if bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let reference = PayloadReference {
            catalog,
            operation,
            digest: Sha256::digest(bytes).into(),
            length: bytes.len(),
        };
        for index in 0..reference.page_count() {
            let start = index * PAYLOAD_PAGE_BYTES;
            let end = (start + PAYLOAD_PAGE_BYTES).min(bytes.len());
            self.put_page(PayloadPage {
                reference: reference.clone(),
                index: u16::try_from(index).map_err(|_| ValidationError::Key)?,
                bytes: bytes[start..end].to_vec(),
            })
            .await?;
        }
        Ok(reference)
    }

    /// # Errors
    /// Rejects missing, malformed, misbound or checksum-invalid payload pages.
    pub async fn get(&self, reference: &PayloadReference) -> Result<Vec<u8>, CatalogError> {
        reference.validate()?;
        let mut bytes = Vec::with_capacity(reference.length);
        for index in 0..reference.page_count() {
            let index = u16::try_from(index).map_err(|_| ValidationError::Key)?;
            let key = reference.page_key(index)?;
            let value = self
                .store
                .get(&key.encode()?)
                .await?
                .ok_or(ValidationError::Record)?;
            let StorageRecord::PayloadPage(page) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if page.reference != *reference {
                return Err(ValidationError::IdentityMismatch.into());
            }
            bytes.extend_from_slice(&page.bytes);
        }
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if digest != reference.digest {
            return Err(ValidationError::Record.into());
        }
        Ok(bytes)
    }

    async fn put_page(&self, page: PayloadPage) -> Result<(), CatalogError> {
        let key = page.reference.page_key(page.index)?.encode()?;
        let bytes = StorageRecord::PayloadPage(Box::new(page)).encode()?;
        match self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(existing)) if existing.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(ValidationError::Record.into()),
        }
    }
}
