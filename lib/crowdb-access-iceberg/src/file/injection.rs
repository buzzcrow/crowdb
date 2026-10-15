use std::sync::Arc;

use crate::{
    catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore},
    error::ValidationError,
    key::{StorageKey, TableStorageKey, TableStorageScope},
    operation::{mutation_identity, RequestIdentity, RETRY_WINDOW_MS},
    record::StorageRecord,
};

use super::{DirectFileLocations, FileIdentity, FileRecord, FileRepository, FileTree, TableLocation};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeInjection {
    pub identity: RequestIdentity,
    pub activation_epoch: u64,
    pub principal: [u8; 32],
    pub stem: String,
    pub file: FileRecord,
}

impl NativeInjection {
    /// # Errors
    /// Rejects legacy ownership, unsupported content and unbounded retention.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.file.validate()?;
        DirectFileLocations::validate_stem(&self.stem)?;
        if self.activation_epoch == 0
            || self.file.location.table().storage_namespace.is_none()
            || self.stem.contains('/')
            || !self
                .file
                .location
                .relative_key()
                .starts_with(&format!("f/v1/{}.", self.stem))
        {
            return Err(ValidationError::Record);
        }
        self.identity
            .issued_ms
            .checked_add(RETRY_WINDOW_MS)
            .ok_or(ValidationError::Deadline)?;
        Ok(())
    }

    /// # Errors
    /// Rejects legacy storage identities.
    pub fn key(&self) -> Result<StorageKey, ValidationError> {
        injection_key(self.file.location.table(), self.identity)
    }

    #[must_use]
    pub fn retained(&self, now_ms: u64) -> bool {
        now_ms <= self.identity.issued_ms.saturating_add(RETRY_WINDOW_MS)
    }
}

#[derive(Clone, Debug)]
pub struct NativeInjectionRequest {
    pub context: CatalogContext,
    pub table: TableLocation,
    pub identity: RequestIdentity,
    pub principal: [u8; 32],
    pub stem: String,
    pub digest: [u8; 32],
}

impl NativeInjectionRequest {
    fn validate(&self, now_ms: u64) -> Result<(), ValidationError> {
        self.context.validate()?;
        self.identity.validate(now_ms)?;
        DirectFileLocations::validate_stem(&self.stem)?;
        if self.table.catalog != self.context.catalog
            || self.table.storage_namespace.is_none()
            || self.stem.contains('/')
        {
            return Err(ValidationError::IdentityMismatch);
        }
        Ok(())
    }

    fn matches(&self, receipt: &NativeInjection) -> bool {
        receipt.identity == self.identity
            && receipt.activation_epoch == self.context.activation_epoch
            && receipt.file.location.table() == self.table
            && receipt.principal == self.principal
            && receipt.stem == self.stem
            && receipt.file.digest == self.digest
    }
}

pub struct NativeInjectionRepository {
    store: Arc<dyn CatalogStore>,
}

impl NativeInjectionRepository {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// Replays a selected upload without allocating a new tree or changing its URL.
    /// # Errors
    /// Rejects expired, foreign, changed or logically deleted upload retries.
    pub async fn replay(
        &self,
        request: &NativeInjectionRequest,
        now_ms: u64,
    ) -> Result<Option<FileRecord>, CatalogError> {
        request.validate(now_ms)?;
        check_context(self.store.as_ref(), request.context).await?;
        let key = injection_key(request.table, request.identity)?;
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::NativeInjection(receipt) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if !request.matches(&receipt) {
            return Err(CatalogError::Conflict);
        }
        self.publish(request.context, &receipt.file).await.map(Some)
    }

    /// Selects one durable descriptor before publishing its sole filename authority.
    /// # Errors
    /// Rejects changed payloads and preserves the durable winner after uncertain replies.
    pub async fn finish(
        &self,
        request: &NativeInjectionRequest,
        owner: FileIdentity,
        tree: FileTree,
        locations: &DirectFileLocations,
        now_ms: u64,
    ) -> Result<FileRecord, CatalogError> {
        request.validate(now_ms)?;
        if owner.table != request.table || tree.digest != request.digest {
            return Err(CatalogError::Conflict);
        }
        check_context(self.store.as_ref(), request.context).await?;
        let file = locations.parquet(
            owner.table,
            &request.stem,
            owner.file,
            tree.length,
            tree.digest,
            tree.root.ok_or(ValidationError::Record)?,
        )?;
        let receipt = NativeInjection {
            identity: request.identity,
            activation_epoch: request.context.activation_epoch,
            principal: request.principal,
            stem: request.stem.clone(),
            file,
        };
        let key = receipt.key()?;
        let bytes = StorageRecord::NativeInjection(Box::new(receipt.clone())).encode()?;
        let encoded = key.encode()?;
        let selected = match self
            .store
            .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
            .await
        {
            Ok(CasOutcome::Applied(_)) => receipt,
            outcome => {
                let value = match outcome {
                    Ok(CasOutcome::Conflict(Some(value))) => value,
                    Ok(CasOutcome::Conflict(None)) => return Err(CatalogError::Conflict),
                    Err(error) => self
                        .store
                        .get(&encoded)
                        .await?
                        .ok_or(CatalogError::Store(error))?,
                    Ok(CasOutcome::Applied(_)) => return Err(ValidationError::Record.into()),
                };
                let StorageRecord::NativeInjection(receipt) = StorageRecord::decode(&key, &value.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                if !request.matches(&receipt) {
                    return Err(CatalogError::Conflict);
                }
                *receipt
            }
        };
        self.publish(request.context, &selected.file).await
    }

    async fn publish(&self, context: CatalogContext, file: &FileRecord) -> Result<FileRecord, CatalogError> {
        FileRepository::new(self.store.clone())
            .publish(context, file)
            .await
    }
}

fn injection_key(table: TableLocation, identity: RequestIdentity) -> Result<StorageKey, ValidationError> {
    Ok(StorageKey::NativeTable(TableStorageKey {
        table: table.storage_path().ok_or(ValidationError::Key)?,
        scope: TableStorageScope::Operation,
        suffix: format!("{}/injection", identity.operation),
    }))
}
