use std::sync::Arc;

use crate::{
    catalog::{CasOutcome, CatalogError, CatalogStore, RootState},
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId, SystemScope},
    operation::mutation_identity,
    record::StorageRecord,
};

use super::{ChunkRoot, FileIdentity};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileWriteIntent {
    pub identity: OperationId,
    pub owner: FileIdentity,
    pub root: ChunkRoot,
    pub created_ms: u64,
    pub not_before_ms: u64,
    pub deleting: bool,
}

impl FileWriteIntent {
    /// Registers exact ownership before disk IO and resolves an uncertain KV reply.
    /// # Errors
    /// Rejects fenced owners, mutable reclamation state and unconfirmed persistence.
    pub async fn register(&self, store: Arc<dyn CatalogStore>) -> Result<(), CatalogError> {
        self.validate()?;
        if self.deleting || self.not_before_ms != 0 {
            return Err(ValidationError::Record.into());
        }
        register_intent(&store, self).await
    }

    #[must_use]
    pub fn key(&self) -> IcebergKey {
        let mut suffix = self.owner.table.table.as_bytes().to_vec();
        suffix.extend_from_slice(self.owner.file.as_bytes());
        suffix.extend_from_slice(self.identity.as_bytes());
        IcebergKey::Catalog {
            catalog: self.owner.table.catalog,
            scope: CatalogScope::FileWriteIntent,
            suffix,
        }
    }

    #[must_use]
    pub fn fence_key(owner: FileIdentity) -> IcebergKey {
        let mut suffix = owner.table.table.as_bytes().to_vec();
        suffix.extend_from_slice(owner.file.as_bytes());
        IcebergKey::Catalog {
            catalog: owner.table.catalog,
            scope: CatalogScope::FileWriteFence,
            suffix,
        }
    }

    /// # Errors
    /// Rejects invalid physical ranges and deletion without durable retention.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.root.validate()?;
        if self.created_ms == 0
            || (self.not_before_ms != 0 && self.not_before_ms < self.created_ms)
            || (self.deleting && self.not_before_ms == 0)
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

pub(crate) async fn check_write_fence(
    store: &dyn CatalogStore,
    owner: FileIdentity,
) -> Result<(), CatalogError> {
    let key = FileWriteIntent::fence_key(owner);
    if let Some(value) = store.get(&key.encode()?).await? {
        StorageRecord::decode(&key, &value.bytes)?;
        return Err(CatalogError::Busy);
    }
    Ok(())
}

pub(crate) async fn register_intent(
    store: &Arc<dyn CatalogStore>,
    intent: &FileWriteIntent,
) -> Result<(), CatalogError> {
    check_writer(store.as_ref(), intent.owner).await?;
    let key = intent.key().encode()?;
    let bytes = StorageRecord::FileWriteIntent(Box::new(intent.clone())).encode()?;
    let outcome = store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await;
    match outcome {
        Ok(CasOutcome::Applied(_)) => {}
        Ok(CasOutcome::Conflict(Some(value))) if value.bytes == bytes => {}
        Ok(CasOutcome::Conflict(_)) => return Err(CatalogError::Conflict),
        Err(error) => {
            if store.get(&key).await?.map_or(true, |value| value.bytes != bytes) {
                return Err(error.into());
            }
        }
    }
    check_writer(store.as_ref(), intent.owner).await
}

async fn check_writer(store: &dyn CatalogStore, owner: FileIdentity) -> Result<(), CatalogError> {
    check_write_fence(store, owner).await?;
    let key = IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: Vec::new(),
    };
    let value = store
        .get(&key.encode()?)
        .await?
        .ok_or(CatalogError::Uninitialized)?;
    let StorageRecord::Active(root) = StorageRecord::decode(&key, &value.bytes)? else {
        return Err(ValidationError::Record.into());
    };
    if root.state != RootState::Ready || root.context.catalog != owner.table.catalog {
        return Err(CatalogError::Busy);
    }
    let key = crate::table::head_key(owner.table.catalog, owner.table.table);
    if let Some(value) = store.get(&key.encode()?).await? {
        let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if head.lifecycle != crate::table::TableLifecycle::Ready {
            return Err(CatalogError::Busy);
        }
    }
    Ok(())
}
