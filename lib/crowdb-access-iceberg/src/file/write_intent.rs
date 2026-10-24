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
    /// Registers exact ownership before disk IO; an uncertain reply stops the write.
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
        .await?;
    match outcome {
        CasOutcome::Applied(_) => {}
        CasOutcome::Conflict(Some(value)) if value.bytes == bytes => {}
        CasOutcome::Conflict(_) => return Err(CatalogError::Conflict),
    }
    check_writer(store.as_ref(), intent.owner).await
}

async fn check_writer(store: &dyn CatalogStore, owner: FileIdentity) -> Result<(), CatalogError> {
    let root_key = IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: Vec::new(),
    };
    let head_key = crate::table::storage_head_key(owner.table);
    let root_bytes = root_key.encode()?;
    let head_bytes = head_key.encode()?;
    let ((), root, head) = tokio::try_join!(
        check_write_fence(store, owner),
        async { store.get(&root_bytes).await.map_err(CatalogError::from) },
        async { store.get(&head_bytes).await.map_err(CatalogError::from) },
    )?;
    let value = root.ok_or(CatalogError::Uninitialized)?;
    let StorageRecord::Active(root) = StorageRecord::decode(&root_key, &value.bytes)? else {
        return Err(ValidationError::Record.into());
    };
    if root.state != RootState::Ready || root.context.catalog != owner.table.catalog {
        return Err(CatalogError::Busy);
    }
    if let Some(value) = head {
        let head = match StorageRecord::decode(&head_key, &value.bytes)? {
            StorageRecord::TableHead(head) => head,
            StorageRecord::RetiredTable(_) => return Err(CatalogError::Busy),
            _ => return Err(ValidationError::Record.into()),
        };
        if head.lifecycle != crate::table::TableLifecycle::Ready {
            return Err(CatalogError::Busy);
        }
    }
    Ok(())
}
