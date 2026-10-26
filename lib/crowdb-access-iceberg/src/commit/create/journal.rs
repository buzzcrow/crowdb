use std::sync::Arc;

use super::TableCreateOperation;
use crate::{
    catalog::{check_context, CatalogContext, CatalogError, CatalogStore},
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId},
    record::StorageRecord,
};

pub struct TableCreateJournal {
    store: Arc<dyn CatalogStore>,
}

impl TableCreateJournal {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// # Errors
    /// Rejects retired contexts, corrupt records and foreign activation epochs.
    pub async fn load(
        &self,
        context: CatalogContext,
        identity: OperationId,
    ) -> Result<Option<TableCreateOperation>, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::TableCreateOperation,
            suffix: identity.as_bytes().to_vec(),
        };
        let value = self.store.get(&key.encode()?).await?;
        let result = value
            .map(|value| {
                let StorageRecord::TableCreateOperation(operation) =
                    StorageRecord::decode(&key, &value.bytes)?
                else {
                    return Err(ValidationError::Record);
                };
                if operation.context != context {
                    return Err(ValidationError::IdentityMismatch);
                }
                Ok(*operation)
            })
            .transpose()?;
        check_context(self.store.as_ref(), context).await?;
        Ok(result)
    }
}
