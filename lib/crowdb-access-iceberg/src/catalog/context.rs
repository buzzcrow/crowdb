use crate::error::ValidationError;
use crate::key::{IcebergKey, SystemScope};
use crate::record::StorageRecord;

use super::{CatalogContext, CatalogError, CatalogStore, RootState};

pub(crate) async fn check_context<Store: CatalogStore + ?Sized>(
    store: &Store,
    context: CatalogContext,
) -> Result<(), CatalogError> {
    context.validate()?;
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
    if root.context != context {
        return Err(CatalogError::Conflict);
    }
    if root.state != RootState::Ready {
        return Err(CatalogError::Busy);
    }
    Ok(())
}
