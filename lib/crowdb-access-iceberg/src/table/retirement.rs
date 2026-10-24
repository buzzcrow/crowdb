use crate::{
    catalog::{CasOutcome, CatalogError, CatalogStore},
    error::ValidationError,
    file::TableLocation,
    operation::mutation_identity,
    record::StorageRecord,
};

use super::{storage_head_key, TableHead};

pub(crate) enum TableSlot {
    Retired,
    Existing(Box<TableHead>),
}

pub(crate) async fn retire_absent_table(
    store: &dyn CatalogStore,
    table: TableLocation,
) -> Result<TableSlot, CatalogError> {
    let key = storage_head_key(table);
    let encoded = key.encode()?;
    let bytes = StorageRecord::RetiredTable(table).encode()?;
    match store
        .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
        .await?
    {
        CasOutcome::Applied(_) => Ok(TableSlot::Retired),
        CasOutcome::Conflict(Some(observed)) => match StorageRecord::decode(&key, &observed.bytes)? {
            StorageRecord::RetiredTable(_) => Ok(TableSlot::Retired),
            StorageRecord::TableHead(head) => Ok(TableSlot::Existing(head)),
            _ => Err(ValidationError::Record.into()),
        },
        CasOutcome::Conflict(None) => Err(CatalogError::Busy),
    }
}
