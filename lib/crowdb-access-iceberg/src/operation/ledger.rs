use crowdb_common::hash_slot::{HashSlotFallback, Placement};

use crate::catalog::{CatalogError, CatalogStore, StoredValue};
use crate::error::ValidationError;
use crate::key::{IcebergKey, OperationId, SystemScope};
use crate::record::StorageRecord;

use super::identity::overflow_key;
use super::ledger_key;

pub(crate) enum LedgerLocation {
    Existing(IcebergKey, StoredValue),
    Vacant(IcebergKey),
}

pub(crate) async fn ledger_locate(
    store: &dyn CatalogStore,
    scope: SystemScope,
    operation: OperationId,
) -> Result<LedgerLocation, CatalogError> {
    let slot_key = ledger_key(scope, operation)?;
    let slot = store.get(&slot_key.encode()?).await?;
    let slot_occupied = slot.is_some();
    if let Some(value) = slot {
        let occupant = record_operation(scope, &slot_key, &value)?;
        if HashSlotFallback::placement(Some(occupant.as_bytes()), operation.as_bytes()) == Placement::Slot {
            return Ok(LedgerLocation::Existing(slot_key, value));
        }
    }
    let overflow_key = overflow_key(scope, operation)?;
    if let Some(value) = store.get(&overflow_key.encode()?).await? {
        record_operation(scope, &overflow_key, &value)?;
        return Ok(LedgerLocation::Existing(overflow_key, value));
    }
    Ok(LedgerLocation::Vacant(if slot_occupied {
        overflow_key
    } else {
        slot_key
    }))
}

fn record_operation(
    scope: SystemScope,
    key: &IcebergKey,
    value: &StoredValue,
) -> Result<OperationId, CatalogError> {
    match (scope, StorageRecord::decode(key, &value.bytes)?) {
        (SystemScope::RetryBinding, StorageRecord::Retry(record)) => Ok(record.identity.operation),
        (SystemScope::ManagementOperation | SystemScope::Audit, StorageRecord::Management(record)) => {
            Ok(record.id())
        }
        _ => Err(ValidationError::Record.into()),
    }
}
