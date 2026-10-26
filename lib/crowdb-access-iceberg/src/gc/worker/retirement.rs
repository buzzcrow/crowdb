use crate::{
    catalog::{CasOutcome, CatalogError},
    error::ValidationError,
    file::TableLocation,
    operation::mutation_identity,
    record::StorageRecord,
    table::storage_head_key,
};

use super::{GcWorkError, GcWorker};

impl GcWorker {
    pub(super) async fn retire_mapped_table(&self, table: TableLocation) -> Result<(), GcWorkError> {
        let key = storage_head_key(table).encode()?;
        let current = self
            .repository
            .store
            .get(&key)
            .await
            .map_err(CatalogError::from)?;
        self.retire_table_slot(table, current.as_ref().map(|value| value.bytes.as_slice()))
            .await
    }

    pub(super) async fn retire_table_slot(
        &self,
        table: TableLocation,
        current: Option<&[u8]>,
    ) -> Result<(), GcWorkError> {
        let key = storage_head_key(table);
        let encoded = key.encode()?;
        let retired = StorageRecord::RetiredTable(table).encode()?;
        let mut expected = current.map(<[u8]>::to_vec);
        // One concurrent resource change may be consumed from its CAS observation.
        // Further changes leave this GC step's durable cursor untouched.
        for _ in 0..2 {
            if let Some(bytes) = &expected {
                match StorageRecord::decode(&key, bytes)? {
                    StorageRecord::RetiredTable(_) => return Ok(()),
                    StorageRecord::TableHead(_) => {}
                    _ => return Err(ValidationError::Record.into()),
                }
            }
            let before = expected.as_deref();
            match self
                .repository
                .store
                .compare_exchange(
                    &encoded,
                    before,
                    &retired,
                    mutation_identity(&encoded, before, &retired),
                )
                .await
                .map_err(CatalogError::from)?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(observed) => {
                    expected = observed.map(|value| value.bytes);
                }
            }
        }
        if expected.as_deref() == Some(&retired) {
            return Ok(());
        }
        Err(CatalogError::Busy.into())
    }
}
