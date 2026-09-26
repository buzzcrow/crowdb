use crate::{
    catalog::{check_context, CatalogContext, CatalogError, CatalogLifecycle},
    commit::{TableCreateJournal, TableCreatePhase},
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId, TableId},
    operation::mutation_identity,
    record::StorageRecord,
    table::head_key,
};

use super::{GcPin, ReaderPins};

impl ReaderPins {
    /// # Errors
    /// Rejects inactive authority and overflowing persisted protection bounds.
    pub async fn request_expiry(&self, context: CatalogContext, starts_ms: u64) -> Result<u64, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::Authority(authority) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if authority.lifecycle != CatalogLifecycle::Ready || authority.admission_bounds.request_ms == 0 {
            return Err(CatalogError::Busy);
        }
        starts_ms
            .checked_add(authority.admission_bounds.request_ms)
            .and_then(|deadline| deadline.checked_add(authority.admission_bounds.clock_skew_ms))
            .ok_or(ValidationError::Deadline.into())
    }

    /// # Errors
    /// Rejects fenced tables and missing or changed staged-create authority.
    pub async fn protect_files(
        &self,
        context: CatalogContext,
        table: TableId,
        principal: &str,
        expires_ms: u64,
        now_ms: u64,
    ) -> Result<GcPin, CatalogError> {
        if expires_ms <= now_ms {
            return Err(ValidationError::Deadline.into());
        }
        let key = head_key(context.catalog, table);
        let current = self.store.get(&key.encode()?).await?;
        let stage = if current.is_none() {
            let identity = OperationId::from_bytes(table.as_bytes())?;
            let operation = TableCreateJournal::new(self.store.clone())
                .load(context, identity)
                .await?
                .ok_or(CatalogError::Conflict)?;
            if operation.phase != TableCreatePhase::Staged
                || operation.candidate.table != table
                || operation.stage.as_ref().map_or(true, |stage| {
                    u64::try_from(stage.expires_ms).map_or(true, |expiry| now_ms >= expiry)
                })
            {
                return Err(CatalogError::Busy);
            }
            Some(operation)
        } else {
            None
        };
        let head = if let Some(value) = current {
            let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            *head
        } else {
            stage.as_ref().ok_or(ValidationError::Record)?.candidate.clone()
        };
        let pin = GcPin {
            context,
            identity: OperationId::random(),
            head,
            principal: principal.into(),
            expires_ms,
            released: false,
            operator: false,
            protects_uploads: true,
        };
        if let Some(operation) = stage {
            pin.validate()?;
            let key = pin.key().encode()?;
            let bytes = StorageRecord::GcPin(Box::new(pin.clone())).encode()?;
            match self
                .store
                .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
                .await?
            {
                crate::catalog::CasOutcome::Applied(_) => {}
                crate::catalog::CasOutcome::Conflict(_) => return Err(CatalogError::Conflict),
            }
            if TableCreateJournal::new(self.store.clone())
                .load(context, operation.identity.operation)
                .await?
                .as_ref()
                != Some(&operation)
                || self
                    .store
                    .get(&head_key(context.catalog, table).encode()?)
                    .await?
                    .is_some()
            {
                return Err(CatalogError::Busy);
            }
            check_context(self.store.as_ref(), context).await?;
        } else {
            self.acquire(&pin).await?;
        }
        Ok(pin)
    }
}
