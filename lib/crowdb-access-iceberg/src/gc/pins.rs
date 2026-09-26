use std::sync::Arc;

use crate::{
    catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore},
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcPin {
    pub context: CatalogContext,
    pub identity: OperationId,
    pub head: TableHead,
    pub principal: String,
    pub expires_ms: u64,
    pub released: bool,
    pub operator: bool,
    pub protects_uploads: bool,
}

#[derive(Clone)]
pub struct ReaderPins {
    pub(super) store: Arc<dyn CatalogStore>,
}

impl ReaderPins {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// # Errors
    /// Rejects malformed or mismatched persisted pins.
    pub async fn get(
        &self,
        catalog: crate::key::CatalogId,
        table: crate::key::TableId,
        identity: OperationId,
    ) -> Result<Option<GcPin>, CatalogError> {
        let mut suffix = table.as_bytes().to_vec();
        suffix.extend_from_slice(identity.as_bytes());
        let key = IcebergKey::Catalog {
            catalog,
            scope: CatalogScope::GcPin,
            suffix,
        };
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::GcPin(pin) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if pin.context.catalog != catalog || pin.head.table != table || pin.identity != identity {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok(Some(*pin))
    }

    /// Persists protection before checking the selected head against a concurrent sweep.
    /// # Errors
    /// Rejects retired catalogs, changed heads and reused identities.
    pub async fn acquire(&self, pin: &GcPin) -> Result<(), CatalogError> {
        pin.validate()?;
        if pin.released
            || pin.head.lifecycle == TableLifecycle::Reclaiming
            || (!pin.operator && pin.head.lifecycle != TableLifecycle::Ready)
        {
            return Err(ValidationError::Record.into());
        }
        check_context(self.store.as_ref(), pin.context).await?;
        let key = pin.key().encode()?;
        let bytes = StorageRecord::GcPin(Box::new(pin.clone())).encode()?;
        match self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?
        {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(existing)) if existing.bytes == bytes => {}
            CasOutcome::Conflict(_) => return Err(CatalogError::Conflict),
        }
        let key = head_key(pin.head.catalog, pin.head.table);
        let current = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Conflict)?;
        if StorageRecord::decode(&key, &current.bytes)?
            != StorageRecord::TableHead(Box::new(pin.head.clone()))
        {
            return Err(CatalogError::Busy);
        }
        check_context(self.store.as_ref(), pin.context).await
    }

    /// # Errors
    /// Rejects a changed pin; retries preserve the same release result.
    pub async fn release(&self, pin: &GcPin) -> Result<(), CatalogError> {
        pin.validate()?;
        let mut released = pin.clone();
        released.released = true;
        let key = pin.key().encode()?;
        let before = StorageRecord::GcPin(Box::new(pin.clone())).encode()?;
        let after = StorageRecord::GcPin(Box::new(released)).encode()?;
        match self
            .store
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(existing)) if existing.bytes == after => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Conflict),
        }
    }
}

impl GcPin {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        let mut suffix = self.head.table.as_bytes().to_vec();
        suffix.extend_from_slice(self.identity.as_bytes());
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::GcPin,
            suffix,
        }
    }

    /// # Errors
    /// Rejects mismatched roots, unbounded principals and unbounded reader lifetimes.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.context.validate()?;
        self.head.validate()?;
        if self.head.catalog != self.context.catalog
            || self.principal.is_empty()
            || self.principal.len() > 256
            || self.principal.contains('\0')
            || (!self.operator && self.expires_ms == 0)
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    #[must_use]
    pub const fn protects(&self, now_ms: u64) -> bool {
        !self.released && (self.expires_ms == 0 || now_ms < self.expires_ms)
    }
}
