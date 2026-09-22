use std::sync::Arc;

use crate::catalog::{CasOutcome, CatalogContext, CatalogError, CatalogStore, RootState};
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, SystemScope};
use crate::record::StorageRecord;

use super::{ledger_key, mutation_identity, RequestIdentity, RETRY_WINDOW_MS};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryRecord {
    pub identity: RequestIdentity,
    pub principal: String,
    pub route: String,
    pub digest: [u8; 32],
    pub context: CatalogContext,
    pub retained_until_ms: u64,
    pub status: u16,
    pub body: Vec<u8>,
}

impl RetryRecord {
    /// # Errors
    /// Rejects invalid contexts, unbounded fields and non-final result statuses.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.context.validate()?;
        if self.principal.is_empty()
            || self.principal.len() > 256
            || self.principal.contains('\0')
            || self.route.is_empty()
            || self.route.len() > 1024
            || self.route.contains('\0')
            || self.body.len() > 16 * 1024
            || (self.status == 0 && !self.body.is_empty())
            || !(self.status == 0 || terminal_status(self.status))
            || self.retained_until_ms <= self.identity.issued_ms
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    #[must_use]
    pub fn result_key(&self) -> IcebergKey {
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::Operation,
            suffix: self.identity.operation.as_bytes().to_vec(),
        }
    }

    fn same_request(&self, other: &Self) -> bool {
        self.identity == other.identity
            && self.principal == other.principal
            && self.route == other.route
            && self.digest == other.digest
            && self.context == other.context
    }
}

#[derive(Debug)]
pub enum RetryAdmission {
    New(RetryRecord),
    Resume(RetryRecord),
    Replay(RetryRecord),
}

pub struct RetryLedger {
    store: Arc<dyn CatalogStore>,
}

impl RetryLedger {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// # Errors
    /// Rejects changed input/principal/domain, expired keys, collisions and storage failures.
    pub async fn begin(&self, mut request: RetryRecord, now_ms: u64) -> Result<RetryAdmission, CatalogError> {
        request.status = 0;
        request.body.clear();
        request.retained_until_ms = now_ms
            .checked_add(RETRY_WINDOW_MS)
            .and_then(|time| time.checked_add(30_000))
            .ok_or(ValidationError::Deadline)?;
        request.validate()?;
        self.check_context(request.context).await?;
        let key = ledger_key(SystemScope::RetryBinding, request.identity.operation)?;
        let previous = self.store.get(&key.encode()?).await?;
        if let Some(value) = &previous {
            let StorageRecord::Retry(existing) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if existing.identity.operation == request.identity.operation {
                if !existing.same_request(&request) || now_ms > existing.retained_until_ms {
                    return Err(CatalogError::Conflict);
                }
                return self.existing(*existing).await;
            }
            if existing.status == 0 || now_ms <= existing.retained_until_ms {
                return Err(CatalogError::Busy);
            }
        }
        request.identity.validate(now_ms)?;
        let bytes = StorageRecord::Retry(Box::new(request.clone())).encode()?;
        if self
            .cas(
                &key,
                previous.as_ref().map(|value| value.bytes.as_slice()),
                &bytes,
            )
            .await?
        {
            Ok(RetryAdmission::New(request))
        } else {
            Err(CatalogError::Busy)
        }
    }

    /// # Errors
    /// Rejects conflicting final results, domain changes, expired bindings and storage failures.
    pub async fn finish(
        &self,
        mut request: RetryRecord,
        status: u16,
        body: Vec<u8>,
        now_ms: u64,
    ) -> Result<bool, CatalogError> {
        if !terminal_status(status) {
            return Ok(false);
        }
        self.check_context(request.context).await?;
        let key = ledger_key(SystemScope::RetryBinding, request.identity.operation)?;
        let previous = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Conflict)?;
        let StorageRecord::Retry(binding) = StorageRecord::decode(&key, &previous.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if !binding.same_request(&request) || now_ms > binding.retained_until_ms {
            return Err(CatalogError::Conflict);
        }
        request.retained_until_ms = binding.retained_until_ms;
        request.status = status;
        request.body = body;
        request.validate()?;
        let result_key = request.result_key();
        let result = StorageRecord::Retry(Box::new(request.clone())).encode()?;
        if !self.cas(&result_key, None, &result).await? {
            let existing = self
                .store
                .get(&result_key.encode()?)
                .await?
                .ok_or(CatalogError::Conflict)?;
            if existing.bytes != result {
                return Err(CatalogError::Conflict);
            }
        }
        request.body.clear();
        let binding = StorageRecord::Retry(Box::new(request)).encode()?;
        self.cas(&key, Some(&previous.bytes), &binding).await?;
        Ok(true)
    }

    async fn existing(&self, binding: RetryRecord) -> Result<RetryAdmission, CatalogError> {
        let key = binding.result_key();
        let Some(value) = self.store.get(&key.encode()?).await? else {
            if binding.status != 0 {
                return Err(ValidationError::Record.into());
            }
            return Ok(RetryAdmission::Resume(binding));
        };
        let StorageRecord::Retry(result) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if !binding.same_request(&result)
            || result.status == 0
            || binding.retained_until_ms != result.retained_until_ms
            || (binding.status != 0 && binding.status != result.status)
        {
            return Err(ValidationError::Record.into());
        }
        if binding.status == 0 {
            let binding_key = ledger_key(SystemScope::RetryBinding, binding.identity.operation)?;
            let previous = StorageRecord::Retry(Box::new(binding.clone())).encode()?;
            let mut completed = binding;
            completed.status = result.status;
            self.cas(
                &binding_key,
                Some(&previous),
                &StorageRecord::Retry(Box::new(completed)).encode()?,
            )
            .await?;
        }
        Ok(RetryAdmission::Replay(*result))
    }

    async fn check_context(&self, context: CatalogContext) -> Result<(), CatalogError> {
        let key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Uninitialized)?;
        let StorageRecord::Active(root) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if root.state != RootState::Ready || root.context != context {
            return Err(CatalogError::Conflict);
        }
        Ok(())
    }

    async fn cas(
        &self,
        key: &IcebergKey,
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, CatalogError> {
        let key = key.encode()?;
        Ok(matches!(
            self.store
                .compare_exchange(&key, expected, value, mutation_identity(&key, expected, value))
                .await?,
            CasOutcome::Applied(_)
        ))
    }
}

fn terminal_status(status: u16) -> bool {
    matches!(status, 200 | 201 | 204 | 400 | 403 | 404 | 406 | 409 | 422)
}
