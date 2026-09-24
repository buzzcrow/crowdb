use std::sync::Arc;
use std::time::Instant;

use crate::error::ValidationError;
use crate::key::{CatalogId, CatalogScope, IcebergKey, OperationId, SystemScope};
use crate::operation::{
    ledger_key, mutation_identity, ManagementAction, ManagementOperation, ManagementPhase, ManagementRequest,
    RETRY_WINDOW_MS,
};
use crate::record::StorageRecord;

use super::{
    ActiveCatalogRecord, CasOutcome, CatalogAuthority, CatalogStore, ClearBounds, RootState, StoreError,
};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    Invalid(#[from] ValidationError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("catalog operation conflicts with current state or request identity")]
    Conflict,
    #[error("catalog maintenance or bounded operation capacity is busy")]
    Busy,
    #[error("catalog is not initialized")]
    Uninitialized,
    #[error("management privilege is required")]
    Forbidden,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementPrivilege {
    None,
    Manage,
    Clear,
}

pub struct CatalogRepository {
    pub(super) store: Arc<dyn CatalogStore>,
    pub(super) bounds: ClearBounds,
}

impl CatalogRepository {
    /// # Errors
    /// Rejects invalid timing limits before any storage access.
    pub fn new(store: Arc<dyn CatalogStore>, bounds: ClearBounds) -> Result<Self, CatalogError> {
        bounds.completion_deadline(0)?;
        Ok(Self { store, bounds })
    }

    /// # Errors
    /// Returns malformed storage, missing catalog, or maintenance errors.
    pub async fn status(&self) -> Result<(ActiveCatalogRecord, CatalogAuthority), CatalogError> {
        let (root, _) = self.root().await?.ok_or(CatalogError::Uninitialized)?;
        let authority = self.authority(root.context.catalog).await?;
        Ok((root, authority))
    }

    /// # Errors
    /// Rejects unauthorized requests, conflicts, expired identities and busy recovery.
    pub async fn execute(
        &self,
        request: ManagementRequest,
        privilege: ManagementPrivilege,
        now_ms: u64,
    ) -> Result<CatalogAuthority, CatalogError> {
        if privilege == ManagementPrivilege::None
            || (request.action == ManagementAction::Clear && privilege != ManagementPrivilege::Clear)
        {
            return Err(CatalogError::Forbidden);
        }
        request.validate()?;
        let started = Instant::now();
        let key = ledger_key(SystemScope::ManagementOperation, request.identity.operation)?;
        for _ in 0..32 {
            if let Some(operation) = self.operation(request.identity.operation).await? {
                if now_ms > operation.retained_until_ms {
                    return Err(ValidationError::Deadline.into());
                }
                if operation.request.digest() != request.digest() {
                    return Err(CatalogError::Conflict);
                }
                if let Some(result) = self.resume(operation, elapsed_now(now_ms, started)?).await? {
                    return Ok(result);
                }
                continue;
            }
            request.identity.validate(now_ms)?;
            let operation = self
                .prepare(request.clone(), elapsed_now(now_ms, started)?)
                .await?;
            self.install_operation(&key, &operation, now_ms).await?;
        }
        Err(CatalogError::Busy)
    }

    /// # Errors
    /// Returns storage corruption or maintenance still inside its persisted grace.
    pub async fn recover(&self, now_ms: u64) -> Result<(), CatalogError> {
        let started = Instant::now();
        for _ in 0..32 {
            let Some((root, _)) = self.root().await? else {
                return Ok(());
            };
            let operation = self
                .operation(root.operation)
                .await?
                .ok_or(ValidationError::Record)?;
            if root.state == RootState::Ready && operation.terminal() {
                return Ok(());
            }
            self.resume(operation, elapsed_now(now_ms, started)?).await?;
        }
        Err(CatalogError::Busy)
    }

    pub(super) async fn root(&self) -> Result<Option<(ActiveCatalogRecord, Vec<u8>)>, CatalogError> {
        let key = root_key();
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::Active(root) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        Ok(Some((root, value.bytes)))
    }

    pub(super) async fn authority(&self, catalog: CatalogId) -> Result<CatalogAuthority, CatalogError> {
        let key = authority_key(catalog);
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        decode_authority(catalog, &value.bytes)
    }

    pub(super) async fn operation(
        &self,
        identity: OperationId,
    ) -> Result<Option<ManagementOperation>, CatalogError> {
        let key = ledger_key(SystemScope::ManagementOperation, identity)?;
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::Management(operation) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        Ok((operation.id() == identity).then_some(*operation))
    }

    pub(super) async fn cas(
        &self,
        key: &IcebergKey,
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, CatalogError> {
        let key = key.encode()?;
        let identity = mutation_identity(&key, expected, value);
        Ok(matches!(
            self.store
                .compare_exchange(&key, expected, value, identity)
                .await?,
            CasOutcome::Applied(_)
        ))
    }

    pub(super) async fn phase(
        &self,
        operation: &ManagementOperation,
        phase: ManagementPhase,
    ) -> Result<bool, CatalogError> {
        let mut next = operation.clone();
        next.phase = phase;
        self.cas(
            &ledger_key(SystemScope::ManagementOperation, operation.id())?,
            Some(&operation_bytes(operation)?),
            &operation_bytes(&next)?,
        )
        .await
    }

    async fn prepare(
        &self,
        request: ManagementRequest,
        now_ms: u64,
    ) -> Result<ManagementOperation, CatalogError> {
        let started = Instant::now();
        let root = self.root().await?;
        if let Some((current, _)) = &root {
            if current.state != RootState::Ready {
                self.recover(elapsed_now(now_ms, started)?).await?;
                return Err(CatalogError::Busy);
            }
            let previous = self
                .operation(current.operation)
                .await?
                .ok_or(ValidationError::Record)?;
            if !previous.terminal() {
                self.resume(previous, elapsed_now(now_ms, started)?).await?;
                return Err(CatalogError::Busy);
            }
        }
        let original_authority = match &root {
            Some((current, _)) => Some(self.authority(current.context.catalog).await?),
            None => None,
        };
        let valid = match (request.action, &root) {
            (ManagementAction::Initialize, None) => true,
            (ManagementAction::Rename, Some((current, _))) => {
                current.context.activation_epoch == request.expected_epoch
            }
            (ManagementAction::Clear, Some((current, _))) => {
                current.context.activation_epoch == request.expected_epoch
                    && request.confirmation == Some(current.context.catalog)
            }
            _ => false,
        };
        let candidate = if request.action == ManagementAction::Rename {
            root.as_ref()
                .map_or_else(CatalogId::random, |(current, _)| current.context.catalog)
        } else {
            CatalogId::random()
        };
        if valid && request.action == ManagementAction::Clear {
            root.as_ref()
                .ok_or(ValidationError::Record)?
                .0
                .context
                .replacement(candidate)?;
        }
        let bounds = original_authority
            .as_ref()
            .map_or(self.bounds, |authority| authority.admission_bounds);
        let bounds = if valid && request.action == ManagementAction::Clear {
            bounds.cover(self.bounds)
        } else {
            bounds
        };
        let mut result = if request.action == ManagementAction::Rename && valid {
            original_authority
                .as_ref()
                .ok_or(ValidationError::Record)?
                .renamed(request.display_name.clone())?
        } else {
            CatalogAuthority::new(candidate, request.display_name.clone())?
        };
        result.admission_bounds = bounds;
        Ok(ManagementOperation {
            request,
            candidate,
            phase: if valid {
                ManagementPhase::Prepared
            } else {
                ManagementPhase::Conflict
            },
            original_root: root.map_or_else(Vec::new, |(_, bytes)| bytes),
            original_authority: original_authority
                .map(|value| StorageRecord::Authority(value).encode())
                .transpose()?
                .unwrap_or_default(),
            result_authority: StorageRecord::Authority(result).encode()?,
            bounds,
            publication_proof: Vec::new(),
            grace_completed_ms: 0,
            retained_until_ms: now_ms
                .checked_add(RETRY_WINDOW_MS)
                .and_then(|time| time.checked_add(self.bounds.clock_skew_ms))
                .ok_or(ValidationError::Deadline)?,
        })
    }

    async fn install_operation(
        &self,
        key: &IcebergKey,
        operation: &ManagementOperation,
        now_ms: u64,
    ) -> Result<(), CatalogError> {
        let previous = self.store.get(&key.encode()?).await?;
        if let Some(value) = &previous {
            let StorageRecord::Management(old) = StorageRecord::decode(key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if old.id() == operation.id() {
                return Ok(());
            }
            if !old.terminal()
                || now_ms <= old.retained_until_ms
                || self
                    .root()
                    .await?
                    .is_some_and(|(root, _)| root.operation == old.id())
            {
                return Err(CatalogError::Busy);
            }
        }
        self.cas(
            key,
            previous.as_ref().map(|value| value.bytes.as_slice()),
            &operation_bytes(operation)?,
        )
        .await?;
        Ok(())
    }
}

pub(super) fn root_key() -> IcebergKey {
    IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: Vec::new(),
    }
}

pub(super) fn elapsed_now(now_ms: u64, started: Instant) -> Result<u64, CatalogError> {
    let elapsed = u64::try_from(started.elapsed().as_millis()).map_err(|_| ValidationError::Deadline)?;
    now_ms
        .checked_add(elapsed)
        .ok_or_else(|| ValidationError::Deadline.into())
}
pub(super) fn authority_key(catalog: CatalogId) -> IcebergKey {
    IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    }
}
pub(super) fn operation_bytes(operation: &ManagementOperation) -> Result<Vec<u8>, ValidationError> {
    StorageRecord::Management(Box::new(operation.clone())).encode()
}
pub(super) fn decode_authority(catalog: CatalogId, bytes: &[u8]) -> Result<CatalogAuthority, CatalogError> {
    match StorageRecord::decode(&authority_key(catalog), bytes)? {
        StorageRecord::Authority(authority) => Ok(authority),
        _ => Err(ValidationError::Record.into()),
    }
}
