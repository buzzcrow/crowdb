use crate::error::ValidationError;
use crate::key::SystemScope;
use crate::operation::{ledger_key, ManagementAction, ManagementOperation, ManagementPhase};
use crate::record::StorageRecord;

use super::repository::{authority_key, decode_authority, elapsed_now, operation_bytes, root_key};
use super::{
    ActiveCatalogRecord, CatalogAuthority, CatalogContext, CatalogError, CatalogRepository, ClearTransition,
    RootState,
};

impl CatalogRepository {
    pub(super) async fn resume(
        &self,
        operation: ManagementOperation,
        now_ms: u64,
    ) -> Result<Option<CatalogAuthority>, CatalogError> {
        let started = std::time::Instant::now();
        if operation.terminal() {
            self.audit(&operation, now_ms).await?;
            if operation.phase == ManagementPhase::Conflict {
                return Err(CatalogError::Conflict);
            }
            return Ok(Some(decode_authority(
                operation.candidate,
                &operation.result_authority,
            )?));
        }
        let current = self.root().await?;
        let owned = current
            .as_ref()
            .is_some_and(|(root, _)| root.operation == operation.id());
        if !owned {
            if operation.phase == ManagementPhase::Published {
                return Err(ValidationError::Record.into());
            }
            let expected = if operation.original_root.is_empty() {
                None
            } else {
                Some(operation.original_root.as_slice())
            };
            if current.as_ref().map(|(_, bytes)| bytes.as_slice()) != expected {
                self.phase(&operation, ManagementPhase::Conflict).await?;
                return Ok(None);
            }
            let claimed = Self::claimed_root(&operation)?;
            self.cas(&root_key(), expected, &StorageRecord::Active(claimed).encode()?)
                .await?;
            return Ok(None);
        }
        let (root, bytes) = current.ok_or(ValidationError::Record)?;
        if operation.phase == ManagementPhase::Published {
            if root.state == RootState::Ready {
                let mut completed = operation.clone();
                completed.phase = ManagementPhase::Complete;
                self.audit(&completed, now_ms).await?;
                self.phase(&operation, ManagementPhase::Complete).await?;
            } else {
                let ready = ActiveCatalogRecord {
                    state: RootState::Ready,
                    ..root
                };
                self.cas(&root_key(), Some(&bytes), &StorageRecord::Active(ready).encode()?)
                    .await?;
            }
            return Ok(None);
        }
        match operation.request.action {
            ManagementAction::Initialize | ManagementAction::Rename | ManagementAction::Activate => {
                self.publish_authority(&operation).await?;
                self.phase(&operation, ManagementPhase::Published).await?;
            }
            ManagementAction::Clear => {
                self.advance_clear(&operation, root, &bytes, elapsed_now(now_ms, started)?)
                    .await?;
            }
        }
        Ok(None)
    }

    fn claimed_root(operation: &ManagementOperation) -> Result<ActiveCatalogRecord, CatalogError> {
        let context = if operation.request.action == ManagementAction::Initialize {
            CatalogContext {
                catalog: operation.candidate,
                activation_epoch: 1,
            }
        } else {
            let StorageRecord::Active(root) = StorageRecord::decode(&root_key(), &operation.original_root)?
            else {
                return Err(ValidationError::Record.into());
            };
            if operation.request.action == ManagementAction::Clear {
                root.context.replacement(operation.candidate)?;
            }
            root.context
        };
        Ok(ActiveCatalogRecord {
            context,
            operation: operation.id(),
            state: if operation.request.action == ManagementAction::Initialize {
                RootState::Initializing
            } else {
                RootState::Fencing
            },
        })
    }

    async fn advance_clear(
        &self,
        operation: &ManagementOperation,
        root: ActiveCatalogRecord,
        bytes: &[u8],
        now_ms: u64,
    ) -> Result<(), CatalogError> {
        match root.state {
            RootState::Fencing => {
                let transition = ClearTransition::new(
                    operation.id(),
                    root.context,
                    operation.candidate,
                    now_ms,
                    operation.bounds,
                )?;
                self.cas(
                    &root_key(),
                    Some(bytes),
                    &StorageRecord::Active(ActiveCatalogRecord {
                        state: RootState::Maintenance(transition),
                        ..root
                    })
                    .encode()?,
                )
                .await?;
            }
            RootState::Maintenance(transition) => {
                self.publish_authority(operation).await?;
                self.cas(
                    &root_key(),
                    Some(bytes),
                    &StorageRecord::Active(ActiveCatalogRecord {
                        context: transition.replacement,
                        state: RootState::Published(transition),
                        ..root
                    })
                    .encode()?,
                )
                .await?;
            }
            RootState::Published(transition) => {
                if !transition.grace_elapsed(now_ms)? {
                    return Err(CatalogError::Busy);
                }
                let mut retired =
                    decode_authority(transition.previous.catalog, &operation.original_authority)?;
                retired.lifecycle = super::CatalogLifecycle::Retired;
                let bytes = StorageRecord::Authority(retired).encode()?;
                let key = authority_key(transition.previous.catalog);
                if !self
                    .cas(&key, Some(&operation.original_authority), &bytes)
                    .await?
                {
                    let actual = self
                        .store
                        .get(&key.encode()?)
                        .await?
                        .ok_or(ValidationError::Record)?;
                    if actual.bytes != bytes {
                        return Err(ValidationError::Record.into());
                    }
                }
                let mut published = operation.clone();
                published.phase = ManagementPhase::Published;
                published.publication_proof = StorageRecord::Active(root).encode()?;
                published.grace_completed_ms = now_ms;
                self.cas(
                    &ledger_key(SystemScope::ManagementOperation, operation.id())?,
                    Some(&operation_bytes(operation)?),
                    &operation_bytes(&published)?,
                )
                .await?;
            }
            _ => return Err(ValidationError::Record.into()),
        }
        Ok(())
    }

    async fn publish_authority(&self, operation: &ManagementOperation) -> Result<(), CatalogError> {
        let key = authority_key(operation.candidate);
        let expected = if matches!(
            operation.request.action,
            ManagementAction::Rename | ManagementAction::Activate
        ) {
            Some(operation.original_authority.as_slice())
        } else {
            None
        };
        if self.cas(&key, expected, &operation.result_authority).await? {
            return Ok(());
        }
        let actual = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        if actual.bytes != operation.result_authority {
            return Err(ValidationError::Record.into());
        }
        Ok(())
    }

    async fn audit(&self, operation: &ManagementOperation, now_ms: u64) -> Result<(), CatalogError> {
        let key = ledger_key(SystemScope::Audit, operation.id())?;
        let bytes = operation_bytes(operation)?;
        let old = self.store.get(&key.encode()?).await?;
        if let Some(value) = &old {
            if value.bytes == bytes {
                return Ok(());
            }
            let StorageRecord::Management(previous) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if previous.id() == operation.id() || now_ms <= previous.retained_until_ms {
                return Err(CatalogError::Busy);
            }
        }
        if !self
            .cas(&key, old.as_ref().map(|value| value.bytes.as_slice()), &bytes)
            .await?
        {
            return Err(CatalogError::Busy);
        }
        Ok(())
    }
}
