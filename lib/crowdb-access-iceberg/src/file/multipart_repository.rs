use std::sync::Arc;

use crate::catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore};
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, OperationId};
use crate::operation::mutation_identity;
use crate::record::StorageRecord;

use super::{MultipartPhase, MultipartSession};

mod parts;

pub struct MultipartRepository {
    store: Arc<dyn CatalogStore>,
}

impl MultipartRepository {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// Persists a session after the caller has reserved global admission credits.
    /// # Errors
    /// Rejects noninitial sessions, expired admission and conflicting upload identities.
    pub async fn begin(
        &self,
        session: &MultipartSession,
        now_ms: u64,
    ) -> Result<MultipartSession, CatalogError> {
        session.validate()?;
        if session.phase != MultipartPhase::Open
            || session.revision != 1
            || session.part_count != 0
            || session.pending.is_some()
        {
            return Err(ValidationError::Record.into());
        }
        check_context(self.store.as_ref(), session.context).await?;
        if let Some(existing) = self.load(session.context, session.upload).await? {
            return matching_request(session, existing);
        }
        check_live(session, now_ms)?;
        let key = session.key().encode()?;
        let value = encode(session)?;
        let outcome = self
            .store
            .compare_exchange(&key, None, &value, mutation_identity(&key, None, &value))
            .await?;
        check_context(self.store.as_ref(), session.context).await?;
        match outcome {
            CasOutcome::Applied(_) => Ok(session.clone()),
            CasOutcome::Conflict(Some(value)) => {
                matching_request(session, decode(&session.key(), &value.bytes)?)
            }
            CasOutcome::Conflict(None) => Err(CatalogError::Busy),
        }
    }

    /// # Errors
    /// Rejects retired contexts and corrupt or foreign session authorities.
    pub async fn load(
        &self,
        context: CatalogContext,
        upload: OperationId,
    ) -> Result<Option<MultipartSession>, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::MultipartSession,
            suffix: upload.as_bytes().to_vec(),
        };
        let session = self
            .store
            .get(&key.encode()?)
            .await?
            .map(|value| decode(&key, &value.bytes))
            .transpose()?;
        if session.as_ref().is_some_and(|session| session.context != context) {
            return Err(ValidationError::IdentityMismatch.into());
        }
        check_context(self.store.as_ref(), context).await?;
        Ok(session)
    }

    /// Logically aborts without deleting any parts or checkpoint evidence.
    /// # Errors
    /// Rejects unresolved part mutations and publication that already won its fence.
    pub async fn abort(&self, session: &MultipartSession) -> Result<bool, CatalogError> {
        session.validate()?;
        if session.pending.is_some() {
            return Err(CatalogError::Busy);
        }
        if !matches!(session.phase, MultipartPhase::Open | MultipartPhase::Completing) {
            return Err(CatalogError::Conflict);
        }
        let mut next = increment(session)?;
        next.phase = MultipartPhase::Aborted;
        self.exchange(session, &next).await
    }

    async fn exchange(
        &self,
        previous: &MultipartSession,
        next: &MultipartSession,
    ) -> Result<bool, CatalogError> {
        let key = previous.key().encode()?;
        let expected = encode(previous)?;
        let value = encode(next)?;
        check_context(self.store.as_ref(), previous.context).await?;
        let outcome = self
            .store
            .compare_exchange(
                &key,
                Some(&expected),
                &value,
                mutation_identity(&key, Some(&expected), &value),
            )
            .await?;
        check_context(self.store.as_ref(), previous.context).await?;
        Ok(matches!(outcome, CasOutcome::Applied(_)))
    }
}

fn check_live(session: &MultipartSession, now_ms: u64) -> Result<(), CatalogError> {
    if now_ms < session.created_ms || now_ms >= session.expires_ms {
        return Err(CatalogError::Conflict);
    }
    Ok(())
}

fn increment(session: &MultipartSession) -> Result<MultipartSession, CatalogError> {
    let mut next = session.clone();
    next.revision = session.revision.checked_add(1).ok_or(ValidationError::Record)?;
    Ok(next)
}

fn matching_request(
    request: &MultipartSession,
    existing: MultipartSession,
) -> Result<MultipartSession, CatalogError> {
    if request.context != existing.context
        || request.upload != existing.upload
        || request.owner != existing.owner
        || request.location != existing.location
        || request.principal != existing.principal
        || request.created_ms != existing.created_ms
        || request.expires_ms != existing.expires_ms
        || request.limits != existing.limits
    {
        return Err(CatalogError::Conflict);
    }
    Ok(existing)
}

fn encode(session: &MultipartSession) -> Result<Vec<u8>, ValidationError> {
    StorageRecord::MultipartSession(Box::new(session.clone())).encode()
}

fn decode(key: &IcebergKey, bytes: &[u8]) -> Result<MultipartSession, ValidationError> {
    let StorageRecord::MultipartSession(session) = StorageRecord::decode(key, bytes)? else {
        return Err(ValidationError::Record);
    };
    Ok(*session)
}
