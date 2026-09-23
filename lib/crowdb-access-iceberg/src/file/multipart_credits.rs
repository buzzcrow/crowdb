use std::sync::Arc;

use crate::catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore};
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, OperationId};
use crate::operation::{mutation_identity, PayloadStore};
use crate::record::StorageRecord;

use super::{
    MultipartAdmissionLimits, MultipartAdmissionRecord, MultipartCredit, MultipartCreditAction,
    MultipartCreditMutation, MultipartPhase, MultipartRepository, MultipartSession,
};

mod settle;

pub struct MultipartAdmission {
    store: Arc<dyn CatalogStore>,
    sessions: MultipartRepository,
}

impl MultipartAdmission {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self {
            sessions: MultipartRepository::new(store.clone()),
            store,
        }
    }

    /// Installs immutable catalog-wide admission limits or checks the existing policy.
    /// # Errors
    /// Rejects invalid bounds, conflicting policy, retired contexts and storage failures.
    pub async fn initialize(
        &self,
        context: CatalogContext,
        limits: MultipartAdmissionLimits,
    ) -> Result<MultipartAdmissionRecord, CatalogError> {
        context.validate()?;
        limits.validate()?;
        if let Some(record) = self.load(context).await? {
            return if record.limits == limits {
                Ok(record)
            } else {
                Err(CatalogError::Conflict)
            };
        }
        let record = MultipartAdmissionRecord {
            context,
            policy: OperationId::random(),
            revision: 1,
            limits,
            sessions: 0,
            reserved_bytes: 0,
            pending: None,
        };
        let key = record.key().encode()?;
        let bytes = encode(&record)?;
        check_context(self.store.as_ref(), context).await?;
        self.store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?;
        let actual = self.load(context).await?.ok_or(ValidationError::Record)?;
        if actual.limits != limits {
            return Err(CatalogError::Conflict);
        }
        Ok(actual)
    }

    /// # Errors
    /// Rejects retired contexts and corrupt or foreign policy records.
    pub async fn load(
        &self,
        context: CatalogContext,
    ) -> Result<Option<MultipartAdmissionRecord>, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::MultipartAdmission,
            suffix: Vec::new(),
        };
        let value = self.store.get(&key.encode()?).await?;
        let record = value
            .map(|value| {
                let StorageRecord::MultipartAdmission(record) = StorageRecord::decode(&key, &value.bytes)?
                else {
                    return Err(ValidationError::Record);
                };
                if record.context != context {
                    return Err(ValidationError::IdentityMismatch);
                }
                Ok(*record)
            })
            .transpose()?;
        check_context(self.store.as_ref(), context).await?;
        Ok(record)
    }

    /// Reserves the session's complete staged-byte ceiling before creating its authority.
    /// # Errors
    /// Rejects stale or occupied policies, expired requests, exhausted credits and identity reuse.
    pub async fn reserve(
        &self,
        record: &MultipartAdmissionRecord,
        session: &MultipartSession,
        now_ms: u64,
    ) -> Result<bool, CatalogError> {
        ready(record)?;
        initial(session)?;
        if session.context != record.context || session.credit.is_some() {
            return Err(ValidationError::IdentityMismatch.into());
        }
        if let Some(existing) = self.sessions.load(session.context, session.upload).await? {
            let existing = super::multipart_repository::matching_request(session, existing)?;
            return if existing
                .credit
                .is_some_and(|credit| credit.policy == record.policy)
            {
                Ok(true)
            } else {
                Err(CatalogError::Conflict)
            };
        }
        if now_ms < session.created_ms || now_ms >= session.expires_ms {
            return Err(CatalogError::Conflict);
        }
        let mut after = session.clone();
        after.credit = Some(MultipartCredit {
            policy: record.policy,
            sequence: record.revision + 1,
            released: false,
        });
        let mutation = MultipartCreditMutation {
            action: MultipartCreditAction::Reserve,
            upload: session.upload,
            reservation_bytes: session.limits.max_staged_bytes,
            before: None,
            after: self.snapshot(&after).await?,
        };
        let next = record.proposed(mutation)?;
        if !self.exchange(record, &next).await? {
            return Ok(false);
        }
        self.settle(&next).await
    }

    /// Returns credits only for a terminal session, retaining a durable released receipt.
    /// # Errors
    /// Rejects live sessions, foreign receipts, occupied journals and storage failures.
    pub async fn release(
        &self,
        record: &MultipartAdmissionRecord,
        session: &MultipartSession,
    ) -> Result<bool, CatalogError> {
        ready(record)?;
        session.validate()?;
        let credit = session.credit.ok_or(ValidationError::Record)?;
        if session.context != record.context
            || credit.policy != record.policy
            || credit.sequence > record.revision
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        if !terminal(session) {
            return Err(CatalogError::Conflict);
        }
        let Some(current) = self.sessions.load(session.context, session.upload).await? else {
            return Err(ValidationError::Record.into());
        };
        if current.credit.is_some_and(|value| value.released) {
            return Ok(current.credit
                == Some(MultipartCredit {
                    released: true,
                    ..credit
                }));
        }
        if current != *session {
            return Ok(false);
        }
        let mut after = session.clone();
        after.revision = session.revision.checked_add(1).ok_or(ValidationError::Record)?;
        after.credit = Some(MultipartCredit {
            released: true,
            ..credit
        });
        let mutation = MultipartCreditMutation {
            action: MultipartCreditAction::Release,
            upload: session.upload,
            reservation_bytes: session.limits.max_staged_bytes,
            before: Some(self.snapshot(session).await?),
            after: self.snapshot(&after).await?,
        };
        let next = record.proposed(mutation)?;
        if !self.exchange(record, &next).await? {
            return Ok(false);
        }
        self.settle(&next).await
    }

    async fn snapshot(
        &self,
        session: &MultipartSession,
    ) -> Result<crate::operation::PayloadReference, CatalogError> {
        let bytes = StorageRecord::MultipartSession(Box::new(session.clone())).encode()?;
        PayloadStore::new(self.store.clone())
            .put(session.context.catalog, session.upload, &bytes)
            .await
    }

    async fn exchange(
        &self,
        before: &MultipartAdmissionRecord,
        after: &MultipartAdmissionRecord,
    ) -> Result<bool, CatalogError> {
        let key = before.key().encode()?;
        let expected = encode(before)?;
        let value = encode(after)?;
        check_context(self.store.as_ref(), before.context).await?;
        let outcome = self
            .store
            .compare_exchange(
                &key,
                Some(&expected),
                &value,
                mutation_identity(&key, Some(&expected), &value),
            )
            .await?;
        check_context(self.store.as_ref(), before.context).await?;
        Ok(matches!(outcome, CasOutcome::Applied(_)))
    }
}

fn ready(record: &MultipartAdmissionRecord) -> Result<(), CatalogError> {
    record.validate()?;
    if record.pending.is_some() {
        return Err(CatalogError::Busy);
    }
    record.revision.checked_add(2).ok_or(ValidationError::Record)?;
    Ok(())
}

fn initial(session: &MultipartSession) -> Result<(), CatalogError> {
    session.validate()?;
    if session.phase != MultipartPhase::Open
        || session.revision != 1
        || session.part_count != 0
        || session.pending.is_some()
    {
        return Err(ValidationError::Record.into());
    }
    Ok(())
}

fn terminal(session: &MultipartSession) -> bool {
    matches!(
        session.phase,
        MultipartPhase::Published | MultipartPhase::Aborted | MultipartPhase::Conflicted
    )
}

fn encode(record: &MultipartAdmissionRecord) -> Result<Vec<u8>, ValidationError> {
    StorageRecord::MultipartAdmission(Box::new(record.clone())).encode()
}
