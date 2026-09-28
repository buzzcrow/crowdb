use crate::catalog::{CasOutcome, CatalogError};
use crate::error::ValidationError;
use crate::operation::{mutation_identity, PayloadReference, PayloadStore};
use crate::record::StorageRecord;

use super::{
    MultipartAdmission, MultipartAdmissionRecord, MultipartCredit, MultipartCreditAction, MultipartSession,
};

impl MultipartAdmission {
    /// Completes one durable admission journal without repeating its counter change.
    /// # Errors
    /// Rejects corrupt snapshots, foreign receipts, retired contexts and storage failures.
    pub async fn settle(&self, record: &MultipartAdmissionRecord) -> Result<bool, CatalogError> {
        record.validate()?;
        let mutation = record.pending.as_ref().ok_or(ValidationError::Record)?;
        let after = self.read_snapshot(record, &mutation.after).await?;
        let before = match &mutation.before {
            Some(reference) => Some(self.read_snapshot(record, reference).await?),
            None => None,
        };
        validate_transition(record, before.as_ref(), &after)?;
        if self.load(record.context).await?.as_ref() != Some(record) {
            return Ok(false);
        }
        let key = after.key().encode()?;
        let expected = before
            .map(|session| StorageRecord::MultipartSession(Box::new(session)).encode())
            .transpose()?;
        let value = StorageRecord::MultipartSession(Box::new(after.clone())).encode()?;
        let outcome = self
            .store
            .compare_exchange(
                &key,
                expected.as_deref(),
                &value,
                mutation_identity(&key, expected.as_deref(), &value),
            )
            .await?;
        if let CasOutcome::Conflict(observed) = outcome {
            let Some(observed) = observed else {
                return Err(ValidationError::Record.into());
            };
            let StorageRecord::MultipartSession(current) =
                StorageRecord::decode(&after.key(), &observed.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            super::super::multipart_repository::matching_request(&after, (*current).clone())?;
            if current.credit != after.credit || current.revision < after.revision {
                if self.load(record.context).await?.as_ref() != Some(record) {
                    return Ok(false);
                }
                return Err(ValidationError::Record.into());
            }
        }
        let mut next = record.clone();
        next.revision = record.revision.checked_add(1).ok_or(ValidationError::Record)?;
        next.pending = None;
        self.exchange(record, &next).await
    }

    async fn read_snapshot(
        &self,
        record: &MultipartAdmissionRecord,
        reference: &PayloadReference,
    ) -> Result<MultipartSession, CatalogError> {
        let bytes = PayloadStore::new(self.store.clone()).get(reference).await?;
        let key = crate::key::IcebergKey::Catalog {
            catalog: record.context.catalog,
            scope: crate::key::CatalogScope::MultipartSession,
            suffix: reference.operation.as_bytes().to_vec(),
        };
        let StorageRecord::MultipartSession(session) = StorageRecord::decode(&key, &bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if session.context != record.context {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok(*session)
    }
}

fn validate_transition(
    record: &MultipartAdmissionRecord,
    before: Option<&MultipartSession>,
    after: &MultipartSession,
) -> Result<(), CatalogError> {
    let mutation = record.pending.as_ref().ok_or(ValidationError::Record)?;
    if after.upload != mutation.upload || after.limits.max_staged_bytes != mutation.reservation_bytes {
        return Err(ValidationError::Record.into());
    }
    match mutation.action {
        MultipartCreditAction::Reserve => {
            super::initial(after)?;
            if before.is_some()
                || after.credit
                    != Some(MultipartCredit {
                        policy: record.policy,
                        sequence: record.revision,
                        released: false,
                    })
            {
                return Err(ValidationError::Record.into());
            }
        }
        MultipartCreditAction::Release => {
            let before = before.ok_or(ValidationError::Record)?;
            let credit = before.credit.ok_or(ValidationError::Record)?;
            if !super::terminal(before)
                || credit.released
                || credit.policy != record.policy
                || credit.sequence >= record.revision
            {
                return Err(ValidationError::Record.into());
            }
            let mut expected = before.clone();
            expected.revision = before.revision.checked_add(1).ok_or(ValidationError::Record)?;
            expected.credit = Some(MultipartCredit {
                released: true,
                ..credit
            });
            if expected != *after {
                return Err(ValidationError::Record.into());
            }
        }
    }
    Ok(())
}
