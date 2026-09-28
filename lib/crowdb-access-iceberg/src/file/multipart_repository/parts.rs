use crate::catalog::{check_context, CasOutcome, CatalogError};
use crate::error::ValidationError;
use crate::file::{MultipartPart, MultipartPartMutation, MultipartPhase, MultipartSession};
use crate::key::{CatalogScope, IcebergKey};
use crate::operation::mutation_identity;
use crate::record::StorageRecord;

use super::{check_live, increment, MultipartRepository};

impl MultipartRepository {
    /// Reads a committed part only while the supplied session snapshot stays current.
    /// # Errors
    /// Rejects unresolved mutations, stale snapshots, invalid numbers and corrupt parts.
    pub async fn part(
        &self,
        session: &MultipartSession,
        number: u16,
    ) -> Result<Option<MultipartPart>, CatalogError> {
        session.validate()?;
        if number == 0 || number > session.limits.max_parts {
            return Err(ValidationError::Record.into());
        }
        if session.pending.is_some() {
            return Err(CatalogError::Busy);
        }
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Err(CatalogError::Busy);
        }
        let mut suffix = session.upload.as_bytes().to_vec();
        suffix.extend_from_slice(&number.to_be_bytes());
        let key = IcebergKey::Catalog {
            catalog: session.context.catalog,
            scope: CatalogScope::MultipartPart,
            suffix,
        };
        let part = self.read_part(&key).await?;
        if let Some(part) = &part {
            part.validate_for(session)?;
        }
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Err(CatalogError::Busy);
        }
        Ok(part)
    }

    /// Reserves the new count and byte total before making a part visible.
    /// # Errors
    /// Rejects stale revisions, expired sessions, exhausted limits and pending mutations.
    pub async fn reserve_part(
        &self,
        session: &MultipartSession,
        part: &MultipartPart,
        now_ms: u64,
    ) -> Result<bool, CatalogError> {
        check_live(session, now_ms)?;
        let mut after = part.clone();
        after.modified_ms = now_ms;
        after.validate_for(session)?;
        if session.phase != MultipartPhase::Open {
            return Err(CatalogError::Conflict);
        }
        if session.pending.is_some() {
            return Err(CatalogError::Busy);
        }
        session.revision.checked_add(2).ok_or(ValidationError::Record)?;
        check_context(self.store.as_ref(), session.context).await?;
        let before = self.read_part(&part.key()).await?;
        if let Some(before) = &before {
            before.validate_for(session)?;
        }
        let mut next = increment(session)?;
        next.part_count = session
            .part_count
            .checked_add(u16::from(before.is_none()))
            .ok_or(ValidationError::Record)?;
        next.staged_bytes = session
            .staged_bytes
            .checked_sub(before.as_ref().map_or(0, |part| part.tree.length))
            .and_then(|bytes| bytes.checked_add(part.tree.length))
            .ok_or(ValidationError::Record)?;
        next.pending = Some(MultipartPartMutation { before, after });
        self.exchange(session, &next).await
    }

    /// Helps one durable part mutation and then clears its session fence.
    /// # Errors
    /// Rejects corrupt mutations, retired contexts and uncertain storage writes.
    pub async fn settle_part(&self, session: &MultipartSession) -> Result<bool, CatalogError> {
        session.validate()?;
        let pending = session.pending.as_ref().ok_or(ValidationError::Record)?;
        let mut next = increment(session)?;
        next.pending = None;
        let key = pending.after.key().encode()?;
        let expected = pending.before.as_ref().map(encode_part).transpose()?;
        let value = encode_part(&pending.after)?;
        check_context(self.store.as_ref(), session.context).await?;
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Ok(false);
        }
        let outcome = self
            .store
            .compare_exchange(
                &key,
                expected.as_deref(),
                &value,
                mutation_identity(&key, expected.as_deref(), &value),
            )
            .await?;
        match outcome {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(existing)) if existing.bytes == value => {}
            CasOutcome::Conflict(_) => {
                if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
                    return Ok(false);
                }
                return Err(ValidationError::Record.into());
            }
        }
        self.exchange(session, &next).await
    }

    async fn read_part(&self, key: &IcebergKey) -> Result<Option<MultipartPart>, CatalogError> {
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::MultipartPart(part) = StorageRecord::decode(key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        Ok(Some(*part))
    }
}

fn encode_part(part: &MultipartPart) -> Result<Vec<u8>, ValidationError> {
    StorageRecord::MultipartPart(Box::new(part.clone())).encode()
}
