use crate::{catalog::CatalogError, error::ValidationError, record::StorageRecord};

use super::{CandidatePhase, GcCandidate, GcPhase, GcRepository, GcTask, GcTaskKind};

impl GcRepository {
    /// Selects one immutable generation-indexed deletion cursor for a physical file.
    /// # Errors
    /// Conflicting concurrent claims must be retried by reading their durable winner.
    pub async fn claim_candidate(&self, proposed: &GcCandidate) -> Result<GcCandidate, CatalogError> {
        proposed.validate()?;
        if proposed.phase != CandidatePhase::Retained || proposed.revision != 1 {
            return Err(ValidationError::Record.into());
        }
        let claim_key = proposed.claim_key();
        let selected = if let Some(value) = self.store.get(&claim_key.encode()?).await? {
            let StorageRecord::GcCandidate(selected) = StorageRecord::decode(&claim_key, &value.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            *selected
        } else {
            self.change(
                &claim_key,
                None,
                &StorageRecord::GcCandidate(Box::new(proposed.clone())),
            )
            .await?;
            proposed.clone()
        };
        if selected.file != proposed.file {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let key = selected.key();
        if let Some(value) = self.store.get(&key.encode()?).await? {
            let StorageRecord::GcCandidate(current) = StorageRecord::decode(&key, &value.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if current.file != selected.file {
                return Err(ValidationError::IdentityMismatch.into());
            }
            return Ok(*current);
        }
        self.candidate(None, &selected).await?;
        Ok(selected)
    }

    pub(super) async fn verify_claim(&self, candidate: &GcCandidate) -> Result<(), CatalogError> {
        let key = candidate.claim_key();
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::GcCandidate(claim) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if claim.key() != candidate.key() || claim.file != candidate.file {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok(())
    }

    pub(super) async fn adopt_candidate(
        &self,
        task: &GcTask,
        candidate: &GcCandidate,
    ) -> Result<(), CatalogError> {
        self.verify_claim(candidate).await?;
        let previous = self
            .task(task.context.catalog, candidate.task)
            .await?
            .ok_or(ValidationError::Record)?;
        if previous.context != task.context
            || previous.paused
            || previous.phase == GcPhase::Quarantined
            || (previous.phase == GcPhase::Complete && previous.kind != GcTaskKind::LiveTable)
            || previous
                .head
                .as_ref()
                .map_or(true, |head| head.table != candidate.file.location.table().table)
            || !matches!(
                (task.kind, previous.kind),
                (
                    GcTaskKind::RetiredCatalog,
                    GcTaskKind::PurgeTable | GcTaskKind::LiveTable
                ) | (
                    GcTaskKind::PurgeTable | GcTaskKind::LiveTable,
                    GcTaskKind::LiveTable
                )
            )
            || (task.kind == GcTaskKind::LiveTable && previous.phase != GcPhase::Complete)
            || candidate.phase == CandidatePhase::Complete
        {
            return Err(CatalogError::Busy);
        }
        let mut next = candidate.clone();
        next.task = task.identity;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        next.not_before_ms = next.not_before_ms.max(task.not_before_ms);
        self.change(
            &candidate.key(),
            Some(&StorageRecord::GcCandidate(Box::new(candidate.clone()))),
            &StorageRecord::GcCandidate(Box::new(next)),
        )
        .await
    }
}
