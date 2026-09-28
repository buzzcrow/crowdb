use crate::file::{ChunkEntry, FileTreeWriter, MultipartPhase};

use super::{CatalogError, GcCandidate, GcTask, GcWorkError, GcWorker, StorageRecord, ValidationError};

impl GcWorker {
    pub(super) async fn assembly_reclaimed(
        &self,
        session: &crate::file::MultipartSession,
    ) -> Result<bool, GcWorkError> {
        let file = GcCandidate::assembly_file(session)?;
        let mut suffix = session.owner.table.table.as_bytes().to_vec();
        suffix.extend_from_slice(file.file.as_bytes());
        let key = super::IcebergKey::Catalog {
            catalog: session.context.catalog,
            scope: crate::key::CatalogScope::GcClaim,
            suffix,
        };
        let Some(value) = self
            .repository
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?
        else {
            return Ok(false);
        };
        let StorageRecord::GcCandidate(claim) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let key = claim.key();
        let Some(value) = self
            .repository
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?
        else {
            return Ok(false);
        };
        let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        self.repository.verify_claim(&candidate).await?;
        Ok(candidate.file == file
            && candidate.assembly.is_some()
            && candidate.phase == super::CandidatePhase::Complete)
    }

    pub(super) async fn verify_assembly(
        &self,
        task: &GcTask,
        candidate: &GcCandidate,
        now_ms: u64,
    ) -> Result<(), GcWorkError> {
        let Some(session) = &candidate.assembly else {
            return Ok(());
        };
        let key = session.key();
        let value = self
            .repository
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::MultipartSession(current) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if current.context != session.context
            || current.owner != session.owner
            || current.phase != session.phase
            || current.completion != session.completion
            || current.published != session.published
        {
            return Err(CatalogError::Conflict.into());
        }
        if !self
            .repository
            .session_is_abandoned(task, &current, now_ms)
            .await?
        {
            return Err(CatalogError::Busy.into());
        }
        Ok(())
    }

    pub(super) async fn advance_assembly(
        &self,
        candidate: &GcCandidate,
        next: &mut GcCandidate,
    ) -> Result<bool, GcWorkError> {
        let Some(session) = &candidate.assembly else {
            return Ok(false);
        };
        if candidate.next_root == u16::MAX {
            return Ok(false);
        }
        let completion = session.completion.as_ref().ok_or(ValidationError::Record)?;
        let checkpoint = completion
            .progress
            .writer
            .as_ref()
            .ok_or(ValidationError::Record)?;
        let roots = if session.phase == MultipartPhase::Published {
            Vec::new()
        } else if let Some(tree) = &completion.candidate {
            tree.root
                .iter()
                .map(|root| ChunkEntry {
                    root: root.clone(),
                    length: tree.length,
                })
                .collect()
        } else {
            if checkpoint.root.logical_length > u64::from(self.limits.step_bytes) {
                return Err(super::FileIoError::Bounds.into());
            }
            FileTreeWriter::checkpoint_roots(self.blocks.clone(), session.owner, checkpoint).await?
        };
        if usize::from(candidate.next_root) > roots.len() {
            return Err(ValidationError::Record.into());
        }
        if let Some(entry) = roots.get(usize::from(candidate.next_root)) {
            next.cursor.frames.push(super::super::ReclaimFrame {
                root: entry.root.clone(),
                length: entry.length,
                next_child: 0,
            });
            next.next_root += 1;
        } else {
            next.cursor.pending = Some(checkpoint.root.clone());
            next.next_root = u16::MAX;
        }
        Ok(true)
    }
}
