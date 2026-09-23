use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::key::{FileId, OperationId};
use crate::operation::PayloadReference;

use super::{AssemblyProgress, FileContent, FileDigest, FileIdentity, FileLocation, FileTree};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MultipartLimits {
    pub max_parts: u16,
    pub max_part_bytes: u64,
    pub max_file_bytes: u64,
    pub max_staged_bytes: u64,
    pub ttl_ms: u64,
}

impl MultipartLimits {
    /// # Errors
    /// Rejects missing or incoherent independent multipart limits.
    pub fn validate(self) -> Result<(), ValidationError> {
        if self.max_parts == 0
            || self.max_parts > 10_000
            || self.max_part_bytes == 0
            || self.max_part_bytes > self.max_file_bytes
            || self.max_file_bytes > self.max_staged_bytes
            || self.max_staged_bytes > u64::MAX / 8
            || self.ttl_ms == 0
            || self.ttl_ms > 7 * 24 * 60 * 60 * 1000
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultipartPhase {
    Open,
    Completing,
    Publishing,
    Published,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartCompletion {
    pub selection: PayloadReference,
    pub selected_parts: u16,
    pub progress: AssemblyProgress,
    pub candidate: Option<FileTree>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartSession {
    pub context: CatalogContext,
    pub upload: OperationId,
    pub owner: FileIdentity,
    pub location: FileLocation,
    pub principal: [u8; 32],
    pub revision: u64,
    pub created_ms: u64,
    pub expires_ms: u64,
    pub limits: MultipartLimits,
    pub phase: MultipartPhase,
    pub part_count: u16,
    pub staged_bytes: u64,
    pub completion: Option<MultipartCompletion>,
    pub published: Option<FileId>,
}

impl MultipartSession {
    /// # Errors
    /// Rejects mismatched identities, invalid bounds and incoherent durable phases.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.context.validate()?;
        self.limits.validate()?;
        if self.owner.table != self.location.table()
            || self.owner.table.catalog != self.context.catalog
            || self.revision == 0
            || self.expires_ms.checked_sub(self.created_ms) != Some(self.limits.ttl_ms)
            || self.part_count > self.limits.max_parts
            || self.staged_bytes > self.limits.max_staged_bytes
            || self.staged_bytes > u64::from(self.part_count).saturating_mul(self.limits.max_part_bytes)
            || (self.part_count == 0 && self.staged_bytes != 0)
        {
            return Err(ValidationError::Record);
        }
        if let Some(completion) = &self.completion {
            completion.validate(self)?;
        }
        let candidate = self
            .completion
            .as_ref()
            .and_then(|completion| completion.candidate.as_ref());
        let valid = match self.phase {
            MultipartPhase::Open => self.completion.is_none() && self.published.is_none(),
            MultipartPhase::Completing => {
                self.completion.is_some() && candidate.is_none() && self.published.is_none()
            }
            MultipartPhase::Publishing => candidate.is_some() && self.published.is_none(),
            MultipartPhase::Published => candidate.is_some() && self.published.is_some(),
            MultipartPhase::Aborted => self.published.is_none(),
        };
        if !valid {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

impl MultipartCompletion {
    fn validate(&self, session: &MultipartSession) -> Result<(), ValidationError> {
        self.selection.validate()?;
        let progress = &self.progress;
        if self.selection.catalog != session.context.catalog
            || self.selection.operation != session.upload
            || self.selection.length == 0
            || self.selection.digest != progress.selection
            || self.selected_parts == 0
            || self.selected_parts > session.part_count
            || progress.next_part > self.selected_parts
            || progress.completed_bytes > session.limits.max_file_bytes
            || progress.completed_bytes > session.staged_bytes
            || progress.part_offset > progress.completed_bytes
            || progress.active.is_some() != progress.part_digest.is_some()
            || progress.active.is_some() != (progress.part_offset > 0)
            || (progress.writer.is_none() && (progress.next_part != 0 || progress.completed_bytes != 0))
        {
            return Err(ValidationError::Record);
        }
        if let Some(checkpoint) = &progress.writer {
            checkpoint.root.validate()?;
            if checkpoint.root.height != 0 {
                return Err(ValidationError::Record);
            }
        }
        if let (Some(part), Some(digest)) = (&progress.active, &progress.part_digest) {
            let digest = FileDigest::restore(part.owner, digest).map_err(|_| ValidationError::Record)?;
            if part.owner.table != session.owner.table
                || part.owner.file == session.owner.file
                || part.length > session.limits.max_part_bytes
                || progress.part_offset >= part.length
                || digest.length() != progress.part_offset
            {
                return Err(ValidationError::Record);
            }
        }
        let done = progress.next_part == self.selected_parts;
        if done && (progress.part_offset != 0 || progress.active.is_some()) {
            return Err(ValidationError::Record);
        }
        if let Some(candidate) = &self.candidate {
            validate_tree(candidate)?;
            if !done || candidate.length != progress.completed_bytes {
                return Err(ValidationError::Record);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartPart {
    pub upload: OperationId,
    pub number: u16,
    pub revision: u64,
    pub owner: FileIdentity,
    pub tree: FileTree,
}

impl MultipartPart {
    /// # Errors
    /// Rejects invalid part numbers, revisions and inconsistent physical bytes.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.number == 0 || self.number > 10_000 || self.revision == 0 {
            return Err(ValidationError::Record);
        }
        validate_tree(&self.tree)
    }

    /// # Errors
    /// Rejects foreign sessions, tables, output identity reuse and independent limits.
    pub fn validate_for(&self, session: &MultipartSession) -> Result<(), ValidationError> {
        self.validate()?;
        session.validate()?;
        if self.upload != session.upload
            || self.owner.table != session.owner.table
            || self.owner.file == session.owner.file
            || self.number > session.limits.max_parts
            || self.tree.length > session.limits.max_part_bytes
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

fn validate_tree(tree: &FileTree) -> Result<(), ValidationError> {
    FileContent::Chunks {
        root: tree.root.clone(),
    }
    .validate(tree.length, &tree.digest)
}
