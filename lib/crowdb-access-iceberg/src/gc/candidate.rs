use crate::{
    error::ValidationError,
    file::{ContentFormat, FileContent, FileKind, FileRecord, MultipartPart},
    key::{CatalogScope, IcebergKey, OperationId},
};

use super::TreeReclaimCursor;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CandidatePhase {
    Retained,
    Deleting,
    Deferred,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcCandidate {
    pub task: OperationId,
    pub generation: u64,
    pub first_seen_ms: u64,
    pub not_before_ms: u64,
    pub revision: u64,
    pub phase: CandidatePhase,
    pub completed_round: u64,
    pub file: FileRecord,
    pub part: Option<MultipartPart>,
    pub cursor: TreeReclaimCursor,
}

impl GcCandidate {
    /// # Errors
    /// Rejects an invalid part tree or synthetic location.
    pub fn part_file(part: &MultipartPart) -> Result<FileRecord, ValidationError> {
        part.validate()?;
        let location = part
            .owner
            .table
            .file(&format!("gc-parts/{}/{:05}.parquet", part.upload, part.number))?;
        let file = FileRecord {
            file: part.owner.file,
            location,
            kind: FileKind::Unbound,
            format: ContentFormat::Parquet,
            length: part.tree.length,
            digest: part.tree.digest,
            content: FileContent::Chunks {
                root: part.tree.root.clone(),
            },
            hint: None,
        };
        file.validate()?;
        Ok(file)
    }

    #[must_use]
    pub fn claim_key(&self) -> IcebergKey {
        let mut suffix = self.file.location.table().table.as_bytes().to_vec();
        suffix.extend_from_slice(self.file.file.as_bytes());
        IcebergKey::Catalog {
            catalog: self.file.location.table().catalog,
            scope: CatalogScope::GcClaim,
            suffix,
        }
    }

    #[must_use]
    pub fn key(&self) -> IcebergKey {
        let mut suffix = self.file.location.table().table.as_bytes().to_vec();
        suffix.extend_from_slice(&self.generation.to_be_bytes());
        suffix.extend_from_slice(self.file.file.as_bytes());
        IcebergKey::Catalog {
            catalog: self.file.location.table().catalog,
            scope: CatalogScope::GcCandidate,
            suffix,
        }
    }

    /// # Errors
    /// Rejects inconsistent retention, identity or deletion cursor.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.file.validate()?;
        self.cursor.validate()?;
        if self
            .part
            .as_ref()
            .is_some_and(|part| Self::part_file(part).as_ref() != Ok(&self.file))
        {
            return Err(ValidationError::Record);
        }
        if self.first_seen_ms == 0
            || self.not_before_ms < self.first_seen_ms
            || self.revision == 0
            || ((self.phase == CandidatePhase::Complete) != (self.completed_round != 0))
            || self.cursor.owner.table != self.file.location.table()
            || self.cursor.owner.file != self.file.file
            || (self.phase == CandidatePhase::Retained && self.cursor != TreeReclaimCursor::new(&self.file)?)
            || (self.phase == CandidatePhase::Complete
                && (!self.cursor.frames.is_empty() || self.cursor.pending.is_some()))
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}
