use crate::{
    error::ValidationError,
    file::{ContentFormat, FileContent, FileKind, FileRecord, MultipartPart, MultipartSession},
    key::{CatalogScope, FileId, IcebergKey, OperationId},
};

use super::TreeReclaimCursor;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CandidatePhase {
    Retained,
    Deleting,
    Deferred,
    Complete,
    Sealing,
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
    pub assembly: Option<Box<MultipartSession>>,
    pub next_root: u16,
    pub cursor: TreeReclaimCursor,
}

impl GcCandidate {
    pub(crate) fn assembly_claim_key(&self) -> Option<IcebergKey> {
        let session = self.assembly.as_ref()?;
        let mut suffix = session.owner.table.table.as_bytes().to_vec();
        suffix.extend_from_slice(session.owner.file.as_bytes());
        Some(IcebergKey::Catalog {
            catalog: session.context.catalog,
            scope: CatalogScope::GcAssemblyClaim,
            suffix,
        })
    }

    pub(crate) fn assembly_file(session: &MultipartSession) -> Result<FileRecord, ValidationError> {
        session.validate()?;
        let checkpoint = session
            .completion
            .as_ref()
            .and_then(|completion| completion.progress.writer.as_ref())
            .ok_or(ValidationError::Record)?;
        let file = FileRecord {
            file: FileId::from_bytes(&checkpoint.root.digest[..16])?,
            location: session
                .owner
                .table
                .file(&format!("gc-checkpoints/{}.parquet", session.upload))?,
            kind: FileKind::Unbound,
            format: ContentFormat::Parquet,
            length: checkpoint.root.logical_length,
            digest: checkpoint.root.digest,
            content: FileContent::Chunks {
                root: Some(checkpoint.root.clone()),
            },
            hint: None,
        };
        file.validate()?;
        Ok(file)
    }

    pub(crate) fn initial_cursor(&self) -> Result<TreeReclaimCursor, ValidationError> {
        if let Some(session) = &self.assembly {
            Ok(TreeReclaimCursor {
                owner: session.owner,
                frames: Vec::new(),
                pending: None,
            })
        } else {
            TreeReclaimCursor::new(&self.file)
        }
    }

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
        if let Some(session) = &self.assembly {
            if self.part.is_some()
                || Self::assembly_file(session)? != self.file
                || self.cursor.owner != session.owner
                || !matches!(
                    session.phase,
                    crate::file::MultipartPhase::Published
                        | crate::file::MultipartPhase::Aborted
                        | crate::file::MultipartPhase::Conflicted
                )
                || (self.next_root > 9 * 255 && self.next_root != u16::MAX)
                || (self.phase == CandidatePhase::Complete && self.next_root != u16::MAX)
            {
                return Err(ValidationError::Record);
            }
        } else if self.next_root != 0 || self.cursor.owner.file != self.file.file {
            return Err(ValidationError::Record);
        }
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
            || (self.phase == CandidatePhase::Retained
                && (self.cursor != self.initial_cursor()? || self.next_root != 0))
            || (self.phase == CandidatePhase::Sealing
                && (self.cursor != self.initial_cursor()? || self.next_root != 0))
            || (self.phase == CandidatePhase::Complete
                && (!self.cursor.frames.is_empty() || self.cursor.pending.is_some()))
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}
