use crate::{
    error::ValidationError,
    file::FileLocation,
    key::{CatalogScope, FileId, IcebergKey, OperationId},
};

use super::{AvroMarkCursor, ReachableKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcNode {
    pub continuation: Option<crate::operation::PayloadReference>,
    pub head: Option<crate::table::TableHead>,
    pub task: OperationId,
    pub file: FileId,
    pub location: FileLocation,
    pub digest: [u8; 32],
    pub kind: ReachableKind,
    pub cursor: AvroMarkCursor,
    pub complete: bool,
}

impl GcNode {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        self.scoped_key(CatalogScope::GcNode)
    }

    #[must_use]
    pub fn pending_key(&self) -> IcebergKey {
        self.scoped_key(CatalogScope::GcPending)
    }

    fn scoped_key(&self, scope: CatalogScope) -> IcebergKey {
        let mut suffix = self.task.as_bytes().to_vec();
        suffix.extend_from_slice(self.file.as_bytes());
        IcebergKey::Catalog {
            catalog: self.location.table().catalog,
            scope,
            suffix,
        }
    }

    /// # Errors
    /// Rejects invalid or excessive durable traversal state.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if let Some(reference) = &self.continuation {
            reference.validate()?;
            if reference.catalog != self.location.table().catalog
                || reference.operation != self.task
                || reference.length > crate::operation::PAYLOAD_PAGE_BYTES
            {
                return Err(ValidationError::Record);
            }
        }
        if let Some(head) = &self.head {
            head.validate()?;
            if self.kind != ReachableKind::Metadata
                || head.metadata_file != self.file
                || head.metadata_location != self.location
                || head.metadata_digest != self.digest
            {
                return Err(ValidationError::Record);
            }
        }
        if !self.cursor.checkpoint.is_empty() && self.cursor.checkpoint.len() != 189
            || self.cursor.record_offset > 1_000_000
            || self.kind == ReachableKind::File && !self.complete
            || self.kind == ReachableKind::Metadata && !self.cursor.checkpoint.is_empty()
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}
