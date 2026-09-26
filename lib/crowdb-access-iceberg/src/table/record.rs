use crate::{
    error::ValidationError,
    file::FileLocation,
    key::{CatalogId, FileId, NamespaceId, OperationId, TableId},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableLifecycle {
    Ready,
    Tombstone,
    Reclaiming,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableHead {
    pub catalog: CatalogId,
    pub table: TableId,
    pub namespace: NamespaceId,
    pub name: String,
    pub name_epoch: u64,
    pub lifecycle: TableLifecycle,
    pub generation: u64,
    pub metadata_file: FileId,
    pub metadata_location: FileLocation,
    pub metadata_digest: [u8; 32],
    pub format_version: u8,
    pub table_uuid: Option<uuid::Uuid>,
    pub operation_fence: u64,
    pub pending_operation: Option<OperationId>,
}

impl TableHead {
    /// # Errors
    /// Rejects invalid revisions, identity, version and unfenced tombstones.
    pub fn validate(&self) -> Result<(), ValidationError> {
        super::name_key(self.catalog, self.namespace, &self.name)?;
        if self.name_epoch == 0
            || self.generation == 0
            || self.operation_fence == 0
            || !(1..=3).contains(&self.format_version)
            || self.metadata_location.table().catalog != self.catalog
            || self.metadata_location.table().table != self.table
            || (self.format_version > 1 && self.table_uuid.is_none())
            || (self.lifecycle != TableLifecycle::Ready && self.pending_operation.is_none())
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableMappingState {
    Reserved,
    Published,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableMapping {
    pub catalog: CatalogId,
    pub namespace: NamespaceId,
    pub name: String,
    pub table: TableId,
    pub name_epoch: u64,
    pub operation: OperationId,
    pub state: TableMappingState,
}

impl TableMapping {
    /// # Errors
    /// Rejects invalid names and zero name epochs.
    pub fn validate(&self) -> Result<(), ValidationError> {
        super::name_key(self.catalog, self.namespace, &self.name)?;
        if self.name_epoch == 0 {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    #[must_use]
    pub fn resolves(&self, head: &TableHead) -> bool {
        self.state == TableMappingState::Published
            && head.lifecycle == TableLifecycle::Ready
            && self.catalog == head.catalog
            && self.namespace == head.namespace
            && self.table == head.table
            && self.name == head.name
            && self.name_epoch == head.name_epoch
    }
}
