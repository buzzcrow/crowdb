use std::sync::Arc;

use async_trait::async_trait;

mod auxiliary;
mod fence;
mod snapshots;
pub use auxiliary::{CandidateAuxiliaryLimits, CandidateAuxiliarySummary};
pub use snapshots::{CandidateSnapshotLimits, CandidateSnapshotSummary};

use super::{PriorManifestSource, TableCreateOperation};
use crate::{
    catalog::{CatalogContext, CatalogStore},
    file::{AvroBlocks, AvroLimits, FileBlockStore, FileKind, FileLocation, FileRecord, FileRepository},
    manifest::{
        ManifestContext, ManifestMetadata, ManifestVersion, SnapshotFileSource, SnapshotManifestError,
        SnapshotManifestSource, SnapshotValidationError,
    },
    table::TableMetadataDocument,
};
use fence::CandidateFence;

/// Immutable file resolution for one candidate and its still-selected input generation.
pub struct CandidateFileSource {
    files: FileRepository,
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    context: CatalogContext,
    fence: CandidateFence,
    candidate: Arc<TableMetadataDocument>,
    framing: AvroLimits,
}

impl CandidateFileSource {
    /// Binds file resolution to a candidate successor without granting publication authority.
    /// # Errors
    /// Rejects mismatched table, namespace, lifecycle and generation fences.
    pub fn new(
        store: Arc<dyn CatalogStore>,
        blocks: Arc<dyn FileBlockStore>,
        context: CatalogContext,
        prior: Arc<PriorManifestSource>,
        candidate: Arc<TableMetadataDocument>,
        framing: AvroLimits,
    ) -> Result<Self, SnapshotValidationError> {
        let previous = &prior.selected().head;
        let next = candidate.selected_head();
        if previous.catalog != context.catalog
            || previous.catalog != next.catalog
            || previous.table != next.table
            || previous.namespace != next.namespace
            || previous.name != next.name
            || previous.name_epoch != next.name_epoch
            || previous.lifecycle != next.lifecycle
            || previous.generation.checked_add(1) != Some(next.generation)
            || previous.operation_fence > next.operation_fence
            || previous.metadata_file == next.metadata_file
            || previous.metadata_location == next.metadata_location
            || previous
                .table_uuid
                .is_some_and(|uuid| next.table_uuid != Some(uuid))
        {
            return Err(SnapshotValidationError::Binding);
        }
        Ok(Self {
            files: FileRepository::new(store.clone()),
            store,
            blocks,
            context,
            fence: CandidateFence::Generation(prior),
            candidate,
            framing,
        })
    }

    /// Binds initial file checks to an exact durable reservation, without inventing a prior head.
    /// # Errors
    /// Rejects a non-reserved operation or a candidate not frozen by that operation.
    pub fn for_creation(
        store: Arc<dyn CatalogStore>,
        blocks: Arc<dyn FileBlockStore>,
        operation: &TableCreateOperation,
        candidate: Arc<TableMetadataDocument>,
        framing: AvroLimits,
    ) -> Result<Self, SnapshotValidationError> {
        operation.validate().map_err(file_error)?;
        if operation.phase != super::TableCreatePhase::Reserved
            || candidate.selected_head() != &operation.candidate
        {
            return Err(SnapshotValidationError::Binding);
        }
        Ok(Self {
            files: FileRepository::new(store.clone()),
            store,
            blocks,
            context: operation.context,
            fence: CandidateFence::Creation(Box::new(operation.clone())),
            candidate,
            framing,
        })
    }

    async fn ensure_current(&self) -> Result<(), SnapshotValidationError> {
        self.fence
            .check(self.store.clone(), self.context)
            .await
            .map_err(file_error)
    }

    async fn load(&self, location: &FileLocation) -> Result<FileRecord, SnapshotValidationError> {
        self.ensure_current().await?;
        let head = self.candidate.selected_head();
        if location.table().catalog != head.catalog || location.table().table != head.table {
            return Err(SnapshotValidationError::Binding);
        }
        let record = self
            .files
            .load(self.context, location)
            .await
            .map_err(file_error)?
            .ok_or(SnapshotValidationError::Unavailable)?;
        self.ensure_current().await?;
        Ok(record)
    }
}

#[async_trait]
impl SnapshotFileSource for CandidateFileSource {
    async fn resolve(&self, location: &FileLocation) -> Result<FileRecord, SnapshotValidationError> {
        self.load(location).await
    }
}

#[async_trait]
impl SnapshotManifestSource for CandidateFileSource {
    async fn resolve(
        &self,
        location: &FileLocation,
    ) -> Result<(FileRecord, ManifestContext), SnapshotManifestError> {
        if let Some(prior) = self.fence.prior().filter(|prior| prior.contains(location)) {
            return prior.resolve(location).await;
        }
        let record = self
            .load(location)
            .await
            .map_err(manifest_error)?
            .bind_kind(FileKind::Manifest)
            .map_err(manifest_error)?;
        let reader = AvroBlocks::open(self.blocks.clone(), record.clone(), self.framing)
            .await
            .map_err(manifest_error)?;
        let metadata = ManifestMetadata::parse(reader.metadata()).map_err(manifest_error)?;
        let version = match metadata.version {
            ManifestVersion::V1 => 1,
            ManifestVersion::V2 => 2,
            ManifestVersion::V3 => 3,
        };
        if version > self.candidate.selected_head().format_version {
            return Err(SnapshotManifestError::Unavailable);
        }
        let schema: serde_json::Value =
            serde_json::from_slice(metadata.schema_json).map_err(manifest_error)?;
        let schema_id = metadata
            .schema_id
            .or_else(|| {
                schema
                    .get("schema-id")
                    .and_then(serde_json::Value::as_i64)
                    .and_then(|value| i32::try_from(value).ok())
            })
            .unwrap_or(0);
        let spec_id = metadata.partition_spec_id.unwrap_or(0);
        let context = self
            .candidate
            .manifest_context_with_retained_history(schema_id, spec_id, 1_000_000)
            .map_err(manifest_error)?;
        context
            .validate_metadata(metadata, spec_id)
            .map_err(manifest_error)?;
        self.ensure_current().await.map_err(manifest_error)?;
        Ok((record, context))
    }
}

fn file_error(error: impl std::error::Error + Send + Sync + 'static) -> SnapshotValidationError {
    SnapshotValidationError::Source(Box::new(error))
}

fn manifest_error(error: impl std::error::Error + Send + Sync + 'static) -> SnapshotManifestError {
    SnapshotManifestError::Source(Box::new(error))
}
