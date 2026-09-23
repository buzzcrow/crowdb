//! Streaming manifest semantics, separate from physical file content identity.

mod context;
mod deletion_vectors;
mod entry;
mod inheritance;
mod list;
mod list_reader;
mod list_selection;
mod metadata;
mod parquet;
mod reader;
mod snapshot_identity;
mod snapshot_reader;
mod snapshot_rows;
mod summary;
pub use context::{
    ManifestContext, ManifestContextError, PartitionField, PartitionTransform, PrimitiveType, SchemaDefault,
    SchemaField,
};
pub use deletion_vectors::{
    SnapshotDvError, SnapshotDvLimits, SnapshotDvScope, SnapshotDvSummary, SnapshotDvValidator, SnapshotFile,
};
pub use list_reader::ManifestListReader;
pub use list_selection::ManifestListSelection;
pub use reader::ManifestReader;
pub use snapshot_identity::{SnapshotIdentityError, SnapshotIdentityIndex, SnapshotIdentityLimits};
pub use snapshot_reader::{
    SnapshotManifestError, SnapshotManifestLimits, SnapshotManifestReader, SnapshotManifestSource,
    SnapshotManifestSummary,
};
pub use summary::PartitionSummary;

pub use entry::{
    ManifestEntryError, ManifestEntryProjection, ManifestEntryRecords, ManifestEntryState,
    ManifestFileFields, ManifestMetrics, ManifestScalarEntry, PartitionValue,
};

pub use list::{ManifestListEntry, ManifestListError, ManifestListProjection, ManifestListRecords};
pub use metadata::{ManifestMetadata, ManifestMetadataError};
pub use parquet::{
    read_parquet_selection, read_selected_parquet_metadata, validate_parquet_position_deletes,
    validate_parquet_schema, ParquetFieldMapping, ParquetSelection, PositionDeleteLimits,
    PositionDeleteSummary, PositionDeleteTargets, SelectedParquetError, SelectedParquetSchema,
};

pub use inheritance::{
    EntryStatus, FileContentKind, InheritedEntry, ManifestContent, ManifestEntry, ManifestInheritance,
    ManifestInheritanceError, ManifestVersion,
};
