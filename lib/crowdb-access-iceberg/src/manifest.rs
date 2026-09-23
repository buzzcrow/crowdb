//! Streaming manifest semantics, separate from physical file content identity.

mod context;
mod deletion_vectors;
mod entry;
mod inheritance;
mod list;
mod list_reader;
mod list_selection;
mod metadata;
mod reader;
mod snapshot_reader;
mod summary;
pub use context::{
    ManifestContext, ManifestContextError, PartitionField, PartitionTransform, PrimitiveType, SchemaField,
};
pub use deletion_vectors::{
    SnapshotDvError, SnapshotDvLimits, SnapshotDvScope, SnapshotDvSummary, SnapshotDvValidator, SnapshotFile,
};
pub use list_reader::ManifestListReader;
pub use list_selection::ManifestListSelection;
pub use reader::ManifestReader;
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

pub use inheritance::{
    EntryStatus, FileContentKind, InheritedEntry, ManifestContent, ManifestEntry, ManifestInheritance,
    ManifestInheritanceError, ManifestVersion,
};
