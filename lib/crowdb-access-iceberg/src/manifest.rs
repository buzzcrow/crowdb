//! Streaming manifest semantics, separate from physical file content identity.

mod context;
mod deletion_vectors;
mod entry;
mod inheritance;
mod list;
mod list_reader;
mod metadata;
mod reader;
mod summary;
pub use context::{
    ManifestContext, ManifestContextError, PartitionField, PartitionTransform, PrimitiveType, SchemaField,
};
pub use deletion_vectors::{
    SnapshotDvError, SnapshotDvLimits, SnapshotDvScope, SnapshotDvSummary, SnapshotDvValidator, SnapshotFile,
};
pub use list_reader::ManifestListReader;
pub use reader::ManifestReader;
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
