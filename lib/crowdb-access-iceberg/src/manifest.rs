//! Streaming manifest semantics, separate from physical file content identity.

mod entry;
mod inheritance;
mod list;
mod metadata;

pub use entry::{
    ManifestEntryError, ManifestEntryProjection, ManifestEntryRecords, ManifestEntryState,
    ManifestFileFields, ManifestMetrics, ManifestScalarEntry,
};

pub use list::{ManifestListEntry, ManifestListError, ManifestListProjection, ManifestListRecords};
pub use metadata::{ManifestMetadata, ManifestMetadataError};

pub use inheritance::{
    EntryStatus, FileContentKind, InheritedEntry, ManifestContent, ManifestEntry, ManifestInheritance,
    ManifestInheritanceError, ManifestVersion,
};
