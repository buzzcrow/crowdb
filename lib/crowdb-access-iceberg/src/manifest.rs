//! Streaming manifest semantics, separate from physical file content identity.

mod entry;
mod inheritance;
mod list;

pub use entry::{
    ManifestEntryError, ManifestEntryProjection, ManifestEntryRecords, ManifestEntryState,
    ManifestFileFields, ManifestScalarEntry,
};

pub use list::{ManifestListEntry, ManifestListError, ManifestListProjection, ManifestListRecords};

pub use inheritance::{
    EntryStatus, FileContentKind, InheritedEntry, ManifestContent, ManifestEntry, ManifestInheritance,
    ManifestInheritanceError, ManifestVersion,
};
