//! Streaming manifest semantics, separate from physical file content identity.

mod inheritance;
mod list;

pub use list::{ManifestListEntry, ManifestListError, ManifestListProjection, ManifestListRecords};

pub use inheritance::{
    EntryStatus, FileContentKind, InheritedEntry, ManifestContent, ManifestEntry, ManifestInheritance,
    ManifestInheritanceError, ManifestVersion,
};
