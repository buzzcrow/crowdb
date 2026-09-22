//! Multipart namespace identity and bounded authority properties.

mod authority;
mod identifier;
mod journal;
mod key;
mod operation;
mod properties;
mod storage;

pub use authority::{NamespaceAuthority, NamespaceLifecycle, NamespaceMapping, NamespaceMappingState};
pub use identifier::{NamespaceIdentifier, MAX_IDENTIFIER_BYTES, MAX_NAMESPACE_LEVELS};
pub use journal::NamespaceJournal;
pub use key::{authority_key, child_range, name_key};
pub use operation::{
    NamespaceAction, NamespaceMutation, NamespaceOperation, NamespaceOutcome, NamespacePhase,
};
pub use properties::{NamespaceProperties, PropertyChanges, PropertyUpdate, MAX_PROPERTIES};
pub use storage::{ChildScan, NamespaceStore};
