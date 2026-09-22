//! Multipart namespace identity and bounded authority properties.

mod admission;
mod authority;
mod create;
mod create_recovery;
mod drop;
mod drop_fence;
mod drop_finish;
mod drop_probe;
mod identifier;
mod journal;
mod key;
mod list;
mod list_token;
mod operation;
mod properties;
mod publication;
mod recovery;
mod recovery_scan;
mod repair;
mod repository;
mod reservation;
mod storage;
mod update;
mod update_recovery;

pub use authority::{NamespaceAuthority, NamespaceLifecycle, NamespaceMapping, NamespaceMappingState};
pub use create::{NamespaceCreateRequest, NamespaceCreator};
pub use drop::{NamespaceDropRequest, NamespaceDropper};
pub use identifier::{NamespaceIdentifier, MAX_IDENTIFIER_BYTES, MAX_NAMESPACE_LEVELS};
pub use journal::NamespaceJournal;
pub use key::{authority_key, child_range, name_key};
pub use list::{NamespaceListPage, NamespaceLister};
pub use operation::{
    NamespaceAction, NamespaceMutation, NamespaceOperation, NamespaceOutcome, NamespacePhase,
};
pub use properties::{NamespaceProperties, PropertyChanges, PropertyUpdate, MAX_PROPERTIES};
pub use recovery::{NamespaceRecovery, NamespaceRecoveryPage};
pub use recovery_scan::{NamespaceRecoveryScan, NamespaceRecoveryStore};
pub use repository::NamespaceRepository;
pub use storage::{ChildScan, NamespaceStore};
pub use update::NamespacePropertyRequest;
