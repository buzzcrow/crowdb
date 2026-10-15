//! Versioned identities and ordered binary storage keys.

mod codec;
mod identity;
mod name;
mod storage;
mod storage_path;
mod table_storage;

pub use codec::{CatalogScope, IcebergKey, SystemScope, MAX_KEY_BYTES};
pub use identity::{CatalogId, FileId, NamespaceId, OperationId, TableId};
pub use name::NameSuffix;
pub use storage::StorageKey;
pub use storage_path::StorageTablePath;
pub use table_storage::{TableStorageKey, TableStorageScope};
