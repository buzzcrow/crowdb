//! Versioned identities and ordered binary storage keys.

mod codec;
mod identity;
mod name;

pub use codec::{CatalogScope, IcebergKey, SystemScope, MAX_KEY_BYTES};
pub use identity::{CatalogId, FileId, NamespaceId, OperationId, TableId};
pub use name::NameSuffix;
