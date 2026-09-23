//! Bounded table identity and generation-qualified metadata selection.

mod key;
mod metadata;
mod record;
mod repository;

pub use key::{head_key, name_key};
pub use metadata::{
    read_table_metadata_document, TableMetadataDocument, TableMetadataError, TableMetadataLimits,
    TableSnapshot,
};
pub use record::{TableHead, TableLifecycle, TableMapping, TableMappingState};
pub use repository::{SelectedTable, TableRepository};
