//! Bounded table identity and generation-qualified metadata selection.

mod key;
mod list;
mod load;
mod metadata;
mod record;
mod repository;

pub use key::{head_key, name_key};
pub use list::{TableListLimits, TableListPage, TableLister};
pub use load::{SnapshotLoadingMode, TableLoad, TableLoadError, TableLoader};
pub use metadata::{
    read_table_metadata_document, TableMetadataDocument, TableMetadataError, TableMetadataLimits,
    TableSnapshot,
};
pub use record::{TableHead, TableLifecycle, TableMapping, TableMappingState};
pub use repository::{SelectedTable, TableRepository};
