//! Bounded table identity and generation-qualified metadata selection.

mod key;
mod lifecycle;
mod list;
mod load;
mod metadata;
mod record;
mod repository;

pub use key::{head_key, name_key};
pub use lifecycle::{
    TableLifecycleAction, TableLifecycleOperation, TableLifecyclePhase, TableLifecycleRequest,
    TableLifecycles, TablePurgeTask,
};
pub use list::{TableListLimits, TableListPage, TableLister};
pub use load::{SnapshotLoadingMode, TableLoad, TableLoadError, TableLoader};
pub(crate) use metadata::decode_bounded_json;
pub(crate) use metadata::validate_auxiliary_definition;
pub(crate) use metadata::validate_metadata_payloads;
pub use metadata::{
    read_table_metadata_document, TableMetadataDocument, TableMetadataError, TableMetadataLimits,
    TableSnapshot,
};
pub(crate) use metadata::{schema_default_identity, validate_layout_definitions, validate_schema_definition};
pub use record::{TableHead, TableLifecycle, TableMapping, TableMappingState};
pub use repository::{SelectedTable, TableRepository};
