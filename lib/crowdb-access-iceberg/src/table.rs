//! Bounded table identity and generation-qualified metadata selection.

mod key;
mod record;
mod repository;

pub use key::{head_key, name_key};
pub use record::{TableHead, TableLifecycle, TableMapping, TableMappingState};
pub use repository::{SelectedTable, TableRepository};
