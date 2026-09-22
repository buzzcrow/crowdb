//! Stable catalog identity and capability contracts.

mod capability;
mod context;
mod deadline;
mod recovery;
mod repository;
mod root;
mod state;
mod storage;

pub use capability::{Capabilities, FormatAction, FormatSupport};
pub(crate) use context::check_context;
pub use deadline::ClearBounds;
pub use repository::{CatalogError, CatalogRepository, ManagementPrivilege};
pub use root::{ActiveCatalogRecord, RootState};
pub use state::{CatalogAuthority, CatalogContext, CatalogLifecycle, ClearTransition};
pub use storage::{CasOutcome, CatalogStore, RoutedCatalogStore, StoreError, StoredValue};
