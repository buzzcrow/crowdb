//! Stable catalog identity and capability contracts.

mod capability;
mod deadline;
mod root;
mod state;

pub use capability::{Capabilities, FormatAction, FormatSupport};
pub use deadline::ClearBounds;
pub use root::{ActiveCatalogRecord, RootState};
pub use state::{CatalogAuthority, CatalogContext, CatalogLifecycle, ClearTransition};
