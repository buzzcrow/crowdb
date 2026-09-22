//! Stable catalog identity and capability contracts.

mod capability;
mod deadline;
mod state;

pub use capability::{Capabilities, FormatAction, FormatSupport};
pub use deadline::ClearBounds;
pub use state::{CatalogAuthority, CatalogContext, CatalogLifecycle, ClearTransition};
