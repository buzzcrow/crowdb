//! Independent Iceberg listener and catalog-management runtime.

mod http;
mod recovery;
mod runtime;

pub use http::{serve, IcebergHttpService};
pub use runtime::{run, IcebergRuntimeConfig};
