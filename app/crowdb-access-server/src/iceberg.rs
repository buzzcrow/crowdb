//! Independent Iceberg listener and catalog-management runtime.

mod http;
mod runtime;

pub use http::{serve, IcebergHttpService};
pub use runtime::{run, IcebergRuntimeConfig};
