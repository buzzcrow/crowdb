//! Independent Iceberg listener and catalog-management runtime.

mod body;
mod http;
mod namespace_read;
mod recovery;
mod runtime;

pub use http::{serve, IcebergHttpService};
pub use runtime::{run, IcebergRuntimeConfig};
