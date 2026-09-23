//! Independent Iceberg listener and catalog-management runtime.

mod body;
mod file_auth;
mod http;
mod namespace_read;
mod namespace_request;
mod namespace_write;
mod recovery;
mod runtime;

pub use file_auth::authenticate_file_request;
pub use http::{serve, IcebergHttpService};
pub use runtime::{run, IcebergRuntimeConfig};
