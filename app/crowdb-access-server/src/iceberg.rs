//! Independent Iceberg listener and catalog-management runtime.

mod body;
mod file_auth;
mod file_body;
mod http;
mod namespace_read;
mod namespace_request;
mod namespace_write;
mod recovery;
mod runtime;

pub use file_auth::authenticate_file_request;
pub use file_body::{FileBodyError, FileReadBody, FileResponseBudget};
pub use http::{serve, IcebergHttpService};
pub use runtime::{run, IcebergRuntimeConfig};
