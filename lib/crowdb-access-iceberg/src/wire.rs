//! Iceberg HTTP configuration and authentication, independent of storage records.

mod auth;
mod config;
mod retry;

pub use auth::{BearerAuthenticator, Principal};
pub use config::{CatalogConfig, IcebergErrorResponse};
pub use retry::RequestKey;
