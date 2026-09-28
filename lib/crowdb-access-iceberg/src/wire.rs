//! Iceberg HTTP configuration and authentication, independent of storage records.

mod auth;
mod config;
mod credentials;
mod retry;

pub use auth::{BearerAuthenticator, Principal};
pub use config::{CatalogConfig, IcebergErrorResponse};
pub use credentials::{
    FileDelegationLimits, FileDelegationTarget, LoadCredentialsResponse, StorageCredential,
};
pub use retry::RequestKey;
