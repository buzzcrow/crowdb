// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Catalog-aware routed client for the chunk-backed KV service.

mod catalog;
mod client;
mod compose;
mod config;
mod error;
mod identity;
mod ordered;
mod transport;

pub use catalog::{CatalogCache, CatalogMap, CatalogSource};
pub use client::ChunkKvClient;
pub use compose::{BatchItem, ComposedItemError, MultiGetItemResult};
pub use config::ClientConfig;
pub use error::{ClientError, Result};
pub use identity::RequestIdentityAllocator;
pub use ordered::{MultiScanContinuation, MultiScanPage, MultiScanRequest};
pub use transport::ChunkKvTransport;
