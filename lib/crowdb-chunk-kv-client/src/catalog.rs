// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Catalog routing and request identity allocation.

mod identity;
mod store;

pub use identity::RequestIdentityAllocator;
pub use store::{CatalogCache, CatalogMap, CatalogSource};
