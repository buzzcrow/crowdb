// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable write coordinators owned by the diskdb service.

mod free_batch;

pub use free_batch::{FreeBatchPersist, FreeBatcher, PersistFuture};
