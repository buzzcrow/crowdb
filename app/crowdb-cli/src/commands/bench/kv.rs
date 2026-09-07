// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV-layer bench workloads (prepare / read / write / scan) against a
//! crowdb-kv store. The shared client builder is re-exported for the
//! `disk` and `chunk` sub-modules, which also drive a KV client.

pub mod client;
pub mod prepare;
pub mod read;
pub mod scan;
pub mod write;

pub(crate) use client::{build_kv_client, KvClientTunables};
