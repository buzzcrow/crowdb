// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Core KV client: topology cache, retry/idempotency, and `ReadMode` routing.

pub mod admin;
pub mod core;
pub mod retry;
pub mod topology;

pub use core::{
    new_client_id, BatchOp, CrowdbKvClient, GetOutcome, JournalOp, JournalScanOutcome, ScanOutcome,
    WriteOutcome,
};
