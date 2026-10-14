// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Dataset HTTP SDK. Payload transfers use bounded parts and immutable snapshots.

mod client;
mod models;
pub mod wire;

pub use client::DatasetClient;
pub use crowdb_sdk_core::Error;
pub use models::*;
