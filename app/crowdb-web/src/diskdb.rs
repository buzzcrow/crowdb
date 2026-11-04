// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskDB` REST proxy and instance lifecycle handlers.

pub mod lifecycle;
pub mod proxy;
mod remote;
pub(crate) use remote::configuration as planned_configuration;
#[cfg(feature = "test-util")]
pub use remote::configuration as configuration_for_tests;

pub use lifecycle::*;
pub use proxy::*;
