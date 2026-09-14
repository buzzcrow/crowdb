// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskDB` REST proxy and instance lifecycle handlers.

pub mod lifecycle;
pub mod proxy;

pub use lifecycle::*;
pub use proxy::*;
