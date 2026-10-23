// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Trusted internal storage access with shared routing and retained host memory.
//!
//! This crate belongs to the main workspace. The independently distributed
//! HTTP SDK has no dependency on this native storage and FFI stack.

mod admission;
pub mod buffer;
pub mod context;
pub mod error;
pub mod runtime;
pub mod s3;
pub mod topology;

pub use context::{Cancellation, OperationContext};
pub use error::{CapacityKind, Error, Result};
pub use runtime::{ClientRuntime, RuntimeConfig, RuntimeLimits, RuntimeResources};
