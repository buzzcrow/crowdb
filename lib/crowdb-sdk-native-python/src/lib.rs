// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicitly selected native Dataset bindings; no HTTP or implicit replay.
mod client;
mod control;
mod error;
mod lease;
mod runtime;
mod stream;

use pyo3::prelude::*;

#[pymodule(gil_used = false)]
fn crowdb_dataset_native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<runtime::NativeRuntime>()?;
    module.add_class::<client::Dataset>()?;
    module.add_class::<stream::Batch>()?;
    module.add_class::<lease::Lease>()?;
    error::register(module)
}
