// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_sdk_native::buffer::BufferLease;
use pyo3::{prelude::*, types::PyBytes};

/// Immutable owner; slicing retains storage and `to_bytes` explicitly copies.
#[pyclass(frozen, module = "crowdb_dataset_native")]
pub struct Lease {
    pub buffer: BufferLease,
    #[pyo3(get)]
    pub field: usize,
}

#[pymethods]
impl Lease {
    fn __len__(&self) -> usize {
        self.buffer.as_slice().len()
    }

    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.buffer.as_slice())
    }

    fn slice(&self, start: usize, end: usize) -> PyResult<Self> {
        Ok(Self {
            buffer: self
                .buffer
                .slice(start..end)
                .map_err(|e| super::error::failure(&e))?,
            field: self.field,
        })
    }

    #[getter]
    fn logical_offset(&self) -> u64 {
        self.buffer.metadata().logical_offset
    }
    #[getter]
    fn allocation_capacity(&self) -> usize {
        self.buffer.metadata().allocation_capacity
    }
    #[getter]
    fn registration_id(&self) -> u32 {
        self.buffer.metadata().registration_id
    }
}
