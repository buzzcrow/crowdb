// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    control,
    error::{failure, ClosedError, NativeError},
    runtime::Resources,
    stream::Batch,
};
use crowdb_access_dataset::{DatasetIdentity, NamespacePath, OperationId, SnapshotId};
use crowdb_sdk_native::{
    dataset::{BatchRequest, DatasetClient},
    OperationContext,
};
use pyo3::prelude::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[pyclass(frozen, module = "crowdb_dataset_native")]
pub struct Dataset {
    pub resources: Arc<Resources>,
    pub client: DatasetClient,
    closed: AtomicBool,
}

impl Dataset {
    pub fn new(
        py: Python<'_>,
        resources: Arc<Resources>,
        name: String,
        namespace: Vec<String>,
    ) -> PyResult<Self> {
        let namespace = NamespacePath::new(namespace).map_err(|e| NativeError::new_err(e.to_string()))?;
        let identity =
            DatasetIdentity::new(namespace, name).map_err(|e| NativeError::new_err(e.to_string()))?;
        let client = py
            .detach(|| {
                let _entered = resources.executor().enter();
                DatasetClient::new(resources.native(), identity)
            })
            .map_err(|e| failure(&e))?;
        Ok(Self {
            resources,
            client,
            closed: AtomicBool::new(false),
        })
    }
    fn context(&self, seconds: f64) -> PyResult<OperationContext> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ClosedError::new_err("Dataset facade is closed"));
        }
        self.resources.context(seconds)
    }
}

#[pymethods]
impl Dataset {
    /// Execute a typed control command encoded as JSON with the shared Rust schema.
    #[pyo3(signature=(command, timeout=30.0))]
    fn call(&self, py: Python<'_>, command: &str, timeout: f64) -> PyResult<String> {
        let context = self.context(timeout)?;
        if command.len() > 1024 * 1024 {
            return Err(NativeError::new_err("control input exceeds 1 MiB"));
        }
        let command: control::Command =
            serde_json::from_str(command).map_err(|e| NativeError::new_err(e.to_string()))?;
        py.detach(|| {
            self.resources
                .executor()
                .block_on(control::execute(&self.client, context, command))
        })
        .map(|v| v.to_string())
        .map_err(|e| failure(&e))
    }

    #[pyo3(signature=(snapshot, sample_ids, fields, timeout=30.0))]
    fn read_batch(
        &self,
        py: Python<'_>,
        snapshot: &[u8],
        sample_ids: Vec<Vec<u8>>,
        fields: Vec<String>,
        timeout: f64,
    ) -> PyResult<Batch> {
        let context = self.context(timeout)?;
        let cancel = context.cancellation.clone();
        let snapshot = SnapshotId::from_bytes(snapshot).map_err(|e| NativeError::new_err(e.to_string()))?;
        let read = py
            .detach(|| {
                self.resources.executor().block_on(self.client.read_batch(
                    context,
                    BatchRequest {
                        snapshot,
                        sample_ids,
                        fields,
                    },
                ))
            })
            .map_err(|e| failure(&e))?;
        Ok(Batch::new(self.resources.clone(), read, cancel))
    }

    #[pyo3(signature=(read_id, timeout=30.0))]
    fn next_batch(&self, py: Python<'_>, read_id: &[u8], timeout: f64) -> PyResult<Batch> {
        let context = self.context(timeout)?;
        let cancel = context.cancellation.clone();
        let id = OperationId::from_bytes(read_id).map_err(|e| NativeError::new_err(e.to_string()))?;
        let read = py
            .detach(|| {
                self.resources
                    .executor()
                    .block_on(self.client.next_batch(context, id))
            })
            .map_err(|e| failure(&e))?;
        Ok(Batch::new(self.resources.clone(), read, cancel))
    }

    /// Copies Python bytes once into the final transport frame allocation.
    #[pyo3(signature=(upload, index, data, timeout=30.0))]
    fn upload_part(
        &self,
        py: Python<'_>,
        upload: &[u8],
        index: u32,
        data: &[u8],
        timeout: f64,
    ) -> PyResult<String> {
        let context = self.context(timeout)?;
        let id = OperationId::from_bytes(upload).map_err(|e| NativeError::new_err(e.to_string()))?;
        if data.is_empty() || data.len() > crowdb_access_dataset::MAX_UPLOAD_PART_BYTES {
            return Err(NativeError::new_err("invalid upload part size"));
        }
        let buffer = self
            .resources
            .native()
            .prepare_framed(data)
            .map_err(|e| failure(&e))?;
        let result = py
            .detach(|| {
                self.resources
                    .executor()
                    .block_on(self.client.upload_framed_part(context, id, index, buffer))
            })
            .map_err(|e| failure(&e))?;
        serde_json::to_string(&result).map_err(|e| NativeError::new_err(e.to_string()))
    }

    /// Stops new operations on this facade; existing streams own their lifetimes.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}
