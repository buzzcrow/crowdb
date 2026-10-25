// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    client::Dataset,
    error::{failure, NativeError},
};
use crowdb_sdk_native::{ClientRuntime, OperationContext, RuntimeConfig};
use pyo3::prelude::*;
use std::{
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub struct Resources {
    executor: Option<tokio::runtime::Runtime>,
    native: Option<ClientRuntime>,
    pid: u32,
    sequence: AtomicU64,
}

impl Resources {
    pub fn check_process(&self) -> PyResult<()> {
        if self.pid != std::process::id() {
            return Err(NativeError::new_err(
                "create a native runtime in each worker process",
            ));
        }
        Ok(())
    }
    pub fn native(&self) -> &ClientRuntime {
        self.native.as_ref().expect("owned runtime")
    }
    pub fn executor(&self) -> &tokio::runtime::Runtime {
        self.executor.as_ref().expect("owned executor")
    }
    pub fn context(&self, seconds: f64) -> PyResult<OperationContext> {
        self.check_process()?;
        let duration =
            Duration::try_from_secs_f64(seconds).map_err(|_| NativeError::new_err("invalid timeout"))?;
        let deadline = Instant::now()
            .checked_add(duration)
            .ok_or_else(|| NativeError::new_err("timeout overflow"))?;
        let number = self
            .sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| NativeError::new_err("request sequence exhausted"))?;
        Ok(OperationContext::new(
            NonZeroU64::new(number).expect("starts at one"),
            deadline,
        ))
    }
}

impl Drop for Resources {
    fn drop(&mut self) {
        let executor = self.executor.take().expect("owned executor");
        let native = self.native.take().expect("owned runtime");
        if self.pid != std::process::id() {
            std::mem::forget(native);
            std::mem::forget(executor);
            return;
        }
        // Final release may run on an executor worker. Join owned RPC workers
        // from a separate cleanup thread before destroying that executor.
        std::thread::spawn(move || {
            if let Err(error) = executor.block_on(native.shutdown()) {
                eprintln!("native Python runtime cleanup failed: {error}");
            }
            drop(native);
            executor.shutdown_background();
        });
    }
}

#[pyclass(frozen, module = "crowdb_dataset_native")]
pub struct NativeRuntime {
    pub resources: Arc<Resources>,
}

#[pymethods]
impl NativeRuntime {
    #[new]
    #[pyo3(signature=(seeds, options="{}"))]
    fn new(py: Python<'_>, seeds: Vec<String>, options: &str) -> PyResult<Self> {
        if options.len() > 64 * 1024 {
            return Err(NativeError::new_err("runtime options exceed 64 KiB"));
        }
        let options: Options =
            serde_json::from_str(options).map_err(|e| NativeError::new_err(e.to_string()))?;
        py.detach(move || {
            let executor = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| NativeError::new_err(e.to_string()))?;
            let mut config = RuntimeConfig::new(seeds);
            config.limits.held_bytes = options.held_bytes;
            config.limits.producer_bytes = options.producer_bytes;
            config.limits.requests = options.requests;
            config.data_rdma = Some(options.data_rdma);
            config.chunk_kv_transport.metadata_rdma = Some(options.metadata_rdma);
            config.chunk_kv_transport.aggregate_rdma = Some(options.aggregate_rdma);
            let native = executor
                .block_on(ClientRuntime::connect(config))
                .map_err(|e| failure(&e))?;
            Ok(Self {
                resources: Arc::new(Resources {
                    executor: Some(executor),
                    native: Some(native),
                    pid: std::process::id(),
                    sequence: AtomicU64::new(1),
                }),
            })
        })
    }

    #[pyo3(signature=(name, namespace=None))]
    fn dataset(&self, py: Python<'_>, name: String, namespace: Option<Vec<String>>) -> PyResult<Dataset> {
        self.resources.check_process()?;
        let namespace =
            namespace.unwrap_or_else(|| crowdb_access_dataset::NamespacePath::root().segments().to_vec());
        Dataset::new(py, self.resources.clone(), name, namespace)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.resources.check_process()?;
        py.detach(|| {
            self.resources
                .executor()
                .block_on(self.resources.native().shutdown())
        })
        .map_err(|e| failure(&e))
    }

    fn status(&self) -> PyResult<String> {
        self.resources.check_process()?;
        let s = self.resources.native().resources().status();
        Ok(
            serde_json::json!({"closed":s.closed,"requests":s.requests,"producer_bytes":s.producer_bytes,
            "producer_bytes_high_water":s.producer_bytes_high_water,
            "read_flow":self.resources.native().read_flow_metrics(),
            "held_bytes":s.held_bytes,"held_bytes_high_water":s.held_bytes_high_water,
            "compatibility_copy_bytes":s.compatibility_copy_bytes})
            .to_string(),
        )
    }

    /// Retained observations are hints; execution always validates current routing.
    #[pyo3(signature=(refresh=false, timeout=30.0))]
    fn topology(&self, py: Python<'_>, refresh: bool, timeout: f64) -> PyResult<String> {
        let context = self.resources.context(timeout)?;
        let view = py
            .detach(|| {
                if refresh {
                    self.resources
                        .executor()
                        .block_on(self.resources.native().refresh_topology(context))
                } else {
                    self.resources.native().topology()
                }
            })
            .map_err(|e| failure(&e))?;
        let metadata: serde_json::Value = serde_json::from_slice(&view.metadata_transport_json)
            .map_err(|e| NativeError::new_err(e.to_string()))?;
        let data: Option<serde_json::Value> = view
            .data_transport_json
            .as_ref()
            .map(|bytes| serde_json::from_slice(bytes))
            .transpose()
            .map_err(|e| NativeError::new_err(e.to_string()))?;
        Ok(
            serde_json::json!({"catalog_age_ms":view.catalog_age().map(|age|age.as_millis()),
            "metadata":metadata,"data":data,"authoritative":false})
            .to_string(),
        )
    }
}

#[derive(serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Options {
    held_bytes: usize,
    producer_bytes: usize,
    requests: usize,
    data_rdma: crowdb_rpc_ffi::RdmaConfig,
    metadata_rdma: crowdb_rpc_ffi::RdmaConfig,
    aggregate_rdma: crowdb_rpc_ffi::RdmaConfig,
}
impl Default for Options {
    fn default() -> Self {
        let limits = crowdb_sdk_native::RuntimeLimits::default();
        Self {
            held_bytes: limits.held_bytes,
            producer_bytes: limits.producer_bytes,
            requests: limits.requests,
            data_rdma: crowdb_rpc_ffi::RdmaConfig::default(),
            metadata_rdma: crowdb_rpc_ffi::RdmaConfig::default(),
            aggregate_rdma: crowdb_rpc_ffi::RdmaConfig::default(),
        }
    }
}
