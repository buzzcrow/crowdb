// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Python owns ergonomics; Rust owns the reusable runtime, HTTP and operations.
mod control;
mod operation;
mod stream;
mod upload;
use crowdb_sdk_core::{Error, HttpConfig};
use crowdb_sdk_dataset::DatasetClient;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::runtime::Runtime;

struct Credentials(arc_swap::ArcSwapOption<String>);
impl crowdb_sdk_core::CredentialProvider for Credentials {
    fn bearer(&self) -> Result<Option<String>, Error> {
        Ok(self.0.load_full().map(|v| (*v).clone()))
    }
}
struct Resources {
    runtime: Option<Runtime>,
    client: Option<DatasetClient>,
    pid: u32,
    closed: AtomicBool,
    credentials: Arc<Credentials>,
    operations: AtomicUsize,
    max_operations: usize,
}
impl Resources {
    fn check(&self) -> PyResult<()> {
        if self.pid != std::process::id() {
            return Err(PyRuntimeError::new_err(
                "create a Dataset client inside each worker process; inherited clients cannot be used",
            ));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("Dataset client is closed"));
        }
        Ok(())
    }
    fn budget(self: &Arc<Self>, bytes: usize) -> Result<InputBudget, Error> {
        self.operations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.max_operations).then_some(count + 1)
            })
            .map_err(|_| Error::Capacity)?;
        let operation = OperationPermit(self.clone());
        let bytes = self.client().reserve_transfer(bytes)?;
        Ok(InputBudget {
            _bytes: bytes,
            _operation: operation,
        })
    }
    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("runtime owned until final resource release")
    }
    fn client(&self) -> DatasetClient {
        self.client
            .as_ref()
            .expect("client owned until final resource release")
            .clone()
    }
}
struct OperationPermit(Arc<Resources>);
impl Drop for OperationPermit {
    fn drop(&mut self) {
        self.0.operations.fetch_sub(1, Ordering::AcqRel);
    }
}
struct InputBudget {
    _bytes: crowdb_sdk_core::BufferPermit,
    _operation: OperationPermit,
}
impl Drop for Resources {
    fn drop(&mut self) {
        if self.pid != std::process::id() {
            if let Some(client) = self.client.take() {
                std::mem::forget(client);
            }
            if let Some(runtime) = self.runtime.take() {
                std::mem::forget(runtime);
            }
        } else if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}
#[derive(serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Options {
    max_requests: usize,
    max_in_flight_bytes: usize,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_idle_connections_per_host: usize,
    connect_timeout: f64,
    request_timeout: f64,
    overall_timeout: f64,
    stream_timeout: f64,
    pool_idle_timeout: f64,
    max_attempts: usize,
    retry_backoff: f64,
    ca_pem: Option<String>,
}
impl Default for Options {
    fn default() -> Self {
        let config = HttpConfig::default();
        Self {
            max_requests: config.max_requests,
            max_in_flight_bytes: config.max_in_flight_bytes,
            max_request_bytes: config.max_request_bytes,
            max_response_bytes: config.max_response_bytes,
            max_idle_connections_per_host: config.max_idle_connections_per_host,
            connect_timeout: config.connect_timeout.as_secs_f64(),
            request_timeout: config.request_timeout.as_secs_f64(),
            overall_timeout: config.overall_timeout.as_secs_f64(),
            stream_timeout: config.stream_timeout.as_secs_f64(),
            pool_idle_timeout: config.pool_idle_timeout.as_secs_f64(),
            max_attempts: config.max_attempts,
            retry_backoff: config.retry_backoff.as_secs_f64(),
            ca_pem: None,
        }
    }
}
impl Options {
    fn config(self) -> PyResult<HttpConfig> {
        let duration = |value| {
            std::time::Duration::try_from_secs_f64(value)
                .map_err(|_| PyRuntimeError::new_err("invalid SDK duration"))
        };
        Ok(HttpConfig {
            max_requests: self.max_requests,
            max_in_flight_bytes: self.max_in_flight_bytes,
            max_request_bytes: self.max_request_bytes,
            max_response_bytes: self.max_response_bytes,
            max_idle_connections_per_host: self.max_idle_connections_per_host,
            connect_timeout: duration(self.connect_timeout)?,
            request_timeout: duration(self.request_timeout)?,
            overall_timeout: duration(self.overall_timeout)?,
            stream_timeout: duration(self.stream_timeout)?,
            pool_idle_timeout: duration(self.pool_idle_timeout)?,
            max_attempts: self.max_attempts,
            retry_backoff: duration(self.retry_backoff)?,
            ca_pem: self.ca_pem.map(String::into_bytes),
        })
    }
}
#[pyclass(frozen)]
struct NativeClient {
    resources: Arc<Resources>,
}
#[pymethods]
impl NativeClient {
    #[new]
    #[pyo3(signature=(endpoint,name,bearer=None,options="{}"))]
    fn new(
        py: Python<'_>,
        endpoint: String,
        name: String,
        bearer: Option<String>,
        options: &str,
    ) -> PyResult<Self> {
        if options.len() > 128 * 1024 {
            return Err(PyRuntimeError::new_err("SDK configuration limit"));
        }
        let options: Options =
            serde_json::from_str(options).map_err(|_| PyRuntimeError::new_err("invalid SDK options"))?;
        let config = options.config()?;
        py.detach(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| PyRuntimeError::new_err("cannot create SDK runtime"))?;
            let entered = runtime.enter();
            let max_operations = config.max_requests;
            let credentials = Arc::new(Credentials(arc_swap::ArcSwapOption::from(bearer.map(Arc::new))));
            let client =
                DatasetClient::with_credential_provider(&endpoint, &name, credentials.clone(), config)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            drop(entered);
            Ok(Self {
                resources: Arc::new(Resources {
                    runtime: Some(runtime),
                    client: Some(client),
                    pid: std::process::id(),
                    closed: AtomicBool::new(false),
                    credentials,
                    operations: AtomicUsize::new(0),
                    max_operations,
                }),
            })
        })
    }
    fn start(&self, operation: &str, arguments: &str) -> PyResult<operation::NativeOperation> {
        self.resources.check()?;
        if arguments.len() > 8 * 1024 * 1024 || operation.len() > 64 {
            return Ok(operation::NativeOperation::failed(
                self.resources.clone(),
                &Error::Limit,
            ));
        }
        let budget = match self.resources.budget(arguments.len() * 8 + 4096) {
            Ok(b) => b,
            Err(error) => return Ok(operation::NativeOperation::failed(self.resources.clone(), &error)),
        };
        Ok(operation::NativeOperation::spawn(
            self.resources.clone(),
            operation.to_owned(),
            arguments.to_owned(),
            budget,
        ))
    }
    fn part(&self, id: &str, index: u32, data: &[u8]) -> PyResult<operation::NativeOperation> {
        upload::part(&self.resources, id, index, data, false)
    }
    fn part_v2(&self, id: &str, index: u32, data: &[u8]) -> PyResult<operation::NativeOperation> {
        upload::part(&self.resources, id, index, data, true)
    }
    fn stream(&self, kind: &str, arguments: &str) -> PyResult<stream::NativeStream> {
        self.resources.check()?;
        if arguments.len() > 8 * 1024 * 1024 || kind.len() > 64 {
            return Ok(stream::NativeStream::failed(
                self.resources.clone(),
                &Error::Limit,
            ));
        }
        let budget = match self.resources.budget(arguments.len() * 8 + 4096) {
            Ok(b) => b,
            Err(error) => return Ok(stream::NativeStream::failed(self.resources.clone(), &error)),
        };
        Ok(stream::NativeStream::spawn(
            self.resources.clone(),
            kind.to_owned(),
            arguments.to_owned(),
            budget,
        ))
    }
    fn refresh_bearer(&self, bearer: Option<String>) -> PyResult<()> {
        self.resources.check()?;
        if bearer
            .as_ref()
            .is_some_and(|token| token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()))
        {
            return Err(PyRuntimeError::new_err("invalid bearer credential"));
        }
        self.resources.credentials.0.store(bearer.map(Arc::new));
        Ok(())
    }
    fn close(&self) {
        self.resources.closed.store(true, Ordering::Release);
    }
}
fn failure(error: &Error) -> String {
    if let Error::Ambiguous { identity, source } = error {
        let mut record: serde_json::Value = serde_json::from_str(&failure(source)).expect("own error JSON");
        record["identity"] = serde_json::Value::String(identity.clone());
        return record.to_string();
    }
    let (code, status, retryable) = match &error {
        Error::Service { status, error } => (error.code.as_str(), *status, error.retryable),
        Error::Stream { error } => (error.code.as_str(), 0, error.retryable),
        Error::Integrity => ("integrity", 0, false),
        Error::Capacity => ("capacity", 0, true),
        Error::Deadline => ("timeout", 0, true),
        Error::Transport(_) => ("transport", 0, true),
        Error::Limit => ("limit", 0, false),
        Error::Io(_) => ("io", 0, false),
        _ => ("invalid", 0, false),
    };
    serde_json::json!({"code":code,"status":status,"retryable":retryable,"message":error.to_string()})
        .to_string()
}
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeClient>()?;
    m.add_class::<operation::NativeOperation>()?;
    m.add_class::<stream::NativeStream>()?;
    Ok(())
}
