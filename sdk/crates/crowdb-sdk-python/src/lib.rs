// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Python owns ergonomics; Rust owns the reusable runtime, HTTP and operations.
mod operation;
mod stream;
use crowdb_sdk_core::{Error, HttpConfig};
use crowdb_sdk_dataset::DatasetClient;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::sync::{
    atomic::{AtomicBool, Ordering},
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
    request_timeout: f64,
    stream_timeout: f64,
    ca_pem: Option<Vec<u8>>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            max_requests: 8,
            max_in_flight_bytes: 134_217_728,
            request_timeout: 60.0,
            stream_timeout: 1800.0,
            ca_pem: None,
        }
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
        if options.len() > 8192 {
            return Err(PyRuntimeError::new_err("SDK configuration limit"));
        }
        let options: Options =
            serde_json::from_str(options).map_err(|_| PyRuntimeError::new_err("invalid SDK options"))?;
        let Options {
            max_requests,
            max_in_flight_bytes,
            request_timeout,
            stream_timeout,
            ca_pem,
        } = options;
        if !request_timeout.is_finite()
            || !stream_timeout.is_finite()
            || request_timeout <= 0.0
            || stream_timeout <= 0.0
        {
            return Err(PyRuntimeError::new_err("invalid deadlines"));
        }
        py.detach(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| PyRuntimeError::new_err("cannot create SDK runtime"))?;
            let entered = runtime.enter();
            let config = HttpConfig {
                max_requests,
                max_in_flight_bytes,
                request_timeout: std::time::Duration::from_secs_f64(request_timeout),
                stream_timeout: std::time::Duration::from_secs_f64(stream_timeout),
                ca_pem,
                ..HttpConfig::default()
            };
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
                }),
            })
        })
    }
    fn start(&self, operation: String, arguments: String) -> PyResult<operation::NativeOperation> {
        self.resources.check()?;
        if arguments.len() > 8 * 1024 * 1024 {
            return Err(PyRuntimeError::new_err("metadata exceeds SDK limit"));
        }
        Ok(operation::NativeOperation::spawn(
            self.resources.clone(),
            operation,
            arguments,
        ))
    }
    fn part(&self, id: String, index: u32, data: &[u8]) -> PyResult<operation::NativeOperation> {
        self.resources.check()?;
        if data.is_empty() || data.len() > 4 * 1024 * 1024 {
            return Err(PyRuntimeError::new_err("upload part must contain 1..4 MiB"));
        }
        let client = self.resources.client();
        let credit = match client.reserve_transfer(data.len()) {
            Ok(credit) => credit,
            Err(error) => {
                return Ok(operation::NativeOperation::spawn_future(
                    self.resources.clone(),
                    async move { Err(error) },
                ))
            }
        };
        let data = bytes::Bytes::copy_from_slice(data);
        Ok(operation::NativeOperation::spawn_future(
            self.resources.clone(),
            async move {
                let _credit = credit;
                Ok(serde_json::to_string(
                    &client.upload_part(&id, index, data).await?,
                )?)
            },
        ))
    }
    fn stream(&self, kind: String, arguments: String) -> PyResult<stream::NativeStream> {
        self.resources.check()?;
        if arguments.len() > 8 * 1024 * 1024 {
            return Err(PyRuntimeError::new_err("metadata exceeds SDK limit"));
        }
        Ok(stream::NativeStream::spawn(
            self.resources.clone(),
            kind,
            arguments,
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
