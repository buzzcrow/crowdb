// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{failure, Resources};
use arc_swap::ArcSwapOption;
use crowdb_sdk_dataset::{DatasetClient, Error};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::sync::Arc;
use tokio::{sync::Notify, task::AbortHandle};

struct Completion {
    result: ArcSwapOption<Result<String, String>>,
    ready: Notify,
}
struct CompletionGuard(Arc<Completion>);
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if self.0.result.load().is_none() {
            self.0.result.store(Some(Arc::new(Err("{\"code\":\"cancelled\",\"message\":\"operation cancelled\",\"retryable\":false,\"status\":0}".into()))));
        }
        self.0.ready.notify_waiters();
    }
}
#[pyclass(frozen)]
pub struct NativeOperation {
    resources: Arc<Resources>,
    completion: Arc<Completion>,
    abort: AbortHandle,
}
impl NativeOperation {
    pub(super) fn spawn(resources: Arc<Resources>, name: String, args: String) -> Self {
        let client = resources.client();
        Self::spawn_future(resources, async move { dispatch(client, &name, &args).await })
    }
    pub(super) fn spawn_future(
        resources: Arc<Resources>,
        future: impl std::future::Future<Output = Result<String, Error>> + Send + 'static,
    ) -> Self {
        let completion = Arc::new(Completion {
            result: ArcSwapOption::empty(),
            ready: Notify::new(),
        });
        let guard = CompletionGuard(completion.clone());
        let task = resources.runtime().spawn(async move {
            let guard = guard;
            let result = future.await.map_err(|e| failure(&e));
            guard.0.result.store(Some(Arc::new(result)));
            drop(guard);
        });
        Self {
            resources,
            completion,
            abort: task.abort_handle(),
        }
    }
}
#[pymethods]
impl NativeOperation {
    fn result(&self, py: Python<'_>) -> PyResult<(bool, String)> {
        // Closed clients do not invalidate already-returned operation handles.
        if self.resources.pid != std::process::id() {
            return Err(PyRuntimeError::new_err("inherited operation cannot be used"));
        }
        py.detach(|| {
            self.resources.runtime().block_on(async {
                loop {
                    let ready = self.completion.ready.notified();
                    tokio::pin!(ready);
                    ready.as_mut().enable();
                    if let Some(result) = self.completion.result.load_full() {
                        return Ok(match result.as_ref() {
                            Ok(value) => (true, value.clone()),
                            Err(error) => (false, error.clone()),
                        });
                    }
                    ready.await;
                }
            })
        })
    }
    fn cancel(&self) {
        if self.resources.pid == std::process::id() {
            self.abort.abort();
        }
    }
}
impl Drop for NativeOperation {
    fn drop(&mut self) {
        if self.resources.pid == std::process::id() {
            self.abort.abort();
        }
    }
}
async fn dispatch(c: DatasetClient, name: &str, args: &str) -> Result<String, Error> {
    let v: serde_json::Value = serde_json::from_str(args)?;
    let id = || {
        v.get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Invalid("operation identity".into()))
    };
    macro_rules! output {
        ($value:expr) => {
            Ok(serde_json::to_string(&$value?)?)
        };
    }
    match name {
        "create" => output!(c.create().await),
        "open" => output!(c.open().await),
        "prepare" => output!(c.prepare(&serde_json::from_value(v)?).await),
        "publish" => output!(c.publish(&serde_json::from_value(v)?).await),
        "operation" => output!(c.operation(id()?).await),
        "snapshot" => output!(c.snapshot(id()?).await),
        "manifest" => output!(c.manifest(id()?).await),
        "retain" => output!(c.retain(id()?).await),
        "release" => output!(c.release(id()?).await),
        "set_stable" => output!(c.set_stable(id()?).await),
        "create_upload" => output!(c.create_upload(&serde_json::from_value(v)?).await),
        "upload_status" => output!(c.upload_status(id()?).await),
        "complete_upload" => output!(c.complete_upload(id()?).await),
        "abort_upload" => output!(c.abort_upload(id()?).await),
        "upload_file" => output!(
            c.upload_file(
                v["path"]
                    .as_str()
                    .ok_or_else(|| Error::Invalid("file path".into()))?,
                id()?
            )
            .await
        ),
        "download_to_path" => {
            c.download_to_path(
                v["snapshot"]
                    .as_str()
                    .ok_or_else(|| Error::Invalid("snapshot".into()))?,
                v["sample"]
                    .as_str()
                    .ok_or_else(|| Error::Invalid("sample".into()))?,
                v["field"]
                    .as_str()
                    .ok_or_else(|| Error::Invalid("field".into()))?,
                v["path"].as_str().ok_or_else(|| Error::Invalid("path".into()))?,
            )
            .await?;
            Ok("null".into())
        }
        "start_read" => output!(c.start_read(&serde_json::from_value(v)?).await),
        "resume_read" => output!(c.resume_read(id()?).await),
        "close_read" => output!(c.close_read(id()?).await),
        "acknowledge" => output!(
            c.acknowledge(id()?, &serde_json::from_value(v["ack"].clone())?)
                .await
        ),
        "progress" => output!(c.progress(id()?).await),
        "reclaim" => output!(c.reclaim(id()?).await),
        "reclaim_status" => output!(c.reclaim_status(id()?).await),
        "list_snapshots" => output!(
            c.list_snapshots(
                v["after"].as_str(),
                usize::try_from(v["limit"].as_u64().unwrap_or(100)).map_err(|_| Error::Limit)?
            )
            .await
        ),
        _ => Err(Error::Invalid("unknown SDK operation".into())),
    }
}
