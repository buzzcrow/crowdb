// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    error::{failure, NativeError},
    lease::Lease,
    runtime::Resources,
};
use crowdb_sdk_native::{
    dataset::{BatchBody, FieldBuffer, ReadBatch},
    Cancellation, Error, Result,
};
use pyo3::prelude::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{mpsc, oneshot};

type Pull = oneshot::Sender<Result<Option<FieldBuffer>>>;

#[pyclass(frozen, module = "crowdb_dataset_native")]
pub struct Batch {
    resources: Arc<Resources>,
    sender: mpsc::Sender<Pull>,
    cancel: Cancellation,
    pulling: AtomicBool,
    #[pyo3(get)]
    header: String,
}

impl Batch {
    pub fn new(resources: Arc<Resources>, read: ReadBatch, cancel: Cancellation) -> Self {
        let h = read.header;
        let fields: Vec<_> = h
            .fields
            .iter()
            .map(|f| {
                serde_json::json!({"sample_id":f.sample_id,
            "name":f.name,"state":format!("{:?}", f.state).to_lowercase(),"length":f.length,"md5":f.md5})
            })
            .collect();
        let header = serde_json::json!({"snapshot":h.snapshot,"sample_ids":h.sample_ids,"fields":fields,
            "next":h.next,"end":h.end})
        .to_string();
        let (sender, receiver) = mpsc::channel(1);
        resources
            .executor()
            .spawn(produce(read.body, receiver, cancel.clone()));
        Self {
            resources,
            sender,
            cancel,
            pulling: AtomicBool::new(false),
            header,
        }
    }
}

struct PullGuard<'a>(&'a AtomicBool);
impl Drop for PullGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[pymethods]
impl Batch {
    /// Pulls one retained buffer; None certifies verified EOF, never acknowledgement.
    fn next(&self, py: Python<'_>) -> PyResult<Option<Lease>> {
        self.resources.check_process()?;
        if self.pulling.swap(true, Ordering::AcqRel) {
            return Err(NativeError::new_err("one consumer may pull a batch at a time"));
        }
        let _guard = PullGuard(&self.pulling);
        py.detach(|| {
            self.resources.executor().block_on(async {
                if self.cancel.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                let (sender, receiver) = oneshot::channel();
                self.sender.send(sender).await.map_err(|_| Error::Cancelled)?;
                receiver.await.map_err(|_| Error::Cancelled)?.map(|item| {
                    item.map(|f| Lease {
                        field: f.field,
                        buffer: f.buffer,
                    })
                })
            })
        })
        .map_err(|e| failure(&e))
    }

    fn close(&self) {
        self.cancel.cancel();
    }
    fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn produce(mut body: BatchBody, mut receiver: mpsc::Receiver<Pull>, cancel: Cancellation) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => { body.cancel(); return; }
            request = receiver.recv() => {
                let Some(reply) = request else { return; };
                let result = tokio::select! {
                    biased;
                    () = cancel.cancelled() => { body.cancel(); Err(Error::Cancelled) }
                    result = body.next() => result,
                };
                if reply.send(result).is_err() { return; }
            }
        }
    }
}
