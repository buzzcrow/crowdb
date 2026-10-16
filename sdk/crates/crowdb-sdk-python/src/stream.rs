// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{failure, InputBudget, Resources};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use crowdb_sdk_dataset::{wire::BatchEvent, Error};
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyBytes};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::{sync::Notify, task::AbortHandle};

struct HeaderText {
    text: String,
    _credit: crowdb_sdk_core::BufferPermit,
}
enum Event {
    Header(HeaderText),
    Data(Bytes),
    FieldEnd,
    End,
    Failure(String),
}
struct Queue {
    slots: [ArcSwapOption<Event>; 2],
    head: AtomicUsize,
    tail: AtomicUsize,
    closed: AtomicBool,
    cancelled: AtomicBool,
    reading: AtomicBool,
    available: Notify,
    space: Notify,
}
impl Queue {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| ArcSwapOption::empty()),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            reading: AtomicBool::new(false),
            available: Notify::new(),
            space: Notify::new(),
        }
    }
    async fn push(&self, event: Event) -> bool {
        loop {
            let changed = self.space.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.cancelled.load(Ordering::Acquire) {
                return false;
            }
            let tail = self.tail.load(Ordering::Acquire);
            if tail.wrapping_sub(self.head.load(Ordering::Acquire)) < self.slots.len() {
                self.slots[tail % 2].store(Some(Arc::new(event)));
                self.tail.store(tail.wrapping_add(1), Ordering::Release);
                self.available.notify_one();
                return true;
            }
            changed.await;
        }
    }
    async fn pop(&self) -> Option<Arc<Event>> {
        loop {
            let changed = self.available.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.cancelled.load(Ordering::Acquire) {
                return None;
            }
            let head = self.head.load(Ordering::Acquire);
            if head != self.tail.load(Ordering::Acquire) {
                let result = self.slots[head % 2].swap(None);
                self.head.store(head.wrapping_add(1), Ordering::Release);
                self.space.notify_one();
                return result;
            }
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            changed.await;
        }
    }
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        for slot in &self.slots {
            slot.store(None);
        }
        self.available.notify_waiters();
        self.space.notify_waiters();
    }
}
struct ProducerGuard(Arc<Queue>);
impl Drop for ProducerGuard {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        if self.0.cancelled.load(Ordering::Acquire) {
            self.0.cancel();
        }
        self.0.available.notify_waiters();
    }
}
struct ReadingGuard<'a>(&'a AtomicBool);
impl Drop for ReadingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
#[pyclass(frozen)]
pub struct NativeStream {
    resources: Arc<Resources>,
    queue: Arc<Queue>,
    abort: Option<AbortHandle>,
}
impl NativeStream {
    pub(super) fn failed(resources: Arc<Resources>, error: &Error) -> Self {
        let queue = Arc::new(Queue::new());
        queue.slots[0].store(Some(Arc::new(Event::Failure(failure(error)))));
        queue.tail.store(1, Ordering::Release);
        queue.closed.store(true, Ordering::Release);
        Self {
            resources,
            queue,
            abort: None,
        }
    }
    pub(super) fn spawn(resources: Arc<Resources>, kind: String, args: String, budget: InputBudget) -> Self {
        let queue = Arc::new(Queue::new());
        let producer = ProducerGuard(queue.clone());
        let client = resources.client();
        let task = resources.runtime().spawn(async move {
            let guard = producer;
            let _budget = budget;
            let result = async {
                let mut stream = match kind.as_str() {
                    "batch" => client.stream_batch(&serde_json::from_str(&args)?).await?,
                    "next" => client.next_batch(&args).await?,
                    "v2-next" => client.control().next_batch(&args.parse()?).await?,
                    _ => return Err(Error::Invalid("stream operation".into())),
                };
                while let Some(event) = stream.next_event().await? {
                    let event = match event {
                        BatchEvent::Header(h) => {
                            let credit =
                                client.reserve_transfer(crowdb_sdk_dataset::wire::MAX_HEADER_FRAME)?;
                            Event::Header(HeaderText {
                                text: serde_json::to_string(h.as_ref())?,
                                _credit: credit,
                            })
                        }
                        BatchEvent::Data(b) => Event::Data(b),
                        BatchEvent::FieldEnd => Event::FieldEnd,
                        BatchEvent::End => Event::End,
                    };
                    if !guard.0.push(event).await {
                        break;
                    }
                }
                Ok::<_, Error>(())
            }
            .await;
            if let Err(error) = result {
                guard.0.push(Event::Failure(failure(&error))).await;
            }
        });
        Self {
            resources,
            queue,
            abort: Some(task.abort_handle()),
        }
    }
}
#[pymethods]
impl NativeStream {
    fn next(&self, py: Python<'_>) -> PyResult<Option<(String, Py<PyAny>)>> {
        if self.resources.pid != std::process::id() {
            return Err(PyRuntimeError::new_err("inherited stream cannot be used"));
        }
        if self
            .queue
            .reading
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(PyRuntimeError::new_err(
                "a stream allows one consumer; use independent streams for concurrency",
            ));
        }
        let _reading = ReadingGuard(&self.queue.reading);
        let event = py.detach(|| self.resources.runtime().block_on(self.queue.pop()));
        Ok(match event.as_deref() {
            Some(Event::Header(h)) => Some((
                "header".into(),
                h.text.as_str().into_pyobject(py)?.into_any().unbind(),
            )),
            Some(Event::Data(b)) => Some(("data".into(), PyBytes::new(py, b).into_any().unbind())),
            Some(Event::FieldEnd) => Some(("field_end".into(), py.None())),
            Some(Event::End) => Some(("end".into(), py.None())),
            Some(Event::Failure(e)) => Some(("error".into(), e.into_pyobject(py)?.into_any().unbind())),
            None => None,
        })
    }
    fn cancel(&self) {
        if self.resources.pid == std::process::id() {
            self.queue.cancel();
            if let Some(abort) = &self.abort {
                abort.abort();
            }
        }
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if self.resources.pid != std::process::id() {
            return Err(PyRuntimeError::new_err("inherited stream cannot be used"));
        }
        self.cancel();
        py.detach(|| {
            self.resources.runtime().block_on(async {
                loop {
                    let closed = self.queue.available.notified();
                    tokio::pin!(closed);
                    closed.as_mut().enable();
                    if self.queue.closed.load(Ordering::Acquire) {
                        break;
                    }
                    closed.await;
                }
            });
        });
        Ok(())
    }
}
impl Drop for NativeStream {
    fn drop(&mut self) {
        if self.resources.pid == std::process::id() {
            self.queue.cancel();
            if let Some(abort) = &self.abort {
                abort.abort();
            }
        }
    }
}
