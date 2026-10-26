// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Mutex;

use crowdb_chunk_kv_client::{ClientError, Result};
use crowdb_protocol::chunk_kv::PointOperation;
use tokio::sync::oneshot;

pub(crate) struct TestPointBarrier {
    pub(crate) started: oneshot::Receiver<()>,
    pub(crate) resume: oneshot::Sender<()>,
}

#[derive(Clone, Debug)]
pub(crate) struct TestPointEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) mutation: bool,
}

struct TestPause {
    key: Vec<u8>,
    mutation: bool,
    after: bool,
    skip: usize,
    started: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

struct TestGetFailure {
    key: Vec<u8>,
    skip: usize,
}

#[derive(Default)]
struct TestPointState {
    log: Vec<TestPointEntry>,
    pause: Option<TestPause>,
    failure: Option<TestGetFailure>,
    failure_used: bool,
}

impl TestPointState {
    fn observe(&mut self, operation: &PointOperation) -> Result<Option<oneshot::Receiver<()>>> {
        let key = operation.key();
        let mutation = !matches!(operation, PointOperation::Get { .. });
        self.log.push(TestPointEntry {
            key: key.to_vec(),
            mutation,
        });
        if !mutation {
            if let Some(failure) = self.failure.as_mut().filter(|failure| failure.key == key) {
                if failure.skip == 0 {
                    self.failure = None;
                    self.failure_used = true;
                    return Err(ClientError::Transport("injected upload read failure".into()));
                }
                failure.skip -= 1;
            }
        }
        Ok(self.pause_for(operation, false))
    }

    fn pause_for(&mut self, operation: &PointOperation, after: bool) -> Option<oneshot::Receiver<()>> {
        let mutation = !matches!(operation, PointOperation::Get { .. });
        if let Some(pause) = self.pause.as_mut().filter(|pause| {
            pause.key == operation.key() && pause.mutation == mutation && pause.after == after
        }) {
            if pause.skip == 0 {
                let pause = self.pause.take().unwrap();
                let _ = pause.started.send(());
                return Some(pause.resume);
            }
            pause.skip -= 1;
        }
        None
    }
}

#[derive(Default)]
pub(super) struct TestPoints(Mutex<TestPointState>);

impl TestPoints {
    pub(super) fn pause(&self, key: Vec<u8>, mutation: bool, after: bool, skip: usize) -> TestPointBarrier {
        let (started, receiver) = oneshot::channel();
        let (resume, release) = oneshot::channel();
        let mut state = self.0.lock().unwrap();
        assert!(state.pause.is_none());
        state.pause = Some(TestPause {
            key,
            mutation,
            after,
            skip,
            started,
            resume: release,
        });
        TestPointBarrier {
            started: receiver,
            resume,
        }
    }

    pub(super) fn fail_get(&self, key: Vec<u8>, skip: usize) {
        let mut state = self.0.lock().unwrap();
        assert!(state.failure.is_none());
        state.failure = Some(TestGetFailure { key, skip });
        state.failure_used = false;
    }

    pub(super) fn get_fault_consumed(&self) -> bool {
        let state = self.0.lock().unwrap();
        state.failure.is_none() && state.failure_used
    }

    pub(super) fn log(&self) -> Vec<TestPointEntry> {
        self.0.lock().unwrap().log.clone()
    }

    pub(super) async fn before(&self, operation: &PointOperation) -> Result<()> {
        let resume = {
            let mut state = self.0.lock().unwrap();
            state.observe(operation)?
        };
        Self::wait(resume).await
    }

    pub(super) async fn after(&self, operation: &PointOperation) -> Result<()> {
        let resume = {
            let mut state = self.0.lock().unwrap();
            state.pause_for(operation, true)
        };
        Self::wait(resume).await
    }

    async fn wait(resume: Option<oneshot::Receiver<()>>) -> Result<()> {
        if let Some(resume) = resume {
            resume
                .await
                .map_err(|_| ClientError::Transport("test barrier was cancelled".into()))?;
        }
        Ok(())
    }
}
