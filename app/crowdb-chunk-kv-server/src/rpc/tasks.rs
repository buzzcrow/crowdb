// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! One atomic gate covers handler admission through the last owned request tail.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;
const CLOSED: usize = 1 << (usize::BITS - 1);
#[derive(Default)]
pub(super) struct RpcTasks {
    state: AtomicUsize,
    changed: Notify,
}
pub(super) struct RpcTask {
    owner: Arc<RpcTasks>,
}
impl RpcTasks {
    pub fn enter(self: &Arc<Self>) -> Option<RpcTask> {
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            if observed & CLOSED != 0 || observed == CLOSED - 1 {
                return None;
            }
            match self.state.compare_exchange_weak(
                observed,
                observed + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(RpcTask { owner: self.clone() }),
                Err(actual) => observed = actual,
            }
        }
    }
    pub async fn close_and_drain(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
        loop {
            let change = self.changed.notified();
            tokio::pin!(change);
            change.as_mut().enable();
            if self.state.load(Ordering::Acquire) == CLOSED {
                return;
            }
            change.await;
        }
    }
}
impl Drop for RpcTask {
    fn drop(&mut self) {
        if self.owner.state.fetch_sub(1, Ordering::AcqRel) == CLOSED + 1 {
            self.owner.changed.notify_waiters();
        }
    }
}
