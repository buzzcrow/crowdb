// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_tree_ffi::{ArmedApply, BatchOp as NativeOp, Crowdbtree, CtError, ExtOp};

use crate::kv::{AdmittedApply, ApplyAdmissionError, ApplyAdmissionResult, Batch, KVFuture, Op};

struct NativeAdmission(ArmedApply);

impl AdmittedApply for NativeAdmission {
    fn apply(self: Box<Self>, slot: u64) -> KVFuture<Result<(), String>> {
        let operation = self.0;
        KVFuture::Pending(Box::pin(async move {
            operation.apply(slot).await.map_err(|error| error.to_string())
        }))
    }
}

pub(super) fn prepare(tree: &Arc<Crowdbtree>, payload: &Bytes) -> ApplyAdmissionResult {
    let batch =
        Batch::try_decode(payload).map_err(|error| ApplyAdmissionError::Rejected(error.to_owned()))?;
    let operations: Vec<_> = batch
        .ops
        .iter()
        .map(|entry| match &entry.op {
            Op::Put(value) => NativeOp::Put {
                key: &entry.key,
                value,
            },
            Op::Delete => NativeOp::Delete { key: &entry.key },
        })
        .collect();
    let prepared = tree.prepare_proposal(&operations).map_err(admission_error)?;
    let armed = prepared.arm().map_err(admission_error)?;
    Ok(Some(Box::new(NativeAdmission(armed))))
}

fn admission_error(error: CtError) -> ApplyAdmissionError {
    match error {
        CtError::Unavailable => ApplyAdmissionError::Busy,
        other => ApplyAdmissionError::Rejected(format!("native apply admission: {other}")),
    }
}

pub(super) fn replay(tree: Arc<Crowdbtree>, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>> {
    let operations = batch
        .ops
        .iter()
        .map(|entry| match &entry.op {
            Op::Put(value) => ExtOp::Put {
                key: entry.key.clone(),
                value: value.clone(),
            },
            Op::Delete => ExtOp::Delete {
                key: entry.key.clone(),
            },
        })
        .collect();
    KVFuture::Pending(Box::pin(async move {
        tree.apply_replay(slot, operations)
            .await
            .map_err(|error| error.to_string())
    }))
}

pub(super) fn replay_empty(tree: Arc<Crowdbtree>, slot: u64) -> KVFuture<Result<(), String>> {
    KVFuture::Pending(Box::pin(async move {
        tree.apply_replay(slot, Vec::new())
            .await
            .map_err(|error| error.to_string())
    }))
}
