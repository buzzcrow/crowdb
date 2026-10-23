// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{dataset, metadata};
use crowdb_sdk_native::{
    buffer::BufferLease,
    dataset::{BatchBody, BatchRequest},
    RuntimeLimits,
};
use std::sync::{atomic::Ordering, Arc};

pub struct TestCleanupFixture {
    pub dataset: dataset::TestFixture,
    pub gate: Arc<metadata::TestMutationGate>,
    pub body: BatchBody,
    pub held: BufferLease,
}

impl TestCleanupFixture {
    pub async fn new() -> Self {
        let gate = Arc::new(metadata::TestMutationGate::default());
        let metadata = metadata::client_with_gate(Arc::clone(&gate)).await;
        let dataset = dataset::TestFixture::with_metadata(
            RuntimeLimits {
                requests: 1,
                ..RuntimeLimits::default()
            },
            metadata,
        )
        .await;
        let mut read = dataset
            .client
            .read_batch(
                dataset::context(2),
                BatchRequest {
                    snapshot: dataset.snapshot,
                    sample_ids: vec![vec![1]],
                    fields: vec!["value".into()],
                },
            )
            .await
            .unwrap();
        let held = read.body.next().await.unwrap().unwrap().buffer;
        gate.hold.store(true, Ordering::Release);
        Self {
            dataset,
            gate,
            body: read.body,
            held,
        }
    }
}
