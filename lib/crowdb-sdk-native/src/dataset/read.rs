// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod body;
mod field;
pub use body::{BatchBody, FieldBuffer};

use super::DatasetClient;
use crate::runtime::RequestLease;
use crate::{OperationContext, Result};
use crowdb_access_dataset::{DatasetReads, FieldLocator, ReadCancellation, SampleView, SnapshotId};
use std::sync::Arc;

pub struct BatchRequest {
    pub snapshot: SnapshotId,
    pub sample_ids: Vec<Vec<u8>>,
    pub fields: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldState {
    Value,
    Missing,
    Tombstone,
}

#[derive(Clone, Debug)]
pub struct BatchField {
    pub sample_id: Vec<u8>,
    pub name: String,
    pub state: FieldState,
    pub length: u64,
    pub md5: Option<[u8; 16]>,
}

#[derive(Clone, Debug)]
pub struct BatchHeader {
    pub snapshot: SnapshotId,
    pub sample_ids: Vec<Vec<u8>>,
    pub fields: Vec<BatchField>,
    pub next: Option<String>,
    pub end: bool,
}

pub struct ReadBatch {
    pub header: Arc<BatchHeader>,
    pub body: BatchBody,
}

impl DatasetClient {
    /// Reads an explicit immutable snapshot/projection without creating a cursor.
    /// # Errors
    /// Returns missing data, invalid projection, admission or storage failure.
    pub async fn read_batch(&self, context: OperationContext, batch: BatchRequest) -> Result<ReadBatch> {
        let request = self.runtime.resources.admit(context)?;
        self.batch(request, batch, None, true).await
    }

    /// Fetches a caller-owned continuation. Retrying it replays the same batch.
    /// # Errors
    /// Returns invalid continuations, deletion admission, bounded-work or storage errors.
    pub async fn next_batch(&self, context: OperationContext, cursor: &str) -> Result<ReadBatch> {
        let request = self.runtime.resources.admit(context)?;
        let reads = DatasetReads::new(Arc::clone(&self.authority));
        let batch = request
            .run(async {
                Ok(reads
                    .next(&self.identity, cursor, &ReadCancellation::new())
                    .await?)
            })
            .await?;
        let end = batch.next.is_none();
        self.batch(
            request,
            BatchRequest {
                snapshot: batch.snapshot,
                sample_ids: batch.ids,
                fields: batch.fields,
            },
            batch.next,
            end,
        )
        .await
    }

    async fn batch(
        &self,
        request: RequestLease,
        batch: BatchRequest,
        next: Option<String>,
        end: bool,
    ) -> Result<ReadBatch> {
        let samples = if batch.sample_ids.is_empty() {
            request
                .run(async {
                    Ok(self
                        .authority
                        .check_snapshot_readable(&self.identity, batch.snapshot)
                        .await?)
                })
                .await?;
            Vec::new()
        } else {
            request
                .run(async {
                    Ok(self
                        .authority
                        .resolve_inspection_batch_locators(
                            &self.identity,
                            batch.snapshot,
                            &batch.sample_ids,
                            &batch.fields,
                            &ReadCancellation::new(),
                        )
                        .await?)
                })
                .await?
        };
        let (fields, locators) = descriptors(&batch, samples);
        let header = Arc::new(BatchHeader {
            snapshot: batch.snapshot,
            sample_ids: batch.sample_ids,
            fields,
            next,
            end,
        });
        let body = BatchBody::new(
            Arc::clone(&header),
            locators,
            request,
            Arc::clone(&self.runtime.resources),
            Arc::clone(&self.uploads),
        );
        Ok(ReadBatch { header, body })
    }
}

fn descriptors(
    batch: &BatchRequest,
    samples: Vec<SampleView>,
) -> (Vec<BatchField>, Vec<(usize, FieldLocator)>) {
    let mut fields = Vec::new();
    let mut locators = Vec::new();
    for mut sample in samples {
        for name in &batch.fields {
            let locator = sample.fields.remove(name);
            let (state, length, md5) = match &locator {
                Some(FieldLocator::Inline { value, md5 }) => {
                    (FieldState::Value, value.len() as u64, Some(*md5))
                }
                Some(FieldLocator::Chunk { length, md5, .. }) => (FieldState::Value, *length, Some(*md5)),
                Some(FieldLocator::Tombstone) => (FieldState::Tombstone, 0, None),
                None => (FieldState::Missing, 0, None),
            };
            if let Some(value @ (FieldLocator::Inline { .. } | FieldLocator::Chunk { .. })) = locator {
                locators.push((fields.len(), value));
            }
            fields.push(BatchField {
                sample_id: sample.sample_id.clone(),
                name: name.clone(),
                state,
                length,
                md5,
            });
        }
    }
    (fields, locators)
}
