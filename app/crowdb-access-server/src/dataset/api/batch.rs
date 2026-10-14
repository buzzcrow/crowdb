// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{error::ApiError, read_json, snapshot, ApiResult};
use crate::dataset::{unix_seconds, DatasetBody, DatasetHttpService};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{DatasetIdentity, FieldLocator, SampleView, SnapshotId};
use futures::stream;
use http_body_util::{BodyExt, StreamBody};
use hyper::{
    body::{Bytes, Frame, Incoming},
    Request, Response,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Deserialize)]
pub(super) struct BatchRequest {
    pub snapshot: String,
    pub sample_ids: Vec<String>,
    pub fields: Vec<String>,
}
#[derive(Serialize)]
struct BatchField {
    sample: String,
    field: String,
    state: &'static str,
    length: u64,
    md5: Option<String>,
}
#[derive(Serialize)]
struct Header {
    snapshot: String,
    sample_ids: Vec<String>,
    fields: Vec<BatchField>,
    next: Option<String>,
    end: bool,
}

impl DatasetHttpService {
    pub(super) async fn batch_route(
        &self,
        identity: &DatasetIdentity,
        request: Request<Incoming>,
    ) -> ApiResult {
        let request: BatchRequest = read_json(request).await?;
        self.batch(identity, request, None, true, None).await
    }
    pub(super) async fn batch(
        &self,
        identity: &DatasetIdentity,
        request: BatchRequest,
        next: Option<String>,
        end: bool,
        session: Option<crowdb_access_dataset::OperationId>,
    ) -> ApiResult {
        let id = snapshot(&request.snapshot)?;
        let ids = request
            .sample_ids
            .iter()
            .map(|value| {
                let bytes = STANDARD.decode(value).map_err(|_| ApiError::invalid())?;
                if bytes.is_empty() || bytes.len() > 1024 || STANDARD.encode(&bytes) != *value {
                    return Err(ApiError::invalid());
                }
                Ok(bytes)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let authority = self.reads.authority();
        let handle = crowdb_access_dataset::OperationId::random();
        authority
            .touch_read_handle(identity, id, handle, unix_seconds(), 60)
            .await?;
        let lease = Arc::new(Lease {
            authority: authority.clone(),
            identity: identity.clone(),
            snapshot: id,
            handle,
            session,
            failed: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&lease);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(std::time::Duration::from_secs(15));
            loop {
                timer.tick().await;
                let Some(lease) = weak.upgrade() else { break };
                let result = async {
                    if let Some(session) = lease.session {
                        crowdb_access_dataset::DatasetSessions::new(lease.authority.clone())
                            .resume(&lease.identity, session, unix_seconds())
                            .await?;
                    }
                    lease
                        .authority
                        .touch_read_handle(&lease.identity, lease.snapshot, lease.handle, unix_seconds(), 60)
                        .await
                }
                .await;
                if let Err(error) = result {
                    lease.failed.store(true, Ordering::Release);
                    tracing::error!(%error,"Dataset stream lease renewal failed; delivery cancelled");
                    break;
                }
            }
        });
        let samples = if ids.is_empty() {
            Vec::new()
        } else {
            authority
                .resolve_batch_locators(identity, id, &ids, &request.fields)
                .await?
        };
        let header = make_header(id, &request.sample_ids, &request.fields, &samples, next, end);
        let encoded = serde_json::to_vec(&header).map_err(|_| ApiError::integrity())?;
        if encoded.len() > 8 * 1024 * 1024 {
            return Err(ApiError::too_large());
        }
        let mut prefix = Vec::from(b"CDBDS001".as_slice());
        prefix.extend_from_slice(&frame(1, &Bytes::from(encoded))?);
        let locators = samples
            .into_iter()
            .flat_map(|sample| {
                request
                    .fields
                    .iter()
                    .filter_map(move |name| sample.fields.get(name).cloned())
                    .filter(|v| !matches!(v, FieldLocator::Tombstone))
            })
            .collect();
        let state = BatchState {
            uploads: self.uploads.clone(),
            locators,
            index: 0,
            part: 0,
            pending: Some(Bytes::from(prefix)),
            finished: false,
            lease,
        };
        Ok(Response::builder()
            .header("content-type", "application/vnd.crowdb.dataset-batch.v1")
            .body(stream_body(state))
            .expect("static response"))
    }
}
fn make_header(
    snapshot: SnapshotId,
    ids: &[String],
    fields: &[String],
    samples: &[SampleView],
    next: Option<String>,
    end: bool,
) -> Header {
    let mut values = Vec::new();
    for (sample, id) in samples.iter().zip(ids) {
        for name in fields {
            let (state, length, md5) = match sample.fields.get(name) {
                Some(FieldLocator::Inline { value, md5 }) => {
                    ("value", value.len() as u64, Some(hex::encode(md5)))
                }
                Some(FieldLocator::Chunk { length, md5, .. }) => ("value", *length, Some(hex::encode(md5))),
                Some(FieldLocator::Tombstone) => ("tombstone", 0, None),
                None => ("missing", 0, None),
            };
            values.push(BatchField {
                sample: id.clone(),
                field: name.clone(),
                state,
                length,
                md5,
            });
        }
    }
    Header {
        snapshot: snapshot.to_string(),
        sample_ids: ids.to_vec(),
        fields: values,
        next,
        end,
    }
}
struct Lease {
    authority: Arc<crowdb_access_dataset::DatasetAuthority>,
    identity: DatasetIdentity,
    snapshot: SnapshotId,
    handle: crowdb_access_dataset::OperationId,
    session: Option<crowdb_access_dataset::OperationId>,
    failed: AtomicBool,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let authority = self.authority.clone();
        let identity = self.identity.clone();
        let snapshot = self.snapshot;
        let handle = self.handle;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
  if let Err(error)=authority.release_read_handle(&identity,snapshot,handle).await {tracing::error!(%error,"Dataset read release failed; lease expiry protects disconnected reads");}
 });
        }
    }
}
struct BatchState {
    uploads: Option<Arc<crowdb_access_dataset::DatasetUploads>>,
    locators: Vec<FieldLocator>,
    index: usize,
    part: u32,
    pending: Option<Bytes>,
    finished: bool,
    lease: Arc<Lease>,
}
fn stream_body(state: BatchState) -> DatasetBody {
    let frames = stream::unfold(state, |mut state| async move {
        if state.finished {
            return None;
        }
        let result = state.next().await;
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => {
                state.finished = true;
                let record = serde_json::json!({"code":error.code(),"message":error.code(),"request_id":crowdb_access_dataset::OperationId::random().to_string(),"retryable":error.retryable()});
                frame(4, &Bytes::from(record.to_string())).expect("bounded error frame")
            }
        };
        let owner = FrameOwner {
            bytes,
            lease: state.lease.clone(),
        };
        Some((
            Ok::<_, crate::dataset::BoxError>(Frame::data(Bytes::from_owner(owner))),
            state,
        ))
    });
    StreamBody::new(frames).boxed_unsync()
}
impl BatchState {
    async fn next(&mut self) -> Result<Bytes, ApiError> {
        if self.lease.failed.load(Ordering::Acquire) {
            return Err(crowdb_access_dataset::AuthorityError::from(
                crowdb_access_dataset::DatasetError::ReadCancelled,
            )
            .into());
        }
        if let Some(session) = self.lease.session {
            crowdb_access_dataset::DatasetSessions::new(self.lease.authority.clone())
                .check_open(&self.lease.identity, session)
                .await?;
        }
        if let Some(bytes) = self.pending.take() {
            return Ok(bytes);
        }
        let Some(locator) = self.locators.get(self.index) else {
            self.finished = true;
            return frame(0, &Bytes::new());
        };
        match locator {
            FieldLocator::Inline { value, md5 } => {
                if self.part > 0 {
                    return self.end_field();
                }
                if value.is_empty() {
                    return self.end_field();
                }
                let bytes = Bytes::copy_from_slice(value);
                let digest = *md5;
                self.part = 1;
                self.data_frame(bytes, digest)
            }
            FieldLocator::Chunk { location, length, .. } => {
                let uploads = self.uploads.as_ref().ok_or_else(ApiError::unsupported)?;
                let metadata = uploads.metadata(location).await?;
                if u64::from(self.part) >= length.div_ceil(u64::from(metadata.part_size)) {
                    return self.end_field();
                }
                let (bytes, digest) = uploads.read_part_verified(location, self.part).await?;
                self.part += 1;
                self.data_frame(bytes, digest)
            }
            FieldLocator::Tombstone => Err(ApiError::integrity()),
        }
    }
    fn data_frame(&mut self, bytes: Bytes, digest: [u8; 16]) -> Result<Bytes, ApiError> {
        let length = u32::try_from(16 + bytes.len()).map_err(|_| ApiError::too_large())?;
        let mut header = Vec::with_capacity(21);
        header.push(2);
        header.extend_from_slice(&length.to_be_bytes());
        header.extend_from_slice(&digest);
        self.pending = Some(bytes);
        Ok(Bytes::from(header))
    }
    fn end_field(&mut self) -> Result<Bytes, ApiError> {
        self.index += 1;
        self.part = 0;
        frame(3, &Bytes::new())
    }
}
fn frame(kind: u8, bytes: &Bytes) -> Result<Bytes, ApiError> {
    let length = u32::try_from(bytes.len()).map_err(|_| ApiError::too_large())?;
    let mut output = Vec::with_capacity(5 + bytes.len());
    output.push(kind);
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(Bytes::from(output))
}
struct FrameOwner {
    bytes: Bytes,
    lease: Arc<Lease>,
}
impl AsRef<[u8]> for FrameOwner {
    fn as_ref(&self) -> &[u8] {
        let _ = &self.lease;
        &self.bytes
    }
}
