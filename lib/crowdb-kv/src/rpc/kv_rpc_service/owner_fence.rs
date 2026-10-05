// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::cluster::group_owner_fence::OWNER_PREFIX;
use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::{FBKvBatchWriteRequest, KvBatchItem, KvErrorCode, KvResponse};
use bytes::Bytes;

pub(super) async fn write(
    store: &PxKvStore,
    request: &FBKvBatchWriteRequest<'_>,
    items: &[KvBatchItem],
) -> Option<KvResponse> {
    let fence = request.owner_fence()?;
    let key = Bytes::copy_from_slice(fence.key().map_or(&[], |key| key.bytes()));
    let expected = Bytes::copy_from_slice(fence.expected_value().map_or(&[], |value| value.bytes()));
    let condition = fence.record_precondition().map(|record| {
        (
            Bytes::copy_from_slice(record.key().map_or(&[], |key| key.bytes())),
            record.expected_revision(),
        )
    });
    if request.version() != 2
        || request.client_id() == 0
        || !key.starts_with(OWNER_PREFIX)
        || expected.is_empty()
        || items.iter().any(|item| item.key.starts_with(OWNER_PREFIX))
        || condition
            .as_ref()
            .is_some_and(|(record, _)| !items.iter().any(|item| item.key == *record))
    {
        return Some(KvResponse::cas_error(
            KvErrorCode::KvErrorCasFailed,
            0,
            "invalid read-only DiskGroup ownership fence",
            request.request_id(),
            request.request_create_ms(),
        ));
    }
    Some(
        store
            .propose_owner_write_and_respond(
                request.group_id(),
                PxKvStore::encode_kv_batch_items(items),
                key,
                expected,
                condition,
                request.client_id(),
                request.seq(),
                request.request_id(),
                request.request_create_ms(),
            )
            .await,
    )
}
