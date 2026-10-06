// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::{FBKvBatchWriteRequest, KvBatchItem, KvErrorCode, KvResponse};
use bytes::Bytes;
use crowdb_protocol::owner_fence::{is_owner_fence_key, valid_owner_fence, valid_owner_fence_scope};

pub(super) fn valid_conditional_put(
    group_id: u64,
    client_id: u64,
    key: &[u8],
    condition: &[u8],
    value: &[u8],
) -> bool {
    client_id != 0
        && key == condition
        && valid_owner_fence_scope(key, group_id)
        && (!is_owner_fence_key(key)
            || key.starts_with(b"/diskdb/ownership-fence/")
            || valid_owner_fence(key, value))
}

pub(super) fn valid_conditional_batch(
    group_id: u64,
    client_id: u64,
    key: &Bytes,
    items: &[KvBatchItem],
) -> bool {
    client_id != 0
        && items.iter().any(|item| item.key == *key)
        && valid_owner_fence_scope(key, group_id)
        && items.iter().all(|item| {
            !is_owner_fence_key(&item.key)
                || (item.key == *key
                    && (item.key.starts_with(b"/diskdb/ownership-fence/")
                        || (!item.is_delete && valid_owner_fence(&item.key, &item.value))))
        })
}

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
        || !valid_owner_fence(&key, &expected)
        || !valid_owner_fence_scope(&key, request.group_id())
        || items.iter().any(|item| is_owner_fence_key(&item.key))
        || condition
            .as_ref()
            .is_some_and(|(record, _)| !items.iter().any(|item| item.key == *record))
    {
        return Some(KvResponse::cas_error(
            KvErrorCode::KvErrorCasFailed,
            0,
            "invalid read-only ownership fence",
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
