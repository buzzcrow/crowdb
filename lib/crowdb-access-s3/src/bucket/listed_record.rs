// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Classify rows in the legacy bucket interval by complete persisted identity.

use crate::metadata::{
    BucketNameRecord, MetadataKey, MetadataRecordError, MultipartPartRecord, MultipartSessionRecord,
    ObjectRecord, TenantId,
};

pub(super) fn decode_bucket_row(
    tenant: &TenantId,
    key: &[u8],
    value: &[u8],
) -> Result<Option<BucketNameRecord>, MetadataRecordError> {
    let error = match BucketNameRecord::decode(value) {
        Ok(record) => {
            if record.tenant == *tenant
                && MetadataKey::bucket_name(tenant, &record.name).is_ok_and(|expected| expected == key)
            {
                return Ok(Some(record));
            }
            MetadataRecordError::Invalid
        }
        Err(error) => error,
    };
    if ObjectRecord::decode(value).is_ok_and(|record| {
        MetadataKey::object(tenant, record.bucket_id, &record.key).is_ok_and(|expected| expected == key)
    }) || MultipartSessionRecord::decode_unbound(value).is_ok_and(|record| {
        MetadataKey::multipart_session(tenant, record.bucket_id, &record.upload_id) == key
    }) || MultipartPartRecord::decode_unbound(value).is_ok_and(|record| {
        MetadataKey::multipart_part(tenant, record.bucket_id, &record.upload_id, record.number)
            .is_ok_and(|expected| expected == key)
            || MetadataKey::multipart_part_generation(
                tenant,
                record.bucket_id,
                &record.upload_id,
                record.number,
                record.revision,
            )
            .is_ok_and(|expected| expected == key)
    }) || MetadataKey::matches_multipart_index(tenant, key, value)
    {
        return Ok(None);
    }
    Err(error)
}
