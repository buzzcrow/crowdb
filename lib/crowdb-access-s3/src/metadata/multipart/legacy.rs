// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only compatibility with historical multipart completion receipts.

use super::{BucketId, MultipartPhase, MultipartRecordError, MultipartSessionRecord, PendingPartMutation};
use crowdb_access_multipart::SelectedPart;
use serde::Deserialize;

#[derive(Deserialize)]
struct LegacySession {
    bucket_id: BucketId,
    object_key: Vec<u8>,
    upload_id: [u8; 16],
    revision: u64,
    phase: MultipartPhase,
    created_ms: u64,
    expires_ms: u64,
    content_type: String,
    attributes: Vec<u8>,
    max_parts: u16,
    max_part_bytes: u64,
    max_object_bytes: u64,
    max_staged_bytes: u64,
    part_count: u16,
    staged_bytes: u64,
    pending: Option<PendingPartMutation>,
    selection: Option<Vec<SelectedPart>>,
    completion_request_digest: Option<[u8; 32]>,
    publication_ms: Option<u64>,
    object_predecessor: Option<LegacyPredecessor>,
    etag: Option<String>,
}

// One-field structs preserve bincode's historical inner Option encoding.
#[derive(Deserialize)]
struct LegacyPredecessor {
    _digest: Option<[u8; 32]>,
}

pub(super) fn decode_session(bytes: &[u8]) -> Result<MultipartSessionRecord, MultipartRecordError> {
    let old: LegacySession = super::decode(*b"S3MS\x03", bytes)?;
    let valid = match old.phase {
        MultipartPhase::Open => {
            old.selection.is_none()
                && old.completion_request_digest.is_none()
                && old.publication_ms.is_none()
                && old.object_predecessor.is_none()
                && old.etag.is_none()
        }
        MultipartPhase::Completing => {
            old.selection.is_some()
                && old.completion_request_digest.is_some()
                && old.publication_ms.is_none()
                && old.object_predecessor.is_none()
                && old.etag.is_none()
        }
        MultipartPhase::Publishing | MultipartPhase::Published => {
            old.selection.is_some()
                && old.completion_request_digest.is_some()
                && old
                    .publication_ms
                    .is_some_and(|time| time >= old.created_ms && time < old.expires_ms)
                && old.object_predecessor.is_some()
                && old
                    .etag
                    .as_ref()
                    .is_some_and(|etag| valid_etag(etag, old.selection.as_ref().map_or(0, Vec::len)))
        }
        MultipartPhase::Aborted => old.etag.is_none(),
        MultipartPhase::Conflicted => false,
    };
    if !valid {
        return Err(MultipartRecordError::Invalid);
    }
    let phase = match old.phase {
        MultipartPhase::Completing | MultipartPhase::Published => MultipartPhase::Publishing,
        other => other,
    };
    Ok(MultipartSessionRecord {
        bucket_id: old.bucket_id,
        object_key: old.object_key,
        upload_id: old.upload_id,
        revision: old.revision,
        phase,
        created_ms: old.created_ms,
        expires_ms: old.expires_ms,
        content_type: old.content_type,
        attributes: old.attributes,
        max_parts: old.max_parts,
        max_part_bytes: old.max_part_bytes,
        max_object_bytes: old.max_object_bytes,
        max_staged_bytes: old.max_staged_bytes,
        part_count: old.part_count,
        staged_bytes: old.staged_bytes,
        pending: old.pending,
        selection: old.selection,
        legacy_observation: Some((old.revision, bytes.to_vec())),
    })
}

fn valid_etag(etag: &str, selected_count: usize) -> bool {
    let Some((digest, count)) = etag.split_once('-') else {
        return false;
    };
    digest.len() == 32
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && count
            .parse::<u16>()
            .is_ok_and(|count| count > 0 && usize::from(count) == selected_count)
}
