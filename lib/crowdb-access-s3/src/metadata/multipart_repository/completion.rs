// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Freeze the exact selected part generations before object publication.

use crowdb_access_multipart::{
    live_at, next_revision, validate_selected_parts, MultipartComposer, SelectedPart,
};
use sha2::{Digest as _, Sha256};

use super::{MultipartPhase, MultipartRepository, MultipartRepositoryError, MultipartSessionRecord};

const MIN_NONFINAL_PART_BYTES: u64 = 5 * 1024 * 1024;

/// One S3 `CompleteMultipartUpload` part reference in request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionPart {
    pub number: u16,
    pub etag: String,
}

impl MultipartRepository {
    /// Validates and freezes an ordered part selection under the session CAS.
    ///
    /// The part records can still race with this phase write. Publication
    /// rechecks every recorded digest and refuses changed generations.
    ///
    /// # Errors
    /// Rejects missing, duplicate, undersized or mismatched parts and
    /// conflicting/expired uploads. No object is published by this step.
    pub async fn freeze_completion(
        &self,
        session: &MultipartSessionRecord,
        requested: &[CompletionPart],
        now_ms: u64,
    ) -> Result<Option<MultipartSessionRecord>, MultipartRepositoryError> {
        let request_digest = request_digest(requested)?;
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        if matches!(
            current.phase,
            MultipartPhase::Publishing | MultipartPhase::Published
        ) && current.completion_request_digest == Some(request_digest)
        {
            return Ok(Some(current));
        }
        if current != *session
            || current.phase != MultipartPhase::Open
            || !live_at(current.created_ms, current.expires_ms, now_ms)
            || requested.is_empty()
            || requested.len() > usize::from(current.max_parts)
        {
            return Err(MultipartRepositoryError::Conflict);
        }
        let mut composer = MultipartComposer::new(current.max_object_bytes);
        let mut selection = Vec::with_capacity(requested.len());
        let mut previous = 0;
        for (index, request) in requested.iter().enumerate() {
            if request.number <= previous || request.number > current.max_parts {
                return Err(MultipartRepositoryError::InvalidPart);
            }
            previous = request.number;
            let part = self
                .part(&current, request.number)
                .await?
                .ok_or(MultipartRepositoryError::InvalidPart)?;
            if index + 1 < requested.len() && part.length < MIN_NONFINAL_PART_BYTES {
                return Err(MultipartRepositoryError::EntityTooSmall);
            }
            if !etag_matches(&request.etag, &part.raw_md5) {
                return Err(MultipartRepositoryError::InvalidPart);
            }
            composer
                .push(part.length, part.raw_md5, &part.locations)
                .map_err(|_| MultipartRepositoryError::InvalidPart)?;
            selection.push(SelectedPart {
                number: part.number,
                revision: part.revision,
                digest: part.selection_digest()?,
            });
        }
        validate_selected_parts(&selection, current.max_parts)
            .map_err(|_| MultipartRepositoryError::InvalidPart)?;
        let assembled = composer
            .finish()
            .map_err(|_| MultipartRepositoryError::InvalidPart)?;
        let mut next = current.clone();
        next.revision = next_revision(current.revision).ok_or(MultipartRepositoryError::Conflict)?;
        next.phase = MultipartPhase::Publishing;
        next.part_count =
            u16::try_from(selection.len()).map_err(|_| MultipartRepositoryError::InvalidPart)?;
        next.staged_bytes = assembled.length;
        next.selection = Some(selection);
        next.completion_request_digest = Some(request_digest);
        next.publication_ms = Some(now_ms);
        let object_key = super::MetadataKey::object(&self.tenant, current.bucket_id, &current.object_key)?;
        next.object_predecessor = Some(
            self.store
                .get(object_key)
                .await?
                .map(|value| Sha256::digest(value.value).into()),
        );
        next.etag = Some(assembled.etag);
        Ok(self.exchange(&current, &next).await?.then_some(next))
    }
}

fn etag_matches(value: &str, raw_md5: &[u8; 16]) -> bool {
    parse_raw_md5(value).is_some_and(|actual| &actual == raw_md5)
}

fn request_digest(requested: &[CompletionPart]) -> Result<[u8; 32], MultipartRepositoryError> {
    let mut sha = Sha256::new();
    sha.update(
        u16::try_from(requested.len())
            .map_err(|_| MultipartRepositoryError::InvalidPart)?
            .to_be_bytes(),
    );
    for part in requested {
        sha.update(part.number.to_be_bytes());
        sha.update(parse_raw_md5(&part.etag).ok_or(MultipartRepositoryError::InvalidPart)?);
    }
    Ok(sha.finalize().into())
}

fn parse_raw_md5(value: &str) -> Option<[u8; 16]> {
    let unquoted = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value);
    if unquoted.len() != 32 {
        return None;
    }
    let mut raw = [0_u8; 16];
    for (output, pair) in raw.iter_mut().zip(unquoted.as_bytes().chunks_exact(2)) {
        *output = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(raw)
}
