// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Freeze the exact selected part generations before object publication.

use super::{
    MultipartExchange, MultipartPhase, MultipartRepository, MultipartRepositoryError, MultipartSessionRecord,
};
use crowdb_access_multipart::{
    live_at, next_revision, validate_selected_parts, MultipartComposer, SelectedPart,
};

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
    /// A reserved part mutation settles before this phase write. Publication
    /// rechecks the selected pointer and generation evidence.
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
        let current = self
            .load(session)
            .await?
            .ok_or(MultipartRepositoryError::Conflict)?;
        let already_bound = current.phase == MultipartPhase::Publishing;
        let bound = self.freeze_current(current, requested, now_ms).await?;
        if already_bound {
            if let Some(current) = &bound {
                for (selected, request) in current.selection.iter().flatten().zip(requested) {
                    let part = self
                        .part_generation(current, selected.number, selected.revision)
                        .await?
                        .ok_or(MultipartRepositoryError::InvalidPart)?;
                    if part.selection_digest()? != selected.digest
                        || !etag_matches(&request.etag, &part.raw_md5)
                    {
                        return Err(MultipartRepositoryError::Conflict);
                    }
                }
            }
        }
        Ok(bound)
    }

    // Only continues the authority freshly loaded by this request. The exact
    // session CAS still rejects an intervening part reservation or abort.
    pub(crate) async fn freeze_current(
        &self,
        current: MultipartSessionRecord,
        requested: &[CompletionPart],
        now_ms: u64,
    ) -> Result<Option<MultipartSessionRecord>, MultipartRepositoryError> {
        validate_request(requested)?;
        if current.phase == MultipartPhase::Publishing {
            let selection = current
                .selection
                .as_ref()
                .ok_or(MultipartRepositoryError::Conflict)?;
            if selection.len() != requested.len()
                || selection
                    .iter()
                    .zip(requested)
                    .any(|(selected, request)| selected.number != request.number)
            {
                return Err(MultipartRepositoryError::Conflict);
            }
            // The active caller validates ETags against the generation records
            // in the same publication read, without a duplicate metadata load.
            return Ok(Some(current));
        }
        if current.pending.is_some() {
            self.settle_pending_part(&current).await?;
            return Ok(None);
        }
        if current.phase != MultipartPhase::Open
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
        Ok(match self.exchange_observed(&current, &next).await? {
            MultipartExchange::Applied => Some(next),
            MultipartExchange::Conflict(Some(observed))
                if observed.phase == MultipartPhase::Publishing && observed.selection == next.selection =>
            {
                Some(*observed)
            }
            MultipartExchange::Conflict(_) => None,
        })
    }
}

pub(super) fn etag_matches(value: &str, raw_md5: &[u8; 16]) -> bool {
    parse_raw_md5(value).is_some_and(|actual| &actual == raw_md5)
}

fn validate_request(requested: &[CompletionPart]) -> Result<(), MultipartRepositoryError> {
    if requested.is_empty() || requested.len() > 10_000 {
        return Err(MultipartRepositoryError::InvalidPart);
    }
    let mut previous = 0;
    for part in requested {
        if part.number <= previous || part.number > 10_000 || parse_raw_md5(&part.etag).is_none() {
            return Err(MultipartRepositoryError::InvalidPart);
        }
        previous = part.number;
    }
    Ok(())
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
