// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    binding::{budget, inspection_error, split_error},
    AuthorityError, Continuation, DatasetError, DatasetIdentity, DatasetReads, ReadPage,
};
use crate::{inspection::work::InspectionWork, ReadCancellation};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use md5::{Digest, Md5};
use std::time::{Duration, Instant};

impl DatasetReads {
    /// Replays the caller's position. No read record, heartbeat or acknowledgement is written.
    /// A partial shuffle group is reconstructed from its immutable start boundary.
    /// # Errors
    /// Rejects invalid continuations, deletion admission, unavailable metadata and bounded-work limits.
    pub async fn next(
        &self,
        identity: &DatasetIdentity,
        token: &str,
        cancel: &ReadCancellation,
    ) -> Result<ReadPage, AuthorityError> {
        let mut cursor = Continuation::decode(identity, token)?;
        self.authority
            .check_snapshot_readable(identity, cursor.snapshot)
            .await?;
        let (mut ids, after) =
            tokio::time::timeout(Duration::from_secs(2), self.group(identity, &cursor, cancel))
                .await
                .map_err(|_| DatasetError::ReadLimitExceeded)??;
        let count = ids.len();
        if let Some(shuffle) = &cursor.spec.shuffle {
            ids.sort_by_cached_key(|id| {
                let mut digest = Md5::new();
                digest.update(shuffle.seed.to_le_bytes());
                digest.update(cursor.snapshot.as_bytes());
                digest.update(id);
                <[u8; 16]>::from(digest.finalize())
            });
        }
        let p = &cursor.spec.partition;
        let slots = u64::from(p.world_size) * u64::from(p.workers);
        let slot = u64::from(p.rank) * u64::from(p.workers) + u64::from(p.worker);
        let ids: Vec<_> = ids
            .into_iter()
            .enumerate()
            .filter_map(|(i, id)| ((cursor.ordinal % slots + i as u64 % slots) % slots == slot).then_some(id))
            .collect();
        if cursor.offset > ids.len() {
            return Err(DatasetError::CursorMismatch.into());
        }
        let end = cursor
            .offset
            .saturating_add(cursor.spec.batch_size)
            .min(ids.len());
        let batch = ids[cursor.offset..end].to_vec();
        let finished = end == ids.len();
        let next = if finished && after.is_none() {
            None
        } else {
            if finished {
                cursor.ordinal = cursor
                    .ordinal
                    .checked_add(count as u64)
                    .ok_or(DatasetError::ReadLimitExceeded)?;
                cursor.offset = 0;
                cursor.after = after.map(|bytes| URL_SAFE_NO_PAD.encode(bytes));
            } else {
                cursor.offset = end;
            }
            Some(cursor.encode()?)
        };
        Ok(ReadPage {
            snapshot: cursor.snapshot,
            ids: batch,
            fields: cursor.spec.fields,
            next,
        })
    }

    async fn group(
        &self,
        identity: &DatasetIdentity,
        cursor: &Continuation,
        cancel: &ReadCancellation,
    ) -> Result<(Vec<Vec<u8>>, Option<Vec<u8>>), AuthorityError> {
        let p = &cursor.spec.partition;
        let size = cursor.spec.shuffle.as_ref().map_or_else(
            || {
                cursor
                    .spec
                    .batch_size
                    .saturating_mul(usize::from(p.workers))
                    .saturating_mul(usize::from(p.world_size))
                    .min(100_000)
            },
            |shuffle| shuffle.group_size,
        );
        let mut ids = Vec::new();
        let mut after = cursor
            .after
            .as_ref()
            .map(|value| URL_SAFE_NO_PAD.decode(value))
            .transpose()
            .map_err(|_| DatasetError::CursorMismatch)?;
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut bytes = 0usize;
        let revision = if let Some(source) = &cursor.split {
            Some(
                self.authority
                    .get_split_revision(identity, cursor.snapshot, source.revision, budget(), cancel)
                    .await
                    .map_err(split_error)?,
            )
        } else {
            None
        };
        for _ in 0..5000 {
            if cancel.is_cancelled() {
                return Err(DatasetError::ReadCancelled.into());
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(DatasetError::ReadLimitExceeded)?;
            let mut limits = budget();
            limits.items = (size - ids.len()).min(20);
            limits.timeout = remaining;
            let page = self
                .authority
                .inspection_selection_page(
                    identity,
                    cursor.snapshot,
                    &cursor.spec.selection,
                    after,
                    limits,
                    cancel,
                )
                .await
                .map_err(inspection_error)?;
            let mut work = InspectionWork::new(self.authority.store.as_ref(), cancel, limits)
                .map_err(inspection_error)?;
            for id in page.entries {
                if let (Some(revision), Some(source)) = (&revision, &cursor.split) {
                    if !self
                        .authority
                        .frozen_split_contains(identity, revision, &source.name, &id, &mut work)
                        .await
                        .map_err(split_error)?
                    {
                        continue;
                    }
                }
                bytes = bytes
                    .checked_add(id.len())
                    .ok_or(DatasetError::ReadLimitExceeded)?;
                if bytes > 8 * 1024 * 1024 {
                    return Err(DatasetError::ReadLimitExceeded.into());
                }
                ids.push(id);
            }
            after = page.after;
            if ids.len() == size || after.is_none() {
                return Ok((ids, after));
            }
        }
        Err(DatasetError::ReadLimitExceeded.into())
    }
}
