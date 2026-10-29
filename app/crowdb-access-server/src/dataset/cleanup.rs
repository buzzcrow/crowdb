// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{unix_seconds, DatasetAuthority, DatasetUploads};
use crowdb_access_dataset::UploadExpiryCursor;
use std::{sync::Arc, time::Duration};

fn next_delay(more: bool, success: bool, idle: Duration) -> Duration {
    if more {
        Duration::from_millis(if success { 50 } else { 500 })
    } else {
        idle
    }
}

pub(super) fn start_cleanup(
    authority: Arc<DatasetAuthority>,
    uploads: Option<Arc<DatasetUploads>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let reclaim_poll = Duration::from_secs(authority.reclaim_grace_seconds().clamp(1, 15));
        let mut dataset_cursor = crowdb_access_dataset::DatasetDeletionSweep::default();
        let mut next_dataset = tokio::time::Instant::now();
        let mut snapshot_cursor = crowdb_access_dataset::SnapshotReclaimSweep::default();
        let mut next_snapshot = tokio::time::Instant::now();
        let mut candidate_cursor = crowdb_access_dataset::SnapshotExpirySweep::default();
        let mut next_candidate = tokio::time::Instant::now();
        let mut composition_cursor = crowdb_access_dataset::composition::CompositionReclaimSweep::default();
        let mut next_composition = tokio::time::Instant::now();
        let mut split_cursor = crowdb_access_dataset::split::SplitReclaimSweep::default();
        let mut next_split = tokio::time::Instant::now();
        let mut revision_cursor = crowdb_access_dataset::split::SplitRevisionReclaimSweep::default();
        let mut next_revision = tokio::time::Instant::now();
        let mut upload_cursor = UploadExpiryCursor::default();
        let mut next_upload = tokio::time::Instant::now();
        loop {
            tokio::select! {
                () = tokio::time::sleep_until(next_dataset) => {
                    let result = authority.reclaim_datasets_page(&mut dataset_cursor,
                        &crowdb_access_dataset::ReadCancellation::new()).await;
                    let delay = next_delay(dataset_cursor.has_more(), result.is_ok(), reclaim_poll);
                    next_dataset = tokio::time::Instant::now() + delay;
                    if let Err(error) = result {
                        tracing::error!(%error, "Dataset namespace cleanup failed; durable deletion marks retained for retry");
                    }
                }

                () = tokio::time::sleep_until(next_revision) => {
                    let result = authority.reclaim_split_revisions_page(&mut revision_cursor,
                        &crowdb_access_dataset::ReadCancellation::new()).await;
                    let delay = next_delay(revision_cursor.has_more(), result.is_ok(), reclaim_poll);
                    next_revision = tokio::time::Instant::now() + delay;
                    if let Err(error) = result {
                        tracing::error!(%error, "Dataset Split revision cleanup failed; deletion marks retained for retry");
                    }
                }
                () = tokio::time::sleep_until(next_candidate) => {
                    let result = authority.expire_snapshot_candidates_page(&mut candidate_cursor,
                        &crowdb_access_dataset::ReadCancellation::new()).await;
                    let delay = next_delay(candidate_cursor.has_more(), result.is_ok(), Duration::from_secs(15));
                    next_candidate = tokio::time::Instant::now() + delay;
                    if let Err(error) = result {
                        tracing::error!(%error, "Dataset candidate expiry failed; resource deadlines retained for retry");
                    }
                }
                () = tokio::time::sleep_until(next_split) => {
                    let result = authority.reclaim_splits_page(&mut split_cursor,
                        &crowdb_access_dataset::ReadCancellation::new()).await;
                    let delay = next_delay(split_cursor.has_more(), result.is_ok(), Duration::from_secs(15));
                    next_split = tokio::time::Instant::now() + delay;
                    if let Err(error) = result {
                        tracing::error!(%error, "Dataset legacy Split cleanup failed; resource facts retained for retry");
                    }
                }
                () = tokio::time::sleep_until(next_composition), if uploads.is_some() => {
                    let Some(uploads) = &uploads else { continue };
                    let result = authority.reclaim_compositions_page(uploads, &mut composition_cursor,
                        &crowdb_access_dataset::ReadCancellation::new()).await;
                    let delay = next_delay(composition_cursor.has_more(), result.is_ok(), Duration::from_secs(15));
                    next_composition = tokio::time::Instant::now() + delay;
                    if let Err(error) = result {
                        tracing::error!(%error, "Dataset legacy Composition cleanup failed; resource facts retained for retry");
                    }
                }
                () = tokio::time::sleep_until(next_snapshot), if uploads.is_some() => {
                    let Some(uploads) = &uploads else { continue };
                    let result = authority.reclaim_due_page(uploads, &mut snapshot_cursor,
                        &crowdb_access_dataset::ReadCancellation::new()).await;
                    let delay = next_delay(snapshot_cursor.has_more(), result.is_ok(), reclaim_poll);
                    next_snapshot = tokio::time::Instant::now() + delay;
                    if let Err(error) = result {
                        if matches!(error, crowdb_access_dataset::SnapshotReclaimError::Limit) {
                            tracing::debug!("Dataset snapshot cleanup yielded at its work deadline");
                        } else {
                            tracing::error!(%error, "Dataset snapshot cleanup failed; durable deletion marks retained for retry");
                        }
                    }
                }
                () = tokio::time::sleep_until(next_upload), if uploads.is_some() => {
                    let Some(uploads) = &uploads else { continue };
                    let result = uploads.expire_step(unix_seconds(), &mut upload_cursor).await;
                    let delay = next_delay(upload_cursor.has_more(), result.is_ok(), Duration::from_secs(15));
                    next_upload = tokio::time::Instant::now() + delay;
                    if let Err(error) = result {
                        tracing::error!(%error, "Dataset upload cleanup failed; retaining cleanup records for retry");
                    }
                }
            }
        }
    })
}
