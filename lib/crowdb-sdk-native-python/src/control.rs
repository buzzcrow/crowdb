// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    composition::CompositionRequest,
    split::{SplitHeadUpdate, SplitPreparation},
    CommitGeneration, DatasetUploads, OperationId, ReadSpec, SnapshotHeadUpdate, SnapshotId, SplitRevisionId,
};
use crowdb_sdk_native::{dataset::DatasetClient, Error, OperationContext, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Create,
    Open,
    Snapshot {
        snapshot: SnapshotId,
    },
    Head,
    CommitStatus {
        attempt: OperationId,
    },
    PrepareSnapshot {
        snapshot: SnapshotId,
        input: CompositionRequest,
    },
    UpdateSnapshotHead {
        snapshot: SnapshotId,
        #[serde(deserialize_with = "Deserialize::deserialize")]
        expected_head: Option<CommitGeneration>,
    },
    PrepareSplitRevision {
        snapshot: SnapshotId,
        input: SplitPreparation,
    },
    UpdateSplitHead {
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        #[serde(deserialize_with = "Deserialize::deserialize")]
        expected_revision: Option<SplitRevisionId>,
        #[serde(deserialize_with = "Deserialize::deserialize")]
        expected_head: Option<CommitGeneration>,
    },
    BindRead {
        spec: ReadSpec,
    },
    CreateUpload {
        upload: OperationId,
        length: u64,
        part_size: u32,
        md5: [u8; 16],
        expires_at: u64,
    },
    CompleteUpload {
        upload: OperationId,
    },
    UploadStatus {
        upload: OperationId,
    },
    UploadLocation {
        upload: OperationId,
    },
    AbortUpload {
        upload: OperationId,
    },
}

fn encoded(value: impl Serialize) -> Result<Value> {
    serde_json::to_value(value).map_err(|e| Error::Storage(e.to_string()))
}
pub async fn execute(client: &DatasetClient, context: OperationContext, command: Command) -> Result<Value> {
    match command {
        Command::UploadLocation { upload } => encoded(DatasetUploads::location(client.identity(), upload)?),
        Command::Create => encoded(client.create(context).await?),
        Command::Open => encoded(client.open(context).await?),
        Command::Snapshot { snapshot } => encoded(client.snapshot(context, snapshot).await?),
        Command::Head => {
            client
                .control(context, |s| async move {
                    {
                        let head = s.authority.inspect_head(&s.identity).await?;
                        Ok(json!({"latest":head.latest,"stable":head.stable,"generation":head.generation}))
                    }
                })
                .await
        }
        Command::CommitStatus { attempt } => {
            client
                .control(context, |s| async move {
                    encoded(s.authority.commit_status(&s.identity, attempt).await?)
                })
                .await
        }
        Command::PrepareSnapshot { snapshot, input } => {
            encoded(client.prepare_snapshot(context, snapshot, &input).await?)
        }
        Command::UpdateSnapshotHead {
            snapshot,
            expected_head,
        } => {
            let head = client
                .update_snapshot_head(
                    context,
                    &SnapshotHeadUpdate {
                        snapshot,
                        expected_head,
                    },
                )
                .await?;
            Ok(json!({"latest":head.latest,"stable":head.stable,"generation":head.generation}))
        }
        Command::PrepareSplitRevision { snapshot, input } => {
            encoded(client.prepare_split_revision(context, snapshot, &input).await?)
        }
        Command::UpdateSplitHead {
            snapshot,
            revision,
            expected_revision,
            expected_head,
        } => encoded(
            client
                .update_split_head(
                    context,
                    snapshot,
                    &SplitHeadUpdate {
                        revision,
                        expected_revision,
                        expected_head,
                    },
                )
                .await?,
        ),
        Command::BindRead { spec } => encoded(client.bind_read(context, spec).await?),
        Command::CreateUpload {
            upload,
            length,
            part_size,
            md5,
            expires_at,
        } => encoded(
            client
                .create_upload(context, upload, length, part_size, md5, expires_at)
                .await?,
        ),
        Command::CompleteUpload { upload } => encoded(client.complete_upload(context, upload).await?),
        Command::UploadStatus { upload } => {
            client
                .control(context, |s| async move {
                    encoded(
                        s.uploads
                            .get(&DatasetUploads::location(&s.identity, upload)?)
                            .await?,
                    )
                })
                .await
        }
        Command::AbortUpload { upload } => {
            client
                .control(context, |s| async move {
                    encoded(
                        s.uploads
                            .abort(&DatasetUploads::location(&s.identity, upload)?)
                            .await?,
                    )
                })
                .await
        }
    }
}
