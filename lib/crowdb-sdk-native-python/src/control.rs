// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    composition::{CompositionEdit, CompositionInput},
    CommitAttempt, DatasetUploads, OperationId, ReadSpec, SnapshotId,
};
use crowdb_sdk_native::{
    dataset::{CompositionStatus, DatasetClient},
    Error, OperationContext, Result,
};
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
    BeginComposition {
        operation: OperationId,
        input: CompositionInput,
    },
    CompositionStatus {
        operation: OperationId,
    },
    EditComposition {
        operation: OperationId,
        edit: CompositionEdit,
    },
    EvaluateComposition {
        operation: OperationId,
        revision: u64,
        page: u64,
    },
    PrepareComposition {
        operation: OperationId,
        revision: u64,
    },
    PublishComposition {
        operation: OperationId,
        attempt: CommitAttempt,
    },
    AbandonComposition {
        operation: OperationId,
        revision: u64,
    },
    CleanupComposition {
        operation: OperationId,
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
fn composition(status: &CompositionStatus) -> Value {
    json!({"draft":status.draft,"uploads":status.uploads})
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
                        Ok(json!({"latest":head.latest,"generation":head.generation}))
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
        Command::BeginComposition { operation, input } => Ok(composition(
            &client.begin_composition(context, operation, &input).await?,
        )),
        Command::CompositionStatus { operation } => {
            Ok(composition(&client.composition_status(context, operation).await?))
        }
        Command::EditComposition { operation, edit } => Ok(composition(
            &client.edit_composition(context, operation, &edit).await?,
        )),
        Command::EvaluateComposition {
            operation,
            revision,
            page,
        } => encoded(
            client
                .evaluate_composition(context, operation, revision, page)
                .await?,
        ),
        Command::PrepareComposition { operation, revision } => Ok(composition(
            &client.prepare_composition(context, operation, revision).await?,
        )),
        Command::PublishComposition { operation, attempt } => {
            encoded(client.publish_composition(context, operation, &attempt).await?)
        }
        Command::AbandonComposition { operation, revision } => Ok(composition(
            &client.abandon_composition(context, operation, revision).await?,
        )),
        Command::CleanupComposition { operation } => {
            let status = client.cleanup_composition(context, operation).await?;
            Ok(
                json!({"complete":status.complete,"retired_upload_owners":status.retired_upload_owners,
                "physical_cleanup_deferred":status.physical_cleanup_deferred}),
            )
        }
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
