// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! Standalone real-service contract consumer, also usable as an ingestion example.
use crowdb_sdk_dataset::{v2, wire::BatchEvent, DatasetClient, FieldDefinition, WorkerPartition};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = std::env::var("CROWDB_DATASET_ENDPOINT")?;
    let name = std::env::var("CROWDB_DATASET_NAME")?;
    let token = std::env::var("CROWDB_DATASET_READ_TOKEN").ok();
    let client = DatasetClient::new(&endpoint, &name, token)?.control();
    client.create().await?;
    local_file(&client).await?;
    let (snapshot, revision, sample) = import(&client).await?;
    inspect(&client, &snapshot, &sample).await?;
    revise_splits(&client, &snapshot, &revision).await?;
    let source = v2::SplitSource {
        snapshot: snapshot.clone(),
        revision: revision.clone(),
        name: "train/a b".into(),
    };
    let mut members = client.split_members(source.clone(), 1)?;
    assert_eq!(members.next_page().await?.unwrap().entries, vec![sample]);
    assert!(members.next_page().await?.is_none());
    read_split(&client, &snapshot, &revision).await?;
    let child = branch(&client, source).await?;
    assert_eq!(client.summary(&child).await?.parent, Some(snapshot.clone()));
    client.set_stable(&snapshot).await?;
    let head = client.head().await?;
    assert_eq!(head.latest, Some(child));
    assert_eq!(head.stable, Some(snapshot));
    println!("split revision SDK round trip passed");
    Ok(())
}

async fn local_file(client: &v2::ControlClient) -> Result<()> {
    let id = v2::OpaqueId::random();
    let path = std::env::temp_dir().join(format!("crowdb-dataset-{id}"));
    tokio::fs::write(&path, b"local file").await?;
    let uploaded = client.upload_file(&path, &id).await;
    assert_eq!(tokio::fs::read(&path).await?, b"local file");
    tokio::fs::remove_file(&path).await?;
    assert_eq!(uploaded?.length, 10.into());
    client.abort_upload(&id).await?;
    Ok(())
}

async fn import(client: &v2::ControlClient) -> Result<(v2::OpaqueId, v2::OpaqueId, v2::SampleId)> {
    let upload = v2::CreateUpload {
        id: v2::OpaqueId::random(),
        length: 4.into(),
        part_size: 2,
        md5: "8d777f385d3dfec8815d20f7496026dc".into(),
    };
    // A remote source adapter supplies a replayable reader and trusted length/digest.
    client.upload_reader(&mut &b"data"[..], &upload).await?;
    client.upload_reader(&mut &b"data"[..], &upload).await?;
    assert!(client.upload_reader(&mut &b"date"[..], &upload).await.is_err());
    let sample = v2::SampleId::new(vec![0, 255, 128])?;
    let operation = v2::OpaqueId::random();
    let splits = vec![v2::DraftSplit {
        name: "train/a b".into(),
        definition: v2::SplitDefinition::Selection {
            predicate: v2::Predicate::Explicit {
                ids: vec![sample.clone()],
            },
        },
        origin: v2::SplitOrigin::Evaluated,
    }];
    let begin = v2::BeginComposition {
        operation: operation.clone(),
        parent: None,
        source: v2::CompositionSource::All,
        schema: Some(v2::Schema {
            version: 1,
            fields: vec![FieldDefinition {
                name: "value".into(),
                required: true,
            }],
        }),
        inherit_parent_splits: false,
        parent_split_revision: None,
        splits,
        policy: None,
    };
    client.begin_composition(&begin).await?;
    let edit = v2::EditComposition {
        edit: v2::OpaqueId::random(),
        expected_revision: 1.into(),
        schema: None,
        samples: vec![v2::SampleChange::Upsert {
            id: sample.clone(),
            fields: vec![v2::Field {
                name: "value".into(),
                value: v2::FieldValue::Upload { id: upload.id },
            }],
        }],
        splits: None,
        policy: None,
    };
    let edited = client.edit_composition(&operation, &edit).await?;
    assert_eq!(edited.revision, 2.into());
    assert_eq!(
        client.edit_composition(&operation, &edit).await?.revision,
        edited.revision
    );
    let prepared = prepare(client, &operation, edited.revision).await?;
    let snapshot = prepared.preparation.unwrap().snapshot;
    let request = v2::PublishComposition {
        attempt: v2::OpaqueId::random(),
        snapshot: snapshot.clone(),
        expected_head: client.head().await?.generation,
    };
    let receipt = client.publish_composition(&operation, &request).await?;
    let lifecycle = client.lifecycle(&snapshot).await?;
    assert_eq!(lifecycle.snapshot, snapshot);
    assert!(lifecycle.latest);
    assert!(!lifecycle.retained);
    assert_eq!(
        client.commit_status(&request.attempt).await?.unwrap().attempt,
        receipt.attempt
    );
    assert_eq!(
        client.publish_composition(&operation, &request).await?.attempt,
        receipt.attempt
    );
    cleanup(client, &operation).await?;
    let revision = client.split_head(&snapshot).await?.revision.unwrap();
    Ok((snapshot, revision, sample))
}

async fn prepare(
    client: &v2::ControlClient,
    operation: &v2::OpaqueId,
    revision: v2::Decimal,
) -> Result<v2::CompositionDraft> {
    let mut page = 0.into();
    let mut done = false;
    for _ in 0..100 {
        let preview = client
            .evaluate_composition(
                operation,
                &v2::EvaluateDraft {
                    expected_revision: revision,
                    expected_page: page,
                },
            )
            .await?;
        if preview.complete {
            done = true;
            break;
        }
        page = preview.next_page;
    }
    assert!(done, "composition evaluation exceeded the example's work budget");
    for _ in 0..100 {
        let draft = client
            .prepare_composition(
                operation,
                &v2::DraftRevision {
                    expected_revision: revision,
                },
            )
            .await?;
        if draft.uploads.publication_ready {
            return Ok(draft);
        }
    }
    Err("composition preparation exceeded the example's work budget".into())
}
async fn cleanup(client: &v2::ControlClient, operation: &v2::OpaqueId) -> Result<()> {
    for _ in 0..100 {
        if client.cleanup_composition(operation).await?.complete {
            return Ok(());
        }
    }
    Err("cleanup exceeded the example's work budget".into())
}
async fn inspect(client: &v2::ControlClient, snapshot: &v2::OpaqueId, sample: &v2::SampleId) -> Result<()> {
    assert_eq!(client.summary(snapshot).await?.sample_count, 1.into());
    assert!(client.contains(snapshot, sample).await?);
    assert!(client.inspection_status(snapshot).await.is_ok());
    let mut schema = client.schema(snapshot.clone(), 1)?;
    assert_eq!(schema.next_page().await?.unwrap().entries[0].name, "value");
    let mut samples = client.samples(snapshot.clone(), 1)?;
    assert_eq!(&samples.next_page().await?.unwrap().entries[0], sample);
    let matrix = client
        .matrix(
            snapshot,
            &v2::MatrixRequest {
                sample_ids: vec![sample.clone()],
                fields: vec!["value".into()],
            },
        )
        .await?;
    assert_eq!(matrix.entries[0].fields[0].as_ref().unwrap().length, 4.into());
    let preview = client.preview(snapshot, sample, "value", 2).await?;
    assert_eq!(preview.data.as_deref(), Some("ZGE="));
    assert!(preview.truncated);
    Ok(())
}

async fn revise_splits(
    client: &v2::ControlClient,
    snapshot: &v2::OpaqueId,
    previous: &v2::OpaqueId,
) -> Result<()> {
    let head = client.head().await?;
    let split_head = client.split_head(snapshot).await?;
    let begin = v2::BeginSplit {
        operation: v2::OpaqueId::random(),
        expected_revision: Some(previous.clone()),
        policy: v2::SplitPolicy::default(),
        splits: vec![],
    };
    client.begin_split(snapshot, &begin).await?;
    let mut page = 0.into();
    let mut done = false;
    for _ in 0..100 {
        let p = client
            .evaluate_split(
                &begin.operation,
                &v2::EvaluateDraft {
                    expected_revision: 1.into(),
                    expected_page: page,
                },
            )
            .await?;
        if p.complete {
            done = true;
            break;
        }
        page = p.next_page;
    }
    assert!(done);
    let revision = client
        .prepare_split(
            &begin.operation,
            &v2::DraftRevision {
                expected_revision: 1.into(),
            },
        )
        .await?;
    assert_eq!(
        client
            .split_revision(snapshot, &revision.revision)
            .await?
            .revision,
        revision.revision
    );
    assert_eq!(
        client.split_head(snapshot).await?.revision,
        Some(previous.clone())
    );
    let update = v2::UpdateSplitHead {
        revision: revision.revision.clone(),
        expected_revision: Some(previous.clone()),
        expected_head: split_head.generation.clone(),
    };
    client.update_split_head(snapshot, &update).await?;
    let stale = v2::UpdateSplitHead {
        revision: previous.clone(),
        ..update
    };
    let conflict = client.update_split_head(snapshot, &stale).await.unwrap_err();
    assert_eq!(
        v2::SplitHead::from_conflict(&conflict).unwrap().revision,
        Some(revision.revision.clone())
    );
    assert_eq!(client.head().await?.generation, head.generation);
    assert_eq!(
        client.split_head(snapshot).await?.revision,
        Some(revision.revision)
    );
    assert_eq!(
        client.split_revision(snapshot, previous).await?.splits[0].count,
        1.into()
    );
    let mut history = client.split_history(snapshot.clone(), 1)?;
    let mut count = 0;
    while let Some(page) = history.next_page().await? {
        count += page.entries.len();
        assert!(count <= 2);
    }
    assert_eq!(count, 2);
    Ok(())
}
async fn branch(client: &v2::ControlClient, source: v2::SplitSource) -> Result<v2::OpaqueId> {
    let operation = v2::OpaqueId::random();
    client
        .begin_composition(&v2::BeginComposition {
            operation: operation.clone(),
            parent: Some(source.snapshot.clone()),
            source: v2::CompositionSource::Split {
                source: source.clone(),
                selection: None,
            },
            schema: None,
            inherit_parent_splits: true,
            parent_split_revision: Some(source.revision),
            splits: vec![],
            policy: None,
        })
        .await?;
    let snapshot = prepare(client, &operation, 1.into())
        .await?
        .preparation
        .unwrap()
        .snapshot;
    client
        .publish_composition(
            &operation,
            &v2::PublishComposition {
                attempt: v2::OpaqueId::random(),
                snapshot: snapshot.clone(),
                expected_head: client.head().await?.generation,
            },
        )
        .await?;
    cleanup(client, &operation).await?;
    Ok(snapshot)
}
async fn read_split(
    client: &v2::ControlClient,
    snapshot: &v2::OpaqueId,
    revision: &v2::OpaqueId,
) -> Result<()> {
    let request = v2::ReadPlan {
        snapshot: snapshot.to_string(),
        selection: v2::Predicate::All,
        fields: vec!["value".into()],
        batch_size: 1,
        partition: WorkerPartition::default(),
        shuffle: None,
        split: Some(v2::ReadSplit {
            revision: Some(revision.clone()),
            name: "train/a b".into(),
        }),
    };
    let bound = client.bind_read(&request).await?;
    assert_eq!(bound.split.as_ref().unwrap().revision, *revision);
    let mut stream = client.next_batch(&bound.cursor).await?;
    let mut data = Vec::new();
    while let Some(event) = stream.next_event().await? {
        if let BatchEvent::Data(bytes) = event {
            data.extend_from_slice(&bytes);
        }
    }
    assert_eq!(data, b"data");
    let mut replay = client.next_batch(&bound.cursor).await?;
    let mut again = Vec::new();
    while let Some(event) = replay.next_event().await? {
        if let BatchEvent::Data(bytes) = event {
            again.extend_from_slice(&bytes);
        }
    }
    assert_eq!(again, data);
    Ok(())
}
