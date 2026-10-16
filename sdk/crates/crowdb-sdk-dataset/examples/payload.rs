// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Functional real-service consumer; deliberately contains no timing or load driver.
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use crowdb_sdk_dataset::{
    wire::BatchEvent, Acknowledge, DatasetClient, Error, FieldDefinition, FieldValue, Manifest, Publication,
    ReadPlan, Sample, Selection, UploadRequest, WorkerPartition,
};
use md5::{Digest, Md5};
use std::{collections::BTreeMap, path::PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SIZE: usize = 9 * 1024 * 1024 + 17;
fn publication(upload: &str, parent: Option<String>) -> Publication {
    Publication {
        operation: uuid::Uuid::new_v4().simple().to_string(),
        parent,
        manifest: Manifest {
            schema: vec![
                FieldDefinition {
                    name: "value".into(),
                    required: true,
                },
                FieldDefinition {
                    name: "label".into(),
                    required: false,
                },
            ],
            samples: (0..4)
                .map(|i| Sample {
                    id: STANDARD.encode(format!("sample-{i}")),
                    fields: BTreeMap::from([
                        (
                            "value".into(),
                            FieldValue::Payload {
                                upload: upload.into(),
                            },
                        ),
                        (
                            "label".into(),
                            FieldValue::Inline {
                                data: STANDARD.encode([i]),
                            },
                        ),
                    ]),
                })
                .collect(),
        },
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = DatasetClient::new(
        &std::env::var("CROWDB_DATASET_ENDPOINT")?,
        &std::env::var("CROWDB_DATASET_NAME")?,
        std::env::var("CROWDB_DATASET_READ_TOKEN").ok(),
    )?;
    let root = PathBuf::from(std::env::var("CROWDB_DATASET_CONSUMER_ROOT")?);
    tokio::fs::create_dir_all(&root).await?;
    let input = root.join("input.bin");
    let metadata = root.join("publication.json");
    if std::env::var("CROWDB_DATASET_PHASE").unwrap_or_else(|_| "write".into()) == "write" {
        write(&client, &input, &metadata).await?;
    } else {
        verify(&client, &root, &input, &metadata).await?;
    }

    Ok(())
}

async fn write(
    client: &DatasetClient,
    input: &std::path::Path,
    metadata: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let dataset = match client.open().await {
        Ok(d) => d,
        Err(Error::Service { status: 404, .. }) => client.create().await?,
        Err(e) => return Err(e.into()),
    };
    let mut file = tokio::fs::File::create(&input).await?;
    let bytes = vec![0x5a; 1024 * 1024];
    let mut hash = Md5::new();
    for _ in 0..9 {
        file.write_all(&bytes).await?;
        hash.update(&bytes);
    }
    file.write_all(&bytes[..17]).await?;
    hash.update(&bytes[..17]);
    file.sync_all().await?;
    drop(file);
    let id = uuid::Uuid::new_v4().simple().to_string();
    let request = UploadRequest {
        id: id.clone(),
        length: SIZE as u64,
        part_size: 1024 * 1024,
        md5: hex::encode(hash.finalize()),
    };
    client.create_upload(&request).await?;
    let receipt = client.upload_part(&id, 0, Bytes::from(bytes)).await?;
    assert_eq!(receipt.index, 0);
    assert!(client.complete_upload(&id).await.is_err());
    // Resume a partially accepted upload with the same stable identity.
    let complete = client.upload_file(&input, &id).await?;
    assert_eq!(complete.parts.len(), 10);
    let publication = publication(&id, dataset.latest.clone());
    let prepared = client.prepare(&publication).await?;
    assert!(!prepared.is_latest);
    assert_eq!(client.open().await?.latest, dataset.latest);
    let published = client.publish(&publication).await?;
    assert_eq!(published.id, prepared.id);
    assert_eq!(client.publish(&publication).await?.id, published.id);
    tokio::fs::write(
        metadata,
        serde_json::to_vec(
            &serde_json::json!({"snapshot":published.id,"publication":publication,"upload":id}),
        )?,
    )
    .await?;
    println!("prepared and published real payload");
    Ok(())
}
async fn verify(
    client: &DatasetClient,
    root: &std::path::Path,
    input: &std::path::Path,
    metadata: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let state: serde_json::Value = serde_json::from_slice(&tokio::fs::read(metadata).await?)?;
    let snapshot = state["snapshot"].as_str().ok_or("snapshot")?;
    let publication: Publication = serde_json::from_value(state["publication"].clone())?;
    assert_eq!(client.operation(&publication.operation).await?.id, snapshot);
    assert_eq!(client.publish(&publication).await?.id, snapshot);
    assert_eq!(client.manifest(snapshot).await?.samples.len(), 4);
    let output = root.join("output.bin");
    client
        .download_to_path(snapshot, &STANDARD.encode(b"sample-0"), "value", &output)
        .await?;
    let mut a = tokio::fs::File::open(input).await?;
    let mut b = tokio::fs::File::open(output).await?;
    let mut left = vec![0; 1024 * 1024];
    let mut right = vec![0; 1024 * 1024];
    let mut length = 0;
    loop {
        let n = a.read(&mut left).await?;
        if n == 0 {
            break;
        }
        b.read_exact(&mut right[..n]).await?;
        assert_eq!(&left[..n], &right[..n]);
        length += n;
    }
    assert_eq!(length, SIZE);
    assert_eq!(b.read(&mut right).await?, 0);
    let mut owned = Vec::new();
    for worker in 0..2 {
        let plan = ReadPlan {
            id: uuid::Uuid::new_v4().simple().to_string(),
            snapshot: "latest".into(),
            selection: Selection::Prefix {
                value: STANDARD.encode(b"sample-"),
            },
            fields: vec!["label".into()],
            batch_size: 1,
            partition: WorkerPartition {
                worker,
                workers: 2,
                rank: 0,
                world_size: 1,
            },
            shuffle: None,
        };
        let session = client.start_read(&plan).await?;
        assert_eq!(session.snapshot, snapshot);
        let mut cursor = session.acknowledged;
        loop {
            let mut stream = client.next_batch(&plan.id).await?;
            let mut header = None;
            while let Some(event) = stream.next_event().await? {
                if let BatchEvent::Header(h) = event {
                    header = Some(h);
                }
            }
            let header = header.ok_or("batch header")?;
            owned.extend(header.sample_ids.iter().cloned());
            assert_eq!(client.progress(&plan.id).await?.cursor, cursor);
            let ack = Acknowledge {
                expected: cursor.clone(),
                next: header.next.clone().ok_or("issued cursor")?,
            };
            cursor = client.acknowledge(&plan.id, &ack).await?.cursor;
            assert_eq!(client.acknowledge(&plan.id, &ack).await?.cursor, cursor);
            if header.end {
                break;
            }
        }
        client.close_read(&plan.id).await?;
    }
    owned.sort();
    owned.dedup();
    assert_eq!(owned.len(), 4);
    client.retain(snapshot).await?;
    client.release(snapshot).await?;
    client.set_stable(snapshot).await?;
    assert_eq!(client.list_snapshots(None, 1).await?.snapshots[0].id, snapshot);
    assert!(client.reclaim(snapshot).await.is_err());
    println!("verified real payload and scoped progress after restart");
    Ok(())
}
