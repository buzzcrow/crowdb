// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Functional control consumer. Requires an already configured Dataset service.

use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_sdk_dataset::{DatasetClient, Error, FieldDefinition, FieldValue, Manifest, Publication, Sample};
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("CROWDB_DATASET_ENDPOINT")?;
    let name = std::env::var("CROWDB_DATASET_NAME")?;
    let bearer = std::env::var("CROWDB_DATASET_READ_TOKEN").ok();
    let client = DatasetClient::new(&endpoint, &name, bearer)?;
    let dataset = match client.open().await {
        Ok(dataset) => dataset,
        Err(Error::Service { status: 404, .. }) => client.create().await?,
        Err(error) => return Err(error.into()),
    };
    let publication = Publication {
        operation: uuid::Uuid::new_v4().simple().to_string(),
        parent: dataset.latest.clone(),
        manifest: Manifest {
            schema: vec![FieldDefinition {
                name: "value".into(),
                required: true,
            }],
            samples: vec![Sample {
                id: STANDARD.encode(b"sdk-control-smoke"),
                fields: BTreeMap::from([(
                    "value".into(),
                    FieldValue::Inline {
                        data: STANDARD.encode(b"hello"),
                    },
                )]),
            }],
        },
    };
    let prepared = client.prepare(&publication).await?;
    assert!(!prepared.is_latest);
    assert_eq!(client.open().await?.latest, dataset.latest);
    let published = client.publish(&publication).await?;
    assert_eq!(prepared.id, published.id);
    assert!(published.is_latest);
    assert_eq!(client.operation(&publication.operation).await?.id, published.id);
    let manifest = client.manifest(&published.id).await?;
    let FieldValue::Inline { data } = &manifest.samples[0].fields["value"] else {
        return Err("unexpected field kind".into());
    };
    assert_eq!(STANDARD.decode(data)?, b"hello");
    println!("published snapshot {}", published.id);
    Ok(())
}
