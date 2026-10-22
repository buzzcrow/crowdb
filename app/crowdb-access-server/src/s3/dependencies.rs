// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fresh read-only dependency evidence, independent of application traffic.

use std::{future::Future, pin::Pin, time::Duration};

use crowdb_access_s3::{metadata::MetadataKey, metrics::DependencyHealth};

use super::operations::ProductionS3Operations;

pub type DependencyProbeFuture = Pin<Box<dyn Future<Output = Option<StorageReadiness>> + Send + 'static>>;

pub struct StorageReadiness {
    pub metadata: Result<String, String>,
    pub chunks: Result<String, String>,
}

impl ProductionS3Operations {
    pub(super) async fn observe_storage(&self) -> StorageReadiness {
        let (metadata, chunks) = tokio::join!(
            bounded("metadata", async {
                self.storage
                    .metadata
                    .get(
                        MetadataKey::bucket_name(&self.config.tenant, b"crowdb-health-observation")
                            .map_err(|error| error.to_string())?,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                Ok("Routed Chunk-KV bucket namespace read succeeded".into())
            }),
            async {
                Box::pin(self.storage.chunks.observe_readiness(Duration::from_millis(750)))
                    .await
                    .map_err(|error| error.to_string())
            },
        );
        StorageReadiness { metadata, chunks }
    }
}

pub(super) async fn bounded(
    dependency: &str,
    probe: impl Future<Output = Result<String, String>>,
) -> Result<String, String> {
    tokio::time::timeout(Duration::from_millis(750), probe)
        .await
        .unwrap_or_else(|_| Err(format!("{dependency} read exceeded its 750 ms deadline")))
}

pub(super) fn health(result: &Result<String, String>) -> DependencyHealth {
    if result.is_ok() {
        DependencyHealth::Ready
    } else {
        DependencyHealth::Unavailable
    }
}
