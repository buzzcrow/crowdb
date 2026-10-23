// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Retained independent routing observations for caller scheduling.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_chunk_kv_client::ChunkKvRangeCatalogMap;
use crowdb_diskio_client::DiskTopologyView;
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::common::InstanceValue;

use crate::{ClientRuntime, Error, OperationContext, Result};

pub(crate) struct CatalogObservation {
    pub catalog: Arc<ChunkKvRangeCatalogMap>,
    pub observed_at: Option<Instant>,
}

#[derive(Clone, Debug)]
pub struct ServiceObservation {
    pub service: &'static str,
    pub observed_at: Instant,
    pub instances: Vec<InstanceValue>,
}

/// The sources do not share a global epoch or an atomic cluster snapshot.
/// Execution always goes through current routing and generation checks.
#[derive(Clone)]
pub struct TopologyView {
    catalog: Arc<CatalogObservation>,
    pub disks: Option<DiskTopologyView>,
    pub services: Arc<Vec<ServiceObservation>>,
    pub metadata_transport_json: Vec<u8>,
    pub data_transport_json: Option<Vec<u8>>,
}

impl TopologyView {
    #[must_use]
    pub fn catalog(&self) -> &Arc<ChunkKvRangeCatalogMap> {
        &self.catalog.catalog
    }

    #[must_use]
    pub fn catalog_age(&self) -> Option<Duration> {
        self.catalog.observed_at.map(|observed| observed.elapsed())
    }
}

impl ClientRuntime {
    /// Inspects retained observations without dialing or publishing placement.
    /// Service observations remain empty until an explicit refresh.
    /// # Errors
    /// Returns native status allocation or topology-observation failures.
    pub fn topology(&self) -> Result<TopologyView> {
        let disks = self.storage.chunks.disk_topology();
        let data_transport_json = if disks.is_some() {
            Some(
                self.storage
                    .chunks
                    .transport_status_json()
                    .map_err(|error| Error::Storage(error.to_string()))?,
            )
        } else {
            None
        };
        Ok(TopologyView {
            catalog: self.storage.catalog.load_full(),
            disks,
            services: self.storage.services.load_full(),
            metadata_transport_json: self
                .storage
                .metadata_transport
                .transport_status_json()
                .map_err(|error| Error::Storage(error.to_string()))?,
            data_transport_json,
        })
    }

    /// Refreshes each authority independently; old views remain safe to inspect.
    /// This never uses a caller-supplied view to override current placement.
    /// # Errors
    /// Returns admission, deadline, cancellation, catalog or endpoint failure.
    pub async fn refresh_topology(&self, context: OperationContext) -> Result<TopologyView> {
        let request = self.resources.admit(context)?;
        request
            .run(async {
                let catalog = self
                    .storage
                    .metadata
                    .refresh_catalog()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                self.record_catalog(&catalog);
                self.storage
                    .chunks
                    .refresh_chunkdb_routes()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                self.storage
                    .chunks
                    .refresh_diskio_routes()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                let registry = ServiceRegistryClient::from_shared(Arc::clone(&self.storage.control));
                let mut services = Vec::with_capacity(3);
                for service in ["chunkdb", "chunk-kv", "diskio"] {
                    let instances = registry
                        .read_all_instances(service)
                        .await
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    services.push(ServiceObservation {
                        service,
                        observed_at: Instant::now(),
                        instances: instances.into_iter().map(|(_, instance)| instance).collect(),
                    });
                }
                self.storage.services.store(Arc::new(services));
                self.topology()
            })
            .await
    }

    fn record_catalog(&self, catalog: &Arc<ChunkKvRangeCatalogMap>) {
        loop {
            let observed = self.storage.catalog.load_full();
            if catalog.generation() < observed.catalog.generation() {
                return;
            }
            let next = Arc::new(CatalogObservation {
                catalog: Arc::clone(catalog),
                observed_at: Some(Instant::now()),
            });
            if Arc::ptr_eq(&self.storage.catalog.compare_and_swap(&observed, next), &observed) {
                return;
            }
        }
    }
}
