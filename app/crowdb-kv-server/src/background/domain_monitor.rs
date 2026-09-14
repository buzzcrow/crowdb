// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Descriptor-driven, leader-tenure-fenced domain monitor supervision.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_kv::cluster::group_operations::{
    KvGroupOperationError, KvGroupOperations, KvGroupScanRequest, KvReadConsistency,
};
use crowdb_protocol::chunk_kv::DomainMonitorDescriptor;
use crowdb_protocol::key::{DomainMonitorKey, TextKey};
use tracing::{debug, info, warn};

use crate::group0_control_plane::Group0ControlPlane;
use crate::store_registry::KvStoreRegistry;

mod chunk_kv;
mod chunkdb;
mod diskdb;

pub use chunk_kv::ChunkKvRangeMonitorDriver;
pub use chunkdb::ChunkdbRangeMonitorDriver;
pub use diskdb::DiskdbOwnershipMonitorDriver;

/// Run one monitor child at a time and reconstruct it after an unexpected
/// return or panic.
pub fn spawn_restarting_monitor<F, Fut>(
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
    make_monitor: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(tokio::sync::watch::Receiver<bool>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            if *stop_rx.borrow() {
                break;
            }
            let mut child = tokio::spawn(make_monitor(stop_rx.clone()));
            tokio::select! {
                result = &mut child => {
                    if !*stop_rx.borrow() {
                        warn!(?result, "domain monitor task exited; restarting");
                        tokio::task::yield_now().await;
                    }
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        let _ = child.await;
                        break;
                    }
                }
            }
        }
    })
}

pub type DomainMonitorFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// One compiled monitor implementation selected by a persisted descriptor.
pub trait DomainMonitorDriver: Send + Sync {
    fn domain(&self) -> &'static str;
    fn driver_version(&self) -> u32;
    fn tick<'a>(
        &'a self,
        control: &'a Group0ControlPlane,
        descriptor: &'a DomainMonitorDescriptor,
    ) -> DomainMonitorFuture<'a>;
}

#[derive(Default)]
pub struct DomainMonitorDrivers {
    drivers: HashMap<(String, u32), Arc<dyn DomainMonitorDriver>>,
}

impl DomainMonitorDrivers {
    #[must_use]
    pub fn new(drivers: Vec<Arc<dyn DomainMonitorDriver>>) -> Self {
        let drivers = drivers
            .into_iter()
            .map(|driver| ((driver.domain().to_string(), driver.driver_version()), driver))
            .collect();
        Self { drivers }
    }

    fn find(&self, descriptor: &DomainMonitorDescriptor) -> Option<Arc<dyn DomainMonitorDriver>> {
        self.drivers
            .get(&(descriptor.domain.clone(), descriptor.driver_version))
            .cloned()
    }
}

pub struct DomainMonitorSupervisorHandle {
    stop_tx: tokio::sync::watch::Sender<bool>,
    supervisor: Option<tokio::task::JoinHandle<()>>,
}

impl DomainMonitorSupervisorHandle {
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    pub async fn stop_and_wait(mut self) {
        self.stop();
        if let Some(supervisor) = self.supervisor.take() {
            let _ = supervisor.await;
        }
    }
}

impl Drop for DomainMonitorSupervisorHandle {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
    }
}

struct RunningMonitor {
    descriptor: DomainMonitorDescriptor,
    stop_tx: tokio::sync::watch::Sender<bool>,
    supervisor: tokio::task::JoinHandle<()>,
}

/// Observe persisted descriptors on this replica and prepare one supervised
/// task for every supported domain. Driver ticks acquire their own local
/// group-0 leader tenure; followers therefore stay prepared but idle.
#[must_use]
pub fn spawn_domain_monitor_supervisor(
    registry: Arc<KvStoreRegistry>,
    drivers: DomainMonitorDrivers,
    discovery_interval: Duration,
) -> DomainMonitorSupervisorHandle {
    let drivers = Arc::new(drivers);
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let supervisor = tokio::spawn(async move {
        let mut running: HashMap<String, RunningMonitor> = HashMap::new();
        let mut interval = tokio::time::interval(discovery_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match discover_descriptors(&registry).await {
                        Ok(descriptors) => reconcile_descriptors(
                            &registry,
                            &drivers,
                            descriptors,
                            &mut running,
                        ),
                        Err(error) => warn!(%error, "domain monitor descriptor discovery failed"),
                    }
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
        for (_, monitor) in running.drain() {
            let _ = monitor.stop_tx.send(true);
            let _ = monitor.supervisor.await;
        }
    });
    DomainMonitorSupervisorHandle {
        stop_tx,
        supervisor: Some(supervisor),
    }
}

async fn discover_descriptors(
    registry: &KvStoreRegistry,
) -> Result<Vec<DomainMonitorDescriptor>, KvGroupOperationError> {
    let store = registry
        .get_store(0)
        .ok_or_else(|| KvGroupOperationError::Unavailable("store 0 is not hosted locally".into()))?;
    let group = store
        .get_group(0)
        .ok_or_else(|| KvGroupOperationError::Unavailable("group 0 is not hosted locally".into()))?;
    let operations = KvGroupOperations::new(group, 1024 * 1024);
    let prefix = Bytes::from(DomainMonitorKey::text_prefix_all());
    let mut start_after = Bytes::new();
    let mut scan_cutoff = 0;
    let mut descriptors = Vec::new();
    loop {
        let scan = operations
            .scan(&KvGroupScanRequest {
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                end_key: Bytes::new(),
                limit: 128,
                consistency: KvReadConsistency::MinAppliedSlot(0),
                keys_only: false,
                count_only: false,
                deadline_ms: 0,
                bounded: true,
                requested_scan_cutoff: scan_cutoff,
                direction: crowdb_kv::kv::ScanDirection::Forward,
            })
            .await?;
        scan_cutoff = scan.scan_cutoff;
        for item in &scan.items {
            let descriptor: DomainMonitorDescriptor = serde_json::from_slice(&item.value)
                .map_err(|error| KvGroupOperationError::Internal(error.to_string()))?;
            descriptor
                .validate()
                .map_err(|error| KvGroupOperationError::Internal(error.to_string()))?;
            let path = std::str::from_utf8(&item.key)
                .map_err(|error| KvGroupOperationError::Internal(error.to_string()))?;
            let key = DomainMonitorKey::from_path(path)
                .map_err(|error| KvGroupOperationError::Internal(error.to_string()))?;
            if key.domain != descriptor.domain {
                return Err(KvGroupOperationError::Internal(format!(
                    "monitor descriptor key domain {} does not match value {}",
                    key.domain, descriptor.domain
                )));
            }
            descriptors.push(descriptor);
        }
        let Some(last) = scan.items.last() else {
            break;
        };
        if !scan.truncated {
            break;
        }
        start_after = last.key.clone();
    }
    Ok(descriptors)
}

fn reconcile_descriptors(
    registry: &Arc<KvStoreRegistry>,
    drivers: &DomainMonitorDrivers,
    descriptors: Vec<DomainMonitorDescriptor>,
    running: &mut HashMap<String, RunningMonitor>,
) {
    let observed: HashSet<_> = descriptors
        .iter()
        .map(|descriptor| descriptor.domain.clone())
        .collect();
    let removed: Vec<_> = running
        .keys()
        .filter(|domain| !observed.contains(*domain))
        .cloned()
        .collect();
    for domain in removed {
        if let Some(monitor) = running.remove(&domain) {
            let _ = monitor.stop_tx.send(true);
            monitor.supervisor.abort();
            info!(%domain, "domain monitor descriptor removed; task stopped");
        }
    }

    for descriptor in descriptors {
        if let Some(existing) = running.get(&descriptor.domain) {
            if existing.descriptor != descriptor {
                warn!(domain = %descriptor.domain, "domain monitor descriptor changed unexpectedly; retaining original task fail-closed");
            }
            continue;
        }
        let Some(driver) = drivers.find(&descriptor) else {
            warn!(
                domain = %descriptor.domain,
                driver_version = descriptor.driver_version,
                "unsupported domain monitor descriptor"
            );
            continue;
        };
        let monitor = spawn_driver(registry, descriptor.clone(), driver);
        running.insert(descriptor.domain.clone(), monitor);
    }
}

fn spawn_driver(
    registry: &Arc<KvStoreRegistry>,
    descriptor: DomainMonitorDescriptor,
    driver: Arc<dyn DomainMonitorDriver>,
) -> RunningMonitor {
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let registry = Arc::clone(registry);
    let child_descriptor = descriptor.clone();
    let supervisor = spawn_restarting_monitor(stop_rx, move |mut child_stop| {
        let registry = Arc::clone(&registry);
        let descriptor = child_descriptor.clone();
        let driver = Arc::clone(&driver);
        async move {
            let mut interval = tokio::time::interval(Duration::from_millis(descriptor.heartbeat_interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => run_driver_tick(&registry, driver.as_ref(), &descriptor).await,
                    changed = child_stop.changed() => {
                        if changed.is_err() || *child_stop.borrow() {
                            break;
                        }
                    }
                }
            }
        }
    });
    RunningMonitor {
        descriptor,
        stop_tx,
        supervisor,
    }
}

async fn run_driver_tick(
    registry: &KvStoreRegistry,
    driver: &dyn DomainMonitorDriver,
    descriptor: &DomainMonitorDescriptor,
) {
    let Some(store) = registry.get_store(0) else {
        return;
    };
    match Group0ControlPlane::acquire(&store).await {
        Ok(control) => {
            if let Err(error) = driver.tick(&control, descriptor).await {
                warn!(domain = %descriptor.domain, %error, "domain monitor tick failed");
            }
        }
        Err(KvGroupOperationError::NotLeader { .. }) => {
            debug!(domain = %descriptor.domain, "domain monitor idle on group-0 follower");
        }
        Err(error) => warn!(domain = %descriptor.domain, %error, "domain monitor leader fence unavailable"),
    }
}
