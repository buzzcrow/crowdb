// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Independent lifecycle and task execution runtimes for each maintenance domain.

use super::RuntimeContext;
use crate::ad_hoc::{AdHocRecoveryManager, AdHocRecoveryShared};
use crate::chunkdb_config::DeploymentMode;
use crate::conversion::{io::ConversionDiskIo, MirrorToEcTaskHandler};
use crate::finalize::FinalizeChunkTaskHandler;
use crate::lifecycle::LifecycleHandler;
use crate::placement_repair::PlacementRepairTaskHandler;
use crate::repair::{RepairCoordinator, RepairStripTaskHandler};
use crate::task::{
    RelocateSegmentTaskHandler, TaskExecutor, TaskHandler, TaskManager, TaskScanner, TaskStore,
};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub(super) struct ExecutorWorkers {
    pub handles: Vec<tokio::task::JoinHandle<()>>,
    pub ad_hoc: Option<Arc<AdHocRecoveryManager>>,
}

impl RuntimeContext {
    #[allow(clippy::too_many_lines)]
    pub(super) async fn start_executor(
        &self,
        handler: Arc<LifecycleHandler>,
        task_store: Arc<TaskStore>,
        task_manager: Arc<TaskManager>,
        repair: Arc<RepairCoordinator>,
    ) -> ExecutorWorkers {
        let Self {
            config,
            workflow_metrics,
            kv,
            pool,
            stop_rx,
            ..
        } = self.clone();
        let ad_hoc_shared = Arc::new(AdHocRecoveryShared::new(
            config.repair.ad_hoc_max_concurrency,
            config.repair.memory_bytes,
            Arc::clone(&workflow_metrics.repair),
        ));
        let io = Arc::new(ConversionDiskIo::deferred(config.conversion_io.clone()));
        let service = ServiceRegistryClient::from_shared(Arc::clone(&kv));
        let hardware = HardwareClient::from_shared(Arc::clone(&kv));
        if let Err(error) = io.refresh(&service, &hardware).await {
            warn!(%error, "background DiskIO discovery will retry while task execution remains enabled");
        }
        let (task_scanner_handle, conversion_route_refresh_handle, ad_hoc_manager) = {
            let conversion_task_handler = Arc::new(MirrorToEcTaskHandler::new(
                Arc::clone(&handler),
                Arc::clone(&task_store),
                Arc::clone(&io),
                Arc::clone(&workflow_metrics.conversion),
                config.conversion.max_bandwidth_mbps,
                config.conversion.max_concurrency,
            ));
            let repair_task_handler = Arc::new(
                RepairStripTaskHandler::new(
                    Arc::clone(&handler),
                    Arc::clone(&task_manager),
                    Arc::clone(&io),
                    config.repair.memory_bytes,
                    config.repair.max_concurrency,
                    config.repair.allow_unsafe_placement,
                    Arc::clone(&workflow_metrics.repair),
                )
                .with_ad_hoc(Arc::clone(&ad_hoc_shared)),
            );
            let placement_repair_task_handler = Arc::new(PlacementRepairTaskHandler::new(
                Arc::clone(&handler),
                Arc::clone(&task_manager),
                Arc::clone(&io),
                Arc::clone(&workflow_metrics.placement),
            ));
            let mut task_handlers: Vec<Arc<dyn TaskHandler>> = vec![
                Arc::new(FinalizeChunkTaskHandler::new(
                    Arc::clone(&handler),
                    Arc::clone(&io),
                )),
                repair_task_handler,
                placement_repair_task_handler,
                Arc::new(RelocateSegmentTaskHandler::new(
                    Arc::clone(&handler),
                    Arc::clone(&task_manager),
                )),
            ];
            if config.deployment.mode != DeploymentMode::TestSingleNode {
                task_handlers.push(conversion_task_handler);
            }
            let executor = Arc::new(
                TaskExecutor::new(
                    Arc::clone(&task_manager),
                    config
                        .conversion
                        .max_concurrency
                        .saturating_add(config.repair.max_concurrency)
                        .saturating_add(config.placement_repair.max_concurrency),
                    task_handlers,
                )
                .expect("unique conversion task handler"),
            );
            let ad_hoc_manager = Arc::new(AdHocRecoveryManager::new(
                Arc::clone(&ad_hoc_shared),
                Arc::clone(&handler),
                Arc::clone(&pool),
                Arc::clone(&repair),
                Arc::clone(&task_store),
                Arc::clone(&task_manager),
                Arc::clone(&executor),
            ));
            let scanner = TaskScanner::new(
                Arc::clone(&task_store),
                Arc::clone(&task_manager),
                Arc::clone(&executor),
                256,
                Duration::from_secs(1),
            );
            let scanner_stop = stop_rx.clone();
            let scanner_handle = tokio::spawn(async move { scanner.run(scanner_stop).await });
            let service = ServiceRegistryClient::from_shared(Arc::clone(&kv));
            let hardware = HardwareClient::from_shared(Arc::clone(&kv));
            let mut refresh_stop = stop_rx.clone();
            let refresh_interval = Duration::from_secs(u64::from(config.topology.refresh_interval_secs));
            let refresh_handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(refresh_interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            if let Err(error) = io.refresh(&service, &hardware).await {
                                warn!(%error, "background conversion DiskIO route refresh failed");
                            }
                        }
                        changed = refresh_stop.changed() => {
                            if changed.is_err() || *refresh_stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            });
            (Some(scanner_handle), Some(refresh_handle), Some(ad_hoc_manager))
        };
        ExecutorWorkers {
            handles: [task_scanner_handle, conversion_route_refresh_handle]
                .into_iter()
                .flatten()
                .collect(),
            ad_hoc: ad_hoc_manager,
        }
    }
}
