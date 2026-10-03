// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Independent lifecycle and task execution runtimes for each maintenance domain.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::allocator::{ChunkAllocator, DiskdbClientPool};
use crate::chunkdb_config::{ChunkdbConfig, DeploymentMode};
use crate::conversion::ConversionCoordinator;
use crate::lifecycle::{ChunkLockMap, LifecycleError, LifecycleHandler};
use crate::metrics::ChunkdbMetrics;
use crate::placement_repair::PlacementRepairCoordinator;
use crate::range_guard::RangeGuard;
use crate::relocation::RelocationCoordinator;
use crate::repair::RepairCoordinator;
use crate::routing::BindingCache;
use crate::service::ChunkdbRpcService;
use crate::storage::ChunkStore;
use crate::task::{TaskManager, TaskStore};
use crate::topology::TopologyCache;
use crowdb_kv_client::CrowdbKvClient;
use crowdb_protocol::chunk_domain::ChunkDomain;

mod background;
mod executor;

#[derive(Clone)]
pub struct RuntimeContext {
    pub config: ChunkdbConfig,
    pub kv: Arc<CrowdbKvClient>,
    pub bindings: BindingCache,
    pub range_guard: Arc<RangeGuard>,
    pub allocator: Arc<ChunkAllocator>,
    pub cache: TopologyCache,
    pub pool: Arc<DiskdbClientPool>,
    pub lock_map: Arc<ChunkLockMap>,
    pub workflow_metrics: Arc<ChunkdbMetrics>,
    pub stop_rx: tokio::sync::watch::Receiver<bool>,
    pub allow_unsafe_ec: bool,
    pub allow_degraded_failure_domains: bool,
}

pub struct DomainRuntime {
    pub rpc_service: ChunkdbRpcService,
    pub conversion: Arc<ConversionCoordinator>,
    pub handles: Vec<tokio::task::JoinHandle<()>>,
}

impl RuntimeContext {
    /// Recover only this domain's owned records before starting its workers.
    ///
    /// # Errors
    /// Returns storage or lifecycle errors during remote-state recovery.
    #[allow(clippy::too_many_lines)]
    pub async fn start(self, domain: ChunkDomain) -> Result<DomainRuntime, LifecycleError> {
        let Self {
            config,
            kv,
            bindings,
            range_guard,
            allocator,
            cache,
            lock_map,
            workflow_metrics,
            allow_unsafe_ec,
            allow_degraded_failure_domains,
            ..
        } = self.clone();
        let instance_id = config
            .server
            .instance_id
            .as_deref()
            .and_then(|id| id.parse::<u64>().ok())
            .filter(|id| *id != 0)
            .ok_or_else(|| {
                LifecycleError::InvalidRequest("runtime requires its assigned service instance ID".into())
            })?;
        let reservation_blocks = range_guard.quota_share(config.reservation.max_blocks) / 2;
        let reservation_bytes = range_guard.quota_share(config.reservation.max_bytes) / 2;

        let store = Arc::new(
            ChunkStore::new(Arc::clone(&kv), bindings.clone()).with_scope(Arc::clone(&range_guard), domain),
        );
        let task_store =
            Arc::new(TaskStore::new(Arc::clone(&kv), bindings).with_scope(Arc::clone(&range_guard), domain));

        // Lifecycle handler.
        let handler = Arc::new(
            LifecycleHandler::new(Arc::clone(&store), allocator, cache)
                .with_deployment_mode(config.deployment.mode)
                .with_placement_tasks(Arc::clone(&task_store))
                .with_range_guard(Arc::clone(&range_guard))
                .with_locks(Arc::clone(&lock_map))
                .with_metrics(Arc::clone(&workflow_metrics))
                .with_reservation_limits(reservation_blocks, reservation_bytes)
                .with_allow_unsafe_ec(allow_unsafe_ec)
                .with_placement_policy(
                    config.placement.failure_domain_priority,
                    allow_degraded_failure_domains,
                )
                .with_layout_validity(Duration::from_millis(config.lifecycle.layout_validity_ms)),
        );
        handler.rebuild_reservation_admission().await?;
        handler.reconcile_pending_chunks().await?;
        // Build the crowdb-rpc server. The RpcServer listens on the RPC
        // port and dispatches to ChunkdbRpcService handlers.
        let rpc_rt_handle = tokio::runtime::Handle::current();
        let task_manager = Arc::new(TaskManager::new(
            Arc::clone(&task_store),
            instance_id,
            config.conversion.task_lease_secs.saturating_mul(1_000),
        ));
        let relocation = Arc::new(RelocationCoordinator::new(Arc::clone(&task_manager)));
        let conversion = Arc::new(
            ConversionCoordinator::new(Arc::clone(&handler), Arc::clone(&task_store))
                .with_enabled(config.deployment.mode != DeploymentMode::TestSingleNode)
                .with_wake(task_manager.wake_handle())
                .with_policy(
                    config.conversion.data_num,
                    config.conversion.code_num,
                    config.conversion.min_mirror_strips,
                    config.conversion.min_seal_age_secs.saturating_mul(1_000),
                ),
        );
        let mut workers = self.start_reservation_workers(&handler, &conversion);
        let repair = Arc::new(
            RepairCoordinator::new(Arc::clone(&handler), Arc::clone(&task_store))
                .with_wake(task_manager.wake_handle())
                .with_metrics(Arc::clone(&workflow_metrics.repair)),
        );
        let placement_repair = Arc::new(
            PlacementRepairCoordinator::new(Arc::clone(&handler), Arc::clone(&task_store))
                .with_wake(task_manager.wake_handle())
                .with_metrics(Arc::clone(&workflow_metrics.placement)),
        );
        workers.extend(self.start_repair_workers(&handler, &repair, &placement_repair));
        let executor = self
            .start_executor(
                Arc::clone(&handler),
                Arc::clone(&task_store),
                Arc::clone(&task_manager),
                repair,
            )
            .await;
        workers.extend(executor.handles);
        let rpc_service =
            ChunkdbRpcService::new(Arc::clone(&handler), Arc::clone(&workflow_metrics), rpc_rt_handle)
                .with_conversion(Arc::clone(&conversion))
                .with_task_store(Arc::clone(&task_store))
                .with_ad_hoc(executor.ad_hoc)
                .with_relocation(relocation);

        Ok(DomainRuntime {
            rpc_service,
            conversion,
            handles: workers,
        })
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
