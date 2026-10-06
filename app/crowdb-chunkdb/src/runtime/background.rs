// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Independent lifecycle and task execution runtimes for each maintenance domain.

use super::{unix_time_ms, RuntimeContext};
use crate::conversion::ConversionCoordinator;
use crate::lifecycle::LifecycleHandler;
use crate::placement_rebalance::PlacementRebalancePlanner;
use crate::placement_repair::PlacementRepairCoordinator;
use crate::repair::RepairCoordinator;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

impl RuntimeContext {
    pub(super) fn start_reservation_workers(
        &self,
        handler: &Arc<LifecycleHandler>,
        conversion: &Arc<ConversionCoordinator>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let Self {
            config,
            range_guard,
            stop_rx,
            ..
        } = self.clone();
        let reservation_reconcile_handle = {
            let conversion = Arc::clone(conversion);
            let authority = Arc::clone(&range_guard);
            let mut stop = stop_rx.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(1));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            match authority.capture().scope(conversion.reconcile_reservations(256, unix_time_ms())).await {
                                Ok(reconciled) if reconciled > 0 => {
                                    info!(reconciled, "expired strip reservations reconciled");
                                }
                                Ok(_) => {}
                                Err(error) => warn!(%error, "strip reservation reconciliation failed"),
                            }
                        }
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            })
        };
        let reservation_admission_handle = {
            let handler = Arc::clone(handler);
            let range_guard = Arc::clone(&range_guard);
            let mut stop = stop_rx.clone();
            let interval = Duration::from_secs(config.reservation.scan_interval_secs);
            let max_blocks = config.reservation.max_blocks;
            let max_bytes = config.reservation.max_bytes;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            handler.update_reservation_limits(
                                range_guard.quota_share(max_blocks) / 2,
                                range_guard.quota_share(max_bytes) / 2,
                            );
                        }
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            })
        };
        let conversion_scan_handle = config.conversion.enabled.then(|| {
            let conversion = Arc::clone(conversion);
            let authority = Arc::clone(&range_guard);
            let mut stop = stop_rx.clone();
            let interval = Duration::from_secs(config.conversion.scan_interval_secs);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            match authority.capture().scope(conversion.trigger_configured_batch(false, 256, unix_time_ms())).await {
                                Ok(accepted_chunks) if accepted_chunks > 0 => {
                                    info!(accepted_chunks, "automatic mirror-to-EC scan admitted chunks");
                                }
                                Ok(_) => {}
                                Err(error) => warn!(%error, "automatic mirror-to-EC scan failed"),
                            }
                        }
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            })
        });
        [
            Some(reservation_reconcile_handle),
            Some(reservation_admission_handle),
            conversion_scan_handle,
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    pub(super) fn start_repair_workers(
        &self,
        handler: &Arc<LifecycleHandler>,
        repair: &Arc<RepairCoordinator>,
        placement_repair: &Arc<PlacementRepairCoordinator>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let Self {
            config,
            pool,
            range_guard,
            stop_rx,
            ..
        } = self.clone();
        let placement_repair_scan_handle = config.placement_repair.enabled.then(|| {
            let placement_repair = Arc::clone(placement_repair);
            let authority = Arc::clone(&range_guard);
            let mut stop = stop_rx.clone();
            let interval = Duration::from_secs(config.placement_repair.scan_interval_secs);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            if let Err(error) = authority.capture().scope(placement_repair.scan_batch(256, unix_time_ms())).await {
                                warn!(%error, "placement repair reconciliation failed");
                            }
                        }
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            })
        });
        let placement_rebalance_handle = config.placement_rebalance.enabled.then(|| {
        let planner = Arc::new(PlacementRebalancePlanner::new(
            Arc::clone(handler),
            Arc::clone(&pool),
            config.placement_rebalance.clone(),
        ));
        let authority = Arc::clone(&range_guard);
        let mut stop = stop_rx.clone();
        let interval = Duration::from_secs(config.placement_rebalance.scan_interval_secs);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match authority.capture().scope(planner.run_once(unix_time_ms())).await {
                            Ok(moved) if moved > 0 => info!(moved, "cross-domain rebalance moves handed to DiskDB"),
                            Ok(_) => {}
                            Err(error) => warn!(%error, "cross-domain rebalance planning failed"),
                        }
                    }
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    });
        let repair_scan_handle = config.repair.enabled.then(|| {
            let repair = Arc::clone(repair);
            let authority = Arc::clone(&range_guard);
            let mut stop = stop_rx.clone();
            let interval = Duration::from_secs(config.repair.scan_interval_secs);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            match authority.capture().scope(repair.scan_batch(256, unix_time_ms())).await {
                                Ok(accepted) if accepted > 0 => {
                                    info!(accepted, "read-repair scan admitted tasks");
                                }
                                Ok(_) => {}
                                Err(error) => warn!(%error, "read-repair scan failed"),
                            }
                        }
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            })
        });
        [
            placement_repair_scan_handle,
            placement_rebalance_handle,
            repair_scan_handle,
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}
