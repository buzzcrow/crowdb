// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ScanPosition;
use crowdb_access_iceberg::{
    catalog::CatalogContext,
    gc::{
        BudgetedGcStore, GcIoBudget, GcLimits, GcLocalScan, GcPhase, GcRepository, GcScan, GcStore,
        GcSystemScan, GcTaskKind, GcWorker,
    },
    key::{CatalogId, CatalogScope, IcebergKey, SystemScope},
    operation::{ManagementAction, ManagementPhase},
    record::StorageRecord,
};
use std::sync::Arc;

type ScanError = Box<dyn std::error::Error + Send + Sync>;
type ScanResult = Result<(), ScanError>;

pub(super) async fn scan_and_advance(
    store: Arc<BudgetedGcStore>,
    worker: &GcWorker,
    budget: &GcIoBudget,
    catalog: CatalogId,
    cursor: &mut ScanPosition,
    active: Option<CatalogContext>,
    limits: GcLimits,
) -> ScanResult {
    budget.reset();
    let first = cursor.next_lane;
    cursor.next_lane = (first + 1) % 4;
    let mut attempted = false;
    let mut first_error = None;
    for offset in 0..4 {
        let lane = (first + offset) % 4;
        let result = match (lane, active) {
            (0, Some(context)) => scan_purge(store.clone(), context, &mut cursor.purge, limits).await,
            (1, Some(context)) => GcRepository::new(store.clone())
                .admit_multipart(context, super::super::runtime::now_ms()?, limits)
                .await
                .map(|_| ())
                .map_err(ScanError::from),
            (2, Some(_)) if !attempted => {
                scan_retired(store.clone(), worker, &mut cursor.system, limits, &mut attempted).await
            }
            (3, _) if !attempted => {
                scan_tasks(
                    store.clone(),
                    worker,
                    catalog,
                    &mut cursor.task,
                    limits,
                    &mut attempted,
                )
                .await
            }
            _ => Ok(()),
        };
        if let Err(error) = result {
            tracing::error!(catalog = %catalog, lane = lane_name(lane), %error,
                "GC discovery lane failed; observed local positions and durable facts retained");
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn lane_name(lane: u8) -> &'static str {
    match lane {
        0 => "purge",
        1 => "multipart",
        2 => "retired",
        _ => "tasks",
    }
}

async fn scan_tasks(
    store: Arc<BudgetedGcStore>,
    worker: &GcWorker,
    catalog: CatalogId,
    after: &mut Vec<u8>,
    limits: GcLimits,
    attempted: &mut bool,
) -> ScanResult {
    let scan = GcLocalScan::catalog(
        &GcScan {
            native: false,
            catalog,
            scope: Some(CatalogScope::GcTask),
            prefix: Vec::new(),
            after: Vec::new(),
            items: usize::from(limits.page_items),
            bytes: limits.page_bytes as usize,
        },
        after.clone(),
    )?;
    let page = store.scan_gc_local(scan.clone()).await?;
    scan.validate_page(&page)?;
    if page.items.is_empty() {
        after.clear();
    }
    let mut first_error = None;
    for item in page.items {
        after.clone_from(&item.key);
        if let Err(error) = advance_task(store.clone(), worker, &item.key, &item.value, attempted).await {
            tracing::error!(catalog = %catalog, raw_key_bytes = item.key.len(), %error,
                "GC task row retained; local discovery continues after observed key");
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
        if *attempted {
            break;
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn advance_task(
    store: Arc<BudgetedGcStore>,
    worker: &GcWorker,
    key: &[u8],
    bytes: &[u8],
    attempted: &mut bool,
) -> ScanResult {
    let key = IcebergKey::decode(key)?;
    let StorageRecord::GcTask(task) = StorageRecord::decode(&key, bytes)? else {
        return Err("GC task scan encountered a non-task record".into());
    };
    if task.kind == GcTaskKind::LiveTable && task.phase != GcPhase::Complete {
        *attempted = true;
        let progress = GcRepository::new(store).retire_live(&task).await?;
        tracing::info!(catalog = %task.context.catalog, task = %task.identity, phase = ?progress.phase,
            "retired legacy live GC task");
    } else if !matches!(task.phase, GcPhase::Complete | GcPhase::Quarantined) && !task.paused {
        let now_ms = super::super::runtime::now_ms()?;
        if now_ms >= task.retry_at_ms {
            *attempted = true;
            let progress = worker.run(&task, now_ms).await?;
            tracing::debug!(catalog = %task.context.catalog, task = %task.identity, phase = ?progress.phase,
                "GC task advanced");
        }
    }
    Ok(())
}

async fn scan_retired(
    store: Arc<BudgetedGcStore>,
    worker: &GcWorker,
    after: &mut Vec<u8>,
    limits: GcLimits,
    attempted: &mut bool,
) -> ScanResult {
    let scan = GcLocalScan::system(
        &GcSystemScan {
            after: Vec::new(),
            items: 1,
            bytes: limits.page_bytes as usize,
        },
        after.clone(),
    )?;
    let page = store.scan_gc_local(scan.clone()).await?;
    scan.validate_page(&page)?;
    let Some(item) = page.items.first() else {
        after.clear();
        return Ok(());
    };
    after.clone_from(&item.key);
    let key = IcebergKey::decode(&item.key)?;
    if !matches!(
        key,
        IcebergKey::System {
            scope: SystemScope::ManagementOperation,
            ..
        }
    ) {
        return Ok(());
    }
    let StorageRecord::Management(operation) = StorageRecord::decode(&key, &item.value)? else {
        return Err("management scan encountered a non-management record".into());
    };
    if operation.request.action == ManagementAction::Clear && operation.phase == ManagementPhase::Complete {
        let task = GcRepository::new(store)
            .admit_retired(&operation, super::super::runtime::now_ms()?, limits)
            .await?;
        let now_ms = super::super::runtime::now_ms()?;
        if !matches!(task.phase, GcPhase::Complete | GcPhase::Quarantined)
            && !task.paused
            && now_ms >= task.retry_at_ms
        {
            *attempted = true;
            worker.run(&task, now_ms).await?;
        }
    }
    Ok(())
}

async fn scan_purge(
    store: Arc<BudgetedGcStore>,
    context: CatalogContext,
    after: &mut Vec<u8>,
    limits: GcLimits,
) -> ScanResult {
    let scan = GcLocalScan::catalog(
        &GcScan {
            native: false,
            catalog: context.catalog,
            scope: Some(CatalogScope::Reclamation),
            prefix: Vec::new(),
            after: Vec::new(),
            items: 1,
            bytes: limits.page_bytes as usize,
        },
        after.clone(),
    )?;
    let page = store.scan_gc_local(scan.clone()).await?;
    scan.validate_page(&page)?;
    let Some(item) = page.items.first() else {
        after.clear();
        return Ok(());
    };
    after.clone_from(&item.key);
    let key = IcebergKey::decode(&item.key)?;
    let StorageRecord::TablePurgeTask(marker) = StorageRecord::decode(&key, &item.value)? else {
        return Err("purge scan encountered a non-purge record".into());
    };
    GcRepository::new(store)
        .admit_purge(context, &marker, super::super::runtime::now_ms()?, limits)
        .await?;
    Ok(())
}
