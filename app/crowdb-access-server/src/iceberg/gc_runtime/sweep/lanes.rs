// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ScanPosition, ScanResult};
use crowdb_access_iceberg::{
    catalog::CatalogContext,
    gc::{
        BudgetedGcStore, GcLimits, GcLocalScan, GcPhase, GcRepository, GcScan, GcStore, GcSystemScan,
        GcTaskKind, GcWorker,
    },
    key::{CatalogId, CatalogScope, IcebergKey, SystemScope},
    operation::{ManagementAction, ManagementPhase},
    record::StorageRecord,
};
use std::sync::Arc;

pub(super) async fn scan_tasks(
    store: Arc<BudgetedGcStore>,
    worker: &GcWorker,
    catalog: CatalogId,
    cursor: &mut ScanPosition,
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
        cursor.task.clone(),
    )?;
    let page = store.scan_gc_local(scan.clone()).await?;
    scan.validate_page(&page)?;
    if page.items.is_empty() {
        cursor.exhausted(3);
    }
    let mut first_error = None;
    for item in page.items {
        cursor.observed(3, &item.key);
        if let Err(error) =
            advance_task(store.clone(), worker, &item.key, &item.value, cursor, attempted).await
        {
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
    cursor: &mut ScanPosition,
    attempted: &mut bool,
) -> ScanResult {
    let key = IcebergKey::decode(key)?;
    let StorageRecord::GcTask(task) = StorageRecord::decode(&key, bytes)? else {
        return Err("GC task scan encountered a non-task record".into());
    };
    let now_ms = super::super::super::runtime::now_ms()?;
    if task.kind == GcTaskKind::LiveTable && task.phase != GcPhase::Complete {
        *attempted = true;
        let progress = GcRepository::new(store).retire_live(&task).await?;
        cursor.advanced(3, &task, &progress, now_ms);
        tracing::info!(catalog = %task.context.catalog, task = %task.identity, phase = ?progress.phase,
            "retired legacy live GC task");
    } else if cursor.runnable(&task, now_ms) {
        *attempted = true;
        let progress = worker.run(&task, now_ms).await?;
        cursor.advanced(3, &task, &progress, now_ms);
        tracing::debug!(catalog = %task.context.catalog, task = %task.identity, phase = ?progress.phase,
            "GC task advanced");
    }
    Ok(())
}

pub(super) async fn scan_retired(
    store: Arc<BudgetedGcStore>,
    worker: &GcWorker,
    cursor: &mut ScanPosition,
    limits: GcLimits,
    attempted: &mut bool,
) -> ScanResult {
    let scan = GcLocalScan::system(
        &GcSystemScan {
            after: Vec::new(),
            items: 1,
            bytes: limits.page_bytes as usize,
        },
        cursor.system.clone(),
    )?;
    let page = store.scan_gc_local(scan.clone()).await?;
    scan.validate_page(&page)?;
    let Some(item) = page.items.first() else {
        cursor.exhausted(2);
        return Ok(());
    };
    cursor.observed(2, &item.key);
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
        let now_ms = super::super::super::runtime::now_ms()?;
        let task = GcRepository::new(store)
            .admit_retired(&operation, now_ms, limits)
            .await?;
        if cursor.runnable(&task, now_ms) {
            *attempted = true;
            let progress = worker.run(&task, now_ms).await?;
            cursor.advanced(2, &task, &progress, now_ms);
        }
    }
    Ok(())
}

pub(super) async fn scan_purge(
    store: Arc<BudgetedGcStore>,
    context: CatalogContext,
    cursor: &mut ScanPosition,
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
        cursor.purge.clone(),
    )?;
    let page = store.scan_gc_local(scan.clone()).await?;
    scan.validate_page(&page)?;
    let Some(item) = page.items.first() else {
        cursor.exhausted(0);
        return Ok(());
    };
    cursor.observed(0, &item.key);
    let key = IcebergKey::decode(&item.key)?;
    let StorageRecord::TablePurgeTask(marker) = StorageRecord::decode(&key, &item.value)? else {
        return Err("purge scan encountered a non-purge record".into());
    };
    let now_ms = super::super::super::runtime::now_ms()?;
    let task = GcRepository::new(store)
        .admit_purge(context, &marker, now_ms, limits)
        .await?;
    if cursor.runnable(&task, now_ms) {
        cursor.queued(3);
    }
    Ok(())
}
