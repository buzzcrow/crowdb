use std::sync::Arc;

use crowdb_access_iceberg::{
    catalog::{
        CatalogContext, CatalogRepository, CatalogStore, ManagementPrivilege, RootState, RoutedCatalogStore,
    },
    gc::{GcLimits, GcRepository, GcTask},
    key::{CatalogId, OperationId, TableId},
    operation::{ManagementAction, ManagementPhase},
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle},
    wire::BearerAuthenticator,
};

use super::gc_runtime::GcRuntimeConfig;
use crate::config::IcebergGcConfig;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(super) async fn manage(
    catalog: &CatalogRepository,
    store: Arc<RoutedCatalogStore>,
    authentication: &BearerAuthenticator,
    arguments: &[String],
    gc_settings: &IcebergGcConfig,
) -> Result<(), BoxError> {
    let token = std::env::var("CROWDB_ICEBERG_TOKEN")?;
    let principal = authentication
        .authenticate(&format!("Bearer {token}"))
        .ok_or("invalid management bearer token")?;
    if principal.management == ManagementPrivilege::None {
        return Err("management privilege is required".into());
    }
    let config = GcRuntimeConfig::from_config(gc_settings)?;
    let limits = config.limits;
    let repository = GcRepository::new(store.clone());
    match arguments.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["limits"] => {
            println!("{}", serde_json::json!({
                "enabled": config.enabled,
                "interval_ms": config.interval_ms,
                "page_items": limits.page_items,
                "page_bytes": limits.page_bytes,
                "step_bytes": limits.step_bytes,
                "step_ms": limits.step_ms,
                "concurrency": limits.concurrency,
                "minimum_retention_ms": limits.minimum_retention_ms,
                "retry_base_ms": limits.retry_base_ms,
                "retry_max_ms": limits.retry_max_ms,
                "corruption_attempts": limits.corruption_attempts,
                "kv_bytes": config.kv_bytes,
                "kv_requests": config.kv_requests,
                "chunk_bytes": config.chunk_bytes,
                "chunk_requests": config.chunk_requests,
            }));
        }
        ["start-table", identity, table] => {
            start_table(catalog, store.as_ref(), &repository, identity, table, limits).await?;
        }
        ["start-retired", identity, catalog_id, epoch] => {
            if principal.management != ManagementPrivilege::Clear {
                return Err("clear privilege is required for retired catalogs".into());
            }
            start_retired(catalog, &repository, identity, catalog_id, epoch, limits).await?;
        }
        ["inspect" | "pause" | "resume" | "retry", catalog_id, identity] => {
            let catalog_id: CatalogId = catalog_id.parse()?;
            let identity: OperationId = identity.parse()?;
            let task = repository.task(catalog_id, identity).await?.ok_or("GC task is missing")?;
            let task = match arguments[0].as_str() {
                "pause" => repository.pause(&task, true).await?,
                "resume" => repository.pause(&task, false).await?,
                "retry" => repository.retry(&task).await?,
                _ => task,
            };
            show(&task);
        }
        _ => return Err("usage: crowdb-access-server iceberg gc limits | start-table UUID TABLE_ID | start-retired UUID CATALOG_ID EPOCH | inspect|pause|resume|retry CATALOG_ID TASK_ID".into()),
    }
    Ok(())
}

async fn start_table(
    catalog: &CatalogRepository,
    store: &RoutedCatalogStore,
    repository: &GcRepository,
    identity: &str,
    table: &str,
    limits: GcLimits,
) -> Result<(), BoxError> {
    let (root, _) = catalog.status().await?;
    if root.state != RootState::Ready {
        return Err("catalog is not ready".into());
    }
    let table: TableId = table.parse()?;
    let identity: OperationId = identity.parse()?;
    let head = load_head(store, root.context.catalog, table).await?;
    if head.lifecycle != TableLifecycle::Tombstone {
        return Err("live-table GC is disabled; only tombstoned tables can be reclaimed".into());
    }
    if let Some(existing) = repository.task(root.context.catalog, identity).await? {
        if existing.context != root.context || existing.head.as_ref() != Some(&head) {
            return Err("GC task identity is already bound to another table state".into());
        }
        show(&existing);
        return Ok(());
    }
    let task = GcTask::plan(
        root.context,
        identity,
        Some(head),
        super::runtime::now_ms()?,
        limits,
    )?;
    repository.create(&task).await?;
    show(&task);
    Ok(())
}

async fn start_retired(
    catalog: &CatalogRepository,
    repository: &GcRepository,
    identity: &str,
    catalog_id: &str,
    epoch: &str,
    limits: GcLimits,
) -> Result<(), BoxError> {
    let context = CatalogContext {
        catalog: catalog_id.parse()?,
        activation_epoch: epoch.parse()?,
    };
    context.validate()?;
    let identity: OperationId = identity.parse()?;
    let clear = catalog
        .operation(identity)
        .await?
        .ok_or("clear operation is missing")?;
    if clear.phase != ManagementPhase::Complete
        || clear.request.action != ManagementAction::Clear
        || clear.request.confirmation != Some(context.catalog)
        || clear.request.expected_epoch != context.activation_epoch
    {
        return Err("clear operation does not authorize this retired context".into());
    }
    let task = repository
        .admit_retired(&clear, super::runtime::now_ms()?, limits)
        .await?;
    show(&task);
    Ok(())
}

async fn load_head(
    store: &RoutedCatalogStore,
    catalog: CatalogId,
    table: TableId,
) -> Result<TableHead, BoxError> {
    let key = head_key(catalog, table);
    let value = store.get(&key.encode()?).await?.ok_or("table head is missing")?;
    let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
        return Err("table head has an invalid record".into());
    };
    Ok(*head)
}

fn show(task: &GcTask) {
    println!(
        "{}",
        serde_json::json!({
            "catalog_id": task.context.catalog.to_string(),
            "task_id": task.identity.to_string(),
            "kind": format!("{:?}", task.kind),
            "phase": format!("{:?}", task.phase),
            "revision": task.revision,
            "paused": task.paused,
            "stalled": format!("{:?}", task.stalled),
            "attempts": task.attempts,
            "retry_at_ms": task.retry_at_ms,
            "marked": task.marked,
            "deleted": task.deleted,
            "reclaimed_bytes": task.reclaimed_bytes,
            "deferred_ranges": task.deferred_ranges,
        })
    );
}
