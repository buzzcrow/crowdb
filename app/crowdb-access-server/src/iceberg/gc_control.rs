use std::sync::Arc;

use crowdb_access_iceberg::{
    catalog::{
        CatalogContext, CatalogLifecycle, CatalogRepository, CatalogStore, ManagementPrivilege, RootState,
        RoutedCatalogStore,
    },
    gc::{GcLimits, GcPin, GcRepository, GcTask, ReaderPins},
    key::{CatalogId, CatalogScope, IcebergKey, OperationId, TableId},
    operation::{ManagementAction, ManagementPhase},
    record::StorageRecord,
    table::{head_key, TableHead},
    wire::BearerAuthenticator,
};

use super::gc_runtime::GcRuntimeConfig;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(super) async fn manage(
    catalog: &CatalogRepository,
    store: Arc<RoutedCatalogStore>,
    authentication: &BearerAuthenticator,
    arguments: &[String],
) -> Result<(), BoxError> {
    let token = std::env::var("CROWDB_ICEBERG_TOKEN")?;
    let principal = authentication
        .authenticate(&format!("Bearer {token}"))
        .ok_or("invalid management bearer token")?;
    if principal.management == ManagementPrivilege::None {
        return Err("management privilege is required".into());
    }
    let config = GcRuntimeConfig::from_env()?;
    let limits = config.limits;
    let repository = GcRepository::new(store.clone());
    let pins = ReaderPins::new(store.clone());
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
            start_retired(catalog, store.as_ref(), &repository, identity, catalog_id, epoch, limits).await?;
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
        ["pin", identity, table] => {
            pin_table(catalog, store.as_ref(), &pins, principal.name, identity, table).await?;
        }
        ["unpin", catalog_id, table, identity] => {
            unpin_table(&pins, principal.name, catalog_id, table, identity).await?;
        }
        _ => return Err("usage: crowdb-iceberg gc limits | start-table UUID TABLE_ID | start-retired UUID CATALOG_ID EPOCH | inspect|pause|resume|retry CATALOG_ID TASK_ID | pin UUID TABLE_ID | unpin CATALOG_ID TABLE_ID PIN_ID".into()),
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
    store: &RoutedCatalogStore,
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
    let (root, _) = catalog.status().await?;
    if root.state != RootState::Ready || root.context.activation_epoch <= context.activation_epoch {
        return Err("retired catalog epoch is not older than the active root".into());
    }
    let key = IcebergKey::Catalog {
        catalog: context.catalog,
        scope: CatalogScope::Authority,
        suffix: Vec::new(),
    };
    let value = store
        .get(&key.encode()?)
        .await?
        .ok_or("retired catalog authority is missing")?;
    let StorageRecord::Authority(authority) = StorageRecord::decode(&key, &value.bytes)? else {
        return Err("retired catalog authority has an invalid record".into());
    };
    if authority.lifecycle != CatalogLifecycle::Retired {
        return Err("catalog is not retired".into());
    }
    if let Some(existing) = repository.task(context.catalog, identity).await? {
        if existing.context != context || existing.head.is_some() {
            return Err("GC task identity is already bound to another catalog state".into());
        }
        show(&existing);
        return Ok(());
    }
    let task = GcTask::plan(context, identity, None, super::runtime::now_ms()?, limits)?;
    repository.create(&task).await?;
    show(&task);
    Ok(())
}

async fn pin_table(
    catalog: &CatalogRepository,
    store: &RoutedCatalogStore,
    pins: &ReaderPins,
    principal: &str,
    identity: &str,
    table: &str,
) -> Result<(), BoxError> {
    let (root, _) = catalog.status().await?;
    if root.state != RootState::Ready {
        return Err("catalog is not ready".into());
    }
    let table: TableId = table.parse()?;
    let pin = GcPin {
        context: root.context,
        identity: identity.parse()?,
        head: load_head(store, root.context.catalog, table).await?,
        principal: principal.to_owned(),
        expires_ms: 0,
        released: false,
        operator: true,
        protects_uploads: true,
    };
    pins.acquire(&pin).await?;
    println!(
        "{}",
        serde_json::json!({"pin": pin.identity.to_string(), "table": table.to_string()})
    );
    Ok(())
}

async fn unpin_table(
    pins: &ReaderPins,
    principal: &str,
    catalog_id: &str,
    table: &str,
    identity: &str,
) -> Result<(), BoxError> {
    let catalog_id: CatalogId = catalog_id.parse()?;
    let table: TableId = table.parse()?;
    let identity: OperationId = identity.parse()?;
    let pin = pins
        .get(catalog_id, table, identity)
        .await?
        .ok_or("GC pin is missing")?;
    if !pin.operator || pin.principal != principal {
        return Err("operator pin is owned by another principal".into());
    }
    pins.release(&pin).await?;
    println!("{}", serde_json::json!({"released": identity.to_string()}));
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
