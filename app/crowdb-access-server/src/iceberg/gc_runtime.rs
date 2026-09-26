use std::{sync::Arc, time::Duration};

use crowdb_access_iceberg::{
    catalog::{CatalogRepository, RootState, RoutedCatalogStore},
    file::FileBlockStore,
    gc::{GcLimits, GcPhase, GcRepository, GcScan, GcStore, GcWorker},
    key::{CatalogId, CatalogScope, IcebergKey},
    record::StorageRecord,
};

pub(super) struct GcRuntimeConfig {
    pub limits: GcLimits,
    pub interval_ms: u64,
    pub catalogs: Vec<CatalogId>,
    pub enabled: bool,
}

impl GcRuntimeConfig {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut limits = GcLimits::default();
        limits.step_bytes = setting("CROWDB_ICEBERG_GC_STEP_BYTES", limits.step_bytes)?;
        limits.step_ms = setting("CROWDB_ICEBERG_GC_STEP_MS", limits.step_ms)?;
        limits.page_items = setting("CROWDB_ICEBERG_GC_PAGE_ITEMS", limits.page_items)?;
        limits.page_bytes = setting("CROWDB_ICEBERG_GC_PAGE_BYTES", limits.page_bytes)?;
        limits.concurrency = setting("CROWDB_ICEBERG_GC_CONCURRENCY", limits.concurrency)?;
        limits.retry_base_ms = setting("CROWDB_ICEBERG_GC_RETRY_BASE_MS", limits.retry_base_ms)?;
        limits.retry_max_ms = setting("CROWDB_ICEBERG_GC_RETRY_MAX_MS", limits.retry_max_ms)?;
        limits.corruption_attempts = setting(
            "CROWDB_ICEBERG_GC_CORRUPTION_ATTEMPTS",
            limits.corruption_attempts,
        )?;
        limits.minimum_retention_ms = setting(
            "CROWDB_ICEBERG_GC_MINIMUM_RETENTION_MS",
            limits.minimum_retention_ms,
        )?;
        limits.validate()?;
        if limits.concurrency != 1 {
            return Err("GC scheduler currently supports one concurrent step".into());
        }
        if limits.minimum_retention_ms < GcLimits::default().minimum_retention_ms {
            return Err("GC retention must be at least seven days".into());
        }
        let interval_ms = setting("CROWDB_ICEBERG_GC_INTERVAL_MS", 1000_u64)?;
        if !(100..=60_000).contains(&interval_ms) {
            return Err("GC interval must be between 100 and 60000 milliseconds".into());
        }
        let catalogs = match std::env::var("CROWDB_ICEBERG_GC_CATALOGS") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => String::new(),
            Err(error) => return Err(error.into()),
        }
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::parse)
        .collect::<Result<Vec<_>, _>>()?;
        if catalogs.len() > 64 {
            return Err("too many GC catalog scopes".into());
        }
        let mut catalogs: Vec<CatalogId> = catalogs;
        catalogs.sort_unstable();
        catalogs.dedup();
        let enabled = match std::env::var("CROWDB_ICEBERG_GC_ENABLED").as_deref() {
            Ok("1") => true,
            Ok("0") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("CROWDB_ICEBERG_GC_ENABLED must be 0 or 1".into()),
        };
        Ok(Self {
            limits,
            interval_ms,
            catalogs,
            enabled,
        })
    }
}

fn setting<T>(name: &str, default: T) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn run(
    catalog: Arc<CatalogRepository>,
    store: Arc<RoutedCatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    config: GcRuntimeConfig,
) {
    if !config.enabled {
        return std::future::pending().await;
    }
    let repository = GcRepository::new(store.clone());
    let worker = match GcWorker::new(repository, blocks, config.limits) {
        Ok(worker) => worker,
        Err(error) => {
            tracing::error!(%error, "GC worker configuration invalid; background processing stopped");
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_millis(config.interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursors = Vec::<(CatalogId, Vec<u8>)>::new();
    let mut index = 0_usize;
    loop {
        interval.tick().await;
        let Ok(Ok((root, _))) =
            tokio::time::timeout(Duration::from_millis(config.interval_ms), catalog.status()).await
        else {
            tracing::warn!("GC catalog status unavailable; retrying later");
            continue;
        };
        if root.state != RootState::Ready {
            continue;
        }
        let mut catalogs = config.catalogs.clone();
        if !catalogs.contains(&root.context.catalog) {
            catalogs.push(root.context.catalog);
        }
        if catalogs.is_empty() {
            continue;
        }
        let selected = catalogs[index % catalogs.len()];
        index = index.wrapping_add(1);
        let cursor = cursors.iter_mut().find(|(catalog, _)| *catalog == selected);
        let after = cursor.as_ref().map_or_else(Vec::new, |(_, after)| after.clone());
        let result = tokio::time::timeout(
            Duration::from_millis(u64::from(config.limits.step_ms) * 2 + 1000),
            scan_and_advance(store.as_ref(), &worker, selected, after, config.limits),
        )
        .await;
        match result {
            Ok(Ok(next)) => {
                if let Some((_, cursor)) = cursor {
                    *cursor = next;
                } else {
                    cursors.push((selected, next));
                }
            }
            Ok(Err(error)) => {
                tracing::error!(catalog = %selected, %error, "GC scheduler scan failed; retrying catalog");
            }
            Err(_) => {
                tracing::warn!(catalog = %selected, "GC scheduler scan exceeded its budget");
            }
        }
    }
}

async fn scan_and_advance(
    store: &RoutedCatalogStore,
    worker: &GcWorker,
    catalog: CatalogId,
    after: Vec<u8>,
    limits: GcLimits,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let scan = GcScan {
        catalog,
        scope: Some(CatalogScope::GcTask),
        prefix: Vec::new(),
        after,
        items: usize::from(limits.page_items),
        bytes: limits.page_bytes as usize,
    };
    let page = store.scan_gc(scan.clone()).await?;
    scan.validate_page(&page)?;
    let mut next = Vec::new();
    for item in page.items {
        next = item.key.clone();
        let key = IcebergKey::decode(&item.key)?;
        let StorageRecord::GcTask(task) = StorageRecord::decode(&key, &item.value)? else {
            return Err("GC task scan encountered a non-task record".into());
        };
        if !matches!(task.phase, GcPhase::Complete | GcPhase::Quarantined) && !task.paused {
            let now_ms = super::runtime::now_ms()?;
            if now_ms >= task.retry_at_ms {
                match worker.run(&task, now_ms).await {
                    Ok(progress) => {
                        tracing::debug!(catalog = %catalog, task = %task.identity, phase = ?progress.phase, "GC task advanced");
                    }
                    Err(error) => {
                        tracing::error!(catalog = %catalog, task = %task.identity, %error, "GC task retained for retry");
                    }
                }
                break;
            }
        }
    }
    Ok(next)
}
