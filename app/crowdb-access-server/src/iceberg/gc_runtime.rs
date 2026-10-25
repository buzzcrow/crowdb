use std::{sync::Arc, time::Duration};

use crate::config::IcebergGcConfig;
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, RootState, RoutedCatalogStore},
    file::FileBlockStore,
    gc::{BudgetedGcBlocks, BudgetedGcStore, GcIoBudget, GcLimits, GcRepository, GcWorker},
    key::CatalogId,
};
use crowdb_chunk_client::ChunkIoClient;

mod schedule;
mod sweep;
use schedule::Schedule;
use sweep::scan_and_advance;

#[cfg(feature = "test-util")]
mod test_util;
use sweep::ScanPosition;
#[cfg(feature = "test-util")]
pub use test_util::TestGcSweep;

pub(super) struct GcRuntimeConfig {
    pub limits: GcLimits,
    pub interval_ms: u64,
    pub catalogs: Vec<CatalogId>,
    pub enabled: bool,
    pub kv_bytes: u64,
    pub kv_requests: u32,
    pub chunk_bytes: u64,
    pub chunk_requests: u32,
}

impl GcRuntimeConfig {
    pub fn from_config(file: &IcebergGcConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut limits = GcLimits::default();
        limits.step_bytes = setting_or("CROWDB_ICEBERG_GC_STEP_BYTES", file.step_bytes, limits.step_bytes)?;
        limits.step_ms = setting_or("CROWDB_ICEBERG_GC_STEP_MS", file.step_ms, limits.step_ms)?;
        limits.page_items = setting_or("CROWDB_ICEBERG_GC_PAGE_ITEMS", file.page_items, limits.page_items)?;
        limits.page_bytes = setting_or("CROWDB_ICEBERG_GC_PAGE_BYTES", file.page_bytes, limits.page_bytes)?;
        limits.concurrency = setting_or(
            "CROWDB_ICEBERG_GC_CONCURRENCY",
            file.concurrency,
            limits.concurrency,
        )?;
        limits.retry_base_ms = setting_or(
            "CROWDB_ICEBERG_GC_RETRY_BASE_MS",
            file.retry_base_ms,
            limits.retry_base_ms,
        )?;
        limits.retry_max_ms = setting_or(
            "CROWDB_ICEBERG_GC_RETRY_MAX_MS",
            file.retry_max_ms,
            limits.retry_max_ms,
        )?;
        limits.corruption_attempts = setting_or(
            "CROWDB_ICEBERG_GC_CORRUPTION_ATTEMPTS",
            file.corruption_attempts,
            limits.corruption_attempts,
        )?;
        limits.minimum_retention_ms = setting_or(
            "CROWDB_ICEBERG_GC_MINIMUM_RETENTION_MS",
            file.minimum_retention_ms,
            limits.minimum_retention_ms,
        )?;
        limits.validate()?;
        if limits.concurrency != 1 {
            return Err("GC scheduler currently supports one concurrent step".into());
        }
        if limits.minimum_retention_ms < GcLimits::default().minimum_retention_ms {
            return Err("GC retention must be at least seven days".into());
        }
        let interval_ms = setting_or("CROWDB_ICEBERG_GC_INTERVAL_MS", file.interval_ms, 30 * 60_000_u64)?;
        if !(100..=24 * 60 * 60_000).contains(&interval_ms) {
            return Err("GC interval must be between 100 and 86400000 milliseconds".into());
        }
        let names = match &file.catalogs {
            Some(names) => names.clone(),
            None => match std::env::var("CROWDB_ICEBERG_GC_CATALOGS") {
                Ok(value) => value.split(',').map(str::to_owned).collect(),
                Err(std::env::VarError::NotPresent) => Vec::new(),
                Err(error) => return Err(error.into()),
            },
        };
        let catalogs = names
            .iter()
            .map(String::as_str)
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
        let enabled = match file.enabled {
            Some(enabled) => enabled,
            None => match std::env::var("CROWDB_ICEBERG_GC_ENABLED").as_deref() {
                Ok("0") => false,
                Ok("1") | Err(std::env::VarError::NotPresent) => true,
                _ => return Err("CROWDB_ICEBERG_GC_ENABLED must be 0 or 1".into()),
            },
        };
        let kv_bytes = setting_or("CROWDB_ICEBERG_GC_KV_BYTES", file.kv_bytes, 64 * 1024 * 1024_u64)?;
        let kv_requests = setting_or("CROWDB_ICEBERG_GC_KV_REQUESTS", file.kv_requests, 128_u32)?;
        let chunk_bytes = setting_or(
            "CROWDB_ICEBERG_GC_CHUNK_BYTES",
            file.chunk_bytes,
            8 * 1024 * 1024_u64,
        )?;
        let chunk_requests = setting_or("CROWDB_ICEBERG_GC_CHUNK_REQUESTS", file.chunk_requests, 128_u32)?;
        if !(4 * 1024 * 1024..=256 * 1024 * 1024).contains(&kv_bytes)
            || !(8..=4096).contains(&kv_requests)
            || !(256 * 1024..=64 * 1024 * 1024).contains(&chunk_bytes)
            || !(1..=4096).contains(&chunk_requests)
        {
            return Err("GC KV or chunk I/O budget is outside supported bounds".into());
        }
        Ok(Self {
            limits,
            interval_ms,
            catalogs,
            enabled,
            kv_bytes,
            kv_requests,
            chunk_bytes,
            chunk_requests,
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

fn setting_or<T>(
    name: &str,
    configured: Option<T>,
    default: T,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match configured {
        Some(value) => Ok(value),
        None => setting(name, default),
    }
}

pub(super) async fn run(
    catalog: Arc<CatalogRepository>,
    store: Arc<RoutedCatalogStore>,
    chunks: ChunkIoClient,
    config: GcRuntimeConfig,
) {
    if !config.enabled {
        return std::future::pending().await;
    }
    let budget = Arc::new(GcIoBudget::new(
        config.kv_bytes,
        config.kv_requests,
        config.chunk_bytes,
        config.chunk_requests,
    ));
    let metered_store = Arc::new(BudgetedGcStore::new(store.clone(), budget.clone()));
    let blocks: Arc<dyn FileBlockStore> = Arc::new(BudgetedGcBlocks::native(
        chunks,
        metered_store.clone(),
        budget.clone(),
    ));
    let repository = GcRepository::new(metered_store.clone());
    let worker = match GcWorker::new(repository, blocks, config.limits) {
        Ok(worker) => worker,
        Err(error) => {
            tracing::error!(%error, "GC worker configuration invalid; background processing stopped");
            return;
        }
    };
    let mut schedule = Schedule::default();
    let mut status_failures = 0_u32;
    loop {
        tokio::time::sleep_until(schedule.next_due()).await;
        let Ok(Ok((root, _))) = tokio::time::timeout(
            Duration::from_millis(config.interval_ms.min(60_000)),
            catalog.status(),
        )
        .await
        else {
            status_failures = status_failures.saturating_add(1);
            tracing::warn!("GC catalog status unavailable; retrying later");
            tokio::time::sleep(Duration::from_millis(
                config.limits.retry_delay_ms(status_failures),
            ))
            .await;
            continue;
        };
        status_failures = 0;
        if root.state != RootState::Ready {
            tokio::time::sleep(Duration::from_millis(config.interval_ms)).await;
            continue;
        }
        schedule.retain_scopes(&config.catalogs, root.context.catalog);
        let Some(slot) = schedule.ready() else {
            continue;
        };
        let selected = schedule.catalog(slot);
        let cursor = schedule.position(slot);
        let before = cursor.progress;
        let result = tokio::time::timeout(
            Duration::from_millis(u64::from(config.limits.step_ms) * 2 + 1000),
            scan_and_advance(
                metered_store.clone(),
                &worker,
                budget.as_ref(),
                selected,
                cursor,
                (selected == root.context.catalog).then_some(root.context),
                config.limits,
            ),
        )
        .await;
        let error = if let Ok(error) = result {
            error
        } else {
            tracing::warn!(catalog = %selected, "GC scheduler window exceeded its budget; observed local positions retained");
            Some("GC scheduler window timed out".into())
        };
        let report = schedule.position(slot).report(before, error.is_some());
        tracing::debug!(catalog = %selected, busy = report.busy, progressed = report.progressed,
            failed = report.failed, retry_at_ms = report.retry_at_ms, "GC scheduler window completed");
        schedule.finish(slot, report, config.interval_ms, config.limits);
    }
}
