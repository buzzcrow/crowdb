use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::{CatalogError, CatalogRepository, RootState, RoutedCatalogStore};
use crowdb_access_iceberg::file::{FileBlockStore, MultipartRecovery};

pub(super) async fn run(
    catalog: Arc<CatalogRepository>,
    store: Arc<RoutedCatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut context = None;
    let mut continuation = None;
    loop {
        interval.tick().await;
        let status = tokio::time::timeout(Duration::from_secs(1), catalog.status()).await;
        let Ok(Ok((root, authority))) = status else {
            continuation = None;
            tracing::warn!(
                ?status,
                "multipart catalog status unavailable; deferring recovery"
            );
            continue;
        };
        if root.state != RootState::Ready || context != Some(root.context) {
            context = Some(root.context);
            continuation = None;
        }
        if root.state != RootState::Ready {
            continue;
        }
        let budget = Duration::from_millis(authority.admission_bounds.request_ms);
        let recovery = MultipartRecovery::new(store.clone(), blocks.clone(), 64 * 1024, 256 * 1024)
            .and_then(|recovery| recovery.with_session_timeout(budget));
        let Ok(recovery) = recovery else {
            tracing::error!("multipart recovery bounds invalid; deferring page until catalog is corrected");
            continue;
        };
        let Some(now_ms) = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        else {
            tracing::error!("multipart recovery clock invalid; deferring expiry processing");
            continue;
        };
        let page_budget = budget.saturating_mul(5).saturating_add(Duration::from_secs(2));
        let result = tokio::time::timeout(
            page_budget,
            recovery.recover_page(root.context, continuation.clone(), now_ms),
        )
        .await;
        match result {
            Ok(Ok(page)) => {
                continuation = page.continuation;
                for (upload, error) in page.failures {
                    tracing::error!(%upload, %error, "multipart recovery failed; retaining evidence for a later sweep");
                }
                tracing::debug!(
                    progressed = page.progressed,
                    deferred = page.deferred,
                    retained = page.retained,
                    awaiting_seal = page.awaiting_seal.len(),
                    "multipart recovery page processed"
                );
            }
            Ok(Err(CatalogError::Busy | CatalogError::Conflict)) => {
                continuation = None;
            }
            Ok(Err(error)) => {
                continuation = None;
                tracing::error!(%error, "multipart recovery scan failed; restarting sweep");
            }
            Err(_) => tracing::warn!("multipart page scan budget exhausted; retrying cursor"),
        }
    }
}
