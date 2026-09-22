use std::sync::Arc;
use std::time::Duration;

use crowdb_access_iceberg::catalog::{CatalogRepository, RootState};
use crowdb_access_iceberg::namespace::NamespaceRecovery;

pub(super) async fn run(repository: Arc<CatalogRepository>, recovery: NamespaceRecovery) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut context = None;
    let mut continuation = None;
    loop {
        interval.tick().await;
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            let (root, _) = repository.status().await?;
            if root.state != RootState::Ready {
                continuation = None;
                return Ok(None);
            }
            if context != Some(root.context) {
                context = Some(root.context);
                continuation = None;
            }
            recovery
                .recover_page(root.context, continuation.clone())
                .await
                .map(Some)
        })
        .await;
        match result {
            Ok(Ok(Some(page))) => {
                continuation = page.continuation;
                for (operation, error) in page.failures {
                    tracing::error!(%operation, %error, "namespace recovery failed; retrying on a later sweep");
                }
                tracing::debug!(
                    completed = page.completed,
                    deferred = page.deferred,
                    "namespace recovery page processed"
                );
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => {
                continuation = None;
                tracing::error!(%error, "namespace recovery scan failed; restarting sweep");
            }
            Err(_) => tracing::warn!("namespace recovery time budget exhausted; retrying page"),
        }
    }
}
