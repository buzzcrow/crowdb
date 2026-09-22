use std::sync::Arc;
use std::time::Duration;

use crowdb_access_iceberg::catalog::{CatalogRepository, RootState};
use crowdb_access_iceberg::namespace::NamespaceRecovery;

pub(super) async fn run(repository: Arc<CatalogRepository>, recovery: NamespaceRecovery) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut context = None;
    let mut continuations = [None, None];
    let mut phase = 0;
    loop {
        interval.tick().await;
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            let (root, _) = repository.status().await?;
            if root.state != RootState::Ready {
                continuations = [None, None];
                return Ok(None);
            }
            if context != Some(root.context) {
                context = Some(root.context);
                continuations = [None, None];
            }
            if phase == 0 {
                recovery
                    .recover_page(root.context, continuations[phase].clone())
                    .await
                    .map(Some)
            } else {
                recovery
                    .repair_page(root.context, continuations[phase].clone())
                    .await
                    .map(Some)
            }
        })
        .await;
        match result {
            Ok(Ok(Some(page))) => {
                continuations[phase] = page.continuation;
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
                continuations[phase] = None;
                tracing::error!(%error, "namespace recovery scan failed; restarting sweep");
            }
            Err(_) => tracing::warn!("namespace recovery time budget exhausted; retrying page"),
        }
        phase = 1 - phase;
    }
}
