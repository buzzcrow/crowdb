use crowdb_access_iceberg::{
    catalog::{CatalogRepository, RootState, RoutedCatalogStore},
    commit::{TableRecovery, TableRecoveryKind},
    file::FileBlockStore,
};
use std::{sync::Arc, time::Duration};

pub(super) async fn run(
    catalog: Arc<CatalogRepository>,
    store: Arc<RoutedCatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
) {
    let recovery = TableRecovery::new(store, blocks, super::table_limits::commits());
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut context = None;
    let mut continuations = [None, None];
    let mut index = 0;
    loop {
        interval.tick().await;
        let Ok(Ok((root, authority))) = tokio::time::timeout(Duration::from_secs(1), catalog.status()).await
        else {
            continuations = [None, None];
            continue;
        };
        if context != Some(root.context) || root.state != RootState::Ready {
            context = Some(root.context);
            continuations = [None, None];
        }
        if root.state != RootState::Ready {
            continue;
        }
        let Ok(now) = super::namespace_write::now_ms()
            .and_then(|now| i64::try_from(now).map_err(|_| super::http::service_unavailable()))
        else {
            continue;
        };
        let kind = [TableRecoveryKind::Create, TableRecoveryKind::Update][index];
        let result = tokio::time::timeout(
            Duration::from_millis(authority.admission_bounds.request_ms),
            recovery.recover_page(root.context, kind, continuations[index].clone(), now),
        )
        .await;
        match result {
            Ok(Ok(page)) => {
                continuations[index] = page.continuation;
                for (operation, error) in page.failures {
                    tracing::debug!(%operation, %error, "table recovery deferred; durable intent retained");
                }
            }
            Ok(Err(error)) => {
                continuations[index] = None;
                tracing::error!(%error, "table recovery scan failed; restarting sweep");
            }
            Err(_) => tracing::warn!("table recovery page deadline exhausted; retaining cursor"),
        }
        index = 1 - index;
    }
}
