use crowdb_chunk_kv_client::MultiScanContinuation;

use crate::catalog::{CatalogContext, CatalogError};
use crate::key::OperationId;

use super::{
    validate_page, MultipartRecovery, MultipartRecoveryPage, MultipartRecoveryScan, MultipartSession,
};

#[derive(Debug)]
pub struct MultipartRecoveryObservation {
    context: CatalogContext,
    continuation: Option<MultiScanContinuation>,
    revisions: Vec<(OperationId, u64)>,
}

impl MultipartRecoveryObservation {
    pub(super) fn unchanged(&self, session: &MultipartSession) -> bool {
        session.context == self.context && self.revisions.contains(&(session.upload, session.revision))
    }
}

impl MultipartRecovery {
    /// Observes one bounded page without copying bytes or claiming a recovery lease.
    /// # Errors
    /// Rejects retired contexts, corrupt pages and invalid scan continuations.
    pub async fn observe_page(
        &self,
        context: CatalogContext,
        continuation: Option<MultiScanContinuation>,
    ) -> Result<MultipartRecoveryObservation, CatalogError> {
        self.repository.check_context(context).await?;
        let scan = MultipartRecoveryScan {
            catalog: context.catalog,
            continuation: continuation.clone(),
        };
        scan.request()?;
        let page = self.store.scan_multipart_sessions(scan.clone()).await?;
        let sessions = validate_page(context, &scan, &page)?;
        self.repository.check_context(context).await?;
        Ok(MultipartRecoveryObservation {
            context,
            continuation,
            revisions: sessions
                .into_iter()
                .map(|session| (session.upload, session.revision))
                .collect(),
        })
    }

    /// Rechecks a page and copies only completion revisions unchanged since observation.
    /// Expiry, publication and journal settlement retain their ordinary recovery rules.
    /// # Errors
    /// Rejects retired contexts and invalid storage; observations never authorize mutations.
    pub async fn recover_observed_page(
        &self,
        observation: MultipartRecoveryObservation,
        now_ms: u64,
    ) -> Result<MultipartRecoveryPage, CatalogError> {
        self.recover_page_inner(
            observation.context,
            observation.continuation.clone(),
            now_ms,
            Some(&observation),
        )
        .await
    }
}
