use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage};

use crate::catalog::{CatalogContext, CatalogError, StoreError};
use crate::error::ValidationError;
use crate::key::{IcebergKey, OperationId};
use crate::record::{StorageRecord, MAX_RECORD_BYTES};

use super::{MultipartPhase, MultipartRepository, MultipartSession, MultipartWorkError};

mod scan;
pub use scan::{MultipartRecoveryScan, MultipartRecoveryStore};

pub struct MultipartRecovery {
    repository: MultipartRepository,
    store: Arc<dyn MultipartRecoveryStore>,
    session_timeout: Option<Duration>,
}

#[derive(Debug)]
pub struct MultipartRecoveryPage {
    pub continuation: Option<MultiScanContinuation>,
    pub progressed: usize,
    pub deferred: usize,
    pub retained: usize,
    pub awaiting_seal: Vec<OperationId>,
    pub failures: Vec<(OperationId, MultipartWorkError)>,
}

impl MultipartRecovery {
    pub fn new<Store: MultipartRecoveryStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            repository: MultipartRepository::new(store.clone()),
            store,
            session_timeout: None,
        }
    }

    /// Bounds each session independently so a slow first session cannot starve its page.
    /// # Errors
    /// Rejects zero or excessively long recovery steps.
    pub fn with_session_timeout(mut self, timeout: Duration) -> Result<Self, ValidationError> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(ValidationError::Deadline);
        }
        self.session_timeout = Some(timeout);
        Ok(self)
    }

    /// Settles legacy metadata and expires abandoned resources without copying file bytes.
    /// # Errors
    /// Rejects retired contexts, corrupt pages, foreign cursors and scan failures.
    pub async fn recover_page(
        &self,
        context: CatalogContext,
        continuation: Option<MultiScanContinuation>,
        now_ms: u64,
    ) -> Result<MultipartRecoveryPage, CatalogError> {
        self.repository.check_context(context).await?;
        let scan = MultipartRecoveryScan {
            catalog: context.catalog,
            continuation,
        };
        scan.request()?;
        let page = self.store.scan_multipart_sessions(scan.clone()).await?;
        let sessions = validate_page(context, &scan, &page)?;
        let mut report = MultipartRecoveryPage {
            continuation: page.continuation,
            progressed: 0,
            deferred: 0,
            retained: 0,
            awaiting_seal: Vec::new(),
            failures: Vec::new(),
        };
        for session in sessions {
            let work = self.recover_session(&session, now_ms);
            let outcome = if let Some(timeout) = self.session_timeout {
                tokio::time::timeout(timeout, work)
                    .await
                    .unwrap_or(Ok(RecoveryAction::Deferred))
            } else {
                work.await
            };
            match outcome {
                Ok(RecoveryAction::Progressed) => report.progressed += 1,
                Ok(RecoveryAction::Deferred) => report.deferred += 1,
                Ok(RecoveryAction::Retained) => report.retained += 1,
                Ok(RecoveryAction::AwaitingSeal) => report.awaiting_seal.push(session.upload),
                Err(error) => report.failures.push((session.upload, error)),
            }
        }
        self.repository.check_context(context).await?;
        Ok(report)
    }

    async fn recover_session(
        &self,
        session: &MultipartSession,
        now_ms: u64,
    ) -> Result<RecoveryAction, MultipartWorkError> {
        let changed = if session.pending.is_some() {
            self.repository.settle_part(session).await?
        } else if matches!(session.phase, MultipartPhase::Open | MultipartPhase::Completing)
            && now_ms >= session.expires_ms
        {
            self.repository.abort(session).await?
        } else if session.phase == MultipartPhase::Completing {
            let completion = session.completion.as_ref().ok_or(ValidationError::Record)?;
            return Ok(if completion.progress.next_part == completion.selected_parts {
                RecoveryAction::AwaitingSeal
            } else {
                RecoveryAction::Retained
            });
        } else if session.phase == MultipartPhase::Publishing {
            self.repository.publish(session).await?.is_applied()
        } else {
            return Ok(RecoveryAction::Retained);
        };
        Ok(if changed {
            RecoveryAction::Progressed
        } else {
            RecoveryAction::Deferred
        })
    }
}

enum RecoveryAction {
    Progressed,
    Deferred,
    Retained,
    AwaitingSeal,
}

fn validate_page(
    context: CatalogContext,
    scan: &MultipartRecoveryScan,
    page: &MultiScanPage,
) -> Result<Vec<MultipartSession>, CatalogError> {
    if let Some(failure) = &page.terminal_failure {
        return Err(StoreError::Rejected(failure.clone()).into());
    }
    let request = scan.request()?;
    if page.items.len() > request.max_items {
        return Err(ValidationError::RecordTooLarge.into());
    }
    let start = request.start.as_ref().ok_or(ValidationError::Key)?;
    let end = request.end.as_ref().ok_or(ValidationError::Key)?;
    let mut last = request
        .continuation
        .as_ref()
        .map_or(start, |cursor| &cursor.last_key);
    let mut sessions = Vec::with_capacity(page.items.len());
    for item in &page.items {
        if item.key <= *last || item.key >= *end || item.revision == 0 || item.value.len() > MAX_RECORD_BYTES
        {
            return Err(ValidationError::Record.into());
        }
        last = &item.key;
        let key = IcebergKey::decode(&item.key)?;
        let StorageRecord::MultipartSession(session) = StorageRecord::decode(&key, &item.value)? else {
            return Err(ValidationError::Record.into());
        };
        if session.context != context {
            return Err(ValidationError::IdentityMismatch.into());
        }
        sessions.push(*session);
    }
    if let Some(cursor) = &page.continuation {
        MultipartRecoveryScan {
            continuation: Some(cursor.clone()),
            ..scan.clone()
        }
        .request()?;
        if cursor.last_key != *last || sessions.is_empty() {
            return Err(ValidationError::Key.into());
        }
    }
    Ok(sessions)
}
