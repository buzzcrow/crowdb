// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Rebuild authority mutations after a definite revision conflict.

use super::KvRootCatalogStore;
use crowdb_paxos_kv_client::Error;
use crowdb_tree_ffi::CtError;
use std::future::Future;

pub(super) enum MutationError {
    Conflict,
    Status(CtError),
}
pub(super) type MutationResult<T> = Result<T, MutationError>;

impl From<CtError> for MutationError {
    fn from(status: CtError) -> Self {
        Self::Status(status)
    }
}
impl MutationError {
    pub(super) fn from_cas(error: &Error) -> Self {
        match error {
            Error::CasFailed { .. } | Error::CasBusy => Self::Conflict,
            // An unknown reply requires the caller's durable operation lookup.
            // Do not treat transport or ownership failures as revision conflicts.
            _ => {
                tracing::warn!(error = %error, "tree authority conditional write failed");
                Self::Status(CtError::Unavailable)
            }
        }
    }
}
impl KvRootCatalogStore {
    pub(super) async fn authority_mutation<T, F, Fut>(&self, mut prepare: F) -> Result<T, CtError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = MutationResult<T>>,
    {
        // Pins, root publication and GC deliberately share one revision. A
        // definite conflict discards all derived counters and proofs; each
        // attempt rereads authority, revalidates ownership and rebuilds ops.
        // Definite conflicts require a fresh derivation, not a storage failure.
        loop {
            match prepare().await {
                Ok(value) => return Ok(value),
                Err(MutationError::Status(status)) => return Err(status),
                Err(MutationError::Conflict) => tokio::task::yield_now().await,
            }
        }
    }
}
