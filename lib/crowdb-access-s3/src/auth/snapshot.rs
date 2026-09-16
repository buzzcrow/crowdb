// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;

use super::{Credential, CredentialProvider};

pub struct CredentialCache {
    snapshot: ArcSwap<HashMap<String, Credential>>,
    refreshed_at: AtomicU64,
    max_staleness_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CredentialCacheError {
    #[error("credential access-key ID cannot be empty")]
    EmptyAccessKey,
    #[error("credential secret cannot be empty")]
    EmptySecret,
    #[error("credential snapshot contains a duplicate access-key ID")]
    DuplicateAccessKey,
}

impl CredentialCache {
    #[must_use]
    pub fn new(max_staleness_seconds: u64) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(HashMap::new()),
            refreshed_at: AtomicU64::new(0),
            max_staleness_seconds,
        }
    }

    /// Atomically replaces the immutable request-path credential snapshot.
    /// Retired secret material is zeroized when its final snapshot owner drops.
    ///
    /// # Errors
    ///
    /// Rejects empty or duplicate credential identities and empty secrets.
    pub fn install(
        &self,
        credentials: impl IntoIterator<Item = (String, Credential)>,
        refreshed_at_unix_seconds: u64,
    ) -> Result<(), CredentialCacheError> {
        let mut next = HashMap::new();
        for (access_key, credential) in credentials {
            if access_key.is_empty() {
                return Err(CredentialCacheError::EmptyAccessKey);
            }
            if credential.secret_key.is_empty() {
                return Err(CredentialCacheError::EmptySecret);
            }
            if next.insert(access_key, credential).is_some() {
                return Err(CredentialCacheError::DuplicateAccessKey);
            }
        }
        self.snapshot.store(Arc::new(next));
        self.refreshed_at
            .store(refreshed_at_unix_seconds, Ordering::Release);
        Ok(())
    }

    #[must_use]
    pub fn lookup_at(&self, access_key: &str, now_unix_seconds: u64) -> Option<Credential> {
        let refreshed_at = self.refreshed_at.load(Ordering::Acquire);
        if refreshed_at == 0 || now_unix_seconds.saturating_sub(refreshed_at) > self.max_staleness_seconds {
            return None;
        }
        self.snapshot.load().get(access_key).cloned()
    }

    #[must_use]
    pub fn is_ready_at(&self, now_unix_seconds: u64) -> bool {
        let refreshed_at = self.refreshed_at.load(Ordering::Acquire);
        refreshed_at != 0 && now_unix_seconds.saturating_sub(refreshed_at) <= self.max_staleness_seconds
    }
}

impl CredentialProvider for CredentialCache {
    fn lookup(&self, access_key: &str) -> Option<Credential> {
        self.lookup_at(access_key, unix_seconds())
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
