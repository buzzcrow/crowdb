// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ensure_live, now_ms, pin_scope, validate_lease, DirectoryRootPinSnapshot};
use crate::{DirectoryMetadataStore, Result, StreamError};

impl DirectoryMetadataStore {
    /// Extends a still-live reader pin. A stale snapshot or expired record can
    /// never reactivate a previously released or expired view.
    /// # Errors
    /// Rejects superseded/expired pins, invalid leases and conflicting metadata.
    pub async fn renew_directory_pin(
        &self,
        snapshot: &DirectoryRootPinSnapshot,
        expires_at_ms: u64,
    ) -> Result<DirectoryRootPinSnapshot> {
        self.validate_pin_snapshot(snapshot)?;
        let (control, revision) = self.load_directory_control(pin_scope(snapshot.pin())).await?;
        let current = self
            .current_directory_pin(snapshot)
            .await?
            .ok_or_else(|| StreamError::ReadUnavailable("root pin has been released".into()))?;
        ensure_live(current.pin())?;
        let previous = current
            .pin
            .expires_at_ms
            .ok_or_else(|| StreamError::InvalidRequest("durable consumer pins do not renew".into()))?;
        if expires_at_ms <= previous {
            return Err(StreamError::InvalidRequest(
                "root pin renewal must extend its deadline".into(),
            ));
        }
        let mut next = current.pin.clone();
        next.expires_at_ms = Some(expires_at_ms);
        validate_lease(&next, now_ms())?;
        let saved = self
            .write_directory_pin(control, revision, current.pin(), Some(&next))
            .await?
            .ok_or_else(|| StreamError::Internal("pin renewal returned no record".into()))?;
        ensure_live(saved.pin())?;
        Ok(saved)
    }

    /// Releases one exact pin revision; an old handle cannot delete a renewed
    /// pin or a later reuse of the same identity. Absence is idempotent.
    /// # Errors
    /// Rejects a different binding, superseded snapshots and conflicting metadata.
    pub async fn release_directory_pin(&self, snapshot: &DirectoryRootPinSnapshot) -> Result<()> {
        self.validate_pin_snapshot(snapshot)?;
        let (control, revision) = self.load_directory_control(pin_scope(snapshot.pin())).await?;
        let Some(current) = self.current_directory_pin(snapshot).await? else {
            return Ok(());
        };
        self.write_directory_pin(control, revision, current.pin(), None)
            .await?;
        Ok(())
    }

    async fn current_directory_pin(
        &self,
        snapshot: &DirectoryRootPinSnapshot,
    ) -> Result<Option<DirectoryRootPinSnapshot>> {
        let current = self
            .load_directory_pin(snapshot.pin.head.stream_name, snapshot.pin.id)
            .await?;
        if current
            .as_ref()
            .is_some_and(|pin| pin.revision != snapshot.revision || pin.pin != snapshot.pin)
        {
            return Err(StreamError::ReadUnavailable(
                "root pin snapshot was superseded".into(),
            ));
        }
        Ok(current)
    }
}
