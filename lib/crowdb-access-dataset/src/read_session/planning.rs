// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    encode, session_key, AuthorityError, CasOutcome, DatasetError, DatasetIdentity, DatasetSessions, Digest,
    Md5, OperationId, Ordering, ReadPlan, ReadSession, SessionRecord, SessionSpec, SnapshotId, TTL,
};

impl DatasetSessions {
    /// # Errors
    /// Rejects invalid plans, conflicting identities, missing snapshots or storage failures.
    pub async fn start(
        &self,
        identity: &DatasetIdentity,
        spec: SessionSpec,
        now: u64,
    ) -> Result<ReadSession, AuthorityError> {
        match self.record(identity, spec.id).await {
            Ok((_, _old)) => {
                if self.read_spec(identity, spec.id).await?.spec != spec {
                    return Err(AuthorityError::CursorConflict);
                }
                return self.resume(identity, spec.id, now).await;
            }
            Err(AuthorityError::NotFound) => {}
            Err(error) => return Err(error),
        }
        let snapshot = match self.read_spec(identity, spec.id).await {
            Ok(old) => {
                if old.spec != spec {
                    return Err(AuthorityError::CursorConflict);
                }
                old.snapshot
            }
            Err(AuthorityError::NotFound) => self.resolve_snapshot(identity, &spec.snapshot).await?,
            Err(error) => return Err(error),
        };
        let bound = self.bind_spec(identity, &spec, snapshot).await?;
        let snapshot = bound.snapshot;
        let ids = self.select_ids(identity, snapshot, &spec).await?;
        self.save_ids(identity, spec.id, &ids).await?;
        let fingerprint: [u8; 16] = Md5::digest(
            bincode::serialize(&(identity.key()?, snapshot, &spec)).map_err(|_| AuthorityError::Corrupt)?,
        )
        .into();
        let record = SessionRecord {
            version: 2,
            id: spec.id,
            fields: spec.fields.clone(),
            batch_size: spec.batch_size,
            count: ids.len(),
            snapshot,
            plan: fingerprint,
            cursor: OperationId::random().to_string(),
            position: 0,
            issued: None,
            deadline: now.saturating_add(TTL),
            closed: false,
        };
        self.authority
            .touch_read_handle(identity, snapshot, spec.id, now, TTL)
            .await?;
        match self
            .authority
            .store
            .compare_exchange(&session_key(identity, spec.id)?, None, &encode(&record)?)
            .await?
        {
            CasOutcome::Applied(_) => Ok(record.public()),
            CasOutcome::Conflict(Some(_)) => self.resume(identity, spec.id, now).await,
            CasOutcome::Conflict(None) => Err(AuthorityError::Corrupt),
        }
    }
    async fn resolve_snapshot(
        &self,
        identity: &DatasetIdentity,
        value: &str,
    ) -> Result<SnapshotId, AuthorityError> {
        let snapshot = match value {
            "latest" => self
                .authority
                .latest(identity)
                .await?
                .ok_or(AuthorityError::SnapshotNotFound)?,
            "stable" => self
                .authority
                .get_dataset(identity)
                .await?
                .stable
                .ok_or(AuthorityError::SnapshotNotFound)?,
            value => {
                let mut id = [0; 16];
                if value.len() != 32
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(DatasetError::InvalidManifest.into());
                }
                for (i, byte) in id.iter_mut().enumerate() {
                    *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)
                        .map_err(|_| DatasetError::InvalidManifest)?;
                }
                SnapshotId::from_bytes(&id)?
            }
        };
        Ok(snapshot)
    }
    async fn select_ids(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        spec: &SessionSpec,
    ) -> Result<Vec<Vec<u8>>, AuthorityError> {
        self.authority.get_snapshot(identity, snapshot).await?;
        let p = &spec.partition;
        if p.workers == 0
            || p.world_size == 0
            || p.worker >= p.workers
            || p.rank >= p.world_size
            || spec.fields.len() > 256
        {
            return Err(DatasetError::InvalidManifest.into());
        }
        let plan = ReadPlan {
            snapshot,
            selection: spec.selection.clone(),
            ordering: Ordering::SampleId,
            projection: spec.fields.clone(),
            batch_size: spec.batch_size,
        };
        plan.validate()?;
        let manifest = self.authority.get_manifest(identity, snapshot).await?;
        if spec
            .fields
            .iter()
            .any(|name| !manifest.schema.fields.iter().any(|f| &f.name == name))
        {
            return Err(DatasetError::InvalidManifest.into());
        }
        let mut ids = plan.scan_ids(&manifest)?;
        if ids.len() > 100_000 {
            return Err(DatasetError::ReadLimitExceeded.into());
        }
        if let Some(shuffle) = &spec.shuffle {
            if shuffle.group_size == 0 || shuffle.group_size > 1_000_000 {
                return Err(DatasetError::InvalidManifest.into());
            }
            for group in ids.chunks_mut(shuffle.group_size) {
                group.sort_by_cached_key(|id| {
                    let mut hash = Md5::new();
                    hash.update(shuffle.seed.to_le_bytes());
                    hash.update(snapshot.as_bytes());
                    hash.update(id);
                    <[u8; 16]>::from(hash.finalize())
                });
            }
        }
        let slot = usize::from(p.rank) * usize::from(p.workers) + usize::from(p.worker);
        let slots = usize::from(p.world_size) * usize::from(p.workers);
        ids = ids
            .into_iter()
            .enumerate()
            .filter_map(|(i, id)| (i % slots == slot).then_some(id))
            .collect();
        Ok(ids)
    }
}
