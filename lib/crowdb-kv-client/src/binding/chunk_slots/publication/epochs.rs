// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Epoch publication changes only group-zero metadata, never data-group fences.

use super::{append_map, assemble, invalid, replace_map, ChunkSlotMapClient, MapOwner};
use crate::{Error, Result};
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotAuthority, ChunkSlotBitmap, ChunkSlotMap};
use crowdb_protocol::key::{ChunkSlotMapHeadKey, TextKey};
use std::collections::HashMap;

impl ChunkSlotMapClient {
    /// Explicitly initialize dynamic epochs before the deployment begins serving.
    ///
    /// # Errors
    /// Returns map validation, entropy or publication errors.
    pub async fn initialize_service_epochs(&self) -> Result<()> {
        if self.read_dynamic_service_snapshot().await?.is_some() {
            return Ok(());
        }
        let service = self.read_service().await?;
        let generation = service
            .head()
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("epoch map", "generation exhausted"))?;
        let incarnation = crowdb_protocol::chunk_slot::ChunkServiceIncarnation::generate()
            .map_err(|error| invalid("epoch map", &error.to_string()))?;
        let mut owners = HashMap::with_capacity(service.bindings().len());
        for binding in service.bindings() {
            let owner = ChunkSlotAuthority::new(binding.owner, incarnation, 1)
                .map_err(|error| invalid("epoch map", &error.to_string()))?;
            owners.insert(owner, binding.slots.clone());
        }
        match self.publish_service_epochs(&assemble(generation, owners)?).await {
            Err(Error::CasFailed { .. } | Error::CasBusy)
                if self.read_dynamic_service_snapshot().await?.is_some() =>
            {
                Ok(())
            }
            result => result,
        }
    }
    /// Advance this instance's owned slot epochs once before a restarted process serves.
    ///
    /// # Errors
    /// Returns a publication conflict or a missing/corrupt dynamic assignment.
    pub async fn regrant_service_epochs(&self, instance_id: u64) -> Result<()> {
        for _ in 0..32 {
            match self.regrant_service_epochs_once(instance_id).await {
                Err(Error::CasFailed { .. } | Error::CasBusy) => {}
                result => return result,
            }
        }
        Err(Error::CasBusy)
    }

    async fn regrant_service_epochs_once(&self, instance_id: u64) -> Result<()> {
        let current = self.read_service_snapshot().await?;
        let slots: Vec<_> = ChunkSlot::all()
            .filter(|slot| current.service().owner(*slot) == instance_id)
            .map(|slot| (slot, instance_id))
            .collect();
        if slots.is_empty() {
            return Ok(());
        }
        let next = current
            .authority()
            .reassign(&slots)
            .map_err(|error| invalid("epoch map", &error.to_string()))?;
        self.publish_service_epochs(&next).await
    }
    /// Atomically publish complete routing and per-slot epochs using the source head revision.
    /// No old-write draining or data-group network checks participate in publication.
    ///
    /// # Errors
    /// Rejects stale publication, epoch rollback/reuse and ambiguous conflicting outcomes.
    pub async fn publish_service_epochs(&self, next: &ChunkSlotMap<ChunkSlotAuthority>) -> Result<()> {
        match self.publish_service_epochs_once(next).await {
            Err(error @ (Error::OutcomeUnknown | Error::CasBusy | Error::CasFailed { .. })) => {
                // A competing identical publisher can commit during preparation,
                // before this caller submits its CAS. Reconcile that boundary too.
                let current = self.read_service_snapshot().await?;
                if current.authority().head() == next.head()
                    && current
                        .authority()
                        .bindings()
                        .iter()
                        .all(|binding| next.bindings().contains(binding))
                {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    async fn publish_service_epochs_once(&self, next: &ChunkSlotMap<ChunkSlotAuthority>) -> Result<()> {
        let before = self
            .read_head::<u64>()
            .await?
            .ok_or_else(|| invalid("epoch map", "service map absent"))?;
        let service = self.read_service().await?;
        let old = if self.read_head::<ChunkSlotAuthority>().await?.is_some() {
            Some(self.read_service_snapshot().await?)
        } else {
            if !self.scan_prefix(&ChunkSlotAuthority::prefix()).await?.is_empty() {
                return Err(invalid("epoch map", "orphan authority bindings"));
            }
            None
        };
        if self.read_head::<u64>().await? != Some(before.clone()) || service.head() != &before.0 {
            return Err(Error::CasFailed {
                current_revision: before.1,
            });
        }
        if next.head().generation
            != before
                .0
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("epoch map", "generation exhausted"))?
        {
            // Lost replies are reconciled without republishing or advancing epochs.
            if old.as_ref().is_some_and(|snapshot| {
                snapshot.authority().head() == next.head()
                    && snapshot
                        .authority()
                        .bindings()
                        .iter()
                        .all(|binding| next.bindings().contains(binding))
            }) {
                return Ok(());
            }
            return Err(invalid(
                "epoch map",
                "publication must advance exactly one generation",
            ));
        }
        validate_epochs(&service, old.as_ref(), next)?;
        let mut routes: HashMap<_, ChunkSlotBitmap> = service
            .bindings()
            .iter()
            .map(|binding| (binding.owner, ChunkSlotBitmap::default()))
            .collect();
        for slot in ChunkSlot::all() {
            routes
                .entry(next.owner(slot).instance_id())
                .or_default()
                .insert(slot);
        }
        let routes = assemble(next.head().generation, routes)?;
        let mut ops = Vec::with_capacity(service.bindings().len() + next.bindings().len() + 4);
        replace_map(&mut ops, &service, &routes)?;
        if let Some(old) = &old {
            replace_map(&mut ops, old.authority(), next)?;
        } else {
            append_map(&mut ops, next)?;
        }
        let path = ChunkSlotMapHeadKey::Service.to_path();
        self.kv
            .batch_write_cas(0, 0, &ops, path.as_bytes(), before.1)
            .await
            .map(|_| ())
    }
}

fn validate_epochs(
    service: &ChunkSlotMap<u64>,
    old: Option<&super::ChunkServiceSnapshot>,
    next: &ChunkSlotMap<ChunkSlotAuthority>,
) -> Result<()> {
    for slot in ChunkSlot::all() {
        let target = next.owner(slot);
        if let Some(old) = &old {
            let previous = old.authority().owner(slot);
            let changed = target.instance_id() != previous.instance_id();
            if target.generation() < previous.generation()
                || ((changed || target.generation() != previous.generation())
                    && previous.generation().checked_add(1) != Some(target.generation()))
            {
                return Err(invalid(
                    "epoch map",
                    "ownership changes must advance the slot epoch",
                ));
            }
        } else if target.instance_id() != service.owner(slot) || target.generation() != 1 {
            return Err(invalid(
                "epoch map",
                "bootstrap must retain routing with initial epoch one",
            ));
        }
    }
    Ok(())
}
