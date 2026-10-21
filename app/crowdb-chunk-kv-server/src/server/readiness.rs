// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A live grant cannot substitute for activated writers of current assignments.

use super::{CatalogSnapshot, ChunkKvService};
use crowdb_chunk_kv::PartitionLifecycle;
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogPartitionState, RequestRouting};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RangeReadiness {
    pub partition_id: crowdb_protocol::chunk_kv::Id128,
    pub owner_epoch: u64,
    pub ready: bool,
    pub detail: String,
}

impl ChunkKvService {
    pub(super) fn assignment_readiness(&self, catalog: &CatalogSnapshot, now: u64) -> Vec<RangeReadiness> {
        catalog
            .entries
            .iter()
            .filter(|entry| {
                entry.owner.instance_id == self.instance_id
                    && entry.state != ChunkKvRangeCatalogPartitionState::Retired
            })
            .map(|entry| {
                let result = self.range_readiness(entry, catalog.generation, now);
                RangeReadiness {
                    partition_id: entry.partition_id,
                    owner_epoch: entry.owner_epoch,
                    ready: result.is_ok(),
                    detail: match result {
                        Ok(detail) | Err(detail) => detail,
                    },
                }
            })
            .collect()
    }

    fn range_readiness(
        &self,
        entry: &crowdb_protocol::chunk_kv::ChunkKvRangeCatalogEntry,
        catalog_generation: u64,
        now: u64,
    ) -> Result<String, String> {
        if !matches!(
            entry.state,
            ChunkKvRangeCatalogPartitionState::Serving
                | ChunkKvRangeCatalogPartitionState::SplitPreparing
                | ChunkKvRangeCatalogPartitionState::SplitFinalizing
        ) {
            return Err(format!(
                "Owned range catalog state {:?} is not serviceable",
                entry.state
            ));
        }
        self.authority
            .authorize(catalog_generation, entry.partition_id, entry.owner_epoch, now)
            .map_err(|error| {
                format!(
                    "Owned range serving authority is unavailable for catalog generation {catalog_generation}: {error:?}; writer recovery required: {:?}",
                    self.recovery_required()
                )
            })?;
        let routing = RequestRouting {
            request_id: crowdb_protocol::chunk_kv::ClientRequestId::default(),
            map_revision: catalog_generation,
            partition_id: entry.partition_id,
            owner_epoch: entry.owner_epoch,
            min_journal_position: None,
            deadline_ms: None,
        };
        let partition = self
            .partition_for_request(&routing, &entry.range.start, entry)
            .ok_or_else(|| "Owned range has no recovered dispatch writer".to_owned())?;
        let writer = partition.split_ingress().map_or(partition.clone(), |ingress| {
            ingress.writer_for_key(&entry.range.start)
        });
        if matches!(
            writer.lifecycle(),
            PartitionLifecycle::Serving
                | PartitionLifecycle::SplitPreparing
                | PartitionLifecycle::SplitFinalizing
        ) {
            Ok(format!(
                "Owned range has current authority and a {:?} writer at catalog generation {}",
                writer.lifecycle(),
                catalog_generation
            ))
        } else {
            Err(format!(
                "Owned range writer is {:?}; waiting for recovery and activation",
                writer.lifecycle()
            ))
        }
    }
}
