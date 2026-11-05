// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Head-fenced bootstrap pages, with the complete intent retained across restart.

use serde::{Deserialize, Serialize};

use super::{decode, invalid, put, ChunkSlotMapClient, MapOwner};
use crate::{Error, GetOutcome, ReadMode, Result};
use crowdb_protocol::chunk_slot::{ChunkSlotBinding, ChunkSlotMap, ChunkSlotMapHead};
use crowdb_protocol::key::TextKey;

// Every proposal must fit the native tree's reserved replay entry window.
pub(super) const MAX_BATCH_RECORDS: usize = 64;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent<O> {
    initializing: Layout<O>,
    next: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Layout<O> {
    head: ChunkSlotMapHead,
    bindings: Vec<ChunkSlotBinding<O>>,
}

pub(super) struct Pending<O> {
    intent: Option<Intent<O>>,
    revision: u64,
}

impl ChunkSlotMapClient {
    // A regular head is already committed. An intent is deliberately not a
    // ChunkSlotMapHead, so ordinary readers fail closed until publication.
    pub(super) async fn initialization_state<O: MapOwner>(
        &self,
        expected: &ChunkSlotMap<O>,
    ) -> Result<Option<Pending<O>>> {
        let key = O::head().to_path();
        let GetOutcome::Found { value, revision } = self
            .kv
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await?
        else {
            return Ok(None);
        };
        if serde_json::from_slice::<ChunkSlotMapHead>(&value).is_ok() {
            self.check_initialized(expected).await?;
            return Ok(Some(Pending {
                intent: None,
                revision,
            }));
        }
        let intent: Intent<O> = decode(key.as_bytes(), &value)?;
        let mut expected_bindings = expected.bindings().to_vec();
        expected_bindings.sort_by_key(|binding| binding.owner.key());
        // The offset refers to this canonical order, independent of caller order.
        if intent.initializing.head != *expected.head()
            || intent.initializing.bindings != expected_bindings
            || intent.next >= expected_bindings.len()
            || intent.next % (MAX_BATCH_RECORDS - 1) != 0
        {
            return Err(invalid(&key, "conflicting or corrupt slot map initialization"));
        }
        Ok(Some(Pending {
            intent: Some(intent),
            revision,
        }))
    }

    pub(super) async fn begin_initialization<O: MapOwner>(&self, map: &ChunkSlotMap<O>) -> Result<()> {
        let key = O::head().to_path();
        let mut bindings = map.bindings().to_vec();
        bindings.sort_by_key(|binding| binding.owner.key());
        let intent = Intent {
            initializing: Layout {
                head: map.head().clone(),
                bindings,
            },
            next: 0,
        };
        let operation = put(key.clone(), &intent)?;
        let pending = match self
            .kv
            .batch_write_cas(0, 0, &[operation], key.as_bytes(), 0)
            .await
        {
            Ok(written) => Pending {
                intent: Some(intent),
                revision: written.revision,
            },
            Err(error @ (Error::OutcomeUnknown | Error::CasFailed { .. } | Error::CasBusy)) => {
                self.initialization_state(map).await?.ok_or(error)?
            }
            Err(error) => return Err(error),
        };
        self.finish_initialization(map, pending).await
    }

    pub(super) async fn finish_initialization<O: MapOwner>(
        &self,
        map: &ChunkSlotMap<O>,
        mut pending: Pending<O>,
    ) -> Result<()> {
        let key = O::head().to_path();
        while let Some(mut intent) = pending.intent {
            let previous = intent.next;
            let end = (previous + MAX_BATCH_RECORDS - 1).min(intent.initializing.bindings.len());
            let mut operations = Vec::with_capacity(end - previous + 1);
            for binding in &intent.initializing.bindings[previous..end] {
                operations.push(put(binding.owner.key(), binding)?);
            }
            intent.next = end;
            let complete = end == intent.initializing.bindings.len();
            operations.push(if complete {
                put(key.clone(), map.head())?
            } else {
                put(key.clone(), &intent)?
            });
            match self
                .kv
                .batch_write_cas(0, 0, &operations, key.as_bytes(), pending.revision)
                .await
            {
                Ok(written) => {
                    pending = Pending {
                        intent: (!complete).then_some(intent),
                        revision: written.revision,
                    };
                }
                Err(error @ (Error::OutcomeUnknown | Error::CasFailed { .. } | Error::CasBusy)) => {
                    let Some(observed) = self.initialization_state(map).await? else {
                        return Err(error);
                    };
                    // Resume only proven durable progress by an identical
                    // initializer. An unchanged/unknown outcome remains an error.
                    if observed
                        .intent
                        .as_ref()
                        .is_some_and(|state| state.next <= previous)
                    {
                        return Err(error);
                    }
                    pending = observed;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
