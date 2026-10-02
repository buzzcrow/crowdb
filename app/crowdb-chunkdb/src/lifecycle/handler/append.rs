// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use tracing::info;

use super::{
    assign_strip_offsets, AppendChunkOutcome, CacheHint, ChunkId, ChunkState, LifecycleError,
    LifecycleHandler, LockPolicy, ProtoStripType, StripAllocType, StripBatchSpec,
};

impl LifecycleHandler {
    #[allow(clippy::too_many_arguments)]
    pub async fn append_chunk(
        &self,
        chunk_id: &ChunkId,
        observed_modify_ts: u64,
        strip_count: u32,
        strip_type: ProtoStripType,
        data_num: u32,
        code_num: u32,
        copy_count: u32,
        unit_count: u32,
    ) -> Result<AppendChunkOutcome, LifecycleError> {
        let capacity_kb = unit_count.saturating_mul(self.topology.snapshot().unit_size_bytes() / 1024);
        self.validate_strip_layout(strip_type, data_num, code_num, copy_count, capacity_kb)?;
        self.check_range(chunk_id)?;

        let guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_append()?;
        if observed_modify_ts != chunk.modify_ts {
            return Ok(AppendChunkOutcome {
                modify_ts: chunk.modify_ts,
                strips: Vec::new(),
                chunk: Some(chunk),
            });
        }

        let snap = self.topology.snapshot();
        let mirror_copies = if copy_count == 0 { 2 } else { copy_count as usize };
        let strip_alloc_type =
            self.protected_degraded_layout(strip_type, &snap)
                .unwrap_or(match strip_type {
                    ProtoStripType::Mirror => StripAllocType::Mirror {
                        copy_count: mirror_copies,
                    },
                    ProtoStripType::Ec => StripAllocType::Ec {
                        data_num: data_num as usize,
                        code_num: code_num as usize,
                    },
                });

        let constraints = self.allocation_constraints(strip_type, &snap);
        let start_seq = if chunk.next_strip_sequence == 0 {
            chunk
                .strips
                .iter()
                .map(|strip| strip.strip_sequence)
                .max()
                .map_or(0, |sequence| sequence.saturating_add(1))
        } else {
            chunk.next_strip_sequence
        };
        let next_strip_sequence = start_seq
            .checked_add(strip_count)
            .ok_or_else(|| LifecycleError::InvalidRequest("chunk strip sequence space exhausted".into()))?;

        drop(guard);
        let appended = self
            .allocator
            .allocate_strips(
                &snap,
                chunk_id,
                StripBatchSpec {
                    strip_type: strip_alloc_type,
                    unit_count,
                    start_sequence: start_seq,
                    strip_count,
                },
                &constraints,
            )
            .await?;
        self.publish_appended(chunk_id, chunk, appended, next_strip_sequence)
            .await
    }

    async fn publish_appended(
        &self,
        chunk_id: &ChunkId,
        mut chunk: super::Chunk,
        mut appended: Vec<super::ChunkStrip>,
        next_strip_sequence: u32,
    ) -> Result<AppendChunkOutcome, LifecycleError> {
        let publication = async {
            let guard = if let Some(locks) = &self.locks {
                Some(
                    locks
                        .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                        .await?,
                )
            } else {
                None
            };
            let current = match &guard {
                Some(guard) => guard.chunk().expect("acquire guarantees chunk").clone(),
                None => self.store.get_chunk(chunk_id).await?,
            };
            ChunkState::from_proto(current.state).check_can_append()?;
            Ok::<_, LifecycleError>((guard, current))
        }
        .await;
        let (mut guard, current) = match publication {
            Ok(result) => result,
            Err(error) => {
                self.allocator.rollback_strips(&appended).await?;
                return Err(error);
            }
        };
        if current.modify_ts != chunk.modify_ts {
            drop(guard);
            self.allocator.rollback_strips(&appended).await?;
            return Ok(AppendChunkOutcome {
                modify_ts: current.modify_ts,
                strips: Vec::new(),
                chunk: Some(current),
            });
        }
        chunk = current;
        assign_strip_offsets(&mut appended, chunk.capacity);

        if let Err(error) = self.commit_strip_segments(&appended).await {
            self.allocator.rollback_strips(&appended).await?;
            return Err(error);
        }
        chunk.strips.extend(appended.iter().cloned());
        chunk.next_strip_sequence = next_strip_sequence;
        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        if let Err(error) = self.store.put_chunk(&chunk).await {
            self.resolve_failed_append(chunk_id, &appended, &mut guard, &error)
                .await?;
            return Err(error.into());
        }
        self.admit_placement_repairs(&chunk);

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, added_strips = appended.len(), "chunk appended");
        Ok(AppendChunkOutcome {
            modify_ts: chunk.modify_ts,
            strips: appended,
            chunk: None,
        })
    }
    async fn resolve_failed_append(
        &self,
        chunk_id: &ChunkId,
        appended: &[super::ChunkStrip],
        guard: &mut Option<super::ChunkGuard>,
        error: &super::StoreError,
    ) -> Result<(), LifecycleError> {
        let observed = self.store.get_chunk(chunk_id).await;
        match observed {
            Ok(current)
                if !appended
                    .iter()
                    .any(|candidate| current.strips.iter().any(|strip| strip == candidate)) =>
            {
                self.allocator.rollback_strips(appended).await?;
            }
            Ok(current) => {
                if let Some(guard) = guard {
                    guard.refresh(current);
                }
                tracing::warn!(?chunk_id, %error, "append metadata outcome uncertain; retaining published strips");
            }
            Err(read_error) => {
                tracing::warn!(?chunk_id, %error, %read_error, "append metadata outcome uncertain; retaining allocated strips");
            }
        }
        Ok(())
    }
}
