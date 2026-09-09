// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Physical mirror and erasure-coded strip reads.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crowdb_common::ec::{decode, EcScheme};
use crowdb_protocol::chunkdb::rpc::{ChunkStrip, EcState, Strip};
use crowdb_protocol::diskdb::rpc::Segment;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::{DiskWriter, ReadError, ReadResult};

/// Result of a strip read together with every segment that failed or was
/// already marked unavailable while satisfying it.
pub(crate) struct ObservedStripRead {
    pub result: ReadResult<Bytes>,
    pub failed_segments: Vec<Segment>,
}

/// Reads byte intersections from one validated strip.
#[derive(Clone)]
pub struct StripReader {
    disk_io: Arc<dyn DiskWriter>,
    recovery_memory: Arc<Semaphore>,
    recovery_memory_limit: usize,
}

impl StripReader {
    pub fn new(
        disk_io: Arc<dyn DiskWriter>,
        recovery_memory: Arc<Semaphore>,
        recovery_memory_limit: usize,
    ) -> Self {
        Self {
            disk_io,
            recovery_memory,
            recovery_memory_limit,
        }
    }

    pub async fn read(
        &self,
        strip: &ChunkStrip,
        durable_bytes: u64,
        offset: u64,
        length: u64,
    ) -> ReadResult<Bytes> {
        self.read_observed(strip, durable_bytes, offset, length)
            .await
            .result
    }

    pub(crate) async fn read_observed(
        &self,
        strip: &ChunkStrip,
        durable_bytes: u64,
        offset: u64,
        length: u64,
    ) -> ObservedStripRead {
        let result = self
            .read_observed_inner(strip, durable_bytes, offset, length)
            .await;
        match result {
            Ok((data, failed_segments)) => ObservedStripRead {
                result: Ok(data),
                failed_segments,
            },
            Err((error, failed_segments)) => ObservedStripRead {
                result: Err(error),
                failed_segments,
            },
        }
    }

    async fn read_observed_inner(
        &self,
        strip: &ChunkStrip,
        durable_bytes: u64,
        offset: u64,
        length: u64,
    ) -> Result<(Bytes, Vec<Segment>), (ReadError, Vec<Segment>)> {
        if length == 0 {
            return Ok((Bytes::new(), Vec::new()));
        }
        let sealed_bytes = kib_to_bytes(strip.sealed_length).map_err(|error| (error, Vec::new()))?;
        let available = sealed_bytes.max(durable_bytes);
        let end = offset.checked_add(length).ok_or_else(|| {
            (
                ReadError::InvalidLocations("strip read range overflows".into()),
                Vec::new(),
            )
        })?;
        if available == 0 || end > available {
            return Err((
                ReadError::NotYetAvailable(format!(
                    "strip {} has {available} durable bytes, requested end {end}",
                    strip.strip_sequence
                )),
                Vec::new(),
            ));
        }
        match strip.strip.as_ref() {
            Some(Strip::MirrorStrip(mirror)) => {
                self.read_mirror(strip, &mirror.segments, offset, length).await
            }
            Some(Strip::EcStrip(ec)) => self.read_ec(strip, ec, offset, length).await,
            None => Err((
                ReadError::InvalidLocations(format!("strip {} has no body", strip.strip_sequence)),
                Vec::new(),
            )),
        }
    }

    async fn read_mirror(
        &self,
        strip: &ChunkStrip,
        segments: &[Segment],
        offset: u64,
        length: u64,
    ) -> Result<(Bytes, Vec<Segment>), (ReadError, Vec<Segment>)> {
        let length = u32::try_from(length).map_err(|_| {
            (
                ReadError::InvalidLocations("mirror read exceeds RPC size".into()),
                Vec::new(),
            )
        })?;
        let unit_bytes = kib_to_bytes(strip.unit_kb).map_err(|error| (error, Vec::new()))?;
        let mut failures = Vec::new();
        let mut failed_segments = Vec::new();
        for segment in segments {
            if strip.unavailable_segments.contains(segment) {
                failures.push("metadata-unavailable".to_string());
                push_unique(&mut failed_segments, *segment);
                continue;
            }
            match self.disk_io.read(segment, unit_bytes, offset, length).await {
                Ok(data) => return Ok((data, failed_segments)),
                Err(error) => {
                    failures.push(error.to_string());
                    push_durable_failure(&mut failed_segments, *segment, &error);
                }
            }
        }
        Err((
            ReadError::DataLoss(format!(
                "every mirror replica for strip {} failed: {}",
                strip.strip_sequence,
                failures.join("; ")
            )),
            failed_segments,
        ))
    }

    async fn read_ec(
        &self,
        strip: &ChunkStrip,
        ec: &crowdb_protocol::chunkdb::rpc::EcStrip,
        offset: u64,
        length: u64,
    ) -> Result<(Bytes, Vec<Segment>), (ReadError, Vec<Segment>)> {
        let geometry = validate_ec_read(strip, ec, offset, length).map_err(|error| (error, Vec::new()))?;
        let EcReadGeometry {
            scheme,
            unit_bytes,
            shard_bytes,
            end,
        } = geometry;

        let mut pieces = Vec::new();
        let mut failed_segments = strip.unavailable_segments.clone();
        let first = usize::try_from(offset / shard_bytes).unwrap_or(usize::MAX);
        let last = usize::try_from((end - 1) / shard_bytes).unwrap_or(usize::MAX);
        for (order, shard_index) in (first..=last).enumerate() {
            let shard_start = shard_index as u64 * shard_bytes;
            let local_start = offset.max(shard_start) - shard_start;
            let local_end = end.min(shard_start + shard_bytes) - shard_start;
            let read_len = u32::try_from(local_end - local_start).map_err(|_| {
                (
                    ReadError::InvalidLocations("EC read exceeds RPC size".into()),
                    failed_segments.clone(),
                )
            })?;
            let unavailable = strip.unavailable_segments.contains(&ec.segments[shard_index]);
            let segment = ec.segments[shard_index];
            let result = if unavailable {
                Err(crate::IoError::ReadFailed("segment is unavailable".into()))
            } else {
                self.disk_io
                    .read(&segment, unit_bytes, local_start, read_len)
                    .await
            };
            pieces.push((order, shard_index, local_start, read_len, result));
        }
        pieces.sort_unstable_by_key(|(order, _, _, _, _)| *order);
        let mut output = BytesMut::with_capacity(usize::try_from(length).unwrap_or(usize::MAX));
        for (_, shard_index, local_start, read_len, result) in pieces {
            if let Ok(data) = result {
                output.extend_from_slice(&data);
            } else if let Err(error) = result {
                let durable_target_failure = error.is_durable_read_failure();
                if durable_target_failure {
                    push_unique(&mut failed_segments, ec.segments[shard_index]);
                }
                if ec.ec_state != EcState::Parity as i32 {
                    return Err((
                        ReadError::DataLoss(format!(
                            "EC strip {} has no durable parity for shard recovery",
                            strip.strip_sequence
                        )),
                        failed_segments,
                    ));
                }
                let recovered = match self
                    .recover_slices(
                        strip,
                        &ec.segments,
                        scheme,
                        unit_bytes,
                        shard_bytes,
                        shard_index,
                        local_start,
                        read_len,
                        durable_target_failure,
                    )
                    .await
                {
                    Ok(observed) => observed,
                    Err((error, observed_failures)) => {
                        extend_unique(&mut failed_segments, observed_failures);
                        return Err((error, failed_segments));
                    }
                };
                extend_unique(&mut failed_segments, recovered.failed_segments);
                output.extend_from_slice(&recovered.data);
            }
        }
        Ok((output.freeze(), failed_segments))
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_slices(
        &self,
        strip: &ChunkStrip,
        segments: &[Segment],
        scheme: EcScheme,
        unit_bytes: u64,
        shard_bytes: u64,
        target: usize,
        offset: u64,
        length: u32,
        durable_target_failure: bool,
    ) -> Result<RecoveredSlice, (ReadError, Vec<Segment>)> {
        let divisor = scheme.total_blocks().saturating_add(1);
        let max_slice = self.recovery_memory_limit / divisor;
        if max_slice == 0 {
            return Err((
                ReadError::InvalidLocations(
                    "EC recovery memory budget is smaller than one byte per shard".into(),
                ),
                Vec::new(),
            ));
        }
        let mut output = BytesMut::with_capacity(length as usize);
        let mut failed_segments = Vec::new();
        let mut consumed = 0u32;
        while consumed < length {
            let part_len = (length - consumed).min(u32::try_from(max_slice).unwrap_or(u32::MAX));
            let recovered = self
                .recover_slice(
                    strip,
                    segments,
                    scheme,
                    unit_bytes,
                    shard_bytes,
                    target,
                    offset + u64::from(consumed),
                    part_len,
                    durable_target_failure,
                )
                .await?;
            extend_unique(&mut failed_segments, recovered.failed_segments);
            output.extend_from_slice(&recovered.data);
            consumed += part_len;
        }
        Ok(RecoveredSlice {
            data: output.freeze(),
            failed_segments,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_slice(
        &self,
        strip: &ChunkStrip,
        segments: &[Segment],
        scheme: EcScheme,
        unit_bytes: u64,
        shard_bytes: u64,
        target: usize,
        offset: u64,
        length: u32,
        durable_target_failure: bool,
    ) -> Result<RecoveredSlice, (ReadError, Vec<Segment>)> {
        let mut failed_segments = Vec::new();
        let memory = (length as usize)
            .checked_mul(scheme.total_blocks().saturating_add(1))
            .ok_or_else(|| {
                (
                    ReadError::InvalidLocations("EC recovery memory overflows".into()),
                    Vec::new(),
                )
            })?;
        let permits = u32::try_from(memory).map_err(|_| {
            (
                ReadError::InvalidLocations("EC recovery memory exceeds semaphore".into()),
                Vec::new(),
            )
        })?;
        let _reservation = self
            .recovery_memory
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| {
                (
                    ReadError::DiskIo("EC recovery memory budget closed".into()),
                    Vec::new(),
                )
            })?;
        let sealed_bytes = kib_to_bytes(strip.sealed_length).map_err(|error| (error, Vec::new()))?;
        let mut shards = vec![None; scheme.total_blocks()];
        let mut reads = JoinSet::new();
        let mut candidates = Vec::with_capacity(segments.len());
        for (index, segment) in segments.iter().enumerate() {
            if index == target || strip.unavailable_segments.contains(segment) {
                if index != target || durable_target_failure || strip.unavailable_segments.contains(segment) {
                    push_unique(&mut failed_segments, *segment);
                }
            } else {
                candidates.push(index);
            }
        }
        let mut candidates = candidates.into_iter();
        let mut available = 0usize;
        loop {
            while available.saturating_add(reads.len()) < scheme.data_num {
                let Some(index) = candidates.next() else {
                    break;
                };
                let actual = if index < scheme.data_num {
                    sealed_bytes
                        .saturating_sub(index as u64 * shard_bytes)
                        .min(shard_bytes)
                } else {
                    shard_bytes
                };
                if offset >= actual {
                    shards[index] = Some(vec![0; length as usize]);
                    available = available.saturating_add(1);
                    continue;
                }
                let disk_io = self.disk_io.clone();
                let segment = segments[index];
                let physical_len = u64::from(length).min(actual - offset) as u32;
                reads.spawn(async move {
                    let result = disk_io.read(&segment, unit_bytes, offset, physical_len).await;
                    (index, result)
                });
            }
            if available >= scheme.data_num || reads.is_empty() {
                break;
            }
            let Some(result) = reads.join_next().await else {
                break;
            };
            let (index, result) =
                result.map_err(|error| (ReadError::DiskIo(error.to_string()), failed_segments.clone()))?;
            match result {
                Ok(data) => {
                    let mut shard = vec![0; length as usize];
                    shard[..data.len()].copy_from_slice(&data);
                    shards[index] = Some(shard);
                    available = available.saturating_add(1);
                }
                Err(error) => push_durable_failure(&mut failed_segments, segments[index], &error),
            }
        }
        let decoded = decode_recoverable(strip, scheme, shards, &failed_segments)?;
        Ok(RecoveredSlice {
            data: Bytes::from(decoded[target].clone()),
            failed_segments,
        })
    }
}

fn decode_recoverable(
    strip: &ChunkStrip,
    scheme: EcScheme,
    shards: Vec<Option<Vec<u8>>>,
    failed_segments: &[Segment],
) -> Result<Vec<Vec<u8>>, (ReadError, Vec<Segment>)> {
    let missing = shards.iter().filter(|shard| shard.is_none()).count();
    if missing > scheme.code_num {
        return Err((
            ReadError::DataLoss(format!(
                "EC strip {} lost {missing} shards with {} parity shards",
                strip.strip_sequence, scheme.code_num
            )),
            failed_segments.to_vec(),
        ));
    }
    decode(scheme, shards).map_err(|error| (ReadError::EcDecode(error.to_string()), failed_segments.to_vec()))
}

struct RecoveredSlice {
    data: Bytes,
    failed_segments: Vec<Segment>,
}

struct EcReadGeometry {
    scheme: EcScheme,
    unit_bytes: u64,
    shard_bytes: u64,
    end: u64,
}

fn validate_ec_read(
    strip: &ChunkStrip,
    ec: &crowdb_protocol::chunkdb::rpc::EcStrip,
    offset: u64,
    length: u64,
) -> ReadResult<EcReadGeometry> {
    let scheme = ec_scheme(ec)?;
    if ec.segments.len() != scheme.total_blocks() {
        return Err(ReadError::InvalidLocations(format!(
            "EC strip {} has {} segments for {}+{}",
            strip.strip_sequence,
            ec.segments.len(),
            scheme.data_num,
            scheme.code_num
        )));
    }
    let unit_bytes = kib_to_bytes(strip.unit_kb)?;
    let shard_bytes = segment_bytes(&ec.segments[0], unit_bytes)?;
    for segment in &ec.segments {
        if segment_bytes(segment, unit_bytes)? != shard_bytes {
            return Err(ReadError::InvalidLocations(format!(
                "EC strip {} has unequal shard sizes",
                strip.strip_sequence
            )));
        }
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| ReadError::InvalidLocations("EC read range overflows".into()))?;
    if shard_bytes == 0 || shard_bytes.saturating_mul(scheme.data_num as u64) < end {
        return Err(ReadError::InvalidLocations(format!(
            "EC strip {} range exceeds its data shards",
            strip.strip_sequence
        )));
    }
    Ok(EcReadGeometry {
        scheme,
        unit_bytes,
        shard_bytes,
        end,
    })
}

fn push_unique(segments: &mut Vec<Segment>, segment: Segment) {
    if !segments.contains(&segment) {
        segments.push(segment);
    }
}

fn push_durable_failure(segments: &mut Vec<Segment>, segment: Segment, error: &crate::IoError) {
    if error.is_durable_read_failure() {
        push_unique(segments, segment);
    }
}

fn extend_unique(segments: &mut Vec<Segment>, additions: Vec<Segment>) {
    for segment in additions {
        push_unique(segments, segment);
    }
}

fn ec_scheme(ec: &crowdb_protocol::chunkdb::rpc::EcStrip) -> ReadResult<EcScheme> {
    let data_num = usize::try_from(ec.data_num)
        .map_err(|_| ReadError::InvalidLocations("EC data count exceeds usize".into()))?;
    let code_num = usize::try_from(ec.code_num)
        .map_err(|_| ReadError::InvalidLocations("EC parity count exceeds usize".into()))?;
    if data_num == 0 || code_num == 0 {
        return Err(ReadError::InvalidLocations(
            "EC scheme contains zero shards".into(),
        ));
    }
    Ok(EcScheme::new(data_num, code_num))
}

fn kib_to_bytes(value: u32) -> ReadResult<u64> {
    u64::from(value)
        .checked_mul(1024)
        .ok_or_else(|| ReadError::InvalidLocations("KiB field overflows bytes".into()))
}

fn segment_bytes(segment: &Segment, unit_bytes: u64) -> ReadResult<u64> {
    u64::from(segment.unit_count)
        .checked_mul(unit_bytes)
        .ok_or_else(|| ReadError::InvalidLocations("segment size overflows".into()))
}
