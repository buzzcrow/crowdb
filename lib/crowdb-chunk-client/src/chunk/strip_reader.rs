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
        if length == 0 {
            return Ok(Bytes::new());
        }
        let sealed_bytes = kib_to_bytes(strip.sealed_length)?;
        let available = sealed_bytes.max(durable_bytes);
        let end = offset
            .checked_add(length)
            .ok_or_else(|| ReadError::InvalidLocations("strip read range overflows".into()))?;
        if available == 0 || end > available {
            return Err(ReadError::NotYetAvailable(format!(
                "strip {} has {available} durable bytes, requested end {end}",
                strip.strip_sequence
            )));
        }
        match strip.strip.as_ref() {
            Some(Strip::MirrorStrip(mirror)) => {
                self.read_mirror(strip, &mirror.segments, offset, length).await
            }
            Some(Strip::EcStrip(ec)) => self.read_ec(strip, ec, offset, length).await,
            None => Err(ReadError::InvalidLocations(format!(
                "strip {} has no body",
                strip.strip_sequence
            ))),
        }
    }

    async fn read_mirror(
        &self,
        strip: &ChunkStrip,
        segments: &[Segment],
        offset: u64,
        length: u64,
    ) -> ReadResult<Bytes> {
        let length = u32::try_from(length)
            .map_err(|_| ReadError::InvalidLocations("mirror read exceeds RPC size".into()))?;
        let unit_bytes = kib_to_bytes(strip.unit_kb)?;
        let mut failures = Vec::new();
        for segment in segments {
            if strip.unavailable_segments.contains(segment) {
                failures.push("metadata-unavailable".to_string());
                continue;
            }
            match self.disk_io.read(segment, unit_bytes, offset, length).await {
                Ok(data) => return Ok(data),
                Err(error) => failures.push(error.to_string()),
            }
        }
        Err(ReadError::DataLoss(format!(
            "every mirror replica for strip {} failed: {}",
            strip.strip_sequence,
            failures.join("; ")
        )))
    }

    async fn read_ec(
        &self,
        strip: &ChunkStrip,
        ec: &crowdb_protocol::chunkdb::rpc::EcStrip,
        offset: u64,
        length: u64,
    ) -> ReadResult<Bytes> {
        if ec.ec_state != EcState::Parity as i32 {
            return Err(ReadError::NotYetAvailable(format!(
                "EC strip {} parity is incomplete",
                strip.strip_sequence
            )));
        }
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

        let mut pieces = Vec::new();
        let first = usize::try_from(offset / shard_bytes).unwrap_or(usize::MAX);
        let last = usize::try_from((end - 1) / shard_bytes).unwrap_or(usize::MAX);
        for (order, shard_index) in (first..=last).enumerate() {
            let shard_start = shard_index as u64 * shard_bytes;
            let local_start = offset.max(shard_start) - shard_start;
            let local_end = end.min(shard_start + shard_bytes) - shard_start;
            let read_len = u32::try_from(local_end - local_start)
                .map_err(|_| ReadError::InvalidLocations("EC read exceeds RPC size".into()))?;
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
            } else {
                let recovered = self
                    .recover_slices(
                        strip,
                        &ec.segments,
                        scheme,
                        unit_bytes,
                        shard_bytes,
                        shard_index,
                        local_start,
                        read_len,
                    )
                    .await?;
                output.extend_from_slice(&recovered);
            }
        }
        Ok(output.freeze())
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
    ) -> ReadResult<Bytes> {
        let divisor = scheme.total_blocks().saturating_add(1);
        let max_slice = self.recovery_memory_limit / divisor;
        if max_slice == 0 {
            return Err(ReadError::InvalidLocations(
                "EC recovery memory budget is smaller than one byte per shard".into(),
            ));
        }
        let mut output = BytesMut::with_capacity(length as usize);
        let mut consumed = 0u32;
        while consumed < length {
            let part_len = (length - consumed).min(u32::try_from(max_slice).unwrap_or(u32::MAX));
            output.extend_from_slice(
                &self
                    .recover_slice(
                        strip,
                        segments,
                        scheme,
                        unit_bytes,
                        shard_bytes,
                        target,
                        offset + u64::from(consumed),
                        part_len,
                    )
                    .await?,
            );
            consumed += part_len;
        }
        Ok(output.freeze())
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
    ) -> ReadResult<Bytes> {
        let memory = (length as usize)
            .checked_mul(scheme.total_blocks().saturating_add(1))
            .ok_or_else(|| ReadError::InvalidLocations("EC recovery memory overflows".into()))?;
        let permits = u32::try_from(memory)
            .map_err(|_| ReadError::InvalidLocations("EC recovery memory exceeds semaphore".into()))?;
        let _reservation = self
            .recovery_memory
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| ReadError::DiskIo("EC recovery memory budget closed".into()))?;
        let sealed_bytes = kib_to_bytes(strip.sealed_length)?;
        let mut shards = vec![None; scheme.total_blocks()];
        let mut reads = JoinSet::new();
        for (index, segment) in segments.iter().enumerate() {
            if index == target || strip.unavailable_segments.contains(segment) {
                continue;
            }
            let actual = if index < scheme.data_num {
                sealed_bytes
                    .saturating_sub(index as u64 * shard_bytes)
                    .min(shard_bytes)
            } else {
                shard_bytes
            };
            if offset >= actual {
                shards[index] = Some(vec![0; length as usize]);
                continue;
            }
            let disk_io = self.disk_io.clone();
            let segment = *segment;
            let physical_len = u64::from(length).min(actual - offset) as u32;
            reads.spawn(async move {
                let result = disk_io.read(&segment, unit_bytes, offset, physical_len).await;
                (index, result)
            });
        }
        while let Some(result) = reads.join_next().await {
            let (index, result) = result.map_err(|error| ReadError::DiskIo(error.to_string()))?;
            if let Ok(data) = result {
                let mut shard = vec![0; length as usize];
                shard[..data.len()].copy_from_slice(&data);
                shards[index] = Some(shard);
            }
        }
        let missing = shards.iter().filter(|shard| shard.is_none()).count();
        if missing > scheme.code_num {
            return Err(ReadError::DataLoss(format!(
                "EC strip {} lost {missing} shards with {} parity shards",
                strip.strip_sequence, scheme.code_num
            )));
        }
        let decoded = decode(scheme, shards).map_err(|error| ReadError::EcDecode(error.to_string()))?;
        Ok(Bytes::from(decoded[target].clone()))
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
