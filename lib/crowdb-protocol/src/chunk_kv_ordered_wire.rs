// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Flatbuffer encoding for chunk-KV ordered read requests.

use flatbuffers::FlatBufferBuilder;

use crate::chunk_kv::{
    ClientRequestId, Id128, RequestRouting, RpcJournalPosition, ScanContinuation, ScanRequest, SeekKind,
    SeekRequest,
};
use crate::chunk_kv_fb::{
    FBChunkKvScanRequest, FBChunkKvScanRequestArgs, FBChunkKvSeekKind, FBChunkKvSeekRequest,
    FBChunkKvSeekRequestArgs,
};
use crate::chunk_kv_wire::{decode_direction, encode_direction, ChunkKvWireError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeekRequestEnvelope {
    pub rpc_request_id: u64,
    pub rpc_create_nano: u64,
    pub request: SeekRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRequestEnvelope {
    pub rpc_request_id: u64,
    pub rpc_create_nano: u64,
    pub request: ScanRequest,
}

/// Encodes one validated seek request.
///
/// # Errors
///
/// Returns an invalid request error when routing is malformed.
pub fn encode_seek_request(
    rpc_request_id: u64,
    rpc_create_nano: u64,
    request: &SeekRequest,
) -> Result<(Vec<u8>, usize), ChunkKvWireError> {
    request
        .routing
        .validate()
        .map_err(|_| ChunkKvWireError::InvalidRequest)?;
    let mut builder = FlatBufferBuilder::new();
    let key = Some(builder.create_vector(&request.key));
    let minimum = request.routing.min_journal_position.unwrap_or_default();
    let root = FBChunkKvSeekRequest::create(
        &mut builder,
        &FBChunkKvSeekRequestArgs {
            id: rpc_request_id,
            rpc_create_nano,
            client_instance_high: request.routing.request_id.client_instance_id.high,
            client_instance_low: request.routing.request_id.client_instance_id.low,
            client_sequence: request.routing.request_id.client_sequence,
            map_revision: request.routing.map_revision,
            partition_high: request.routing.partition_id.high,
            partition_low: request.routing.partition_id.low,
            owner_epoch: request.routing.owner_epoch,
            has_min_journal_position: request.routing.min_journal_position.is_some(),
            min_stream_high: minimum.stream_name.high,
            min_stream_low: minimum.stream_name.low,
            min_journal_offset: minimum.offset,
            has_deadline: request.routing.deadline_ms.is_some(),
            deadline_ms: request.routing.deadline_ms.unwrap_or_default(),
            key,
            kind: encode_seek_kind(request.kind),
        },
    );
    builder.finish(root, None);
    Ok(builder.collapse())
}

/// Decodes and validates one seek request.
///
/// # Errors
///
/// Returns an invalid request error for malformed flatbuffer data, missing
/// fields, or invalid routing.
pub fn decode_seek_request(bytes: &[u8]) -> Result<SeekRequestEnvelope, ChunkKvWireError> {
    let encoded =
        flatbuffers::root::<FBChunkKvSeekRequest>(bytes).map_err(|_| ChunkKvWireError::InvalidRequest)?;
    let request = SeekRequest {
        routing: decode_seek_routing(encoded),
        key: encoded
            .key()
            .map(|value| value.bytes().to_vec())
            .ok_or(ChunkKvWireError::InvalidRequest)?,
        kind: decode_seek_kind(encoded.kind())?,
    };
    request
        .routing
        .validate()
        .map_err(|_| ChunkKvWireError::InvalidRequest)?;
    Ok(SeekRequestEnvelope {
        rpc_request_id: encoded.id(),
        rpc_create_nano: encoded.rpc_create_nano(),
        request,
    })
}

/// Encodes one validated directional scan request.
///
/// # Errors
///
/// Returns an invalid request error when routing, limits, or bounds are
/// malformed.
pub fn encode_scan_request(
    rpc_request_id: u64,
    rpc_create_nano: u64,
    request: &ScanRequest,
) -> Result<(Vec<u8>, usize), ChunkKvWireError> {
    request.validate().map_err(|_| ChunkKvWireError::InvalidRequest)?;
    let mut builder = FlatBufferBuilder::new();
    let start = request.start.as_ref().map(|value| builder.create_vector(value));
    let end = request.end.as_ref().map(|value| builder.create_vector(value));
    let continuation_last_key = request
        .continuation
        .as_ref()
        .map(|value| builder.create_vector(&value.last_key));
    let minimum = request.routing.min_journal_position.unwrap_or_default();
    let continuation = request.continuation.clone().unwrap_or_default();
    let root = FBChunkKvScanRequest::create(
        &mut builder,
        &FBChunkKvScanRequestArgs {
            id: rpc_request_id,
            rpc_create_nano,
            client_instance_high: request.routing.request_id.client_instance_id.high,
            client_instance_low: request.routing.request_id.client_instance_id.low,
            client_sequence: request.routing.request_id.client_sequence,
            map_revision: request.routing.map_revision,
            partition_high: request.routing.partition_id.high,
            partition_low: request.routing.partition_id.low,
            owner_epoch: request.routing.owner_epoch,
            has_min_journal_position: request.routing.min_journal_position.is_some(),
            min_stream_high: minimum.stream_name.high,
            min_stream_low: minimum.stream_name.low,
            min_journal_offset: minimum.offset,
            has_deadline: request.routing.deadline_ms.is_some(),
            deadline_ms: request.routing.deadline_ms.unwrap_or_default(),
            has_start: request.start.is_some(),
            start,
            has_end: request.end.is_some(),
            end,
            direction: encode_direction(request.direction),
            limit: request.limit,
            has_continuation: request.continuation.is_some(),
            continuation_direction: encode_direction(continuation.direction),
            continuation_last_key,
            continuation_partition_high: continuation.partition_id.high,
            continuation_partition_low: continuation.partition_id.low,
            continuation_owner_epoch: continuation.owner_epoch,
            continuation_map_revision: continuation.map_revision,
        },
    );
    builder.finish(root, None);
    Ok(builder.collapse())
}

/// Decodes and validates one directional scan request.
///
/// # Errors
///
/// Returns an invalid request error for malformed flatbuffer data, missing
/// optional payloads selected by presence flags, invalid bounds, or invalid
/// routing.
pub fn decode_scan_request(bytes: &[u8]) -> Result<ScanRequestEnvelope, ChunkKvWireError> {
    let encoded =
        flatbuffers::root::<FBChunkKvScanRequest>(bytes).map_err(|_| ChunkKvWireError::InvalidRequest)?;
    let continuation = if encoded.has_continuation() {
        Some(ScanContinuation {
            direction: decode_direction(encoded.continuation_direction())
                .map_err(|_| ChunkKvWireError::InvalidRequest)?,
            last_key: encoded
                .continuation_last_key()
                .map(|value| value.bytes().to_vec())
                .ok_or(ChunkKvWireError::InvalidRequest)?,
            partition_id: Id128 {
                high: encoded.continuation_partition_high(),
                low: encoded.continuation_partition_low(),
            },
            owner_epoch: encoded.continuation_owner_epoch(),
            map_revision: encoded.continuation_map_revision(),
        })
    } else {
        None
    };
    let request = ScanRequest {
        routing: decode_scan_routing(encoded),
        start: optional_scan_bytes(encoded.has_start(), encoded.start())?,
        end: optional_scan_bytes(encoded.has_end(), encoded.end())?,
        direction: decode_direction(encoded.direction()).map_err(|_| ChunkKvWireError::InvalidRequest)?,
        limit: encoded.limit(),
        continuation,
    };
    request.validate().map_err(|_| ChunkKvWireError::InvalidRequest)?;
    Ok(ScanRequestEnvelope {
        rpc_request_id: encoded.id(),
        rpc_create_nano: encoded.rpc_create_nano(),
        request,
    })
}

fn decode_seek_routing(encoded: FBChunkKvSeekRequest<'_>) -> RequestRouting {
    RequestRouting {
        request_id: ClientRequestId {
            client_instance_id: Id128 {
                high: encoded.client_instance_high(),
                low: encoded.client_instance_low(),
            },
            client_sequence: encoded.client_sequence(),
        },
        map_revision: encoded.map_revision(),
        partition_id: Id128 {
            high: encoded.partition_high(),
            low: encoded.partition_low(),
        },
        owner_epoch: encoded.owner_epoch(),
        min_journal_position: encoded.has_min_journal_position().then_some(RpcJournalPosition {
            stream_name: Id128 {
                high: encoded.min_stream_high(),
                low: encoded.min_stream_low(),
            },
            offset: encoded.min_journal_offset(),
        }),
        deadline_ms: encoded.has_deadline().then_some(encoded.deadline_ms()),
    }
}

fn decode_scan_routing(encoded: FBChunkKvScanRequest<'_>) -> RequestRouting {
    RequestRouting {
        request_id: ClientRequestId {
            client_instance_id: Id128 {
                high: encoded.client_instance_high(),
                low: encoded.client_instance_low(),
            },
            client_sequence: encoded.client_sequence(),
        },
        map_revision: encoded.map_revision(),
        partition_id: Id128 {
            high: encoded.partition_high(),
            low: encoded.partition_low(),
        },
        owner_epoch: encoded.owner_epoch(),
        min_journal_position: encoded.has_min_journal_position().then_some(RpcJournalPosition {
            stream_name: Id128 {
                high: encoded.min_stream_high(),
                low: encoded.min_stream_low(),
            },
            offset: encoded.min_journal_offset(),
        }),
        deadline_ms: encoded.has_deadline().then_some(encoded.deadline_ms()),
    }
}

fn optional_scan_bytes(
    present: bool,
    value: Option<flatbuffers::Vector<'_, u8>>,
) -> Result<Option<Vec<u8>>, ChunkKvWireError> {
    if present {
        value
            .map(|value| Some(value.bytes().to_vec()))
            .ok_or(ChunkKvWireError::InvalidRequest)
    } else {
        Ok(None)
    }
}

fn encode_seek_kind(kind: SeekKind) -> FBChunkKvSeekKind {
    match kind {
        SeekKind::Ceiling => FBChunkKvSeekKind::Ceiling,
        SeekKind::Higher => FBChunkKvSeekKind::Higher,
        SeekKind::Floor => FBChunkKvSeekKind::Floor,
        SeekKind::Lower => FBChunkKvSeekKind::Lower,
    }
}

fn decode_seek_kind(kind: FBChunkKvSeekKind) -> Result<SeekKind, ChunkKvWireError> {
    match kind {
        FBChunkKvSeekKind::Ceiling => Ok(SeekKind::Ceiling),
        FBChunkKvSeekKind::Higher => Ok(SeekKind::Higher),
        FBChunkKvSeekKind::Floor => Ok(SeekKind::Floor),
        FBChunkKvSeekKind::Lower => Ok(SeekKind::Lower),
        _ => Err(ChunkKvWireError::InvalidRequest),
    }
}
