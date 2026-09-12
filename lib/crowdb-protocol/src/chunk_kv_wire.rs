// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `FlatBuffers` encoding for the chunk-KV point-operation RPC.

use flatbuffers::FlatBufferBuilder;
use thiserror::Error;

use crate::chunk_kv::{
    ChunkKvResponse, ChunkKvRpcErrorCode, ClientRequestId, Id128, OperationResult, OwnerHint, PointOperation,
    PointRequest, RequestRouting, RpcCompareCondition, RpcFailure, RpcJournalPosition, RpcValue,
};
use crate::chunk_kv_fb::{
    FBChunkKvCondition, FBChunkKvOperation, FBChunkKvPointRequest, FBChunkKvPointRequestArgs,
    FBChunkKvPointResponse, FBChunkKvPointResponseArgs, FBChunkKvResult, FBChunkKvRetCode,
};

type ByteVectorOffset<'a> = flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>;
type EncodedOperation<'a> = (
    FBChunkKvOperation,
    Option<ByteVectorOffset<'a>>,
    FBChunkKvCondition,
    u64,
    Option<ByteVectorOffset<'a>>,
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointRequestEnvelope {
    pub rpc_request_id: u64,
    pub rpc_create_nano: u64,
    pub request: PointRequest,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkKvWireError {
    #[error("invalid chunk KV point request")]
    InvalidRequest,
    #[error("invalid chunk KV point response")]
    InvalidResponse,
    #[error("unsupported chunk KV point result")]
    UnsupportedResult,
}

/// Encodes one validated point request and its transport envelope.
///
/// # Errors
///
/// Returns `InvalidRequest` when the logical routing envelope is malformed.
pub fn encode_point_request(
    rpc_request_id: u64,
    rpc_create_nano: u64,
    request: &PointRequest,
) -> Result<(Vec<u8>, usize), ChunkKvWireError> {
    request
        .routing
        .validate()
        .map_err(|_| ChunkKvWireError::InvalidRequest)?;
    let mut builder = FlatBufferBuilder::new();
    let key = Some(builder.create_vector(request.operation.key()));
    let (operation, value, condition, condition_revision, condition_value) =
        encode_operation(&mut builder, &request.operation);
    let minimum = request.routing.min_journal_position.unwrap_or_default();
    let root = FBChunkKvPointRequest::create(
        &mut builder,
        &FBChunkKvPointRequestArgs {
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
            operation,
            key,
            value,
            condition,
            condition_revision,
            condition_value,
        },
    );
    builder.finish(root, None);
    Ok(builder.collapse())
}

/// Decodes and validates one point request.
///
/// # Errors
///
/// Returns `InvalidRequest` for malformed `FlatBuffers`, missing operation
/// fields, unknown enum values, or an invalid logical routing envelope.
pub fn decode_point_request(bytes: &[u8]) -> Result<PointRequestEnvelope, ChunkKvWireError> {
    let encoded =
        flatbuffers::root::<FBChunkKvPointRequest>(bytes).map_err(|_| ChunkKvWireError::InvalidRequest)?;
    let key = encoded
        .key()
        .map(|value| value.bytes().to_vec())
        .ok_or(ChunkKvWireError::InvalidRequest)?;
    let operation = decode_operation(encoded, key)?;
    let request = PointRequest {
        routing: RequestRouting {
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
        },
        operation,
    };
    request
        .routing
        .validate()
        .map_err(|_| ChunkKvWireError::InvalidRequest)?;
    Ok(PointRequestEnvelope {
        rpc_request_id: encoded.id(),
        rpc_create_nano: encoded.rpc_create_nano(),
        request,
    })
}

/// Encodes one point response and its transport correlation fields.
///
/// # Errors
///
/// Returns `UnsupportedResult` when a non-point result is supplied.
pub fn encode_point_response(
    rpc_request_id: u64,
    rpc_create_nano: u64,
    response: &ChunkKvResponse,
) -> Result<(Vec<u8>, usize), ChunkKvWireError> {
    let mut builder = FlatBufferBuilder::new();
    let mut args = FBChunkKvPointResponseArgs {
        id: rpc_request_id,
        rpc_create_nano,
        map_revision: response.map_revision,
        ..FBChunkKvPointResponseArgs::default()
    };
    if let Some(position) = response.journal_position {
        args.has_journal_position = true;
        args.journal_stream_high = position.stream_name.high;
        args.journal_stream_low = position.stream_name.low;
        args.journal_offset = position.offset;
    }
    match &response.result {
        Ok(result) => encode_success(&mut builder, &mut args, result)?,
        Err(failure) => encode_failure(&mut builder, &mut args, failure),
    }
    let root = FBChunkKvPointResponse::create(&mut builder, &args);
    builder.finish(root, None);
    Ok(builder.collapse())
}

/// Decodes one typed point response.
///
/// # Errors
///
/// Returns `InvalidResponse` for malformed `FlatBuffers`, unknown enum
/// values, or missing fields required by the encoded result.
pub fn decode_point_response(bytes: &[u8]) -> Result<ChunkKvResponse, ChunkKvWireError> {
    let encoded =
        flatbuffers::root::<FBChunkKvPointResponse>(bytes).map_err(|_| ChunkKvWireError::InvalidResponse)?;
    let journal_position = encoded.has_journal_position().then_some(RpcJournalPosition {
        stream_name: Id128 {
            high: encoded.journal_stream_high(),
            low: encoded.journal_stream_low(),
        },
        offset: encoded.journal_offset(),
    });
    let result = if encoded.ret_code() == FBChunkKvRetCode::Success {
        Ok(decode_success(encoded)?)
    } else {
        Err(decode_failure(encoded)?)
    };
    Ok(ChunkKvResponse {
        map_revision: encoded.map_revision(),
        journal_position,
        result,
    })
}

fn encode_operation<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    operation: &PointOperation,
) -> EncodedOperation<'a> {
    match operation {
        PointOperation::Get { .. } => (FBChunkKvOperation::Get, None, FBChunkKvCondition::None, 0, None),
        PointOperation::Put { value, .. } => (
            FBChunkKvOperation::Put,
            Some(builder.create_vector(value)),
            FBChunkKvCondition::None,
            0,
            None,
        ),
        PointOperation::Delete { .. } => (
            FBChunkKvOperation::Delete,
            None,
            FBChunkKvCondition::None,
            0,
            None,
        ),
        PointOperation::PutIfAbsent { value, .. } => (
            FBChunkKvOperation::PutIfAbsent,
            Some(builder.create_vector(value)),
            FBChunkKvCondition::None,
            0,
            None,
        ),
        PointOperation::CompareExchange { condition, value, .. } => {
            let (condition, revision, condition_value) = encode_condition(builder, condition);
            (
                FBChunkKvOperation::CompareExchange,
                Some(builder.create_vector(value)),
                condition,
                revision,
                condition_value,
            )
        }
        PointOperation::ConditionalDelete { condition, .. } => {
            let (condition, revision, condition_value) = encode_condition(builder, condition);
            (
                FBChunkKvOperation::ConditionalDelete,
                None,
                condition,
                revision,
                condition_value,
            )
        }
    }
}

fn encode_condition<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    condition: &RpcCompareCondition,
) -> (
    FBChunkKvCondition,
    u64,
    Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
) {
    match condition {
        RpcCompareCondition::Revision(revision) => (FBChunkKvCondition::Revision, *revision, None),
        RpcCompareCondition::Value(value) => {
            (FBChunkKvCondition::Value, 0, Some(builder.create_vector(value)))
        }
    }
}

fn decode_operation(
    encoded: FBChunkKvPointRequest<'_>,
    key: Vec<u8>,
) -> Result<PointOperation, ChunkKvWireError> {
    let value = || {
        encoded
            .value()
            .map(|value| value.bytes().to_vec())
            .ok_or(ChunkKvWireError::InvalidRequest)
    };
    match encoded.operation() {
        FBChunkKvOperation::Get => Ok(PointOperation::Get { key }),
        FBChunkKvOperation::Put => Ok(PointOperation::Put { key, value: value()? }),
        FBChunkKvOperation::Delete => Ok(PointOperation::Delete { key }),
        FBChunkKvOperation::PutIfAbsent => Ok(PointOperation::PutIfAbsent { key, value: value()? }),
        FBChunkKvOperation::CompareExchange => Ok(PointOperation::CompareExchange {
            key,
            condition: decode_condition(encoded)?,
            value: value()?,
        }),
        FBChunkKvOperation::ConditionalDelete => Ok(PointOperation::ConditionalDelete {
            key,
            condition: decode_condition(encoded)?,
        }),
        _ => Err(ChunkKvWireError::InvalidRequest),
    }
}

fn decode_condition(encoded: FBChunkKvPointRequest<'_>) -> Result<RpcCompareCondition, ChunkKvWireError> {
    match encoded.condition() {
        FBChunkKvCondition::Revision => Ok(RpcCompareCondition::Revision(encoded.condition_revision())),
        FBChunkKvCondition::Value => encoded
            .condition_value()
            .map(|value| RpcCompareCondition::Value(value.bytes().to_vec()))
            .ok_or(ChunkKvWireError::InvalidRequest),
        _ => Err(ChunkKvWireError::InvalidRequest),
    }
}

fn encode_success<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    args: &mut FBChunkKvPointResponseArgs<'a>,
    result: &OperationResult,
) -> Result<(), ChunkKvWireError> {
    args.ret_code = FBChunkKvRetCode::Success;
    match result {
        OperationResult::Value(value) => {
            args.result = FBChunkKvResult::Value;
            if let Some(value) = value {
                args.found = true;
                args.key = Some(builder.create_vector(&value.key));
                args.value = Some(builder.create_vector(&value.value));
                args.value_revision = value.revision;
            }
        }
        OperationResult::Mutation {
            applied,
            revision,
            observed,
        } => {
            args.result = FBChunkKvResult::Mutation;
            args.mutation_applied = *applied;
            args.has_mutation_revision = revision.is_some();
            args.mutation_revision = revision.unwrap_or_default();
            if let Some(value) = observed {
                args.has_observed = true;
                args.observed_key = Some(builder.create_vector(&value.key));
                args.observed_value = Some(builder.create_vector(&value.value));
                args.observed_revision = value.revision;
            }
        }
        OperationResult::Scan { .. } => {
            return Err(ChunkKvWireError::UnsupportedResult);
        }
    }
    Ok(())
}

fn encode_failure<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    args: &mut FBChunkKvPointResponseArgs<'a>,
    failure: &RpcFailure,
) {
    args.ret_code = encode_error_code(failure.code);
    args.error_msg = Some(builder.create_string(&failure.message));
    if let Some(delay) = failure.retry_after_ms {
        args.has_retry_after = true;
        args.retry_after_ms = delay;
    }
    if let Some(revision) = failure.latest_map_revision {
        args.has_latest_map_revision = true;
        args.latest_map_revision = revision;
    }
    if let Some(owner) = &failure.owner_hint {
        args.has_owner_hint = true;
        args.owner_instance_id = owner.instance_id;
        args.owner_rpc_endpoint = Some(builder.create_string(&owner.rpc_endpoint));
        args.owner_epoch = owner.owner_epoch;
    }
}

fn decode_success(encoded: FBChunkKvPointResponse<'_>) -> Result<OperationResult, ChunkKvWireError> {
    match encoded.result() {
        FBChunkKvResult::Value => {
            let value = if encoded.found() {
                Some(RpcValue {
                    key: required_bytes(encoded.key())?,
                    value: required_bytes(encoded.value())?,
                    revision: encoded.value_revision(),
                })
            } else {
                None
            };
            Ok(OperationResult::Value(value))
        }
        FBChunkKvResult::Mutation => {
            let observed = if encoded.has_observed() {
                Some(RpcValue {
                    key: required_bytes(encoded.observed_key())?,
                    value: required_bytes(encoded.observed_value())?,
                    revision: encoded.observed_revision(),
                })
            } else {
                None
            };
            Ok(OperationResult::Mutation {
                applied: encoded.mutation_applied(),
                revision: encoded
                    .has_mutation_revision()
                    .then_some(encoded.mutation_revision()),
                observed,
            })
        }
        _ => Err(ChunkKvWireError::InvalidResponse),
    }
}

fn decode_failure(encoded: FBChunkKvPointResponse<'_>) -> Result<RpcFailure, ChunkKvWireError> {
    let owner_hint = if encoded.has_owner_hint() {
        Some(OwnerHint {
            instance_id: encoded.owner_instance_id(),
            rpc_endpoint: encoded
                .owner_rpc_endpoint()
                .ok_or(ChunkKvWireError::InvalidResponse)?
                .to_owned(),
            owner_epoch: encoded.owner_epoch(),
        })
    } else {
        None
    };
    Ok(RpcFailure {
        code: decode_error_code(encoded.ret_code())?,
        message: encoded.error_msg().unwrap_or_default().to_owned(),
        retry_after_ms: encoded.has_retry_after().then_some(encoded.retry_after_ms()),
        latest_map_revision: encoded
            .has_latest_map_revision()
            .then_some(encoded.latest_map_revision()),
        owner_hint,
    })
}

fn required_bytes(value: Option<flatbuffers::Vector<'_, u8>>) -> Result<Vec<u8>, ChunkKvWireError> {
    value
        .map(|value| value.bytes().to_vec())
        .ok_or(ChunkKvWireError::InvalidResponse)
}

fn encode_error_code(code: ChunkKvRpcErrorCode) -> FBChunkKvRetCode {
    match code {
        ChunkKvRpcErrorCode::Overloaded => FBChunkKvRetCode::Overloaded,
        ChunkKvRpcErrorCode::WriteStalled => FBChunkKvRetCode::WriteStalled,
        ChunkKvRpcErrorCode::Recovering => FBChunkKvRetCode::Recovering,
        ChunkKvRpcErrorCode::LeaseExpired => FBChunkKvRetCode::LeaseExpired,
        ChunkKvRpcErrorCode::RequestExpired => FBChunkKvRetCode::RequestExpired,
        ChunkKvRpcErrorCode::RequestConflict => FBChunkKvRetCode::RequestConflict,
        ChunkKvRpcErrorCode::NotMyRange => FBChunkKvRetCode::NotMyRange,
        ChunkKvRpcErrorCode::RefreshRequired => FBChunkKvRetCode::RefreshRequired,
        ChunkKvRpcErrorCode::InvalidRequest => FBChunkKvRetCode::InvalidRequest,
        ChunkKvRpcErrorCode::Internal => FBChunkKvRetCode::Internal,
    }
}

fn decode_error_code(code: FBChunkKvRetCode) -> Result<ChunkKvRpcErrorCode, ChunkKvWireError> {
    match code {
        FBChunkKvRetCode::Overloaded => Ok(ChunkKvRpcErrorCode::Overloaded),
        FBChunkKvRetCode::WriteStalled => Ok(ChunkKvRpcErrorCode::WriteStalled),
        FBChunkKvRetCode::Recovering => Ok(ChunkKvRpcErrorCode::Recovering),
        FBChunkKvRetCode::LeaseExpired => Ok(ChunkKvRpcErrorCode::LeaseExpired),
        FBChunkKvRetCode::RequestExpired => Ok(ChunkKvRpcErrorCode::RequestExpired),
        FBChunkKvRetCode::RequestConflict => Ok(ChunkKvRpcErrorCode::RequestConflict),
        FBChunkKvRetCode::NotMyRange => Ok(ChunkKvRpcErrorCode::NotMyRange),
        FBChunkKvRetCode::RefreshRequired => Ok(ChunkKvRpcErrorCode::RefreshRequired),
        FBChunkKvRetCode::InvalidRequest => Ok(ChunkKvRpcErrorCode::InvalidRequest),
        FBChunkKvRetCode::Internal => Ok(ChunkKvRpcErrorCode::Internal),
        _ => Err(ChunkKvWireError::InvalidResponse),
    }
}
