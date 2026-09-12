// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Serde control envelopes for partition-local composed chunk-KV RPCs.

use flatbuffers::FlatBufferBuilder;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::chunk_kv::{BatchMutationRequest, BatchMutationResponse, MultiGetRequest, MultiGetResponse};

#[derive(Debug, Error)]
pub enum ChunkKvGroupWireError {
    #[error("invalid chunk KV group RPC payload: {0}")]
    Invalid(String),
}

use crate::chunk_kv_fb::{
    FBChunkKvGroupRequest, FBChunkKvGroupRequestArgs, FBChunkKvGroupResponse, FBChunkKvGroupResponseArgs,
};

macro_rules! group_wire {
    ($encode:ident, $decode:ident, $ty:ty, $encode_envelope:ident, $decode_envelope:ident) => {
        /// Encodes one typed partition-group control envelope.
        ///
        /// # Errors
        ///
        /// Returns an error when the payload cannot be serialized.
        pub fn $encode(
            rpc_request_id: u64,
            rpc_create_nano: u64,
            payload: &$ty,
        ) -> Result<Vec<u8>, ChunkKvGroupWireError> {
            $encode_envelope(rpc_request_id, rpc_create_nano, payload)
        }

        /// Decodes one typed partition-group control envelope.
        ///
        /// # Errors
        ///
        /// Returns an error for malformed `FlatBuffers` or payload data.
        pub fn $decode(bytes: &[u8]) -> Result<(u64, u64, $ty), ChunkKvGroupWireError> {
            $decode_envelope(bytes)
        }
    };
}

fn encode_request<T: Serialize>(
    rpc_request_id: u64,
    rpc_create_nano: u64,
    payload: &T,
) -> Result<Vec<u8>, ChunkKvGroupWireError> {
    let payload =
        serde_json::to_vec(payload).map_err(|error| ChunkKvGroupWireError::Invalid(error.to_string()))?;
    let mut builder = FlatBufferBuilder::new();
    let payload = Some(builder.create_vector(&payload));
    let root = FBChunkKvGroupRequest::create(
        &mut builder,
        &FBChunkKvGroupRequestArgs {
            id: rpc_request_id,
            rpc_create_nano,
            payload,
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

fn decode_request<T: DeserializeOwned>(bytes: &[u8]) -> Result<(u64, u64, T), ChunkKvGroupWireError> {
    let envelope = flatbuffers::root::<FBChunkKvGroupRequest>(bytes)
        .map_err(|error| ChunkKvGroupWireError::Invalid(error.to_string()))?;
    decode_payload(
        envelope.id(),
        envelope.rpc_create_nano(),
        envelope.payload().map(|payload| payload.bytes()),
    )
}

fn encode_response<T: Serialize>(
    rpc_request_id: u64,
    rpc_create_nano: u64,
    payload: &T,
) -> Result<Vec<u8>, ChunkKvGroupWireError> {
    let payload =
        serde_json::to_vec(payload).map_err(|error| ChunkKvGroupWireError::Invalid(error.to_string()))?;
    let mut builder = FlatBufferBuilder::new();
    let payload = Some(builder.create_vector(&payload));
    let root = FBChunkKvGroupResponse::create(
        &mut builder,
        &FBChunkKvGroupResponseArgs {
            id: rpc_request_id,
            rpc_create_nano,
            payload,
        },
    );
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

fn decode_response<T: DeserializeOwned>(bytes: &[u8]) -> Result<(u64, u64, T), ChunkKvGroupWireError> {
    let envelope = flatbuffers::root::<FBChunkKvGroupResponse>(bytes)
        .map_err(|error| ChunkKvGroupWireError::Invalid(error.to_string()))?;
    decode_payload(
        envelope.id(),
        envelope.rpc_create_nano(),
        envelope.payload().map(|payload| payload.bytes()),
    )
}

fn decode_payload<T: DeserializeOwned>(
    rpc_request_id: u64,
    rpc_create_nano: u64,
    payload: Option<&[u8]>,
) -> Result<(u64, u64, T), ChunkKvGroupWireError> {
    if rpc_request_id == 0 {
        return Err(ChunkKvGroupWireError::Invalid(
            "RPC request identity is zero".into(),
        ));
    }
    let payload = serde_json::from_slice(
        payload.ok_or_else(|| ChunkKvGroupWireError::Invalid("payload is missing".into()))?,
    )
    .map_err(|error| ChunkKvGroupWireError::Invalid(error.to_string()))?;
    Ok((rpc_request_id, rpc_create_nano, payload))
}

group_wire!(
    encode_multi_get_request,
    decode_multi_get_request,
    MultiGetRequest,
    encode_request,
    decode_request
);
group_wire!(
    encode_multi_get_response,
    decode_multi_get_response,
    MultiGetResponse,
    encode_response,
    decode_response
);
group_wire!(
    encode_batch_mutation_request,
    decode_batch_mutation_request,
    BatchMutationRequest,
    encode_request,
    decode_request
);
group_wire!(
    encode_batch_mutation_response,
    decode_batch_mutation_response,
    BatchMutationResponse,
    encode_response,
    decode_response
);
