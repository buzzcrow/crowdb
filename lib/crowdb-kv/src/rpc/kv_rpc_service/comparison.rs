// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Serialize a comparison observation without reading the engine again.

use crowdb_protocol::kv_client_fb::{FBKvCompareObservation, FBKvCompareObservationArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

pub(super) fn encode<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    key: &[u8],
    revision: u64,
    value: &[u8],
) -> WIPOffset<FBKvCompareObservation<'a>> {
    let key = builder.create_vector(key);
    let value = builder.create_vector(value);
    FBKvCompareObservation::create(
        builder,
        &FBKvCompareObservationArgs {
            key: Some(key),
            revision,
            value: Some(value),
        },
    )
}
