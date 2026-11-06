// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
Status RpcChunkTransport::delete_chunk(ChunkId id)
{
    using namespace crowdb::chunkdb::proto;
    if (!valid() || id.empty() || !valid_page_purpose(chunk_purpose(id))) {
        return Status::invalid_argument("chunk deletion requires a concrete tree chunk identity");
    }
    const uint64_t                 request_id = impl_->next_request_id();
    const FBInt128                 wire_id(id.high, id.low);
    flatbuffers::FlatBufferBuilder builder;
    builder.Finish(CreateFBDeleteChunkRequest(builder, request_id, monotonic_nanos(), &wire_id, true));
    std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    RpcResult            result;
    auto                 status = impl_->call_chunkdb<FBDeleteChunkResponse>(
        id, request_id, crowdb::rpc::proto::FBMsgType_EDeleteChunkRequest, control, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<FBDeleteChunkResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB deletion response is malformed");
    }
    status = chunkdb_status(response->ret_code(), response->error_msg());
    if (status.code() == Code::kNotFound) {
        // The caller has already fenced late allocation and durable Deleting
        // forbids new pins. An exact absent identity has no physical strips.
        impl_->forget(id);
        return Status::Ok();
    }
    if (!status.ok()) {
        return status;
    }
    const auto *chunk = response->chunk();
    if (chunk == nullptr || chunk->id() == nullptr || chunk->id()->high() != id.high || chunk->id()->low() != id.low ||
        chunk->state() != FBChunkState_Deleted || chunk->capacity() != 0 ||
        (chunk->strips() != nullptr && !chunk->strips()->empty())) {
        return Status::corruption("ChunkDB deletion has not released every physical strip");
    }
    impl_->forget(id);
    return Status::Ok();
}
} // namespace crowdb::tree::detail
