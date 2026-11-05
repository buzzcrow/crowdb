// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
Status RpcChunkTransport::inspect_fixed_allocation(ChunkId id, uint64_t epoch, ChunkAllocationIdentity *out) const
{
    using namespace crowdb::chunkdb::proto;
    if (!valid() || id.empty() || epoch == 0 || out == nullptr) {
        return Status::invalid_argument();
    }
    const auto                     request_id = impl_->next_request_id();
    const FBInt128                 wire_id(id.high, id.low);
    flatbuffers::FlatBufferBuilder query;
    query.Finish(CreateFBQueryChunkRequest(query, request_id, monotonic_nanos(), &wire_id));
    std::vector<uint8_t> control(query.GetBufferPointer(), query.GetBufferPointer() + query.GetSize());
    RpcResult            result;
    auto                 status = impl_->call_chunkdb<FBQueryChunkResponse>(
        id, request_id, crowdb::rpc::proto::FBMsgType_EQueryChunkRequest, control, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<FBQueryChunkResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("Chunk allocation query is malformed");
    }
    status = chunkdb_status(response->ret_code(), response->error_msg());
    if (!status.ok()) {
        return status;
    }
    const auto *chunk = response->chunk();
    if (chunk == nullptr || chunk->id() == nullptr || chunk->id()->high() != id.high || chunk->id()->low() != id.low) {
        return Status::corruption("Chunk allocation identity differs");
    }
    if (chunk->state() == FBChunkState_Deleted && chunk->capacity() == 0 &&
        (chunk->strips() == nullptr || chunk->strips()->empty())) {
        return Status::not_found();
    }
    if (chunk->writer_epoch() != epoch) {
        return Status::unavailable("retired stage names another physical owner");
    }
    if (chunk->strips() == nullptr || chunk->strips()->empty()) {
        return Status::corruption("Chunk allocation has no physical segments");
    }
    uint64_t bytes = 0;
    for (const auto *strip : *chunk->strips()) {
        const auto *mirror = strip == nullptr ? nullptr : strip->strip_body_as_FBMirrorStrip();
        if (strip == nullptr || strip->strip_type() != FBStripType_Mirror || strip->unit_kb() == 0 ||
            mirror == nullptr || mirror->segments() == nullptr || mirror->segments()->empty()) {
            return Status::corruption("Chunk allocation is not a complete mirror layout");
        }
        const uint64_t unit = static_cast<uint64_t>(strip->unit_kb()) * 1024;
        for (const auto *segment : *mirror->segments()) {
            if (segment == nullptr || segment->unit_count() == 0 || segment->owner_chunk().high() != id.high ||
                segment->owner_chunk().low() != id.low || segment->unit_count() > (UINT64_MAX - bytes) / unit) {
                return Status::corruption("Chunk physical segment identity or capacity differs");
            }
            bytes += segment->unit_count() * unit;
        }
    }
    *out = {.chunk = id, .owner_epoch = epoch, .created_ms = chunk->create_ts_ms(), .physical_bytes = bytes};
    return Status::Ok();
}
} // namespace crowdb::tree::detail
