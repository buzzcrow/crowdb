// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{

namespace
{
constexpr uint64_t kGrowthBytes = 4U * 1024U * 1024U;
}

Status RpcChunkTransport::allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id,
                                                PagePurpose purpose)
{
    if (!valid() || chunk_id == nullptr || logical_capacity == 0 || !valid_page_purpose(purpose) ||
        logical_capacity > std::numeric_limits<uint32_t>::max()) {
        return Status::invalid_argument("tree chunk RPC allocation arguments are invalid");
    }
    const uint64_t request_id       = impl_->next_request_id();
    const uint64_t initial_capacity = std::min(logical_capacity, kGrowthBytes);
    const auto     geometry =
        rpc_chunk_allocation(initial_capacity, impl_->options.strip_capacity_bytes, impl_->options.mirror_copies);
    flatbuffers::FlatBufferBuilder builder;
    auto                           request = crowdb::chunkdb::proto::CreateFBAllocateChunkRequest(
        builder, request_id, monotonic_nanos(), nullptr, geometry.granularity_kb, geometry.strip_count,
        FBStripType_Mirror, 0, 0, impl_->options.mirror_copies == 0 ? 2 : impl_->options.mirror_copies,
        static_cast<crowdb::chunkdb::proto::FBChunkType>(purpose), owner_epoch, impl_->options.writer_lease_ms);
    builder.Finish(request);
    std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    RpcResult            result;
    Status               status = impl_->call_chunkdb<crowdb::chunkdb::proto::FBAllocateChunkResponse>(
        {}, request_id, crowdb::rpc::proto::FBMsgType_EAllocateChunkRequest, control, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<crowdb::chunkdb::proto::FBAllocateChunkResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB allocation response is malformed");
    }
    status = chunkdb_status(response->ret_code(), response->error_msg());
    if (!status.ok()) {
        return status;
    }
    Impl::RemoteChunk remote;
    status = crowdb::tree::detail::RpcChunkTransport::Impl::parse_chunk(response->chunk(), &remote);
    if (!status.ok() || remote.layout.chunk_id.empty() || remote.owner_epoch != owner_epoch ||
        remote.layout.logical_capacity < initial_capacity || chunk_purpose(remote.layout.chunk_id) != purpose) {
        return status.ok() ? Status::corruption("ChunkDB allocation metadata mismatch") : status;
    }
    remote.valid_until_ms         = std::numeric_limits<uint64_t>::max();
    remote.maximum_capacity       = logical_capacity;
    remote.self_fence_deadline_ms = monotonic_millis() + kLivenessSelfFenceMs;
    *chunk_id                     = remote.layout.chunk_id;
    impl_->cache(std::move(remote));
    return Status::Ok();
}

Status RpcChunkTransport::ensure_capacity(ChunkId chunk_id, uint64_t required_bytes)
{
    // Only the ordered planner grows chunks. Mirror submissions never allocate,
    // so concurrent replica writes cannot reserve the same extension twice.
    Impl::RemoteChunk chunk;
    if (!impl_->cached(chunk_id, &chunk) || chunk.maximum_capacity == 0 || chunk.self_fence_deadline_ms == 0 ||
        monotonic_millis() >= chunk.self_fence_deadline_ms) {
        return Status::unavailable("tree chunk growth has no local liveness authority");
    }
    const uint64_t owner_epoch = chunk.owner_epoch;
    const uint64_t maximum     = chunk.maximum_capacity;
    if (required_bytes > maximum) {
        return Status::resource_exhausted("tree chunk growth exceeds its maximum capacity");
    }
    for (uint32_t attempt = 0; attempt != 2; ++attempt) {
        if (chunk.layout.sealed || chunk.owner_epoch != owner_epoch) {
            return Status::unavailable("tree chunk growth is fenced by owner epoch or seal");
        }
        if (required_bytes <= chunk.layout.logical_capacity) {
            return Status::Ok();
        }
        const auto    &first       = chunk.strips.front();
        const uint64_t strip_bytes = first.capacity;
        const uint64_t unit_bytes  = static_cast<uint64_t>(first.unit_kb) * 1024U;
        if (strip_bytes == 0 || strip_bytes % unit_bytes != 0) {
            return Status::corruption("tree chunk growth has invalid strip geometry");
        }
        const uint64_t target = std::min(maximum, ((required_bytes + kGrowthBytes - 1) / kGrowthBytes) * kGrowthBytes);
        const auto     count =
            static_cast<uint32_t>((target - chunk.layout.logical_capacity + strip_bytes - 1) / strip_bytes);
        const uint64_t                 request_id = impl_->next_request_id();
        const FBInt128                 id(chunk_id.high, chunk_id.low);
        flatbuffers::FlatBufferBuilder builder;
        auto                           request = crowdb::chunkdb::proto::CreateFBAppendChunkRequest(
            builder, request_id, monotonic_nanos(), &id, chunk.modify_ts,
            static_cast<uint32_t>(strip_bytes / unit_bytes), count, FBStripType_Mirror, 0, 0,
            static_cast<uint32_t>(first.mirrors.size()));
        builder.Finish(request);
        std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
        RpcResult            result;
        Status               status = impl_->call_chunkdb<crowdb::chunkdb::proto::FBAppendChunkResponse>(
            chunk_id, request_id, crowdb::rpc::proto::FBMsgType_EAppendChunkRequest, control, &result);
        if (!status.ok()) {
            return status;
        }
        const auto *response = verified_response<crowdb::chunkdb::proto::FBAppendChunkResponse>(result.control);
        if (response == nullptr) {
            return Status::corruption("ChunkDB growth response is malformed");
        }
        status = chunkdb_status(response->ret_code(), response->error_msg());
        if (!status.ok()) {
            return status;
        }
        const bool conflict = response->chunk() != nullptr;
        if (!conflict && (response->strips() == nullptr || response->strips()->size() != count)) {
            return Status::corruption("ChunkDB growth returned an unexpected strip count");
        }
        // Refresh after append so a simultaneous lease renewal cannot replace
        // the new revision with an old acknowledged cursor or layout.
        status = impl_->query_remote(chunk_id, &chunk);
        if (!status.ok()) {
            return status;
        }
        if (chunk.layout.chunk_id != chunk_id || chunk.owner_epoch != owner_epoch || chunk.layout.sealed) {
            return Status::unavailable("tree chunk growth lost its writable authority");
        }
        if (!conflict && chunk.layout.logical_capacity < target) {
            return Status::corruption("ChunkDB growth did not provide the requested capacity");
        }
        if (chunk.layout.logical_capacity >= required_bytes) {
            return Status::Ok();
        }
    }
    return Status::unavailable("tree chunk growth revision changed repeatedly");
}

} // namespace crowdb::tree::detail
