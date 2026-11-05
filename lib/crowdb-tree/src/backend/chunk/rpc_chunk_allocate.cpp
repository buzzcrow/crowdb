// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
Status RpcChunkTransport::allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id,
                                                PagePurpose purpose)
{
    return allocate_requested_chunk({}, logical_capacity, owner_epoch, chunk_id, purpose);
}

Status RpcChunkTransport::allocate_fixed_chunk(ChunkId id, uint64_t capacity, uint64_t epoch, PagePurpose purpose)
{
    if (id.empty()) {
        return Status::invalid_argument("fixed chunk allocation requires a nonempty identity");
    }
    ChunkId allocated;
    return allocate_requested_chunk(id, capacity, epoch, &allocated, purpose);
}

Status RpcChunkTransport::allocate_requested_chunk(ChunkId requested, uint64_t logical_capacity, uint64_t owner_epoch,
                                                   ChunkId *chunk_id, PagePurpose purpose) const
{
    if (!valid() || chunk_id == nullptr || logical_capacity == 0 || !valid_page_purpose(purpose) ||
        logical_capacity > std::numeric_limits<uint32_t>::max() ||
        (!requested.empty() && chunk_purpose(requested) != purpose)) {
        return Status::invalid_argument("tree chunk RPC allocation arguments are invalid");
    }
    const uint64_t request_id = impl_->next_request_id();
    const uint64_t initial_capacity =
        requested.empty() ? std::min(logical_capacity, uint64_t{4U * 1024U * 1024U}) : logical_capacity;
    const auto geometry =
        rpc_chunk_allocation(initial_capacity, impl_->options.strip_capacity_bytes, impl_->options.mirror_copies);
    const FBInt128                 requested_id(requested.high, requested.low);
    flatbuffers::FlatBufferBuilder builder;
    auto                           request = crowdb::chunkdb::proto::CreateFBAllocateChunkRequest(
        builder, request_id, monotonic_nanos(), requested.empty() ? nullptr : &requested_id, geometry.granularity_kb,
        geometry.strip_count, FBStripType_Mirror, 0, 0,
        impl_->options.mirror_copies == 0 ? 2 : impl_->options.mirror_copies,
        static_cast<crowdb::chunkdb::proto::FBChunkType>(purpose), owner_epoch, impl_->options.writer_lease_ms);
    builder.Finish(request);
    std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    RpcResult            result;
    Status               status = impl_->call_chunkdb<crowdb::chunkdb::proto::FBAllocateChunkResponse>(
        requested, request_id, crowdb::rpc::proto::FBMsgType_EAllocateChunkRequest, control, &result);
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
    if (!status.ok() || remote.layout.chunk_id.empty() || (!requested.empty() && remote.layout.chunk_id != requested) ||
        remote.owner_epoch != owner_epoch || remote.layout.logical_capacity < initial_capacity ||
        chunk_purpose(remote.layout.chunk_id) != purpose) {
        return status.ok() ? Status::corruption("ChunkDB allocation metadata mismatch") : status;
    }
    remote.maximum_capacity       = logical_capacity;
    remote.valid_until_ms         = std::numeric_limits<uint64_t>::max();
    remote.self_fence_deadline_ms = monotonic_millis() + kLivenessSelfFenceMs;
    *chunk_id                     = remote.layout.chunk_id;
    impl_->cache(std::move(remote));
    return Status::Ok();
}

} // namespace crowdb::tree::detail
