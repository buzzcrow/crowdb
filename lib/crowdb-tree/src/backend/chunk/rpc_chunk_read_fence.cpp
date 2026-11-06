// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
Status RpcChunkTransport::verify_fixed_read_fence(ChunkId id, uint32_t mirrors, ChunkIoContext context) const
{
    if (context.created_ms == 0 || mirrors == 0 || mirrors > kMaxMirrorCopies) {
        return Status::invalid_argument();
    }
    Impl::RemoteChunk chunk;
    if (!impl_->cached_valid(id, &chunk)) {
        auto status = impl_->query_remote(id, &chunk);
        if (!status.ok()) {
            return status;
        }
    }
    for (const auto &strip : chunk.strips) {
        if (strip.mirrors.size() < mirrors) {
            return Status::invalid_argument();
        }
        for (uint32_t index = 0; index < mirrors; ++index) {
            const auto        &segment = strip.mirrors[index];
            RpcChunkRouteLease lease;
            auto               status = impl_->resolve_route(segment, &lease);
            if (!status.ok()) {
                return status;
            }
            const auto                     request_id = impl_->next_request_id();
            const FBInt128                 disk(segment.disk_high, segment.disk_low);
            flatbuffers::FlatBufferBuilder builder;
            builder.Finish(crowdb::diskio::proto::CreateFBDiskReadRequest(
                builder, request_id, monotonic_nanos(), &disk, segment.zone_index, 0, 0, 0, context.created_ms, true));
            std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
            RpcResult            result;
            status = call_rpc(lease.route, request_id, crowdb::rpc::proto::FBMsgType_EDiskReadRequest, control, nullptr,
                              0, &result);
            if (!status.ok()) {
                return status;
            }
            const auto *response = verified_response<crowdb::diskio::proto::FBDiskReadResponse>(result.control);
            if (response == nullptr) {
                return Status::corruption("DiskIO fence response is malformed");
            }
            status = diskio_status(response->ret_code());
            if (!status.ok()) {
                return status;
            }
            if (!response->request_age_checked()) {
                return Status::not_supported("DiskIO cannot fence fixed-pack reads");
            }
        }
    }
    return Status::Ok();
}
} // namespace crowdb::tree::detail
