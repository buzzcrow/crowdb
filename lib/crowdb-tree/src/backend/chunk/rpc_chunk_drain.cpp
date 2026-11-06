// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
namespace
{
template <class Segment>
Status drain_segment(const Segment &segment, const RpcChunkRouteLease &lease, uint64_t request_id, uint64_t offset)
{
    const FBInt128                 disk(segment.disk_high, segment.disk_low);
    flatbuffers::FlatBufferBuilder builder;
    builder.Finish(crowdb::diskio::proto::CreateFBDiskWriteRequest(
        builder, request_id, monotonic_nanos(), &disk, segment.zone_index, offset, 0, offset, wall_time_ms(), true));
    std::vector<uint8_t> request(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    RpcResult            drained;
    auto status = call_rpc(lease.route, request_id, crowdb::rpc::proto::FBMsgType_EDiskWriteRequest, request, nullptr,
                           0, &drained);
    if (!status.ok()) {
        return status;
    }
    const auto *receipt = verified_response<crowdb::diskio::proto::FBDiskWriteResponse>(drained.control);
    if (receipt == nullptr) {
        return Status::corruption("DiskIO drain response is malformed");
    }
    status = diskio_status(receipt->ret_code());
    if (!status.ok()) {
        return status;
    }
    if (!receipt->sync_completed()) {
        return Status::not_supported("DiskIO cannot confirm ordered drain");
    }
    return Status::Ok();
}
} // namespace

Status RpcChunkTransport::drain_fixed_chunk(ChunkId id) const
{
    using namespace crowdb::chunkdb::proto;
    if (!valid() || id.empty() || !valid_page_purpose(chunk_purpose(id))) {
        return Status::invalid_argument();
    }
    const auto                     query_id = impl_->next_request_id();
    const FBInt128                 wire_id(id.high, id.low);
    flatbuffers::FlatBufferBuilder query;
    query.Finish(CreateFBQueryChunkRequest(query, query_id, monotonic_nanos(), &wire_id));
    std::vector<uint8_t> control(query.GetBufferPointer(), query.GetBufferPointer() + query.GetSize());
    RpcResult            result;
    auto                 status = impl_->call_chunkdb<FBQueryChunkResponse>(
        id, query_id, crowdb::rpc::proto::FBMsgType_EQueryChunkRequest, control, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<FBQueryChunkResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB drain query is malformed");
    }
    status = chunkdb_status(response->ret_code(), response->error_msg());
    if (status.code() == Code::kNotFound) {
        return Status::Ok();
    }
    if (!status.ok()) {
        return status;
    }
    const auto *chunk = response->chunk();
    if (chunk == nullptr || chunk->id() == nullptr || chunk->id()->high() != id.high || chunk->id()->low() != id.low) {
        return Status::corruption("ChunkDB drain identity differs");
    }
    if (chunk->strips() == nullptr) {
        return chunk->capacity() == 0 ? Status::Ok() : Status::corruption("ChunkDB drain layout is absent");
    }
    for (const auto *strip : *chunk->strips()) {
        const auto *mirror = strip == nullptr ? nullptr : strip->strip_body_as_FBMirrorStrip();
        if (strip == nullptr || strip->strip_type() != FBStripType_Mirror || strip->unit_kb() == 0 ||
            mirror == nullptr || mirror->segments() == nullptr) {
            return Status::corruption("ChunkDB drain layout is invalid");
        }
        for (const auto *segment : *mirror->segments()) {
            const uint64_t unit_bytes = static_cast<uint64_t>(strip->unit_kb()) * 1024;
            if (unit_bytes == 0 || segment == nullptr || segment->unit_offset() > UINT64_MAX / unit_bytes) {
                return Status::corruption();
            }
            const uint64_t     offset = segment->unit_offset() * unit_bytes;
            Impl::Segment      route_segment{.disk_high   = segment->disk_id().high(),
                                             .disk_low    = segment->disk_id().low(),
                                             .unit_offset = segment->unit_offset(),
                                             .zone_index  = segment->zone_index()};
            RpcChunkRouteLease lease;
            status = impl_->resolve_route(route_segment, &lease);
            if (!status.ok()) {
                return status;
            }
            status = drain_segment(route_segment, lease, impl_->next_request_id(), offset);
            if (!status.ok()) {
                return status;
            }
        }
    }
    return Status::Ok();
}
} // namespace crowdb::tree::detail
