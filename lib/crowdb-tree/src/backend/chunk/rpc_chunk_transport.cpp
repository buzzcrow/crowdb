// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{

struct RpcChunkTransport::Impl::AsyncWrite
{
    Impl                               *owner = nullptr;
    RemoteChunk                         chunk;
    uint32_t                            mirror_index = 0;
    uint64_t                            offset       = 0;
    const uint8_t                      *data         = nullptr;
    size_t                              length       = 0;
    size_t                              consumed     = 0;
    ChunkTransportCompletion            completion;
    std::unique_ptr<RpcChunkRouteLease> disk_lease;

    static void rpc_complete(uint64_t /*unused*/, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data_buffer,
                             crowdb_rpc_status rpc_status, void *context)
    {
        auto     *self = static_cast<AsyncWrite *>(context);
        RpcResult result;
        result.status  = rpc_status;
        result.control = control;
        result.data    = data_buffer;
        Status status  = rpc_status == CROWDB_RPC_OK ? Status::Ok() : Status::unavailable("chunk RPC call failed");
        if (status.ok()) {
            const auto *response = verified_response<crowdb::diskio::proto::FBDiskWriteResponse>(result.control);
            status               = response == nullptr ? Status::corruption("DiskIO write response is malformed")
                                                       : diskio_status(response->ret_code());
        }
        if (!status.ok()) {
            finish(self, std::move(status));
            return;
        }
        self->submit_part();
    }

    static void finish(AsyncWrite *self, Status status)
    {
        const ChunkTransportCompletion completion = self->completion;
        delete self;
        completion.complete(std::move(status));
    }

    void submit_part()
    {
        if (completion.stop_requested()) {
            finish(this, Status::unavailable("chunk RPC write stopped before transport submission"));
            return;
        }
        if (consumed == length) {
            finish(this, Status::Ok());
            return;
        }
        const uint64_t cursor = offset + consumed;
        const auto     strip  = std::find_if(chunk.strips.begin(), chunk.strips.end(), [cursor](const Strip &item) {
            return cursor >= item.chunk_offset && cursor < item.chunk_offset + item.capacity;
        });
        if (strip == chunk.strips.end()) {
            finish(this, Status::resource_exhausted("tree chunk RPC write exceeds allocated strips"));
            return;
        }
        if (mirror_index >= strip->mirrors.size()) {
            finish(this, Status::invalid_argument("tree chunk mirror index exceeds layout"));
            return;
        }
        const size_t part    = std::min<uint64_t>(length - consumed, strip->chunk_offset + strip->capacity - cursor);
        const auto  &segment = strip->mirrors[mirror_index];
        disk_lease           = std::make_unique<RpcChunkRouteLease>();
        Status      status   = owner->resolve_route(segment, disk_lease.get());
        const auto &route    = disk_lease->route;
        if (!status.ok()) {
            finish(this, std::move(status));
            return;
        }
        const uint64_t unit_bytes  = static_cast<uint64_t>(strip->unit_kb) * 1024;
        const uint64_t zone_offset = (segment.unit_offset * unit_bytes) + (cursor - strip->chunk_offset);
        const uint64_t request_id  = owner->next_request_id();
        const FBInt128 disk_id(segment.disk_high, segment.disk_low);
        flatbuffers::FlatBufferBuilder builder;
        auto                           request = crowdb::diskio::proto::CreateFBDiskWriteRequest(
            builder, request_id, monotonic_nanos(), &disk_id, segment.zone_index, zone_offset,
            static_cast<uint32_t>(part), zone_offset, wall_time_ms());
        builder.Finish(request);
        crowdb_rpc_buffer_t control = crowdb_rpc_buffer_create(builder.GetBufferPointer(), builder.GetSize());
        crowdb_rpc_buffer_t payload = crowdb_rpc_buffer_create(data + consumed, static_cast<uint32_t>(part));
        if (control == nullptr || payload == nullptr) {
            if (control != nullptr) {
                crowdb_rpc_buffer_release(control);
            }
            if (payload != nullptr) {
                crowdb_rpc_buffer_release(payload);
            }
            finish(this, Status::resource_exhausted("chunk RPC buffer allocation failed"));
            return;
        }
        consumed += part;
        const crowdb_rpc_status submit = crowdb_rpc_client_send(
            reinterpret_cast<crowdb_rpc_client_t>(route.client), reinterpret_cast<crowdb_rpc_server_t>(route.server),
            reinterpret_cast<crowdb_rpc_conn_t>(route.connection), request_id, control, payload,
            crowdb::rpc::proto::FBMsgType_EDiskWriteRequest, &AsyncWrite::rpc_complete, this);
        if (submit != CROWDB_RPC_OK) {
            finish(this, submit == CROWDB_RPC_ERR_SEND_QUEUE
                             ? Status::resource_exhausted("chunk RPC submission queue is full")
                             : Status::unavailable("chunk RPC submission failed"));
        }
    }
};

void RpcChunkTransport::Impl::submit_write(RemoteChunk chunk, uint32_t mirror_index, uint64_t offset,
                                           const uint8_t *data, size_t length, ChunkTransportCompletion completion)
{
    auto *state = new (std::nothrow) AsyncWrite{
        .owner        = this,
        .chunk        = std::move(chunk),
        .mirror_index = mirror_index,
        .offset       = offset,
        .data         = data,
        .length       = length,
        .consumed     = 0,
        .completion   = completion,
        .disk_lease   = nullptr,
    };
    if (state == nullptr) {
        completion.complete(Status::resource_exhausted("chunk RPC write state allocation failed"));
        return;
    }
    state->submit_part();
}

RpcChunkTransport::RpcChunkTransport(const ct_chunk_rpc_transport_options &options)
    : impl_(std::make_unique<Impl>(options))
{
}

RpcChunkTransport::~RpcChunkTransport() = default;

bool RpcChunkTransport::valid() const
{
    return impl_->valid();
}

Status RpcChunkTransport::write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                                       size_t length)
{
    if ((data == nullptr && length != 0)) {
        return Status::invalid_argument("tree chunk RPC mirror write arguments are invalid");
    }
    Impl::RemoteChunk chunk;
    if (!impl_->cached(chunk_id, &chunk)) {
        return Status::unavailable("tree chunk write has no local liveness authority");
    }
    if (chunk.self_fence_deadline_ms != 0 && monotonic_millis() >= chunk.self_fence_deadline_ms) {
        return Status::unavailable("tree chunk liveness authority has self-fenced");
    }
    size_t consumed = 0;
    while (consumed < length) {
        const uint64_t cursor = offset + consumed;
        const auto strip = std::find_if(chunk.strips.begin(), chunk.strips.end(), [cursor](const Impl::Strip &item) {
            return cursor >= item.chunk_offset && cursor < item.chunk_offset + item.capacity;
        });
        if (strip == chunk.strips.end()) {
            return Status::resource_exhausted("tree chunk RPC write exceeds allocated strips");
        }
        if (mirror_index >= strip->mirrors.size()) {
            return Status::invalid_argument("tree chunk mirror index exceeds layout");
        }
        const size_t       part = std::min<uint64_t>(length - consumed, strip->chunk_offset + strip->capacity - cursor);
        const auto        &segment = strip->mirrors[mirror_index];
        RpcChunkRouteLease lease;
        Status             status = impl_->resolve_route(segment, &lease);
        const auto        &route  = lease.route;
        if (!status.ok()) {
            return status;
        }
        const uint64_t unit_bytes  = static_cast<uint64_t>(strip->unit_kb) * 1024;
        const uint64_t zone_offset = (segment.unit_offset * unit_bytes) + (cursor - strip->chunk_offset);
        const uint64_t request_id  = impl_->next_request_id();
        const FBInt128 disk_id(segment.disk_high, segment.disk_low);
        flatbuffers::FlatBufferBuilder builder;
        auto                           request = crowdb::diskio::proto::CreateFBDiskWriteRequest(
            builder, request_id, monotonic_nanos(), &disk_id, segment.zone_index, zone_offset,
            static_cast<uint32_t>(part), zone_offset, wall_time_ms());
        builder.Finish(request);
        std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
        RpcResult            result;
        status = call_rpc(route, request_id, crowdb::rpc::proto::FBMsgType_EDiskWriteRequest, control, data + consumed,
                          part, &result);
        if (!status.ok()) {
            return status;
        }
        const auto *response = verified_response<crowdb::diskio::proto::FBDiskWriteResponse>(result.control);
        if (response == nullptr) {
            return Status::corruption("DiskIO write response is malformed");
        }
        status = diskio_status(response->ret_code());
        if (!status.ok()) {
            return status;
        }
        consumed += part;
    }
    return Status::Ok();
}

void RpcChunkTransport::submit_write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset,
                                            const uint8_t *data, size_t length, ChunkTransportCompletion completion)
{
    if ((data == nullptr && length != 0)) {
        completion.complete(Status::invalid_argument("tree chunk RPC mirror write arguments are invalid"));
        return;
    }
    Impl::RemoteChunk chunk;
    if (!impl_->cached(chunk_id, &chunk)) {
        completion.complete(Status::unavailable("tree chunk write has no local liveness authority"));
        return;
    }
    if (chunk.self_fence_deadline_ms != 0 && monotonic_millis() >= chunk.self_fence_deadline_ms) {
        completion.complete(Status::unavailable("tree chunk liveness authority has self-fenced"));
        return;
    }
    impl_->submit_write(std::move(chunk), mirror_index, offset, data, length, completion);
}

Status RpcChunkTransport::advance_write(ChunkId chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes)
{
    Impl::RemoteChunk chunk;
    if (!impl_->cached(chunk_id, &chunk)) {
        return Status::unavailable("tree chunk advance has no local liveness authority");
    }
    if (chunk.self_fence_deadline_ms != 0 && monotonic_millis() >= chunk.self_fence_deadline_ms) {
        return Status::unavailable("tree chunk liveness authority has self-fenced");
    }
    if (chunk.layout.acknowledged_bytes != expected_bytes) {
        const uint64_t self_fence_deadline_ms = chunk.self_fence_deadline_ms;
        Status         status                 = impl_->query_remote(chunk_id, &chunk);
        if (!status.ok()) {
            return status;
        }
        chunk.self_fence_deadline_ms = self_fence_deadline_ms;
    }
    const uint64_t                 request_id = impl_->next_request_id();
    const FBInt128                 id(chunk_id.high, chunk_id.low);
    flatbuffers::FlatBufferBuilder builder;
    auto                           request = crowdb::chunkdb::proto::CreateFBAdvanceChunkWriteRequest(
        builder, request_id, monotonic_nanos(), &id, chunk.owner_epoch, chunk.modify_ts, acknowledged_bytes,
        std::numeric_limits<uint32_t>::max(), impl_->options.writer_lease_ms);
    builder.Finish(request);
    std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    RpcResult            result;
    Status               status = impl_->call_chunkdb<crowdb::chunkdb::proto::FBAdvanceChunkWriteResponse>(
        chunk_id, request_id, crowdb::rpc::proto::FBMsgType_EAdvanceChunkWriteRequest, control, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<crowdb::chunkdb::proto::FBAdvanceChunkWriteResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB advance response is malformed");
    }
    status = chunkdb_status(response->ret_code(), response->error_msg());
    Impl::RemoteChunk updated;
    if (status.ok()) {
        status = crowdb::tree::detail::RpcChunkTransport::Impl::parse_chunk(response->chunk(), &updated);
    }
    if (status.ok()) {
        impl_->cache(std::move(updated));
    }
    return status;
}

Status RpcChunkTransport::read_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data,
                                      size_t length) const
{
    if ((data == nullptr && length != 0)) {
        return Status::invalid_argument("tree chunk RPC mirror read arguments are invalid");
    }
    Impl::RemoteChunk chunk;
    Status            status = Status::Ok();
    if (!impl_->cached_valid(chunk_id, &chunk)) {
        status = impl_->query_remote(chunk_id, &chunk);
        if (!status.ok()) {
            return status;
        }
    }
    if (offset > chunk.layout.acknowledged_bytes || length > chunk.layout.acknowledged_bytes - offset) {
        return Status::unavailable("tree chunk RPC read exceeds acknowledged cursor");
    }
    size_t consumed = 0;
    while (consumed < length) {
        const uint64_t cursor = offset + consumed;
        const auto strip = std::find_if(chunk.strips.begin(), chunk.strips.end(), [cursor](const Impl::Strip &item) {
            return cursor >= item.chunk_offset && cursor < item.chunk_offset + item.capacity;
        });
        if (strip == chunk.strips.end()) {
            return Status::corruption("tree chunk RPC read has a layout gap");
        }
        if (mirror_index >= strip->mirrors.size()) {
            return Status::invalid_argument("tree chunk mirror index exceeds layout");
        }
        const size_t       part = std::min<uint64_t>(length - consumed, strip->chunk_offset + strip->capacity - cursor);
        const auto        &segment = strip->mirrors[mirror_index];
        RpcChunkRouteLease lease;
        status            = impl_->resolve_route(segment, &lease);
        const auto &route = lease.route;
        if (!status.ok()) {
            return status;
        }
        const uint64_t unit_bytes  = static_cast<uint64_t>(strip->unit_kb) * 1024;
        const uint64_t zone_offset = (segment.unit_offset * unit_bytes) + (cursor - strip->chunk_offset);
        const uint64_t request_id  = impl_->next_request_id();
        const FBInt128 disk_id(segment.disk_high, segment.disk_low);
        flatbuffers::FlatBufferBuilder builder;
        auto request = crowdb::diskio::proto::CreateFBDiskReadRequest(builder, request_id, monotonic_nanos(), &disk_id,
                                                                      segment.zone_index, zone_offset,
                                                                      static_cast<uint32_t>(part), 0);
        builder.Finish(request);
        std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
        RpcResult            result;
        status =
            call_rpc(route, request_id, crowdb::rpc::proto::FBMsgType_EDiskReadRequest, control, nullptr, 0, &result);
        if (!status.ok()) {
            return status;
        }
        const auto *response = verified_response<crowdb::diskio::proto::FBDiskReadResponse>(result.control);
        if (response == nullptr) {
            return Status::corruption("DiskIO read response is malformed");
        }
        status = diskio_status(response->ret_code());
        if (!status.ok()) {
            return status;
        }
        if (result.data == nullptr || crowdb_rpc_buffer_len(result.data) != part) {
            return Status::corruption("DiskIO read response is malformed");
        }
        std::memcpy(data + consumed, crowdb_rpc_buffer_data(result.data), part);
        consumed += part;
    }
    return Status::Ok();
}

Status RpcChunkTransport::query_chunk(ChunkId chunk_id, ChunkLayout *layout) const
{
    if (layout == nullptr) {
        return Status::invalid_argument("tree chunk RPC layout output is null");
    }
    Impl::RemoteChunk chunk;
    Status            status = impl_->query_remote(chunk_id, &chunk);
    if (status.ok()) {
        *layout = chunk.layout;
    }
    return status;
}

Status RpcChunkTransport::seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes)
{
    Impl::RemoteChunk chunk;
    Status            status = impl_->query_remote(chunk_id, &chunk);
    if (!status.ok()) {
        return status;
    }
    if (chunk.owner_epoch != owner_epoch || chunk.layout.acknowledged_bytes != acknowledged_bytes ||
        acknowledged_bytes > chunk.layout.logical_capacity) {
        return Status::unavailable("tree chunk RPC seal is fenced by owner epoch or cursor");
    }
    const uint64_t                 request_id = impl_->next_request_id();
    const FBInt128                 id(chunk_id.high, chunk_id.low);
    flatbuffers::FlatBufferBuilder builder;
    auto request = crowdb::chunkdb::proto::CreateFBSealChunkRequest(builder, request_id, monotonic_nanos(), &id,
                                                                    acknowledged_bytes);
    builder.Finish(request);
    std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    RpcResult            result;
    status = impl_->call_chunkdb<crowdb::chunkdb::proto::FBSealChunkResponse>(
        chunk_id, request_id, crowdb::rpc::proto::FBMsgType_ESealChunkRequest, control, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<crowdb::chunkdb::proto::FBSealChunkResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB seal response is malformed");
    }
    status = chunkdb_status(response->ret_code(), response->error_msg());
    Impl::RemoteChunk updated;
    if (status.ok()) {
        status = crowdb::tree::detail::RpcChunkTransport::Impl::parse_chunk(response->chunk(), &updated);
    }
    if (status.ok() && (updated.layout.chunk_id != chunk_id || updated.owner_epoch != owner_epoch ||
                        updated.layout.acknowledged_bytes != acknowledged_bytes || !updated.layout.sealed)) {
        return Status::corruption("ChunkDB seal response does not match the exact sealed boundary");
    }
    if (status.ok()) {
        updated.valid_until_ms = std::numeric_limits<uint64_t>::max();
        impl_->cache(std::move(updated));
    }
    return status;
}

} // namespace crowdb::tree::detail

ct_status ct_rpc_chunk_transport_open(const ct_chunk_rpc_transport_options *options, ct_chunk_transport **out)
{
    if (options == nullptr || out == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto transport = std::make_shared<crowdb::tree::detail::RpcChunkTransport>(*options);
    if (!transport->valid()) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto handle       = std::make_unique<ct_chunk_transport>();
    handle->transport = std::move(transport);
    *out              = handle.release();
    return static_cast<ct_status>(crowdb::tree::Code::kOk);
}

void ct_chunk_transport_free(ct_chunk_transport *transport)
{
    delete transport;
}
