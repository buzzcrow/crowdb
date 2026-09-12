// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc_chunk_transport.h"

#include "chunk_c_api_internal.h"
#include "chunkdb_generated.h"
#include "crowdb-rpc/c_api.h"
#include "diskio_generated.h"
#include "msg_type_generated.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <utility>
#include <vector>

namespace crowdb::tree::detail
{
namespace
{

using crowdb::chunkdb::proto::FBChunk;
using crowdb::chunkdb::proto::FBChunkdbRetCode;
using crowdb::chunkdb::proto::FBChunkdbRetCode_Success;
using crowdb::chunkdb::proto::FBChunkState_Sealed;
using crowdb::chunkdb::proto::FBChunkType_BtreePage;
using crowdb::chunkdb::proto::FBStripType_Mirror;
using crowdb::rpc::proto::FBInt128;

struct RpcResult
{
    crowdb_rpc_status   status  = CROWDB_RPC_ERR_CONN_ERROR;
    crowdb_rpc_buffer_t control = nullptr;
    crowdb_rpc_buffer_t data    = nullptr;

    RpcResult()                             = default;
    RpcResult(const RpcResult &)            = delete;
    RpcResult &operator=(const RpcResult &) = delete;

    RpcResult(RpcResult &&other) noexcept
        : status(other.status),
          control(std::exchange(other.control, nullptr)),
          data(std::exchange(other.data, nullptr))
    {
    }

    RpcResult &operator=(RpcResult &&other) noexcept
    {
        if (this != &other) {
            release();
            status  = other.status;
            control = std::exchange(other.control, nullptr);
            data    = std::exchange(other.data, nullptr);
        }
        return *this;
    }

    ~RpcResult()
    {
        release();
    }

    void release()
    {
        if (control != nullptr) {
            crowdb_rpc_buffer_release(control);
            control = nullptr;
        }
        if (data != nullptr) {
            crowdb_rpc_buffer_release(data);
            data = nullptr;
        }
    }
};

struct RpcCallState
{
    std::atomic<bool> done{false};
    RpcResult         result;
};

uint64_t monotonic_nanos()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

uint64_t monotonic_millis()
{
    return monotonic_nanos() / 1'000'000;
}

void rpc_complete(uint64_t /*unused*/, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data, crowdb_rpc_status status,
                  void *context)
{
    auto *state           = static_cast<RpcCallState *>(context);
    state->result.status  = status;
    state->result.control = control;
    state->result.data    = data;
    state->done.store(true, std::memory_order_release);
    state->done.notify_one();
}

Status call_rpc(const ct_chunk_rpc_route &route, uint64_t request_id, uint16_t message_type,
                const std::vector<uint8_t> &control, const uint8_t *data, size_t data_length, RpcResult *out)
{
    if (route.client == nullptr || route.server == nullptr || route.connection == nullptr || out == nullptr ||
        data_length > std::numeric_limits<uint32_t>::max()) {
        return Status::invalid_argument("chunk RPC route or payload is invalid");
    }
    crowdb_rpc_buffer_t control_buffer = crowdb_rpc_buffer_create(control.data(), control.size());
    crowdb_rpc_buffer_t data_buffer =
        data_length == 0 ? nullptr : crowdb_rpc_buffer_create(data, static_cast<uint32_t>(data_length));
    if (control_buffer == nullptr || (data_length != 0 && data_buffer == nullptr)) {
        if (control_buffer != nullptr) {
            crowdb_rpc_buffer_release(control_buffer);
        }
        if (data_buffer != nullptr) {
            crowdb_rpc_buffer_release(data_buffer);
        }
        return Status::resource_exhausted("chunk RPC buffer allocation failed");
    }

    RpcCallState state;
    const auto   submit = crowdb_rpc_client_send_slab(reinterpret_cast<crowdb_rpc_client_t>(route.client),
                                                      reinterpret_cast<crowdb_rpc_server_t>(route.server),
                                                      reinterpret_cast<crowdb_rpc_conn_t>(route.connection), request_id,
                                                      control_buffer, data_buffer, message_type, &rpc_complete, &state);
    if (submit != CROWDB_RPC_OK) {
        return submit == CROWDB_RPC_ERR_SEND_QUEUE ? Status::resource_exhausted("chunk RPC completion slab is full")
                                                   : Status::unavailable("chunk RPC submission failed");
    }
    while (!state.done.load(std::memory_order_acquire)) {
        state.done.wait(false, std::memory_order_acquire);
    }
    *out = std::move(state.result);
    return out->status == CROWDB_RPC_OK ? Status::Ok() : Status::unavailable("chunk RPC call failed");
}

template <typename Response> const Response *verified_response(crowdb_rpc_buffer_t buffer)
{
    if (buffer == nullptr) {
        return nullptr;
    }
    const auto           *bytes  = crowdb_rpc_buffer_data(buffer);
    const auto            length = crowdb_rpc_buffer_len(buffer);
    flatbuffers::Verifier verifier(bytes, length);
    return verifier.VerifyBuffer<Response>(nullptr) ? flatbuffers::GetRoot<Response>(bytes) : nullptr;
}

Status chunkdb_status(FBChunkdbRetCode code)
{
    using namespace crowdb::chunkdb::proto;
    switch (code) {
    case FBChunkdbRetCode_Success:
        return Status::Ok();
    case FBChunkdbRetCode_InvalidArgument:
    case FBChunkdbRetCode_FailedPrecondition:
    case FBChunkdbRetCode_StripIndexOutOfRange:
        return Status::invalid_argument("ChunkDB rejected the tree chunk request");
    case FBChunkdbRetCode_Unavailable:
    case FBChunkdbRetCode_NotMyRange:
    case FBChunkdbRetCode_Aborted:
        return Status::unavailable("ChunkDB could not complete the tree chunk request");
    default:
        return Status::internal_error("ChunkDB returned an unexpected tree chunk result");
    }
}

Status diskio_status(crowdb::diskio::proto::FBDiskIoRetCode code)
{
    using namespace crowdb::diskio::proto;
    switch (code) {
    case FBDiskIoRetCode_Success:
        return Status::Ok();
    case FBDiskIoRetCode_InvalidAlignment:
        return Status::invalid_argument("DiskIO rejected tree page alignment");
    case FBDiskIoRetCode_ConnectionError:
    case FBDiskIoRetCode_DiskNotExist:
    case FBDiskIoRetCode_ZoneNotExist:
        return Status::unavailable("DiskIO tree page target is unavailable");
    default:
        return Status::internal_error("DiskIO tree page operation failed");
    }
}

} // namespace

struct RpcChunkTransport::Impl
{
    struct AsyncWrite;

    struct Segment
    {
        uint64_t disk_high   = 0;
        uint64_t disk_low    = 0;
        uint64_t unit_offset = 0;
        uint32_t zone_index  = 0;
    };

    struct Strip
    {
        uint64_t               chunk_offset = 0;
        uint64_t               capacity     = 0;
        uint32_t               unit_kb      = 0;
        std::array<Segment, 3> mirrors;
    };

    struct RemoteChunk
    {
        ChunkLayout        layout;
        uint64_t           owner_epoch    = 0;
        uint64_t           modify_ts      = 0;
        uint64_t           valid_until_ms = 0;
        std::vector<Strip> strips;
    };

    using RemoteChunks = std::vector<RemoteChunk>;

    explicit Impl(const ct_chunk_rpc_transport_options &configured) : options(configured)
    {
        if (configured.disk_routes != nullptr) {
            disk_routes.assign(configured.disk_routes, configured.disk_routes + configured.disk_route_count);
        }
        options.disk_routes                = nullptr;
        options.disk_route_count           = 0;
        const uint32_t completion_capacity = configured.completion_capacity == 0 ? 256 : configured.completion_capacity;
        const uint64_t timeout_ms          = configured.rpc_timeout_ms == 0 ? 30'000 : configured.rpc_timeout_ms;
        if (configured.chunkdb.client != nullptr) {
            crowdb_rpc_client_set_completion_pool_size(reinterpret_cast<crowdb_rpc_client_t>(configured.chunkdb.client),
                                                       completion_capacity);
            crowdb_rpc_client_start_reaper(reinterpret_cast<crowdb_rpc_client_t>(configured.chunkdb.client),
                                           timeout_ms * 1'000'000, 100'000'000);
            for (const auto &disk : disk_routes) {
                if (disk.route.client != nullptr) {
                    crowdb_rpc_client_set_completion_pool_size(reinterpret_cast<crowdb_rpc_client_t>(disk.route.client),
                                                               completion_capacity);
                    crowdb_rpc_client_start_reaper(reinterpret_cast<crowdb_rpc_client_t>(disk.route.client),
                                                   timeout_ms * 1'000'000, 100'000'000);
                }
            }
        }
    }

    [[nodiscard]] bool valid() const
    {
        return options.chunkdb.client != nullptr && options.chunkdb.server != nullptr &&
               options.chunkdb.connection != nullptr && !disk_routes.empty();
    }

    uint64_t next_request_id() const
    {
        return request_ids.fetch_add(1, std::memory_order_relaxed);
    }

    Status resolve_route(const Segment &segment, ct_chunk_rpc_route *route) const
    {
        auto found = std::find_if(disk_routes.begin(), disk_routes.end(), [&segment](const auto &item) {
            return item.disk_id_high == segment.disk_high && item.disk_id_low == segment.disk_low;
        });
        if (found == disk_routes.end() || found->route.client == nullptr || found->route.server == nullptr ||
            found->route.connection == nullptr) {
            return Status::unavailable("DiskIO route is unavailable");
        }
        *route = found->route;
        return Status::Ok();
    }

    static Status parse_chunk(const FBChunk *chunk, RemoteChunk *out)
    {
        if (chunk == nullptr || chunk->id() == nullptr || chunk->strips() == nullptr) {
            return Status::corruption("ChunkDB returned malformed tree chunk metadata");
        }
        RemoteChunk parsed;
        parsed.layout = {
            .chunk_id           = ChunkId(chunk->id()->high(), chunk->id()->low()),
            .logical_capacity   = chunk->capacity(),
            .acknowledged_bytes = chunk->acknowledged_cursor(),
            .sealed             = chunk->state() == FBChunkState_Sealed,
        };
        parsed.owner_epoch = chunk->writer_epoch();
        parsed.modify_ts   = chunk->modify_ts();
        for (const auto *wire_strip : *chunk->strips()) {
            const auto *mirror = wire_strip == nullptr ? nullptr : wire_strip->strip_body_as_FBMirrorStrip();
            if (wire_strip == nullptr || wire_strip->strip_type() != FBStripType_Mirror || mirror == nullptr ||
                mirror->segments() == nullptr || mirror->segments()->size() != 3 || wire_strip->unit_kb() == 0) {
                return Status::corruption("ChunkDB returned a non-mirror tree chunk layout");
            }
            Strip strip{
                .chunk_offset = wire_strip->chunk_offset(),
                .capacity     = wire_strip->capacity(),
                .unit_kb      = wire_strip->unit_kb(),
                .mirrors      = {},
            };
            for (size_t index = 0; index < strip.mirrors.size(); ++index) {
                const auto *segment  = mirror->segments()->Get(index);
                strip.mirrors[index] = {
                    .disk_high   = segment->disk_id().high(),
                    .disk_low    = segment->disk_id().low(),
                    .unit_offset = segment->unit_offset(),
                    .zone_index  = segment->zone_index(),
                };
            }
            parsed.strips.push_back(strip);
        }
        if (parsed.strips.empty()) {
            return Status::corruption("ChunkDB returned an empty tree chunk layout");
        }
        *out = std::move(parsed);
        return Status::Ok();
    }

    void cache(RemoteChunk chunk) const
    {
        auto current = chunks.load(std::memory_order_acquire);
        for (;;) {
            auto next =
                current == nullptr ? std::make_shared<RemoteChunks>() : std::make_shared<RemoteChunks>(*current);
            auto found =
                std::find_if(next->begin(), next->end(), [id = chunk.layout.chunk_id](const RemoteChunk &item) {
                    return item.layout.chunk_id == id;
                });
            if (found == next->end()) {
                next->push_back(chunk);
            }
            else {
                *found = chunk;
            }
            if (chunks.compare_exchange_weak(current, next, std::memory_order_release, std::memory_order_acquire)) {
                return;
            }
        }
    }

    [[nodiscard]] bool cached(ChunkId chunk_id, RemoteChunk *out) const
    {
        auto current = chunks.load(std::memory_order_acquire);
        if (current == nullptr) {
            return false;
        }
        auto found = std::find_if(current->begin(), current->end(),
                                  [chunk_id](const RemoteChunk &item) { return item.layout.chunk_id == chunk_id; });
        if (found == current->end()) {
            return false;
        }
        *out = *found;
        return true;
    }

    [[nodiscard]] bool cached_valid(ChunkId chunk_id, RemoteChunk *out) const
    {
        return cached(chunk_id, out) && monotonic_millis() < out->valid_until_ms;
    }

    void submit_write(RemoteChunk chunk, uint32_t mirror_index, uint64_t offset, const uint8_t *data, size_t length,
                      ChunkTransportCompletion completion);

    Status query_remote(ChunkId chunk_id, RemoteChunk *out) const
    {
        flatbuffers::FlatBufferBuilder builder;
        const uint64_t                 request_id = next_request_id();
        const FBInt128                 id(chunk_id.high, chunk_id.low);
        auto request = crowdb::chunkdb::proto::CreateFBQueryChunkRequest(builder, request_id, monotonic_nanos(), &id);
        builder.Finish(request);
        std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
        RpcResult            result;
        Status status = call_rpc(options.chunkdb, request_id, crowdb::rpc::proto::FBMsgType_EQueryChunkRequest, control,
                                 nullptr, 0, &result);
        if (!status.ok()) {
            return status;
        }
        const auto *response = verified_response<crowdb::chunkdb::proto::FBQueryChunkResponse>(result.control);
        if (response == nullptr) {
            return Status::corruption("ChunkDB query response is malformed");
        }
        status = chunkdb_status(response->ret_code());
        if (!status.ok()) {
            return status;
        }
        status = parse_chunk(response->chunk(), out);
        if (status.ok()) {
            const uint64_t now  = monotonic_millis();
            out->valid_until_ms = response->layout_validity_ms() > std::numeric_limits<uint64_t>::max() - now
                                    ? std::numeric_limits<uint64_t>::max()
                                    : now + response->layout_validity_ms();
            cache(*out);
        }
        return status;
    }

    ct_chunk_rpc_transport_options                           options;
    std::vector<ct_chunk_rpc_disk_route>                     disk_routes;
    mutable std::atomic<uint64_t>                            request_ids{1};
    mutable std::atomic<std::shared_ptr<const RemoteChunks>> chunks;
};

struct RpcChunkTransport::Impl::AsyncWrite
{
    Impl                    *owner = nullptr;
    RemoteChunk              chunk;
    uint32_t                 mirror_index = 0;
    uint64_t                 offset       = 0;
    const uint8_t           *data         = nullptr;
    size_t                   length       = 0;
    size_t                   consumed     = 0;
    ChunkTransportCompletion completion;

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
        const size_t       part = std::min<uint64_t>(length - consumed, strip->chunk_offset + strip->capacity - cursor);
        const auto        &segment = strip->mirrors[mirror_index];
        ct_chunk_rpc_route route{};
        Status             status = owner->resolve_route(segment, &route);
        if (!status.ok()) {
            finish(this, std::move(status));
            return;
        }
        const uint64_t                 unit_bytes  = static_cast<uint64_t>(strip->unit_kb) * 1024;
        const uint64_t zone_offset = (segment.unit_offset * unit_bytes) + (cursor - strip->chunk_offset);
        const uint64_t                 request_id  = owner->next_request_id();
        const FBInt128                 disk_id(segment.disk_high, segment.disk_low);
        flatbuffers::FlatBufferBuilder builder;
        auto request = crowdb::diskio::proto::CreateFBDiskWriteRequest(builder, request_id, monotonic_nanos(), &disk_id,
                                                                       segment.zone_index, zone_offset,
                                                                       static_cast<uint32_t>(part), zone_offset);
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
        const crowdb_rpc_status submit = crowdb_rpc_client_send_slab(
            reinterpret_cast<crowdb_rpc_client_t>(route.client), reinterpret_cast<crowdb_rpc_server_t>(route.server),
            reinterpret_cast<crowdb_rpc_conn_t>(route.connection), request_id, control, payload,
            crowdb::rpc::proto::FBMsgType_EDiskWriteRequest, &AsyncWrite::rpc_complete, this);
        if (submit != CROWDB_RPC_OK) {
            finish(this, submit == CROWDB_RPC_ERR_SEND_QUEUE
                             ? Status::resource_exhausted("chunk RPC completion slab is full")
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

Status RpcChunkTransport::allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id)
{
    if (!valid() || chunk_id == nullptr || logical_capacity == 0 ||
        logical_capacity > std::numeric_limits<uint32_t>::max()) {
        return Status::invalid_argument("tree chunk RPC allocation arguments are invalid");
    }
    const uint64_t                 request_id     = impl_->next_request_id();
    const auto                     granularity_kb = static_cast<uint32_t>((logical_capacity + 1023) / 1024);
    flatbuffers::FlatBufferBuilder builder;
    auto                           request = crowdb::chunkdb::proto::CreateFBAllocateChunkRequest(
        builder, request_id, monotonic_nanos(), nullptr, granularity_kb, 1, FBStripType_Mirror, 0, 0, 3,
        FBChunkType_BtreePage, owner_epoch, impl_->options.writer_lease_ms);
    builder.Finish(request);
    std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    RpcResult            result;
    Status status = call_rpc(impl_->options.chunkdb, request_id, crowdb::rpc::proto::FBMsgType_EAllocateChunkRequest,
                             control, nullptr, 0, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<crowdb::chunkdb::proto::FBAllocateChunkResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB allocation response is malformed");
    }
    status = chunkdb_status(response->ret_code());
    if (!status.ok()) {
        return status;
    }
    Impl::RemoteChunk remote;
    status = crowdb::tree::detail::RpcChunkTransport::Impl::parse_chunk(response->chunk(), &remote);
    if (!status.ok() || remote.layout.chunk_id.empty() || remote.owner_epoch != owner_epoch ||
        remote.layout.logical_capacity < logical_capacity) {
        return status.ok() ? Status::corruption("ChunkDB allocation metadata mismatch") : status;
    }
    remote.valid_until_ms = std::numeric_limits<uint64_t>::max();
    *chunk_id             = remote.layout.chunk_id;
    impl_->cache(std::move(remote));
    return Status::Ok();
}

Status RpcChunkTransport::write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                                       size_t length)
{
    if (mirror_index >= 3 || (data == nullptr && length != 0)) {
        return Status::invalid_argument("tree chunk RPC mirror write arguments are invalid");
    }
    Impl::RemoteChunk chunk;
    if (!impl_->cached(chunk_id, &chunk)) {
        Status status = impl_->query_remote(chunk_id, &chunk);
        if (!status.ok()) {
            return status;
        }
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
        const size_t       part = std::min<uint64_t>(length - consumed, strip->chunk_offset + strip->capacity - cursor);
        const auto        &segment = strip->mirrors[mirror_index];
        ct_chunk_rpc_route route{};
        Status             status = impl_->resolve_route(segment, &route);
        if (!status.ok()) {
            return status;
        }
        const uint64_t                 unit_bytes  = static_cast<uint64_t>(strip->unit_kb) * 1024;
        const uint64_t zone_offset = (segment.unit_offset * unit_bytes) + (cursor - strip->chunk_offset);
        const uint64_t                 request_id  = impl_->next_request_id();
        const FBInt128                 disk_id(segment.disk_high, segment.disk_low);
        flatbuffers::FlatBufferBuilder builder;
        auto request = crowdb::diskio::proto::CreateFBDiskWriteRequest(builder, request_id, monotonic_nanos(), &disk_id,
                                                                       segment.zone_index, zone_offset,
                                                                       static_cast<uint32_t>(part), zone_offset);
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
    if (mirror_index >= 3 || (data == nullptr && length != 0)) {
        completion.complete(Status::invalid_argument("tree chunk RPC mirror write arguments are invalid"));
        return;
    }
    Impl::RemoteChunk chunk;
    if (!impl_->cached(chunk_id, &chunk)) {
        ChunkTransport::submit_write_mirror(chunk_id, mirror_index, offset, data, length, completion);
        return;
    }
    impl_->submit_write(std::move(chunk), mirror_index, offset, data, length, completion);
}

Status RpcChunkTransport::advance_write(ChunkId chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes)
{
    Impl::RemoteChunk chunk;
    if (!impl_->cached(chunk_id, &chunk) || chunk.layout.acknowledged_bytes != expected_bytes) {
        Status status = impl_->query_remote(chunk_id, &chunk);
        if (!status.ok()) {
            return status;
        }
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
    Status status = call_rpc(impl_->options.chunkdb, request_id,
                             crowdb::rpc::proto::FBMsgType_EAdvanceChunkWriteRequest, control, nullptr, 0, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<crowdb::chunkdb::proto::FBAdvanceChunkWriteResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB advance response is malformed");
    }
    status = chunkdb_status(response->ret_code());
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
    if (mirror_index >= 3 || (data == nullptr && length != 0)) {
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
        const size_t       part = std::min<uint64_t>(length - consumed, strip->chunk_offset + strip->capacity - cursor);
        const auto        &segment = strip->mirrors[mirror_index];
        ct_chunk_rpc_route route{};
        status = impl_->resolve_route(segment, &route);
        if (!status.ok()) {
            return status;
        }
        const uint64_t                 unit_bytes  = static_cast<uint64_t>(strip->unit_kb) * 1024;
        const uint64_t zone_offset = (segment.unit_offset * unit_bytes) + (cursor - strip->chunk_offset);
        const uint64_t                 request_id  = impl_->next_request_id();
        const FBInt128                 disk_id(segment.disk_high, segment.disk_low);
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
        if (response == nullptr || result.data == nullptr || crowdb_rpc_buffer_len(result.data) != part) {
            return Status::corruption("DiskIO read response is malformed");
        }
        status = diskio_status(response->ret_code());
        if (!status.ok()) {
            return status;
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
        acknowledged_bytes > std::numeric_limits<uint32_t>::max()) {
        return Status::unavailable("tree chunk RPC seal is fenced by owner epoch or cursor");
    }
    const uint64_t                 request_id = impl_->next_request_id();
    const FBInt128                 id(chunk_id.high, chunk_id.low);
    flatbuffers::FlatBufferBuilder builder;
    auto request = crowdb::chunkdb::proto::CreateFBSealChunkRequest(builder, request_id, monotonic_nanos(), &id,
                                                                    static_cast<uint32_t>(acknowledged_bytes));
    builder.Finish(request);
    std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    RpcResult            result;
    status = call_rpc(impl_->options.chunkdb, request_id, crowdb::rpc::proto::FBMsgType_ESealChunkRequest, control,
                      nullptr, 0, &result);
    if (!status.ok()) {
        return status;
    }
    const auto *response = verified_response<crowdb::chunkdb::proto::FBSealChunkResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB seal response is malformed");
    }
    status = chunkdb_status(response->ret_code());
    Impl::RemoteChunk updated;
    if (status.ok()) {
        status = crowdb::tree::detail::RpcChunkTransport::Impl::parse_chunk(response->chunk(), &updated);
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
