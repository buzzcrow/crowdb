// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_c_api_internal.h"
#include "chunk_purpose.h"
#include "chunkdb_generated.h"
#include "crowdb-rpc/c_api.h"
#include "diskio_generated.h"
#include "msg_type_generated.h"
#include "rpc_chunk_allocation.h"
#include "rpc_chunk_result.h"
#include "rpc_chunk_route.h"
#include "rpc_chunk_transport.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace crowdb::tree::detail
{
namespace rpc_chunk_detail
{

using crowdb::chunkdb::proto::FBChunk;
using crowdb::chunkdb::proto::FBChunkdbRetCode;
using crowdb::chunkdb::proto::FBChunkdbRetCode_Success;
using crowdb::chunkdb::proto::FBChunkState_Sealed;
using crowdb::chunkdb::proto::FBStripType_Mirror;
using crowdb::rpc::proto::FBInt128;

inline uint64_t monotonic_nanos()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

inline uint64_t wall_time_ms()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

inline uint64_t monotonic_millis()
{
    return monotonic_nanos() / 1'000'000;
}

inline constexpr uint64_t kLivenessRenewalMs   = 12U * 60U * 1000U;
inline constexpr uint64_t kLivenessSelfFenceMs = 13U * 60U * 1000U;

inline Status call_rpc(const ct_chunk_rpc_route &route, uint64_t request_id, uint16_t message_type,
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
    const auto   submit = crowdb_rpc_client_send(reinterpret_cast<crowdb_rpc_client_t>(route.client),
                                                 reinterpret_cast<crowdb_rpc_server_t>(route.server),
                                                 reinterpret_cast<crowdb_rpc_conn_t>(route.connection), request_id,
                                                 control_buffer, data_buffer, message_type, &rpc_complete, &state);
    if (submit != CROWDB_RPC_OK) {
        return submit == CROWDB_RPC_ERR_SEND_QUEUE ? Status::resource_exhausted("chunk RPC submission queue is full")
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

inline Status chunkdb_status(FBChunkdbRetCode code, const flatbuffers::String *error)
{
    using namespace crowdb::chunkdb::proto;
    const std::string message = error == nullptr || error->empty() ? "tree chunk request failed" : error->str();
    switch (code) {
    case FBChunkdbRetCode_Success:
        return Status::Ok();
    case FBChunkdbRetCode_InvalidArgument:
    case FBChunkdbRetCode_FailedPrecondition:
    case FBChunkdbRetCode_StripIndexOutOfRange:
        return Status::invalid_argument("ChunkDB rejected the tree chunk request: " + message);
    case FBChunkdbRetCode_Unavailable:
    case FBChunkdbRetCode_NotMyRange:
    case FBChunkdbRetCode_Aborted:
        return Status::unavailable("ChunkDB could not complete the tree chunk request: " + message);
    default:
        return Status::internal_error("ChunkDB returned an unexpected tree chunk result: " + message);
    }
}

inline Status diskio_status(crowdb::diskio::proto::FBDiskIoRetCode code)
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
    case FBDiskIoRetCode_OldRequest:
        return Status::unavailable("DiskIO tree page target is unavailable");
    default:
        return Status::internal_error("DiskIO tree page operation failed");
    }
}

} // namespace rpc_chunk_detail

using namespace rpc_chunk_detail;

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
        uint64_t             chunk_offset = 0;
        uint64_t             capacity     = 0;
        uint32_t             unit_kb      = 0;
        std::vector<Segment> mirrors;
    };

    struct RemoteChunk
    {
        ChunkLayout        layout;
        uint64_t           owner_epoch            = 0;
        uint64_t           modify_ts              = 0;
        uint64_t           maximum_capacity       = 0;
        uint64_t           valid_until_ms         = 0;
        uint64_t           self_fence_deadline_ms = 0;
        std::vector<Strip> strips;
    };

    using RemoteChunks = std::vector<RemoteChunk>;

    explicit Impl(const ct_chunk_rpc_transport_options &configured)
        : chunkdb_routes(configured),
          disk_resolver(configured.disk_resolver),
          options(configured)
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
        }
        for (const auto &disk : disk_routes) {
            if (disk.route.client != nullptr) {
                crowdb_rpc_client_set_completion_pool_size(reinterpret_cast<crowdb_rpc_client_t>(disk.route.client),
                                                           completion_capacity);
                crowdb_rpc_client_start_reaper(reinterpret_cast<crowdb_rpc_client_t>(disk.route.client),
                                               timeout_ms * 1'000'000, 100'000'000);
            }
        }
        liveness_thread = std::jthread([this](std::stop_token stop) {
            while (!stop.stop_requested()) {
                for (uint32_t second = 0; second != 60 && !stop.stop_requested(); ++second) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                if (!stop.stop_requested()) {
                    renew_due_liveness();
                }
            }
        });
    }

    [[nodiscard]] bool valid() const
    {
        return options.mirror_copies <= kMaxMirrorCopies && options.strip_capacity_bytes % 1024 == 0 &&
               chunkdb_routes.valid() && (disk_resolver.valid() || !disk_routes.empty());
    }

    template <typename Response>
    Status call_chunkdb(ChunkId chunk, uint64_t request_id, uint16_t message_type, const std::vector<uint8_t> &control,
                        RpcResult *result, bool read_only = false) const
    {
        Status status = Status::unavailable("chunk service route retries exhausted");
        for (uint32_t attempt = 0; attempt != 3; ++attempt) {
            bool               submitted = false;
            RpcChunkRouteLease lease;
            status = chunkdb_routes.resolve(chunk, attempt != 0, &lease);
            if (attempt != 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * attempt));
                RpcChunkRouteLease refreshed;
                status = chunkdb_routes.resolve(chunk, false, &refreshed);
                if (status.ok()) {
                    submitted = true;
                    status    = call_rpc(refreshed.route, request_id, message_type, control, nullptr, 0, result);
                }
            }
            else if (status.ok()) {
                submitted = true;
                status    = call_rpc(lease.route, request_id, message_type, control, nullptr, 0, result);
            }
            if (!status.ok()) {
                RpcChunkRouteLease refresh_hint;
                static_cast<void>(chunkdb_routes.resolve(chunk, true, &refresh_hint));
                // A write transport failure can have committed; never blindly
                // repeat allocation or a fenced mutation with an unknown result.
                if (submitted && !read_only) {
                    return status;
                }
                continue;
            }
            const auto *response = verified_response<Response>(result->control);
            if (response == nullptr || response->ret_code() != crowdb::chunkdb::proto::FBChunkdbRetCode_NotMyRange) {
                return status;
            }
            status = Status::unavailable("chunk service rejected slot authority");
        }
        return status;
    }

    uint64_t next_request_id() const
    {
        return request_ids.fetch_add(1, std::memory_order_relaxed);
    }

    Status resolve_route(const Segment &segment, RpcChunkRouteLease *lease) const
    {
        if (disk_resolver.valid()) {
            return disk_resolver.resolve(ChunkId(segment.disk_high, segment.disk_low), false, lease);
        }
        auto found = std::find_if(disk_routes.begin(), disk_routes.end(), [&segment](const auto &item) {
            return item.disk_id_high == segment.disk_high && item.disk_id_low == segment.disk_low;
        });
        if (found == disk_routes.end() || found->route.client == nullptr || found->route.server == nullptr ||
            found->route.connection == nullptr) {
            return Status::unavailable("DiskIO route is unavailable");
        }
        lease->route = found->route;
        return Status::Ok();
    }

    static Status parse_chunk(const FBChunk *chunk, RemoteChunk *out)
    {
        if (chunk == nullptr || chunk->id() == nullptr) {
            return Status::corruption("ChunkDB returned malformed tree chunk metadata");
        }
        // ChunkDB keeps a Deleted tombstone after releasing the physical
        // strips. Treat that state as an unavailable tree root instead of
        // misclassifying the intentionally empty strips vector as malformed
        // metadata. The owner can then repair or retire the assignment.
        if (chunk->state() == crowdb::chunkdb::proto::FBChunkState_Deleted) {
            return Status::unavailable("tree chunk is deleted");
        }
        RemoteChunk parsed;
        parsed.layout = {
            .chunk_id           = ChunkId(chunk->id()->high(), chunk->id()->low()),
            .logical_capacity   = static_cast<uint64_t>(chunk->capacity()) * 1024,
            .acknowledged_bytes = chunk->acknowledged_cursor(),
            .sealed             = chunk->state() == FBChunkState_Sealed,
        };
        const auto purpose = chunk_purpose(parsed.layout.chunk_id);
        if (!valid_page_purpose(purpose) ||
            static_cast<uint8_t>(chunk->chunk_type()) != static_cast<uint8_t>(purpose)) {
            return Status::corruption("ChunkDB tree chunk purpose mismatch");
        }
        parsed.owner_epoch = chunk->writer_epoch();
        parsed.modify_ts   = chunk->modify_ts();
        if (chunk->strips() == nullptr || chunk->strips()->empty()) {
            if (!parsed.layout.sealed || parsed.layout.logical_capacity != 0 || parsed.layout.acknowledged_bytes != 0) {
                return Status::corruption("ChunkDB tree chunk has no valid physical layout");
            }
            *out = std::move(parsed);
            return Status::Ok();
        }
        for (const auto *wire_strip : *chunk->strips()) {
            const auto *mirror = wire_strip == nullptr ? nullptr : wire_strip->strip_body_as_FBMirrorStrip();
            if (wire_strip == nullptr || wire_strip->strip_type() != FBStripType_Mirror || mirror == nullptr ||
                mirror->segments() == nullptr || mirror->segments()->empty() || wire_strip->unit_kb() == 0) {
                return Status::corruption("ChunkDB returned a non-mirror tree chunk layout");
            }
            Strip strip{.chunk_offset = static_cast<uint64_t>(wire_strip->chunk_offset()) * 1024,
                        .capacity     = static_cast<uint64_t>(wire_strip->capacity()) * 1024,
                        .unit_kb      = wire_strip->unit_kb(),
                        .mirrors      = {}};
            strip.mirrors.reserve(mirror->segments()->size());
            for (size_t index = 0; index < mirror->segments()->size(); ++index) {
                const auto *segment = mirror->segments()->Get(index);
                strip.mirrors.push_back({
                    .disk_high   = segment->disk_id().high(),
                    .disk_low    = segment->disk_id().low(),
                    .unit_offset = segment->unit_offset(),
                    .zone_index  = segment->zone_index(),
                });
            }
            parsed.strips.push_back(strip);
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
                if (chunk.modify_ts < found->modify_ts) {
                    return;
                }
                if (chunk.maximum_capacity == 0) {
                    chunk.maximum_capacity = found->maximum_capacity;
                }
                if (chunk.self_fence_deadline_ms == 0) {
                    chunk.self_fence_deadline_ms = found->self_fence_deadline_ms;
                }
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

    void fence_liveness(RemoteChunk chunk) const
    {
        chunk.self_fence_deadline_ms = monotonic_millis();
        cache(std::move(chunk));
    }

    void renew_due_liveness() const
    {
        const auto current = chunks.load(std::memory_order_acquire);
        if (current == nullptr) {
            return;
        }
        const uint64_t now = monotonic_millis();
        for (const RemoteChunk &chunk : *current) {
            if (chunk.layout.sealed || chunk.self_fence_deadline_ms == 0 ||
                now + (kLivenessSelfFenceMs - kLivenessRenewalMs) < chunk.self_fence_deadline_ms) {
                continue;
            }
            const uint64_t                 request_id = next_request_id();
            const FBInt128                 id(chunk.layout.chunk_id.high, chunk.layout.chunk_id.low);
            flatbuffers::FlatBufferBuilder builder;
            auto                           request = crowdb::chunkdb::proto::CreateFBAdvanceChunkWriteRequest(
                builder, request_id, monotonic_nanos(), &id, chunk.owner_epoch, chunk.modify_ts,
                chunk.layout.acknowledged_bytes, std::numeric_limits<uint32_t>::max(), options.writer_lease_ms);
            builder.Finish(request);
            std::vector<uint8_t> control(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
            RpcResult            result;
            Status               status = call_chunkdb<crowdb::chunkdb::proto::FBAdvanceChunkWriteResponse>(
                chunk.layout.chunk_id, request_id, crowdb::rpc::proto::FBMsgType_EAdvanceChunkWriteRequest, control,
                &result);
            const auto *response =
                status.ok() ? verified_response<crowdb::chunkdb::proto::FBAdvanceChunkWriteResponse>(result.control)
                            : nullptr;
            if (response == nullptr || !chunkdb_status(response->ret_code(), response->error_msg()).ok()) {
                fence_liveness(chunk);
                continue;
            }
            RemoteChunk renewed;
            if (parse_chunk(response->chunk(), &renewed).ok()) {
                renewed.self_fence_deadline_ms = monotonic_millis() + kLivenessSelfFenceMs;
                cache(std::move(renewed));
            }
            else {
                fence_liveness(chunk);
            }
        }
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
        Status               status = call_chunkdb<crowdb::chunkdb::proto::FBQueryChunkResponse>(
            chunk_id, request_id, crowdb::rpc::proto::FBMsgType_EQueryChunkRequest, control, &result, true);
        if (!status.ok()) {
            return status;
        }
        const auto *response = verified_response<crowdb::chunkdb::proto::FBQueryChunkResponse>(result.control);
        if (response == nullptr) {
            return Status::corruption("ChunkDB query response is malformed");
        }
        status = chunkdb_status(response->ret_code(), response->error_msg());
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

    RpcChunkRouteResolver                       chunkdb_routes;
    RpcChunkRouteResolver                       disk_resolver;
    ct_chunk_rpc_transport_options              options;
    std::vector<ct_chunk_rpc_disk_route>        disk_routes;
    inline static std::atomic<uint64_t>         request_ids{1};
    mutable AtomicSharedPtr<const RemoteChunks> chunks;
    std::jthread                                liveness_thread;
};

} // namespace crowdb::tree::detail
