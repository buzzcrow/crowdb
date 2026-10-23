// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-rpc/server/message.h"
#include "disk/disk.h"
#include "rpc/dio_server.h"

#include <diskio_generated.h>
#include <msg_type_generated.h>

#include <cerrno>
#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

namespace crowdb::diskio
{
namespace dproto = crowdb::diskio::proto;
namespace rproto = crowdb::rpc::proto;

namespace
{
uint64_t unix_time_ms()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

uint64_t elapsed_nanos(std::chrono::steady_clock::time_point started)
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count());
}

std::optional<std::chrono::steady_clock::time_point> write_deadline(uint64_t created_ms, uint64_t max_age_ms,
                                                                    uint64_t skew_ms)
{
    const auto steady_now = std::chrono::steady_clock::now();
    const auto now_ms     = unix_time_ms();
    if (created_ms == 0 || max_age_ms == 0 || (created_ms > now_ms && created_ms - now_ms > skew_ms) ||
        max_age_ms > UINT64_MAX - skew_ms) {
        return std::nullopt;
    }
    const auto allowance = max_age_ms + skew_ms;
    if (created_ms > UINT64_MAX - allowance || created_ms + allowance <= now_ms) {
        return std::nullopt;
    }
    const auto remaining_ms = created_ms + allowance - now_ms;
    const auto max_wait_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::time_point::max() - steady_now)
            .count();
    if (std::cmp_greater(remaining_ms, max_wait_ms)) {
        return std::nullopt;
    }
    return steady_now + std::chrono::milliseconds(static_cast<int64_t>(remaining_ms));
}
} // namespace

crowdb::rpc::OutFrame *DiskioServer::handle_write(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    auto     msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteResponse);

    flatbuffers::Verifier verifier(request->control.data(), request->control.size());
    if (!verifier.VerifyBuffer<dproto::FBDiskWriteRequest>(nullptr)) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    const auto *fb_req      = ::flatbuffers::GetRoot<dproto::FBDiskWriteRequest>(request->control.data());
    const auto *identity    = fb_req->disk_id();
    DiskId      did         = identity != nullptr ? DiskId{.high = identity->high(), .low = identity->low()} : DiskId{};
    uint32_t    zone_index  = fb_req->zone_index();
    uint64_t    zone_offset = fb_req->zone_offset();
    uint32_t    size        = fb_req->size();
    uint64_t    ordering_zone_offset = fb_req->ordering_zone_offset();
    uint64_t    write_create_time_ms = fb_req->write_create_time_ms();
    const bool  sync                 = fb_req->sync();

    const auto deadline = write_deadline(write_create_time_ms, max_write_request_age_ms_, max_clock_skew_ms_);
    if (!deadline) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_OldRequest));
        return nullptr;
    }

    auto disk = disk_set_->find_disk(did);
    if (disk == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_DiskNotExist));
        return nullptr;
    }

    Zone *zone = disk->find_zone(zone_index);
    if (zone == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_ZoneNotExist));
        return nullptr;
    }
    if (zone->base_offset < 0 || zone->capacity < 0 || ordering_zone_offset > zone_offset ||
        std::cmp_greater(zone_offset, zone->capacity) || size > static_cast<uint64_t>(zone->capacity) - zone_offset ||
        std::cmp_greater(zone_offset, std::numeric_limits<off_t>::max() - zone->base_offset) ||
        (size > 0 && (request->data_buf == nullptr || request->data_buf->len != size))) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    off_t phys_offset = zone->base_offset + static_cast<off_t>(zone_offset);

    crowdb::rpc::Buffer *data_buf = nullptr;
    if (request->data_buf != nullptr && size > 0) {
        data_buf = request->data_buf->ref_clone();
    }
    delete request;

    if (data_buf == nullptr && size > 0) {
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    auto data     = std::shared_ptr<crowdb::rpc::Buffer>(data_buf, [](crowdb::rpc::Buffer *buffer) {
        if (buffer != nullptr) {
            buffer->release();
        }
    });
    auto complete = [this, connection = conn->shared_from_this(), data, req_id, create_nano,
                     msg_type](int16_t code, bool synchronized) {
        auto *control = build_response_ctrl(connection->pool(), req_id, create_nano, code, msg_type, synchronized);
        if (control == nullptr) {
            return;
        }
        auto *out        = crowdb::rpc::build_out_frame(req_id, msg_type, control, nullptr);
        out->create_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        if (!transport_->submit(connection.get(), out)) {
            control->release();
            delete out;
        }
    };
    uint64_t ordering_phys_offset = zone->base_offset + ordering_zone_offset;
    auto     started              = std::chrono::steady_clock::now();
    aligned_writer_.submit_ordered(
        disk, phys_offset, data_buf != nullptr ? data_buf->data : nullptr, size, ordering_phys_offset,
        [this, disk, complete = std::move(complete), sync, size, started](int res) mutable {
            write_latency_->observe(elapsed_nanos(started));
            if (res == -ETIMEDOUT) {
                complete(static_cast<int16_t>(dproto::FBDiskIoRetCode_OldRequest), false);
            }
            else if (res < 0) {
                complete(static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError), false);
            }
            else if (std::cmp_less(res, size)) {
                complete(static_cast<int16_t>(dproto::FBDiskIoRetCode_PartialWrite), false);
            }
            else if (!sync) {
                complete(static_cast<int16_t>(dproto::FBDiskIoRetCode_Success), false);
            }
            else {
                auto flush_started = std::chrono::steady_clock::now();
                disk->engine()->submit_fsync(
                    disk.get(), [this, disk, complete = std::move(complete), flush_started](int result) mutable {
                        fsync_latency_->observe(elapsed_nanos(flush_started));
                        complete(static_cast<int16_t>(result < 0 ? dproto::FBDiskIoRetCode_FsyncFailed
                                                                 : dproto::FBDiskIoRetCode_Success),
                                 result >= 0);
                    });
            }
        },
        *deadline);
    return nullptr;
}

} // namespace crowdb::diskio
