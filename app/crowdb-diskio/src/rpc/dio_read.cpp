// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/metrics.h"
#include "rpc/dio_read_buffer.h"
#include "rpc/dio_request.h"
#include "rpc/dio_server.h"

#include <msg_type_generated.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>

namespace crowdb::diskio
{
namespace dproto = crowdb::diskio::proto;
namespace rproto = crowdb::rpc::proto;

crowdb::rpc::OutFrame *DiskioServer::handle_read(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    uint16_t msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskReadResponse);

    auto *fb_req = decode_disk_request<dproto::FBDiskReadRequest>(*request);
    if (fb_req == nullptr || (request->data_buf != nullptr && request->data_buf->len != 0)) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    DiskId   did                 = parse_disk_id(fb_req->disk_id());
    uint32_t zone_index          = fb_req->zone_index();
    uint64_t zone_offset         = fb_req->zone_offset();
    uint32_t size                = fb_req->size();
    uint64_t test_pattern_offset = fb_req->test_pattern_offset();

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
    if (!valid_disk_range(*zone, zone_offset, size)) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    off_t phys_offset = zone->base_offset + static_cast<off_t>(zone_offset);

    const bool registered = conn->path() == crowdb::rpc::ConnectionPath::Rdma;
    auto      *root       = request->data_buf == nullptr ? nullptr : request->data_buf->allocation_owner;
    const bool receive_owned =
        root != nullptr && root->type == crowdb::rpc::BufferType::Registered && root->registration_handle != 0 &&
        root->last_release_cb != nullptr &&
        root->ownership.load(std::memory_order_acquire) == crowdb::rpc::BufferOwnership::CallerOwned;
    // Control has been decoded, and READ carries no input payload. Retain its
    // existing receive data region until disk and response SEND both finish.
    auto *read_buf = registered ? (receive_owned ? root->slice(0, size) : nullptr) : conn->pool()->alloc(size);
    if (read_buf == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }

    DiskReadBuffer io{.buffer = read_buf, .size = size, .offset = phys_offset, .io_size = size, .aligned = {}};
    const auto     aligned = io.prepare_alignment(*disk, *zone);
    delete request;
    if (aligned != IoRetCode::Success) {
        read_buf->release();
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(aligned));
        return nullptr;
    }
    if (!io.begin()) {
        conn->close();
        return nullptr;
    }
    Disk *disk_ptr = disk.get();
    auto  started  = std::chrono::steady_clock::now();
    auto *io_data  = io.data();
    disk_ptr->engine()->submit_read(disk_ptr, io.offset, io_data, io.io_size, test_pattern_offset,
                                    [transport = transport_, connection = conn->weak_from_this().lock(),
                                     latency = read_latency_, req_id, create_nano, msg_type, io = std::move(io),
                                     disk = std::move(disk), started](int result) mutable {
                                        latency->observe(elapsed_nanos(started));
                                        const auto completed = io.finish(result);
                                        if (!completed.valid_owner) {
                                            if (connection != nullptr)
                                                connection->close();
                                            return;
                                        }
                                        send_response(transport, connection, req_id, create_nano, msg_type,
                                                      static_cast<int16_t>(completed.code), completed.data);
                                    });

    return nullptr;
}

} // namespace crowdb::diskio
