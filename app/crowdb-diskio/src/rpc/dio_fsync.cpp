// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/metrics.h"
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

crowdb::rpc::OutFrame *DiskioServer::handle_fsync(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    uint16_t msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskFsyncResponse);

    auto *fb_req = decode_disk_request<dproto::FBDiskFsyncRequest>(*request);
    if (fb_req == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    DiskId did = parse_disk_id(fb_req->disk_id());

    auto disk = disk_set_->find_disk(did);
    if (disk == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_DiskNotExist));
        return nullptr;
    }

    delete request;

    Disk *disk_ptr = disk.get();
    auto  started  = std::chrono::steady_clock::now();
    disk_ptr->engine()->submit_fsync(disk_ptr, [transport = transport_, connection = conn->weak_from_this().lock(),
                                                latency = fsync_latency_, req_id, create_nano, msg_type, started,
                                                disk    = std::move(disk)](int res) {
        latency->observe(elapsed_nanos(started));
        int16_t ret_code = (res < 0) ? static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError)
                                     : static_cast<int16_t>(dproto::FBDiskIoRetCode_Success);
        send_response(transport, connection, req_id, create_nano, msg_type, ret_code);
    });

    return nullptr;
}

} // namespace crowdb::diskio
