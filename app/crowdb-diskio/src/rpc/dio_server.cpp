// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc/dio_server.h"

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"
#include "disk/disk.h"
#include "rpc/dio_request.h"

#include <diskio_generated.h>
#include <flatbuffers/flatbuffers.h>
#include <msg_type_generated.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>

namespace crowdb::diskio
{

// diskio_generated.h puts types in crowdb::diskio::proto.
// msg_type_generated.h puts FBMsgType in crowdb::rpc::proto.
namespace dproto = crowdb::diskio::proto;
namespace rproto = crowdb::rpc::proto;

namespace
{
crowdb::common::metrics::LatencyHistogram &engine_read_latency_metric()
{
    static auto *metric =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("diskio.engine.read.lh");
    return *metric;
}

crowdb::common::metrics::LatencyHistogram &engine_write_latency_metric()
{
    static auto *metric =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("diskio.engine.write.lh");
    return *metric;
}

crowdb::common::metrics::LatencyHistogram &engine_fsync_latency_metric()
{
    static auto *metric =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("diskio.engine.fsync.lh");
    return *metric;
}

} // namespace

DiskioServer::DiskioServer(std::shared_ptr<DiskSet> disk_set, std::shared_ptr<crowdb::rpc::Transport> transport,
                           uint64_t max_write_request_age_ms, uint64_t max_clock_skew_ms)
    : disk_set_(std::move(disk_set)),
      transport_(std::move(transport)),
      aligned_writer_(),
      read_latency_(&engine_read_latency_metric()),
      write_latency_(&engine_write_latency_metric()),
      fsync_latency_(&engine_fsync_latency_metric()),
      max_write_request_age_ms_(max_write_request_age_ms),
      max_clock_skew_ms_(max_clock_skew_ms)
{
}

// Build a diskio response control buffer (flatbuffer).
crowdb::rpc::Buffer *DiskioServer::build_response_ctrl(crowdb::rpc::BufferPool *pool, uint64_t request_id,
                                                       uint64_t rpc_create_nano, int16_t ret_code, uint16_t msg_type,
                                                       bool sync_completed)
{
    auto                           fb_ret = static_cast<dproto::FBDiskIoRetCode>(ret_code);
    flatbuffers::FlatBufferBuilder fbb(64);
    if (msg_type == static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteResponse)) {
        auto off = dproto::CreateFBDiskWriteResponse(fbb, request_id, rpc_create_nano, fb_ret, sync_completed);
        fbb.Finish(off);
    }
    else if (msg_type == static_cast<uint16_t>(rproto::FBMsgType_EDiskReadResponse)) {
        auto off = dproto::CreateFBDiskReadResponse(fbb, request_id, rpc_create_nano, fb_ret);
        fbb.Finish(off);
    }
    else { // fsync response
        auto off = dproto::CreateFBDiskFsyncResponse(fbb, request_id, rpc_create_nano, fb_ret);
        fbb.Finish(off);
    }
    uint32_t size = fbb.GetSize();
    auto    *buf  = pool->alloc(size);
    if (buf == nullptr) {
        return nullptr;
    }
    buf->write(fbb.GetBufferPointer(), size);
    return buf;
}

void DiskioServer::send_error_response(crowdb::rpc::Connection *conn, uint64_t request_id, uint64_t rpc_create_nano,
                                       uint16_t msg_type, int16_t ret_code)
{
    send_response(transport_, conn->weak_from_this().lock(), request_id, rpc_create_nano, msg_type, ret_code);
}

void DiskioServer::send_response(const std::shared_ptr<crowdb::rpc::Transport> &transport,
                                 const std::shared_ptr<crowdb::rpc::Connection> &connection, uint64_t request_id,
                                 uint64_t created, uint16_t type, int16_t code, crowdb::rpc::Buffer *data,
                                 bool sync_completed)
{
    const auto release = [](crowdb::rpc::Buffer *buffer) {
        if (buffer != nullptr)
            buffer->release();
    };
    std::unique_ptr<crowdb::rpc::Buffer, decltype(release)> payload(data, release);
    if (connection == nullptr || !connection->is_open())
        return;
    auto *control = build_response_ctrl(connection->pool(), request_id, created, code, type, sync_completed);
    if (control == nullptr) {
        connection->close();
        return;
    }
    auto *frame        = crowdb::rpc::build_out_frame(request_id, type, control, payload.release());
    frame->create_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    if (!transport->submit(connection.get(), frame)) {
        control->release();
        frame->release_data();
        delete frame;
        connection->close();
    }
}

void DiskioServer::register_handlers(crowdb::rpc::RpcServer &server)
{
    server.register_handler(
        static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteRequest),
        [this](crowdb::rpc::Frame *req, crowdb::rpc::Connection *conn) { return handle_write(req, conn); });
    server.register_handler(
        static_cast<uint16_t>(rproto::FBMsgType_EDiskReadRequest),
        [this](crowdb::rpc::Frame *req, crowdb::rpc::Connection *conn) { return handle_read(req, conn); });
    server.register_handler(
        static_cast<uint16_t>(rproto::FBMsgType_EDiskFsyncRequest),
        [this](crowdb::rpc::Frame *req, crowdb::rpc::Connection *conn) { return handle_fsync(req, conn); });
}

} // namespace crowdb::diskio
