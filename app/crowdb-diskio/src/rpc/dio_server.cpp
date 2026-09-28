// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc/dio_server.h"

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-protocol/frame.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"
#include "disk/disk.h"

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

uint64_t elapsed_nanos(std::chrono::steady_clock::time_point started)
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count());
}

uint64_t unix_time_ms()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

} // namespace

DiskioServer::DiskioServer(std::shared_ptr<DiskSet> disk_set, crowdb::rpc::SocketTransport *transport,
                           uint64_t max_write_request_age_ms, uint64_t max_clock_skew_ms)
    : disk_set_(std::move(disk_set)),
      transport_(transport),
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
                                                       uint64_t rpc_create_nano, int16_t ret_code, uint16_t msg_type)
{
    auto                           fb_ret = static_cast<dproto::FBDiskIoRetCode>(ret_code);
    flatbuffers::FlatBufferBuilder fbb(64);
    if (msg_type == static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteResponse)) {
        auto off = dproto::CreateFBDiskWriteResponse(fbb, request_id, rpc_create_nano, fb_ret);
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
    std::memcpy(buf->data, fbb.GetBufferPointer(), size);
    buf->write(buf->data, size);
    return buf;
}

void DiskioServer::send_error_response(crowdb::rpc::Connection *conn, uint64_t request_id, uint64_t rpc_create_nano,
                                       uint16_t msg_type, int16_t ret_code)
{
    auto *pool       = conn->pool();
    auto *ctrl       = build_response_ctrl(pool, request_id, rpc_create_nano, ret_code, msg_type);
    auto *out        = crowdb::rpc::build_out_frame(request_id, msg_type, ctrl, nullptr);
    out->create_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    transport_->submit(conn, out);
}

static DiskId parse_disk_id(const crowdb::rpc::proto::FBInt128 *fb_id)
{
    if (fb_id == nullptr) {
        return {0, 0};
    }
    return {fb_id->high(), fb_id->low()};
}

crowdb::rpc::OutFrame *DiskioServer::handle_write(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    uint16_t msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteResponse);

    auto *fb_req = ::flatbuffers::GetRoot<dproto::FBDiskWriteRequest>(request->control.data());
    if (fb_req == nullptr || request->control.size() < 4) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    DiskId   did                  = parse_disk_id(fb_req->disk_id());
    uint32_t zone_index           = fb_req->zone_index();
    uint64_t zone_offset          = fb_req->zone_offset();
    uint32_t size                 = fb_req->size();
    uint64_t ordering_zone_offset = fb_req->ordering_zone_offset();
    uint64_t write_create_time_ms = fb_req->write_create_time_ms();

    const uint64_t allowance = max_write_request_age_ms_ > UINT64_MAX - max_clock_skew_ms_
                                 ? UINT64_MAX
                                 : max_write_request_age_ms_ + max_clock_skew_ms_;
    const uint64_t now_ms    = unix_time_ms();
    if (write_create_time_ms == 0 || (now_ms >= write_create_time_ms && now_ms - write_create_time_ms > allowance)) {
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
    off_t phys_offset = static_cast<off_t>(zone->base_offset + zone_offset);

    crowdb::rpc::Buffer *data_buf = nullptr;
    if (request->data_buf != nullptr && size > 0) {
        data_buf = request->data_buf->ref_clone();
    }
    delete request;

    if (data_buf == nullptr && size > 0) {
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    // An EC shard is opaque DiskIO data.  It can begin with the same two
    // bytes as a public frame because the first data shard carries the
    // original prefix, but it is not itself a frame sequence.  Without an
    // explicit content-kind field, only a single-frame request is
    // unambiguously self-describing at this boundary.
    if (data_buf != nullptr && size <= crowdb::protocol::kMaxFrameBytes && size >= 2 &&
        crowdb::protocol::valid_magic(crowdb::protocol::read_u16_le(data_buf->data))) {
        const auto frame_status =
            crowdb::protocol::validate_frame_sequence(std::span<const uint8_t>(data_buf->data, size));
        if (frame_status != crowdb::protocol::FrameError::Ok) {
            data_buf->release();
            send_error_response(conn, req_id, create_nano, msg_type,
                                static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
            return nullptr;
        }
    }

    uint64_t ordering_phys_offset = zone->base_offset + ordering_zone_offset;
    auto     started              = std::chrono::steady_clock::now();
    aligned_writer_.submit_ordered(disk, phys_offset, data_buf ? data_buf->data : nullptr, size, ordering_phys_offset,
                                   [this, conn, req_id, create_nano, msg_type, data_buf, size, started](int res) {
                                       write_latency_->observe(elapsed_nanos(started));
                                       int16_t ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_Success);
                                       if (res < 0) {
                                           ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError);
                                       }
                                       else if (static_cast<uint32_t>(res) < size) {
                                           ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_PartialWrite);
                                       }
                                       auto *pool = conn->pool();
                                       auto *ctrl = build_response_ctrl(pool, req_id, create_nano, ret_code, msg_type);
                                       auto *out  = crowdb::rpc::build_out_frame(req_id, msg_type, ctrl, nullptr);
                                       out->create_nano = static_cast<uint64_t>(
                                           std::chrono::steady_clock::now().time_since_epoch().count());
                                       transport_->submit(conn, out);
                                       if (data_buf != nullptr) {
                                           data_buf->release();
                                       }
                                   });

    return nullptr;
}

crowdb::rpc::OutFrame *DiskioServer::handle_read(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    uint16_t msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskReadResponse);

    auto *fb_req = ::flatbuffers::GetRoot<dproto::FBDiskReadRequest>(request->control.data());
    if (fb_req == nullptr || request->control.size() < 4) {
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
    if (zone->base_offset < 0 || zone->capacity < 0 || zone_offset > static_cast<uint64_t>(zone->capacity) ||
        size > static_cast<uint64_t>(zone->capacity) - zone_offset ||
        zone_offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max() - zone->base_offset)) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    off_t phys_offset = zone->base_offset + static_cast<off_t>(zone_offset);

    auto *pool     = conn->pool();
    auto *read_buf = pool->alloc(size);
    if (read_buf == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }

    auto                     io_offset = phys_offset;
    size_t                   io_size   = size;
    size_t                   prefix    = 0;
    std::shared_ptr<uint8_t> aligned_data;
    if (disk->is_o_direct()) {
        const size_t alignment = std::max<size_t>(4096, disk->block_size());
        if ((alignment & (alignment - 1)) != 0 || alignment > static_cast<size_t>(std::numeric_limits<off_t>::max()) ||
            static_cast<uint64_t>(phys_offset) + size >
                static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - alignment) {
            read_buf->release();
            delete request;
            send_error_response(conn, req_id, create_nano, msg_type,
                                static_cast<int16_t>(dproto::FBDiskIoRetCode_InvalidAlignment));
            return nullptr;
        }
        prefix    = static_cast<size_t>(phys_offset) % alignment;
        io_offset = phys_offset - static_cast<off_t>(prefix);
        io_size   = ((prefix + size + alignment - 1) / alignment) * alignment;
        if (io_offset < zone->base_offset ||
            io_size > static_cast<size_t>(zone->capacity - (io_offset - zone->base_offset))) {
            read_buf->release();
            delete request;
            send_error_response(conn, req_id, create_nano, msg_type,
                                static_cast<int16_t>(dproto::FBDiskIoRetCode_InvalidAlignment));
            return nullptr;
        }
        aligned_data = std::shared_ptr<uint8_t>(static_cast<uint8_t *>(std::aligned_alloc(alignment, io_size)),
                                                [](uint8_t *data) { std::free(data); });
        if (aligned_data == nullptr) {
            read_buf->release();
            delete request;
            send_error_response(conn, req_id, create_nano, msg_type,
                                static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
            return nullptr;
        }
    }

    delete request;

    Disk *disk_ptr = disk.get();
    auto  started  = std::chrono::steady_clock::now();
    auto *io_data  = aligned_data ? aligned_data.get() : read_buf->data;
    disk_ptr->engine()->submit_read(disk_ptr, io_offset, io_data, io_size, test_pattern_offset,
                                    [this, conn, req_id, create_nano, msg_type, read_buf, size, io_size, prefix,
                                     aligned_data = std::move(aligned_data), disk = std::move(disk), started](int res) {
                                        read_latency_->observe(elapsed_nanos(started));
                                        int16_t ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_Success);
                                        crowdb::rpc::Buffer *data = nullptr;
                                        if (res < 0) {
                                            ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError);
                                        }
                                        else if (static_cast<size_t>(res) < io_size) {
                                            ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_PartialWrite);
                                        }
                                        else {
                                            if (aligned_data) {
                                                std::memcpy(read_buf->data, aligned_data.get() + prefix, size);
                                            }
                                            read_buf->len = size;
                                            data          = read_buf;
                                        }
                                        auto *pool = conn->pool();
                                        auto *ctrl = build_response_ctrl(pool, req_id, create_nano, ret_code, msg_type);
                                        auto *out  = crowdb::rpc::build_out_frame(req_id, msg_type, ctrl, data);
                                        out->create_nano = static_cast<uint64_t>(
                                            std::chrono::steady_clock::now().time_since_epoch().count());
                                        transport_->submit(conn, out);
                                        if (data == nullptr) {
                                            read_buf->release();
                                        }
                                    });

    return nullptr;
}

crowdb::rpc::OutFrame *DiskioServer::handle_fsync(crowdb::rpc::Frame *request, crowdb::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    uint16_t msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskFsyncResponse);

    auto *fb_req = ::flatbuffers::GetRoot<dproto::FBDiskFsyncRequest>(request->control.data());
    if (fb_req == nullptr || request->control.size() < 4) {
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
    disk_ptr->engine()->submit_fsync(disk_ptr, [this, conn, req_id, create_nano, msg_type, started](int res) {
        fsync_latency_->observe(elapsed_nanos(started));
        int16_t ret_code = (res < 0) ? static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError)
                                     : static_cast<int16_t>(dproto::FBDiskIoRetCode_Success);
        auto   *pool     = conn->pool();
        auto   *ctrl     = build_response_ctrl(pool, req_id, create_nano, ret_code, msg_type);
        auto   *out      = crowdb::rpc::build_out_frame(req_id, msg_type, ctrl, nullptr);
        out->create_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        transport_->submit(conn, out);
    });

    return nullptr;
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
