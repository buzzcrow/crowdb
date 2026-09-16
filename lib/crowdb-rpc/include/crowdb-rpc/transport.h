// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/framing.h"

#include <array>
#include <cstdint>

namespace crowdb::rpc
{

// Forward declaration — defined in connection.h. Transport::submit takes
// a Connection* but does not need its full definition.
class Connection;

// ── OutFrame: a frame queued for sending ──────────────────────────
//
// The send queue holds OutFrame*. The worker drains up to BATCH_MAX per
// drain cycle and sends them via scatter-gather (writev). A frame may retain
// a bounded chain of immutable data owners. The first view stays in `data` so
// the existing single-buffer path has no extra indirection or allocation.
// request_id is assigned by RpcClient::call; 0 for one-way messages.
constexpr uint8_t MAX_DATA_VIEWS = 13;

struct OutFrame
{
    uint64_t                                 request_id = 0;
    Header                                   header;
    Buffer                                  *control = nullptr; // pool-allocated; released after send
    Buffer                                  *data    = nullptr; // pool-allocated; nullptr if control-only
    std::array<Buffer *, MAX_DATA_VIEWS - 1> data_tail{};
    uint8_t                                  data_view_count = 0; // zero preserves legacy direct `data` assignment
    uint32_t                                 sent_offset     = 0; // bytes already sent (partial write tracking)
    uint64_t                                 create_nano     = 0; // steady_clock ns at submit/submit_inline

    [[nodiscard]] uint8_t data_views() const
    {
        return data == nullptr ? 0 : (data_view_count == 0 ? 1 : data_view_count);
    }

    [[nodiscard]] Buffer *data_view(uint8_t index) const
    {
        return index == 0 ? data : data_tail[index - 1];
    }

    void release_data()
    {
        const uint8_t count = data_views();
        for (uint8_t index = 0; index < count; ++index) {
            data_view(index)->release();
        }
        data            = nullptr;
        data_view_count = 0;
        data_tail.fill(nullptr);
    }
};

constexpr int BATCH_MAX        = 64;
constexpr int MAX_FRAME_IOVECS = 2 + MAX_DATA_VIEWS; // header + control + data views
constexpr int MAX_BATCH_IOVECS = BATCH_MAX * MAX_FRAME_IOVECS;
static_assert(MAX_BATCH_IOVECS <= 1024, "RPC writev batch exceeds supported IOV_MAX");

// ── Transport interface ───────────────────────────────────────────
//
// Isolates the I/O loop divergence between TCP (epoll/kqueue) and RDMA.
// Framing, correlation, pooling, and handler dispatch are shared.
class Transport
{
  public:
    virtual ~Transport() = default;

    // Submit an OutFrame on a connection (non-blocking). Pushes to the
    // send queue and wakes the worker. Returns true on success, false if
    // the queue is full (backpressure) or the connection is closed. A true
    // return transfers frame ownership to the transport; on false, ownership
    // remains with the caller.
    // The caller (RpcClient) builds the OutFrame with request_id,
    // header, and pool-allocated control/data buffers already set.
    virtual bool submit(Connection *conn, OutFrame *frame) = 0;

    // Register a buffer for this transport. TCP: noop (returns same ptr).
    // RDMA: ibv_reg_mr, returns the MR-backed Buffer.
    virtual Buffer *register_buffer(Buffer *buf) = 0;

    // Shutdown the transport.
    virtual void shutdown() = 0;
};

} // namespace crowdb::rpc
