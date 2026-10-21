// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/c_api.h"

#include <atomic>
#include <utility>

namespace crowdb::tree::detail
{

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

inline void rpc_complete(uint64_t /*unused*/, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                         crowdb_rpc_status status, void *context)
{
    auto *state           = static_cast<RpcCallState *>(context);
    state->result.status  = status;
    state->result.control = control;
    state->result.data    = data;
    state->done.store(true, std::memory_order_release);
    state->done.notify_one();
}

} // namespace crowdb::tree::detail
