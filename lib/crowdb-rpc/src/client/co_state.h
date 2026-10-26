// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/client/co_client.h"

#include <atomic>
#include <coroutine>
#include <cstdlib>

namespace crowdb::rpc
{

class RpcClient;
class Transport;
class Connection;

enum CoStateFlag : int { CO_IDLE, CO_RUNNING, CO_SUSPENDED, CO_RESPONSE_READY };

struct CoState
{
    crowdb_rpc_co_build_request build_fn       = nullptr;
    crowdb_rpc_co_on_response   on_response_fn = nullptr;
    void                       *rust_ctx       = nullptr;
    RpcClient                  *client         = nullptr;
    Transport                  *transport      = nullptr;
    Connection                 *conn           = nullptr;
    uint16_t                    msg_type       = 0;

    uint64_t total_ops        = 0;
    uint64_t total_errors     = 0;
    uint64_t total_latency_ns = 0;
    uint64_t min_latency_ns   = UINT64_MAX;
    uint64_t max_latency_ns   = 0;

    crowdb_rpc_buffer_t     resp_control = nullptr;
    crowdb_rpc_buffer_t     resp_data    = nullptr;
    crowdb_rpc_status       resp_status  = CROWDB_RPC_OK;
    std::coroutine_handle<> handle;
    std::atomic<uint32_t>  *finished_count = nullptr;
    std::atomic<uint32_t>   active_callbacks{0};
    std::atomic<int>        co_state{CO_IDLE};
    size_t                  slot_index  = 0;
    size_t                  pool_mask   = 0;
    uint64_t                next_req_id = 0;
};

inline void co_drop_response(crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data)
{
    if (control != nullptr) {
        crowdb_rpc_buffer_release(control);
    }
    if (data != nullptr) {
        crowdb_rpc_buffer_release(data);
    }
}

struct CoCallbackOperation
{
    CoState *state;

    explicit CoCallbackOperation(CoState *owner) : state(owner)
    {
        state->active_callbacks.fetch_add(1, std::memory_order_acquire);
    }

    ~CoCallbackOperation()
    {
        state->active_callbacks.fetch_sub(1, std::memory_order_release);
    }

    CoCallbackOperation(const CoCallbackOperation &)            = delete;
    CoCallbackOperation &operator=(const CoCallbackOperation &) = delete;
};

// RpcClient claims each request completion once. Publication must be one
// atomic operation: a failed CAS followed by store could miss a concurrent
// RUNNING -> SUSPENDED transition and leave the coroutine asleep forever.
inline void co_on_complete(uint64_t, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data, crowdb_rpc_status status,
                           void *user_data)
{
    auto                     *state = static_cast<CoState *>(user_data);
    const CoCallbackOperation operation(state);
    if (state->co_state.load(std::memory_order_acquire) == CO_IDLE) {
        co_drop_response(control, data);
        return;
    }
    state->resp_control = control;
    state->resp_data    = data;
    state->resp_status  = status;
    const int previous  = state->co_state.exchange(CO_RESPONSE_READY, std::memory_order_acq_rel);
    if (previous == CO_SUSPENDED) {
        state->handle.resume();
    }
}

struct CoAwait
{
    CoState *state;

    [[nodiscard]] constexpr bool await_ready() const noexcept
    {
        return false;
    }

    bool await_suspend(std::coroutine_handle<> handle) const noexcept
    {
        state->handle = handle;
        int expected  = CO_RUNNING;
        return state->co_state.compare_exchange_strong(expected, CO_SUSPENDED, std::memory_order_acq_rel);
    }

    void await_resume() const noexcept
    {
        state->co_state.store(CO_RUNNING, std::memory_order_release);
    }
};

struct CoFinalAwait
{
    CoState *state;

    [[nodiscard]] constexpr bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<>) const noexcept
    {
        // The coroutine is suspended before publication. Do not access the
        // frame after publishing; the joining thread may destroy it.
        state->co_state.store(CO_IDLE, std::memory_order_release);
        state->finished_count->fetch_add(1, std::memory_order_release);
    }

    void await_resume() const noexcept
    {
    }
};

struct CoTask
{
    struct promise_type
    {
        CoState *state;

        explicit promise_type(CoState *owner) : state(owner)
        {
        }

        CoTask get_return_object()
        {
            return CoTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept
        {
            return {};
        }

        CoFinalAwait final_suspend() noexcept
        {
            return {state};
        }

        void return_void() noexcept
        {
        }

        void unhandled_exception() noexcept
        {
            std::abort();
        }
    };

    std::coroutine_handle<promise_type> handle;
};

} // namespace crowdb::rpc
