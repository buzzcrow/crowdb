// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/client/co_client.h"

#include "co_state.h"
#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/client/rpc_client_metrics.h"
#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/rpc_metrics.h"
#include "crowdb-rpc/transport.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

namespace crowdb::rpc
{

// The coroutine body. Captures CoState* by value (it's a pointer,
// stable on the heap). Loops: build → submit → co_await → on_response.
//
// This is a coroutine function (contains co_await). The compiler
// generates the coroutine frame + state machine. The frame is
// heap-allocated on first suspend (one alloc per coroutine, not per-op).
static CoTask co_run(CoState *s)
{
    s->co_state.store(CO_RUNNING, std::memory_order_release);
    while (true) {
        // 1. Build request via Rust callback.
        crowdb_rpc_buffer_t control = nullptr;
        crowdb_rpc_buffer_t data    = nullptr;
        // Per-coroutine request_id: slot_index + N * pool_size.
        // Guarantees no slab slot collision between coroutines.
        uint64_t req_id = s->slot_index + (s->next_req_id * (s->pool_mask + 1));
        s->next_req_id++;
        if (!s->build_fn(s->rust_ctx, req_id, &control, &data)) {
            break;
        }

        // 2. Record start time (after build, before submit).
        // Measures submission, transport and response parsing on the client
        // clock. Request construction and the Rust response handler are outside.
        auto start = std::chrono::steady_clock::now();

        // 3. Submit via send(). user_data = s (CoState*).
        //    co_on_complete will fill s->resp_* and resume.
        //    Extract the inner Buffer* and free the wrapper struct —
        //    send() takes ownership of the Buffer* (the frame holds the
        //    only ref and release_frame frees it). The wrapper would
        //    leak if kept, and releasing it after send would double-free
        //    the inner Buffer (send already released it on failure).
        Buffer *ctrl_buf = (control != nullptr) ? control->buf : nullptr;
        Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;
        if (control != nullptr) {
            control->buf = nullptr;
            delete control;
        }
        if (data != nullptr) {
            data->buf = nullptr;
            delete data;
        }
        bool ok = s->client->send(s->transport, s->conn, req_id, ctrl_buf, data_buf, s->msg_type, co_on_complete, s);

        if (!ok) {
            // Submit failed (send queue full) — send() already released
            // the inner Buffer* via release_frame. Just yield and retry.
            s->total_errors++;
            rpc_submit_retry().inc();
            if (!s->conn->is_open()) {
                break;
            }
            std::this_thread::yield();
            continue;
        }

        // 4. Suspend. co_on_complete resumes us when the response
        //    arrives. After resume, s->resp_* has the response.
        co_await CoAwait{s};

        // 5. Process response via Rust callback.
        auto elapsed    = std::chrono::steady_clock::now() - start;
        auto elapsed_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());

        // Observe e2e latency into crowdb-common histogram (client clock).
        hist_e2e().observe(elapsed_ns);

        bool keep_going =
            s->on_response_fn(s->rust_ctx, req_id, s->resp_control, s->resp_data, s->resp_status, elapsed_ns);

        // Release the response buffers.
        if (s->resp_control != nullptr) {
            crowdb_rpc_buffer_release(s->resp_control);
        }
        if (s->resp_data != nullptr) {
            crowdb_rpc_buffer_release(s->resp_data);
        }

        // Update stats.
        s->total_ops++;
        if (s->resp_status != CROWDB_RPC_OK) {
            s->total_errors++;
        }
        s->total_latency_ns += elapsed_ns;
        s->min_latency_ns = std::min(elapsed_ns, s->min_latency_ns);
        s->max_latency_ns = std::max(elapsed_ns, s->max_latency_ns);

        if (!keep_going) {
            break;
        }
    }
    // Mark idle before final_suspend — late responses will see
    // CO_IDLE and drop (avoiding UB on a done coroutine).
    s->co_state.store(CO_IDLE, std::memory_order_release);
}

} // namespace crowdb::rpc

// ── C API ─────────────────────────────────────────────────────────

extern "C" void crowdb_rpc_co_spawn(crowdb_rpc_client_t client, crowdb_rpc_server_t server, crowdb_rpc_conn_t *conns,
                                    size_t num_conns, uint32_t num_coroutines, uint16_t msg_type,
                                    crowdb_rpc_co_build_request build_request, crowdb_rpc_co_on_response on_response,
                                    void *ctx)
{
    if (client == nullptr || server == nullptr || conns == nullptr || num_conns == 0) {
        return;
    }

    auto *client_ptr = client->client.get();
    auto *transport  = server->server->transport();

    std::atomic<uint32_t> finished_count{0};

    // Create + start N coroutines.
    std::vector<crowdb::rpc::CoTask> tasks;
    tasks.reserve(num_coroutines);

    std::vector<std::unique_ptr<crowdb::rpc::CoState>> states;
    states.reserve(num_coroutines);

    // Compute pool size (next power of two >= num_coroutines).
    size_t pool_size = 1;
    while (pool_size < num_coroutines) {
        pool_size *= 2;
    }
    size_t pool_mask = pool_size - 1;

    for (uint32_t i = 0; i < num_coroutines; i++) {
        auto state            = std::make_unique<crowdb::rpc::CoState>();
        state->build_fn       = build_request;
        state->on_response_fn = on_response;
        state->rust_ctx       = ctx;
        state->client         = client_ptr;
        state->transport      = transport;
        state->conn           = conns[i % num_conns]->conn.get();
        state->msg_type       = msg_type;
        state->finished_count = &finished_count;
        state->slot_index     = i;
        state->pool_mask      = pool_mask;
        state->next_req_id    = 0;

        auto *state_ptr = state.get();
        states.push_back(std::move(state));

        // Create the coroutine (starts suspended).
        auto task = crowdb::rpc::co_run(state_ptr);
        tasks.push_back(std::move(task));
    }

    // Start all coroutines. Each runs until its first co_await, then
    // suspends (yields the I/O worker thread back to epoll_wait).
    // Resume in small batches with a yield between batches to avoid
    // overwhelming the send queue during startup.
    constexpr size_t RESUME_BATCH = 16;
    constexpr auto   BATCH_DELAY  = std::chrono::microseconds(500);
    for (size_t i = 0; i < tasks.size(); i++) {
        tasks[i].handle.resume();
        if ((i + 1) % RESUME_BATCH == 0 && i + 1 < tasks.size()) {
            std::this_thread::sleep_for(BATCH_DELAY);
        }
    }

    // Wait for all coroutines to complete. The coroutines exit when
    // build_fn or on_response_fn returns false (Rust sets this when
    // the deadline is reached).
    while (finished_count.load(std::memory_order_acquire) != num_coroutines) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // Final suspension publishes statistics, but a completing callback can
    // still be unwinding after resume(). Keep both state and Rust context
    // alive until every callback has returned. Request timeouts use RpcClient's
    // reaper and ordinary completion path; never resume a pending await here.
    for (const auto &state : states) {
        while (state->active_callbacks.load(std::memory_order_acquire) != 0) {
            std::this_thread::yield();
        }
    }

    // Aggregate stats.
    uint64_t total_ops        = 0;
    uint64_t total_errors     = 0;
    uint64_t total_latency_ns = 0;
    uint64_t min_latency_ns   = UINT64_MAX;
    uint64_t max_latency_ns   = 0;
    for (auto &state : states) {
        total_ops += state->total_ops;
        total_errors += state->total_errors;
        total_latency_ns += state->total_latency_ns;
        min_latency_ns = std::min(state->min_latency_ns, min_latency_ns);
        max_latency_ns = std::max(state->max_latency_ns, max_latency_ns);
    }
    client->co_stats.total_ops        = total_ops;
    client->co_stats.total_errors     = total_errors;
    client->co_stats.total_latency_ns = total_latency_ns;
    client->co_stats.min_latency_ns   = (total_ops > 0) ? min_latency_ns : 0;
    client->co_stats.max_latency_ns   = max_latency_ns;

    // Destroy the coroutines (frees the coroutine frames).
    for (auto &task : tasks) {
        task.handle.destroy();
    }
}

extern "C" void crowdb_rpc_co_get_stats(crowdb_rpc_client_t client, crowdb_rpc_co_stats_t *out)
{
    if (out == nullptr || client == nullptr) {
        return;
    }
    out->total_ops        = client->co_stats.total_ops;
    out->total_errors     = client->co_stats.total_errors;
    out->total_latency_ns = client->co_stats.total_latency_ns;
    out->min_latency_ns   = client->co_stats.min_latency_ns;
    out->max_latency_ns   = client->co_stats.max_latency_ns;
}
