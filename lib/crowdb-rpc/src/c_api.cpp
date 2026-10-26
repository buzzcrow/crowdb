// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api.h"

#include "crowdb-common/log.h"
#include "crowdb-common/metrics/metrics.h"
#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/client/co_client.h"
#include "crowdb-rpc/client/rpc_client_metrics.h"
#include "crowdb-rpc/rpc_metrics.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport/socket_transport.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>

// All C API functions wrap their body in try/catch(...) to prevent C++
// exceptions from crossing the FFI boundary into Rust, which cannot
// unwind foreign exceptions and would abort the process. This is the
// standard practice for C++ code exposing a C ABI.

// ── Handle wrappers ───────────────────────────────────────────────
// The opaque C handles are wrappers around the C++ objects. The struct
// names match the forward declarations in c_api.h (crowdb_rpc_pool_s, etc.).

// Opaque handle struct definitions are in c_api_internal.h (shared
// with co_client.cpp).

static crowdb_rpc_status submit_status(bool submitted, const crowdb::rpc::Connection *conn)
{
    if (submitted) {
        return CROWDB_RPC_OK;
    }
    return conn != nullptr && !conn->is_open() ? CROWDB_RPC_ERR_CONN_CLOSED : CROWDB_RPC_ERR_SEND_QUEUE;
}

// ── Buffer ────────────────────────────────────────────────────────

crowdb_rpc_buffer_t crowdb_rpc_buffer_alloc(crowdb_rpc_pool_t pool, uint32_t capacity)
{
    try {
        if (pool == nullptr || pool->pool == nullptr) {
            return nullptr;
        }
        crowdb::rpc::Buffer *buf = pool->pool->alloc(capacity);
        if (buf == nullptr) {
            return nullptr;
        }
        return new crowdb_rpc_buffer_s{buf};
    }
    catch (...) {
        return nullptr;
    }
}

void crowdb_rpc_buffer_write(crowdb_rpc_buffer_t buf, const uint8_t *data, uint32_t len)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return;
        }
        buf->buf->write(data, len);
    }
    catch (...) {
    }
}

const uint8_t *crowdb_rpc_buffer_data(crowdb_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return nullptr;
        }
        return buf->buf->payload_data();
    }
    catch (...) {
        return nullptr;
    }
}

uint32_t crowdb_rpc_buffer_len(crowdb_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return 0;
        }
        return buf->buf->len;
    }
    catch (...) {
        return 0;
    }
}

uint32_t crowdb_rpc_buffer_capacity(crowdb_rpc_buffer_t buf)
{
    return (buf != nullptr && buf->buf != nullptr) ? buf->buf->capacity : 0;
}

uint64_t crowdb_rpc_buffer_request_id(crowdb_rpc_buffer_t buf)
{
    return (buf != nullptr && buf->buf != nullptr) ? buf->buf->request_id : 0;
}

uint64_t crowdb_rpc_buffer_stream_id(crowdb_rpc_buffer_t buf)
{
    return (buf != nullptr && buf->buf != nullptr) ? buf->buf->stream_id : 0;
}

uint64_t crowdb_rpc_buffer_sequence(crowdb_rpc_buffer_t buf)
{
    return (buf != nullptr && buf->buf != nullptr) ? buf->buf->sequence : 0;
}

uint64_t crowdb_rpc_buffer_logical_offset(crowdb_rpc_buffer_t buf)
{
    return (buf != nullptr && buf->buf != nullptr) ? buf->buf->logical_offset : 0;
}

uint32_t crowdb_rpc_buffer_registration_id(crowdb_rpc_buffer_t buf)
{
    return (buf != nullptr && buf->buf != nullptr) ? buf->buf->registration_id : 0;
}

int crowdb_rpc_buffer_ownership(crowdb_rpc_buffer_t buf)
{
    return (buf != nullptr && buf->buf != nullptr)
             ? static_cast<int>(buf->buf->ownership.load(std::memory_order_acquire))
             : -1;
}

void crowdb_rpc_buffer_set_metadata(crowdb_rpc_buffer_t buf, uint64_t request_id, uint64_t stream_id, uint64_t sequence,
                                    uint64_t logical_offset, uint32_t registration_id)
{
    if (buf != nullptr && buf->buf != nullptr) {
        buf->buf->set_metadata(request_id, stream_id, sequence, logical_offset, registration_id);
    }
}

int crowdb_rpc_buffer_transition(crowdb_rpc_buffer_t buf, int from, int to)
{
    if (buf == nullptr || buf->buf == nullptr || from < 0 || from > 6 || to < 0 || to > 6) {
        return 0;
    }
    return buf->buf->transition(static_cast<crowdb::rpc::BufferOwnership>(from),
                                static_cast<crowdb::rpc::BufferOwnership>(to))
             ? 1
             : 0;
}

int crowdb_rpc_buffer_quarantine(crowdb_rpc_buffer_t buf)
{
    return (buf != nullptr && buf->buf != nullptr && buf->buf->quarantine()) ? 1 : 0;
}

bool crowdb_rpc_buffer_detach(crowdb_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return false;
        }
        auto *buffer = buf->buf;
        return buffer->pool == nullptr || buffer->pool->detach(buffer);
    }
    catch (...) {
        return false;
    }
}

crowdb_rpc_buffer_t crowdb_rpc_buffer_ref(crowdb_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return nullptr;
        }
        auto clone = std::make_unique<crowdb_rpc_buffer_s>();
        clone->buf = buf->buf->ref_clone();
        return clone->buf == nullptr ? nullptr : clone.release();
    }
    catch (...) {
        return nullptr;
    }
}

void crowdb_rpc_buffer_release(crowdb_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return;
        }
        // All buffers now have a refcount (pool-allocated or standalone).
        // release() decrements and frees on last reference.
        buf->buf->release();
        buf->buf = nullptr;
        delete buf;
    }
    catch (...) {
    }
}

crowdb_rpc_buffer_t crowdb_rpc_buffer_create(const uint8_t *data, uint32_t len)
{
    try {
        if (data == nullptr || len == 0) {
            return nullptr;
        }
        auto *buf = new crowdb::rpc::Buffer;
        buf->data = static_cast<uint8_t *>(std::malloc(len));
        if (buf->data == nullptr) {
            delete buf;
            return nullptr;
        }
        std::memcpy(buf->data, data, len);
        buf->len      = len;
        buf->capacity = len;
        // Allocate a standalone refcount so ref_clone() works (the transport
        // calls ref_clone on send). Pool-allocated buffers get their ref from
        // the pool; standalone buffers need their own.
        buf->ref  = new std::atomic<int32_t>(1);
        buf->pool = nullptr;
        return new crowdb_rpc_buffer_s{buf};
    }
    catch (...) {
        return nullptr;
    }
}

// Create an external buffer wrapping externally-owned memory (zero-copy).
// The data is NOT copied; free_cb(free_ctx) is called on release to drop
// the external owner.
crowdb_rpc_buffer_t crowdb_rpc_buffer_create_external(const uint8_t *data, uint32_t len, void (*free_cb)(void *),
                                                      void *free_ctx)
{
    try {
        if (data == nullptr || len == 0 || free_cb == nullptr) {
            return nullptr;
        }
        auto *buf     = new crowdb::rpc::Buffer;
        buf->data     = const_cast<uint8_t *>(data);
        buf->len      = len;
        buf->capacity = len;
        buf->ref      = new std::atomic<int32_t>(1);
        buf->pool     = nullptr;
        buf->free_cb  = free_cb;
        buf->free_ctx = free_ctx;
        return new crowdb_rpc_buffer_s{buf};
    }
    catch (...) {
        return nullptr;
    }
}

// ── Pool ──────────────────────────────────────────────────────────

crowdb_rpc_pool_t crowdb_rpc_pool_create(uint32_t max_buffers)
{
    try {
        auto *pool = new crowdb::rpc::SystemBufferPool(max_buffers);
        return new crowdb_rpc_pool_s{.pool = pool, .owns = true};
    }
    catch (...) {
        return nullptr;
    }
}

void crowdb_rpc_pool_destroy(crowdb_rpc_pool_t pool)
{
    try {
        if (pool == nullptr) {
            return;
        }
        if (pool->owns && pool->pool != nullptr) {
            delete pool->pool;
        }
        delete pool;
    }
    catch (...) {
    }
}

void crowdb_rpc_client_get_counters(crowdb_rpc_client_t /*client*/, crowdb_rpc_client_counters_t *out)
{
    try {
        if (out == nullptr) {
            return;
        }
        const auto cumulative = [](const auto &counter) { return counter.total() + counter.window(); };
        out->submit_fail      = cumulative(crowdb::rpc::cnt_send_queue_full());
        out->submit_retry     = cumulative(crowdb::rpc::rpc_submit_retry());
        out->resp_missed      = cumulative(crowdb::rpc::rpc_resp_missed());
        out->reaped           = cumulative(crowdb::rpc::rpc_reaped());
    }
    catch (...) {
    }
}

// ── Caller ────────────────────────────────────────────────────────

crowdb_rpc_client_t crowdb_rpc_client_create(void)
{
    try {
        return new crowdb_rpc_client_s{.client = std::make_shared<crowdb::rpc::RpcClient>()};
    }
    catch (...) {
        return nullptr;
    }
}

void crowdb_rpc_client_destroy(crowdb_rpc_client_t client)
{
    try {
        if (client == nullptr) {
            return;
        }
        client->client->clear_handlers();
        client->client->stop_reaper();
        delete client;
    }
    catch (...) {
    }
}

// ── Internal helpers (c_api_internal.h) ───────────────────────────

namespace crowdb::rpc
{

static crowdb_rpc_status rpc_error_to_status(RpcError err)
{
    switch (err) {
    case RpcError::Ok:
        return CROWDB_RPC_OK;
    case RpcError::ConnectionClosed:
        return CROWDB_RPC_ERR_CONN_CLOSED;
    case RpcError::Timeout:
        return CROWDB_RPC_ERR_TIMEOUT;
    case RpcError::SendQueueFull:
        return CROWDB_RPC_ERR_SEND_QUEUE;
    case RpcError::ConnectionError:
        return CROWDB_RPC_ERR_CONN_ERROR;
    default:
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

void invoke_c_complete(crowdb_rpc_on_complete cb, void *user_data, uint64_t request_id, Frame *frame, RpcError err)
{
    if (cb == nullptr) {
        delete frame;
        return;
    }
    crowdb_rpc_buffer_t ctrl_handle = nullptr;
    crowdb_rpc_buffer_t data_handle = nullptr;
    frame_to_c_handles(frame, &ctrl_handle, &data_handle);
    cb(request_id, ctrl_handle, data_handle, rpc_error_to_status(err), user_data);
}

} // namespace crowdb::rpc

void crowdb_rpc_client_attach(crowdb_rpc_client_t client, crowdb_rpc_conn_t conn)
{
    try {
        if (client == nullptr || conn == nullptr) {
            return;
        }
        client->client->attach(conn->conn.get());
        // Connection callbacks must remain valid if the FFI client handle is
        // destroyed before its transport or final connection lease.
        conn->conn->set_on_rpc_close([owner = client->client](crowdb::rpc::Connection *closed) {
            owner->fail_all(closed, crowdb::rpc::RpcError::ConnectionClosed);
        });
    }
    catch (...) {
    }
}

void crowdb_rpc_client_set_completion_pool_size(crowdb_rpc_client_t client, uint32_t max_in_flight)
{
    try {
        if (client == nullptr || max_in_flight == 0) {
            return;
        }
        client->client->set_completion_pool_size(max_in_flight);
    }
    catch (...) {
    }
}

void crowdb_rpc_client_start_reaper(crowdb_rpc_client_t client, uint64_t timeout_ns, uint64_t scan_interval_ns)
{
    try {
        if (client == nullptr || timeout_ns == 0 || scan_interval_ns == 0) {
            return;
        }
        client->client->start_reaper(timeout_ns, scan_interval_ns);
    }
    catch (...) {
    }
}

void crowdb_rpc_client_stop_reaper(crowdb_rpc_client_t client)
{
    try {
        if (client == nullptr) {
            return;
        }
        client->client->stop_reaper();
    }
    catch (...) {
    }
}

void crowdb_rpc_client_dump_pending(crowdb_rpc_client_t client)
{
    try {
        if (client == nullptr) {
            return;
        }
        client->client->dump_pending();
    }
    catch (...) {
    }
}

crowdb_rpc_status crowdb_rpc_client_send(crowdb_rpc_client_t client, crowdb_rpc_server_t server, crowdb_rpc_conn_t conn,
                                         uint64_t request_id, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                         uint16_t msg_type, crowdb_rpc_on_complete on_complete, void *user_data)
{
    try {
        if (client == nullptr || server == nullptr || conn == nullptr || control == nullptr || on_complete == nullptr) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }

        crowdb::rpc::Buffer *ctrl_buf = control->buf;
        crowdb::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;

        // Bump refcount so the client's handle stays valid after submit.
        if (ctrl_buf != nullptr) {
            ctrl_buf->ref_clone();
        }
        if (data_buf != nullptr) {
            data_buf->ref_clone();
        }

        bool ok = client->client->send(server->server->transport(), conn->conn.get(), request_id, ctrl_buf, data_buf,
                                       msg_type, on_complete, user_data);

        // Release the caller's wrapper handles (decrements the ref bumped
        // above and frees the crowdb_rpc_buffer_s struct).
        crowdb_rpc_buffer_release(control);
        if (data != nullptr) {
            crowdb_rpc_buffer_release(data);
        }

        return submit_status(ok, conn->conn.get());
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

uint8_t crowdb_rpc_max_data_views(void)
{
    return crowdb::rpc::MAX_DATA_VIEWS;
}

crowdb_rpc_status crowdb_rpc_client_send_chain(crowdb_rpc_client_t client, crowdb_rpc_server_t server,
                                               crowdb_rpc_conn_t conn, uint64_t request_id, crowdb_rpc_buffer_t control,
                                               crowdb_rpc_buffer_t const *data_views, uint8_t data_view_count,
                                               uint16_t msg_type, crowdb_rpc_on_complete on_complete, void *user_data)
{
    try {
        if (client == nullptr || server == nullptr || conn == nullptr || control == nullptr || data_views == nullptr ||
            data_view_count == 0 || data_view_count > crowdb::rpc::MAX_DATA_VIEWS || on_complete == nullptr) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }

        std::array<crowdb::rpc::Buffer *, crowdb::rpc::MAX_DATA_VIEWS> buffers{};
        uint64_t                                                       total = 0;
        for (uint8_t index = 0; index < data_view_count; ++index) {
            if (data_views[index] == nullptr || data_views[index]->buf == nullptr) {
                return CROWDB_RPC_ERR_INVALID_ARG;
            }
            buffers[index] = data_views[index]->buf;
            total += buffers[index]->len;
        }
        if (total > UINT32_MAX) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }

        control->buf->ref_clone();
        for (uint8_t index = 0; index < data_view_count; ++index) {
            buffers[index]->ref_clone();
        }
        bool ok = client->client->send_chain(server->server->transport(), conn->conn.get(), request_id, control->buf,
                                             buffers.data(), data_view_count, msg_type, on_complete, user_data);

        crowdb_rpc_buffer_release(control);
        for (uint8_t index = 0; index < data_view_count; ++index) {
            crowdb_rpc_buffer_release(data_views[index]);
        }
        return submit_status(ok, conn->conn.get());
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

crowdb_rpc_status crowdb_rpc_client_send_slab(crowdb_rpc_client_t client, crowdb_rpc_server_t server,
                                              crowdb_rpc_conn_t conn, uint64_t request_id, crowdb_rpc_buffer_t control,
                                              crowdb_rpc_buffer_t data, uint16_t msg_type,
                                              crowdb_rpc_on_complete on_complete, void *user_data)
{
    try {
        if (client == nullptr || server == nullptr || conn == nullptr || control == nullptr || on_complete == nullptr) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }

        crowdb::rpc::Buffer *ctrl_buf = control->buf;
        crowdb::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;
        if (ctrl_buf != nullptr) {
            ctrl_buf->ref_clone();
        }
        if (data_buf != nullptr) {
            data_buf->ref_clone();
        }

        bool ok = client->client->send_slab_only(server->server->transport(), conn->conn.get(), request_id, ctrl_buf,
                                                 data_buf, msg_type, on_complete, user_data);
        crowdb_rpc_buffer_release(control);
        if (data != nullptr) {
            crowdb_rpc_buffer_release(data);
        }
        return submit_status(ok, conn->conn.get());
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

// Variant of crowdb_rpc_client_send for server-handler use: conn_handle
// is a raw Connection* (as passed to the dispatch callback), NOT a
// crowdb_rpc_conn_s*. The handler's conn_handle is a Connection* obtained
// from static_cast<void*>(conn) in invoke_c_handler; crowdb_rpc_conn_s
// wraps a shared_ptr<Connection> and is only created by crowdb_rpc_connect.
// Using crowdb_rpc_client_send with a Connection* would dereference invalid
// memory (conn->conn.get() on the wrong struct).
crowdb_rpc_status crowdb_rpc_client_send_conn(crowdb_rpc_client_t client, crowdb_rpc_server_t server, void *conn_handle,
                                              uint64_t request_id, crowdb_rpc_buffer_t control,
                                              crowdb_rpc_buffer_t data, uint16_t msg_type,
                                              crowdb_rpc_on_complete on_complete, void *user_data)
{
    try {
        if (client == nullptr || server == nullptr || conn_handle == nullptr || control == nullptr ||
            on_complete == nullptr) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }

        crowdb::rpc::Buffer *ctrl_buf = control->buf;
        crowdb::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;

        if (ctrl_buf != nullptr) {
            ctrl_buf->ref_clone();
        }
        if (data_buf != nullptr) {
            data_buf->ref_clone();
        }

        auto *conn = static_cast<crowdb::rpc::Connection *>(conn_handle);
        bool  ok   = client->client->send(server->server->transport(), conn, request_id, ctrl_buf, data_buf, msg_type,
                                          on_complete, user_data);

        crowdb_rpc_buffer_release(control);
        if (data != nullptr) {
            crowdb_rpc_buffer_release(data);
        }

        return ok ? CROWDB_RPC_OK : CROWDB_RPC_ERR_SEND_QUEUE;
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

// ── Connection ────────────────────────────────────────────────────

crowdb_rpc_conn_t crowdb_rpc_connect(crowdb_rpc_server_t server, const char *addr, int port)
{
    try {
        if (server == nullptr || addr == nullptr) {
            return nullptr;
        }

        auto conn = server->server->transport()->connect(addr, port);
        if (conn == nullptr) {
            return nullptr;
        }
        return new crowdb_rpc_conn_s{conn};
    }
    catch (...) {
        return nullptr;
    }
}

void crowdb_rpc_conn_destroy(crowdb_rpc_conn_t conn)
{
    try {
        if (conn == nullptr) {
            return;
        }
        delete conn;
    }
    catch (...) {
    }
}

int crowdb_rpc_conn_is_open(crowdb_rpc_conn_t conn)
{
    try {
        return conn != nullptr && conn->conn != nullptr && conn->conn->is_open() ? 1 : 0;
    }
    catch (...) {
        return 0;
    }
}

int crowdb_rpc_conn_connect_refused(crowdb_rpc_conn_t conn)
{
    try {
        return conn != nullptr && conn->conn != nullptr && conn->conn->connect_refused() ? 1 : 0;
    }
    catch (...) {
        return 0;
    }
}

void crowdb_rpc_conn_close(crowdb_rpc_conn_t conn)
{
    try {
        if (conn != nullptr && conn->conn != nullptr) {
            conn->conn->close();
        }
    }
    catch (...) {
    }
}

// ── Built-in echo handler ─────────────────────────────────────────

void crowdb_rpc_server_register_echo_handler(crowdb_rpc_server_t server, uint16_t msg_type)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->register_handler(msg_type, crowdb::rpc::handle_echo);
    }
    catch (...) {
    }
}

// ── Logging ───────────────────────────────────────────────────────

void crowdb_rpc_init_logging(const char *log_dir, const char *level, size_t max_file_mb, size_t max_files,
                             const char *file_prefix)
{
    // If the default logger already exists (crowdb-tree called init_logging
    // first), add a second file sink so rpc messages go to a separate file
    // alongside the tree log. If no logger exists yet, create one — this
    // handles standalone crowdb-rpc usage without crowdb-tree.
    // NOTE: must check g_logger, not g_enabled — g_enabled defaults to true
    // even when init_logging was never called, so the add_log_file branch
    // would be a no-op (g_logger is null) and logs would go to spdlog's
    // default stderr logger.
    if (crowdb::common::logger_initialized()) {
        crowdb::common::add_log_file(log_dir == nullptr ? "" : std::string(log_dir), max_file_mb, max_files,
                                     file_prefix == nullptr ? "crowdb-rpc" : std::string(file_prefix));
    }
    else {
        crowdb::common::init_logging(log_dir == nullptr ? "" : std::string(log_dir),
                                     level == nullptr ? "info" : std::string(level), max_file_mb, max_files,
                                     file_prefix == nullptr ? "crowdb-rpc" : std::string(file_prefix));
    }
}

void crowdb_rpc_flush_logging()
{
    crowdb::common::flush_logging();
}

void crowdb_rpc_add_log_stderr(const char *level)
{
    crowdb::common::add_log_stderr(level == nullptr ? "error" : std::string(level));
}

void crowdb_rpc_shutdown_logging()
{
    crowdb::common::shutdown_logging();
}

void crowdb_rpc_metrics_start(const char *log_path, double interval_secs, size_t max_file_mb, size_t max_files,
                              int console)
{
    crowdb::common::metrics::MetricsRegistry::global().start(
        log_path != nullptr ? std::string(log_path) : std::string(), interval_secs, max_file_mb == 0 ? 30 : max_file_mb,
        max_files == 0 ? 5 : max_files, console != 0);
}

void crowdb_rpc_metrics_stop(void)
{
    crowdb::common::metrics::MetricsRegistry::global().stop();
}

void crowdb_rpc_server_register_conn_count_gauge(crowdb_rpc_server_t server, const char *name)
{
    try {
        if (server == nullptr || name == nullptr) {
            return;
        }
        auto *srv       = server->server;
        auto *transport = srv->transport();
        if (transport == nullptr) {
            return;
        }
        // Capture the server's alive flag (shared_ptr) so the callback
        // returns 0 after the server is destroyed (transport freed in
        // ~RpcServer). The shared_ptr keeps the atomic alive even after
        // the server is deleted.
        auto alive = srv->alive_flag();
        crowdb::common::metrics::MetricsRegistry::global().register_callback_gauge(
            std::string(name), [transport, alive] {
                if (!alive->load(std::memory_order_acquire)) {
                    return uint64_t{0};
                }
                return static_cast<uint64_t>(transport->connection_count());
            });
    }
    catch (...) {
    }
}
