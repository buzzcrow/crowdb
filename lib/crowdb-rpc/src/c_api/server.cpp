// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/server/server.h"

#include "crowdb-rpc/c_api_internal.h"

// ── Server ────────────────────────────────────────────────────────

crowdb_rpc_server_t crowdb_rpc_server_create(crowdb_rpc_pool_t pool)
{
    try {
        crowdb::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
        return new crowdb_rpc_server_s{new crowdb::rpc::RpcServer(bp, 1, 1)};
    }
    catch (...) {
        return nullptr;
    }
}

crowdb_rpc_server_t crowdb_rpc_server_create_with_workers(crowdb_rpc_pool_t pool, uint32_t num_workers)
{
    try {
        crowdb::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
        return new crowdb_rpc_server_s{new crowdb::rpc::RpcServer(bp, 1, num_workers)};
    }
    catch (...) {
        return nullptr;
    }
}

crowdb_rpc_server_t crowdb_rpc_server_create_with_engines(crowdb_rpc_pool_t pool, uint32_t io_engines,
                                                          uint32_t io_workers)
{
    return crowdb_rpc_server_create_with_layout(pool, io_engines, io_workers, 0, crowdb::rpc::RDMA_DATAPATH_CAPACITY);
}

crowdb_rpc_server_t crowdb_rpc_server_create_with_layout(crowdb_rpc_pool_t pool, uint32_t io_engines,
                                                         uint32_t io_workers, uint32_t wire_layout,
                                                         uint32_t data_capacity)
{
    try {
        const auto config =
            crowdb::rpc::RdmaConfig::for_layout(static_cast<crowdb::rpc::RdmaWireLayout>(wire_layout), data_capacity);
        if (!config.valid() || io_engines == 0 || io_workers == 0 || io_workers % io_engines != 0)
            return nullptr;
        crowdb::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
        return new crowdb_rpc_server_s{new crowdb::rpc::RpcServer(bp, io_engines, io_workers, config)};
    }
    catch (...) {
        return nullptr;
    }
}

void crowdb_rpc_server_set_send_queue_capacity(crowdb_rpc_server_t server, uint32_t capacity)
{
    try {
        if (server == nullptr || capacity == 0) {
            return;
        }
        server->server->transport()->set_send_queue_capacity(capacity);
    }
    catch (...) {
    }
}

void crowdb_rpc_server_set_tcp_nodelay(crowdb_rpc_server_t server, int enabled)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->transport()->set_tcp_nodelay(enabled != 0);
    }
    catch (...) {
    }
}

void crowdb_rpc_server_set_quickack(crowdb_rpc_server_t server, int enabled)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->transport()->set_quickack(enabled != 0);
    }
    catch (...) {
    }
}

void crowdb_rpc_server_set_event_write(crowdb_rpc_server_t server, int enabled)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->transport()->set_event_write(enabled != 0);
    }
    catch (...) {
    }
}

void crowdb_rpc_server_destroy(crowdb_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return;
        }
        delete server->server;
        delete server;
    }
    catch (...) {
    }
}

void crowdb_rpc_server_clear_handlers(crowdb_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->clear_handlers();
    }
    catch (...) {
    }
}

static void copy_latency(crowdb_rpc_latency_stats_t *out, const crowdb::rpc::LatencyHistogram &h)
{
    out->count  = h.count.load(std::memory_order_relaxed);
    out->sum_ns = h.sum_ns.load(std::memory_order_relaxed);
    out->min_ns = h.min_ns.load(std::memory_order_relaxed);
    out->max_ns = h.max_ns.load(std::memory_order_relaxed);
}

void crowdb_rpc_server_transport_stats(crowdb_rpc_server_t server, crowdb_rpc_transport_stats_t *out)
{
    try {
        if (server == nullptr || out == nullptr) {
            return;
        }
        auto *t = server->server->transport();
        if (t == nullptr) {
            return;
        }
        auto &s = t->stats();
        copy_latency(&out->submit_to_writev, s.submit_to_writev);
        out->send_queue_rejects = s.send_queue_rejects.load(std::memory_order_relaxed);
    }
    catch (...) {
    }
}

crowdb_rpc_status crowdb_rpc_server_listen(crowdb_rpc_server_t server, const char *addr, int port)
{
    try {
        if (server == nullptr || addr == nullptr) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }
        if (!server->server->listen(addr, port)) {
            return CROWDB_RPC_ERR_CONN_ERROR;
        }
        return CROWDB_RPC_OK;
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

void crowdb_rpc_server_start(crowdb_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->start();
    }
    catch (...) {
    }
}

void crowdb_rpc_server_stop(crowdb_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->stop();
    }
    catch (...) {
    }
}

int crowdb_rpc_server_port(crowdb_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return 0;
        }
        return server->server->listen_port();
    }
    catch (...) {
        return 0;
    }
}
