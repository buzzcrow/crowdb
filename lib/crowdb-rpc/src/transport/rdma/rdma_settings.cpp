// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/socket_transport.h"

#include <charconv>
#include <cstdlib>
#include <cstring>
#include <string_view>

namespace crowdb::rpc
{
namespace
{
bool read_service_port(const char *name, uint32_t &value)
{
    const char *text = std::getenv(name);
    if (text == nullptr || *text == '\0')
        return true;
    const std::string_view input(text);
    const auto             result = std::from_chars(input.data(), input.data() + input.size(), value);
    return result.ec == std::errc{} && result.ptr == input.data() + input.size() && value <= 65535;
}
} // namespace

bool SocketTransport::listen_rdma_service(const std::string &address, int tcp_port)
{
    uint32_t    port   = static_cast<uint32_t>(tcp_port);
    const char *source = nullptr;
    if (rdma_explicit_configuration_) {
        source = rdma_config_.source_address.empty() ? nullptr : rdma_config_.source_address.c_str();
    }
    else {
        const char *override_port = std::getenv("CROWDB_RDMA_E2E_PORT");
        uint32_t    offset        = 0;
        uint32_t    service_slot  = 0;
        if (override_port != nullptr && *override_port != '\0') {
            if (!read_service_port("CROWDB_RDMA_E2E_PORT", port))
                return false;
        }
        else if (!read_service_port("CROWDB_PORT_OFFSET", offset)) {
            return false;
        }
        if (!read_service_port("CROWDB_RDMA_SERVICE_SLOT", service_slot) ||
            static_cast<uint64_t>(port) + offset + service_slot > 65535)
            return false;
        port += offset + service_slot;
        source = std::getenv("CROWDB_RDMA_SOURCE_ADDR");
    }
    return port != 0 && port <= 65535 && listen_rdma(source == nullptr ? address : source, static_cast<int>(port));
}

bool SocketTransport::configure_rdma(RdmaConfig config, bool enabled, bool allow_tcp_fallback, bool listen_enabled)
{
    if (started_.load(std::memory_order_acquire) || connection_count() != 0 || !config.valid())
        return false;
    config.use_environment       = false;
    config.require_tcp_preflight = true;
#ifdef CROWDB_RPC_HAVE_RDMA
    std::unique_ptr<RdmaTransport> replacement;
    if (enabled) {
        replacement = std::make_unique<RdmaTransport>(config, pool_);
        if (!replacement->ready())
            return false;
    }
    rdma_transport_ = std::move(replacement);
    if (rdma_transport_ != nullptr) {
        for (const auto &engine : engines_)
            rdma_transport_->register_events(engine.get());
        rdma_transport_->set_connection_handler([this](const std::shared_ptr<Connection> &connection) {
            attach_rdma_connection(connection);
            if (rdma_accept_handler_)
                rdma_accept_handler_(connection);
        });
        set_external_handler([this](uint64_t tag) { rdma_transport_->handle_external_event(tag); });
    }
    else {
        set_external_handler({});
    }
#else
    if (enabled)
        return false;
#endif
    rdma_config_                 = std::move(config);
    rdma_enabled_                = enabled;
    allow_tcp_fallback_          = allow_tcp_fallback;
    rdma_listen_enabled_         = enabled && listen_enabled;
    rdma_explicit_configuration_ = true;
    return true;
}

bool SocketTransport::rdma_listener_enabled() const
{
    if (rdma_explicit_configuration_)
        return rdma_listen_enabled_;
    const auto *enabled = std::getenv("CROWDB_RDMA_ENABLED");
    const auto *listen  = std::getenv("CROWDB_RDMA_LISTEN");
    return enabled != nullptr && std::strcmp(enabled, "1") == 0 && (listen == nullptr || std::strcmp(listen, "0") != 0);
}
} // namespace crowdb::rpc
