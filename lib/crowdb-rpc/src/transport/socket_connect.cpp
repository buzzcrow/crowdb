// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/log.h"
#include "crowdb-rpc/transport/socket_transport.h"
#include "rdma/rdma_preflight.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace crowdb::rpc
{
std::shared_ptr<Connection> SocketTransport::connect(const std::string &addr, int port)
{
    if (rdma_explicit_configuration_ && !rdma_enabled_)
        return connect_tcp(addr, port);
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr && rdma_transport_->ready()) {
        auto &stats = rdma_transport_->stats();
        stats.preflight_attempts.fetch_add(1, std::memory_order_relaxed);
        auto probe = RdmaPreflight::connect(addr, port, local_rdma_capability(), rdma_transport_->source_address());
        if (probe != nullptr &&
            validate_rdma_capability(probe->peer, RDMA_PROTOCOL_VERSION, probe->peer.generation, {}) &&
            probe->peer.wire_layout == rdma_transport_->config().wire_layout) {
            const auto &cm_address  = probe->peer.cm_address;
            const auto  destination = cm_address == "0.0.0.0" ? addr : cm_address;
            auto        connection  = rdma_transport_->connect(destination, probe->peer.port, &probe->peer);
            if (connection != nullptr) {
                for (auto &engine : engines_)
                    rdma_transport_->register_events(engine.get());
                attach_rdma_connection(connection);
                return connection;
            }
        }
        else {
            stats.preflight_failures.fetch_add(1, std::memory_order_relaxed);
        }
        if (!allow_tcp_fallback_) {
            CRB_LOG_WARN("RDMA preflight/setup failed; TCP fallback disabled addr={} port={}", addr, port);
            return nullptr;
        }
        stats.fallback_count.fetch_add(1, std::memory_order_relaxed);
        if (probe != nullptr) {
            const int fd = probe->release_fd();
            prepare_tcp_socket(fd);
            auto connection = create_connection(fd, addr + ":" + std::to_string(port));
            connection->set_peer_capability(probe->peer);
            return connection;
        }
        return connect_tcp(addr, port);
    }
#endif
    if (!allow_tcp_fallback_) {
        CRB_LOG_WARN("RDMA connection unavailable; TCP fallback disabled addr={} port={}", addr, port);
        return nullptr;
    }
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr)
        rdma_transport_->stats().fallback_count.fetch_add(1, std::memory_order_relaxed);
#endif
    return connect_tcp(addr, port);
}

void SocketTransport::prepare_tcp_socket(int fd) const
{
    int nodelay = tcp_nodelay_ ? 1 : 0;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#ifdef __linux__
    if (quickack_) {
        int quickack = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
    }
#endif
}

std::shared_ptr<Connection> SocketTransport::connect_tcp(const std::string &addr, int port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        CRB_LOG_WARN("connect: socket() failed addr={} port={} errno={} ({})", addr, port, errno, std::strerror(errno));
        return nullptr;
    }

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) <= 0) {
        CRB_LOG_WARN("connect: inet_pton failed addr={} port={} errno={} ({})", addr, port, errno,
                     std::strerror(errno));
        ::close(fd);
        return nullptr;
    }

    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        CRB_LOG_WARN("connect: nonblocking setup failed addr={} port={} errno={} ({})", addr, port, errno,
                     std::strerror(errno));
        ::close(fd);
        return nullptr;
    }

    // A missing peer must not block the caller's executor while TCP retries
    // its handshake. Queued writes wait for writability in the worker;
    // handshake failures use the same error/close path as established peers.
    if (::connect(fd, reinterpret_cast<struct sockaddr *>(&sa), sizeof(sa)) < 0 && errno != EINPROGRESS) {
        CRB_LOG_WARN("connect: connect() failed addr={} port={} errno={} ({})", addr, port, errno,
                     std::strerror(errno));
        ::close(fd);
        return nullptr;
    }

    prepare_tcp_socket(fd);

    auto conn = create_connection(fd, addr + ":" + std::to_string(port));
    send_rdma_handshake(conn);
    CRB_LOG_INFO("rpc transport: connection initiated -> {}:{} conn_id={}", addr, port,
                 static_cast<long long>(conn->id()));
    return conn;
}

} // namespace crowdb::rpc
