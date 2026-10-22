// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_preflight.h"

#include "crowdb-rpc/framing.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <utility>

namespace crowdb::rpc
{
namespace
{
using Clock = std::chrono::steady_clock;

bool wait_for(int fd, short events, Clock::time_point deadline)
{
    for (;;) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (remaining <= 0)
            return false;
        pollfd    descriptor{fd, events, 0};
        const int result = ::poll(&descriptor, 1, static_cast<int>(std::min<int64_t>(remaining, INT32_MAX)));
        if (result > 0)
            return (descriptor.revents & events) != 0;
        if (result == 0 || errno != EINTR)
            return false;
    }
}

bool transfer(int fd, std::span<uint8_t> bytes, bool writing, Clock::time_point deadline)
{
    while (!bytes.empty()) {
        int flags = 0;
#ifdef MSG_NOSIGNAL
        flags = MSG_NOSIGNAL;
#endif
        const auto count =
            writing ? ::send(fd, bytes.data(), bytes.size(), flags) : ::recv(fd, bytes.data(), bytes.size(), 0);
        if (count > 0) {
            bytes = bytes.subspan(static_cast<size_t>(count));
            continue;
        }
        if (count == 0)
            return false;
        if (errno == EINTR)
            continue;
        if ((errno != EAGAIN && errno != EWOULDBLOCK) || !wait_for(fd, writing ? POLLOUT : POLLIN, deadline))
            return false;
    }
    return true;
}
} // namespace

RdmaPreflight::~RdmaPreflight()
{
    if (fd >= 0)
        ::close(fd);
}

int RdmaPreflight::release_fd()
{
    return std::exchange(fd, -1);
}

std::unique_ptr<RdmaPreflight> RdmaPreflight::connect(const std::string &address, int port,
                                                      const RdmaEndpointCapability &local,
                                                      const std::string &source_address, uint32_t timeout_ms)
{
    if (port <= 0 || port > 65535 || timeout_ms == 0)
        return nullptr;
    auto connection = std::make_unique<RdmaPreflight>();
    connection->fd  = ::socket(AF_INET, SOCK_STREAM, 0);
    if (connection->fd < 0 || ::fcntl(connection->fd, F_SETFL, O_NONBLOCK) != 0)
        return nullptr;
#ifdef SO_NOSIGPIPE
    int nosigpipe = 1;
    ::setsockopt(connection->fd, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, sizeof(nosigpipe));
#endif
    sockaddr_in peer{};
    peer.sin_family = AF_INET;
    peer.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, address.c_str(), &peer.sin_addr) != 1)
        return nullptr;
    if (!source_address.empty()) {
        sockaddr_in source{};
        source.sin_family = AF_INET;
        if (::inet_pton(AF_INET, source_address.c_str(), &source.sin_addr) != 1 ||
            ::bind(connection->fd, reinterpret_cast<sockaddr *>(&source), sizeof(source)) != 0)
            return nullptr;
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    const int  result   = ::connect(connection->fd, reinterpret_cast<sockaddr *>(&peer), sizeof(peer));
    if (result != 0 && (errno != EINPROGRESS || !wait_for(connection->fd, POLLOUT, deadline)))
        return nullptr;
    int       error = 0;
    socklen_t size  = sizeof(error);
    if (::getsockopt(connection->fd, SOL_SOCKET, SO_ERROR, &error, &size) != 0 || error != 0)
        return nullptr;

    Header header;
    header.msg_type = RDMA_HANDSHAKE_MSG_TYPE;
    header.msg_size = RDMA_HANDSHAKE_SIZE;
    header.flags    = FLAG_ONE_WAY;
    std::array<uint8_t, HEADER_SIZE + RDMA_HANDSHAKE_SIZE> request{};
    serialize_header(request.data(), header);
    const auto capability = serialize_rdma_handshake(local);
    std::copy(capability.begin(), capability.end(), request.begin() + HEADER_SIZE);
    if (!transfer(connection->fd, request, true, deadline))
        return nullptr;

    std::array<uint8_t, HEADER_SIZE> response_header{};
    if (!transfer(connection->fd, response_header, false, deadline))
        return nullptr;
    header = parse_header(response_header.data());
    if (header.magic != MAGIC || header.msg_type != RDMA_HANDSHAKE_MSG_TYPE || header.msg_offset != HEADER_SIZE ||
        header.data_size != 0 || header.msg_size < RDMA_LEGACY_HANDSHAKE_SIZE || header.msg_size > RDMA_HANDSHAKE_SIZE)
        return nullptr;
    std::array<uint8_t, RDMA_HANDSHAKE_SIZE> response{};
    const auto                               bytes = std::span(response).first(header.msg_size);
    if (!transfer(connection->fd, bytes, false, deadline) || !parse_rdma_handshake(bytes, &connection->peer))
        return nullptr;
    return connection;
}
} // namespace crowdb::rpc
