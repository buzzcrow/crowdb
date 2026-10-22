// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/server/server.h"

#include "crowdb-common/log.h"
#include "crowdb-rpc/client/client.h" // RpcClient, RpcError (for request_client_ + fail_all)
#include "crowdb-rpc/rpc_metrics.h"
#include "crowdb-rpc/server/handler.h"
#include "crowdb-rpc/transport/rdma/rdma_capability.h"
#include "msg_type_generated.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_set>

namespace crowdb::rpc
{

namespace
{
std::mutex                      active_listeners_mutex;
std::unordered_set<std::string> active_listeners;

std::string listener_key(const std::string &addr, int port)
{
    return addr + ":" + std::to_string(port);
}
} // namespace

RpcServer::RpcServer(BufferPool *pool, uint32_t io_engines, uint32_t io_workers, RdmaConfig rdma_config)
    : pool_(pool),
      owns_pool_(pool == nullptr)
{
    if (pool_ == nullptr) {
        pool_ = new SystemBufferPool();
    }
    transport_ = std::make_unique<SocketTransport>(io_engines, io_workers, pool_, std::move(rdma_config));
}

RpcServer::~RpcServer()
{
    stop();
    // Mark as dead before destroying the transport so that any
    // callback gauge (e.g. conn_count_gauge) that captures a raw
    // transport pointer returns 0 instead of accessing freed memory.
    alive_->store(false, std::memory_order_release);
    transport_.reset();
    if (owns_pool_) {
        delete pool_;
    }
}

bool RpcServer::listen(const std::string &addr, int port)
{
    listen_addr_ = addr.empty() ? "0.0.0.0" : addr;
    listen_fd_   = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        const int error = errno;
        CRB_LOG_ERROR("rpc server socket failed: addr={} port={} errno={} ({})", addr, port, error,
                      std::strerror(error));
        return false;
    }

    int opt = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        const int error = errno;
        CRB_LOG_ERROR("rpc server SO_REUSEADDR failed; continuing without address reuse: addr={} port={} errno={} ({})",
                      addr, port, error, std::strerror(error));
    }
#ifdef SO_REUSEPORT
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        const int error = errno;
        CRB_LOG_ERROR("rpc server SO_REUSEPORT failed: addr={} port={} errno={} ({})", addr, port, error,
                      std::strerror(error));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
#endif

    if (port != 0) {
        const std::string lock_path = "/tmp/crowdb-rpc-port-" + std::to_string(port) + ".lock";
        listener_lock_fd_           = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0600);
        if (listener_lock_fd_ < 0 || ::flock(listener_lock_fd_, LOCK_EX | LOCK_NB) < 0) {
            const int error = errno;
            CRB_LOG_ERROR("rpc server listener lease failed: port={} errno={} ({})", port, error, std::strerror(error));
            if (listener_lock_fd_ >= 0) {
                ::close(listener_lock_fd_);
                listener_lock_fd_ = -1;
            }
            ::close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }
    }

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(static_cast<uint16_t>(port));
    if (addr == "0.0.0.0" || addr.empty()) {
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    else {
        ::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr);
    }

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&sa), sizeof(sa)) < 0) {
        const int error = errno;
        CRB_LOG_ERROR("rpc server bind failed: addr={} port={} errno={} ({})", addr, port, error, std::strerror(error));
        ::close(listen_fd_);
        listen_fd_ = -1;
        if (listener_lock_fd_ >= 0) {
            ::close(listener_lock_fd_);
            listener_lock_fd_ = -1;
        }
        return false;
    }

    if (::listen(listen_fd_, 128) < 0) {
        const int error = errno;
        CRB_LOG_ERROR("rpc server listen failed: addr={} port={} errno={} ({})", addr, port, error,
                      std::strerror(error));
        ::close(listen_fd_);
        listen_fd_ = -1;
        if (listener_lock_fd_ >= 0) {
            ::close(listener_lock_fd_);
            listener_lock_fd_ = -1;
        }
        return false;
    }

    struct sockaddr_in bound{};
    socklen_t          bound_len = sizeof(bound);
    if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr *>(&bound), &bound_len) == 0) {
        listen_port_ = ntohs(bound.sin_port);
    }
    const std::string key = listener_key(addr, listen_port_);
    {
        std::scoped_lock lock(active_listeners_mutex);
        if (!active_listeners.insert(key).second) {
            CRB_LOG_ERROR("rpc server listener already active: addr={} port={}", addr, listen_port_);
            ::close(listen_fd_);
            listen_fd_ = -1;
            if (listener_lock_fd_ >= 0) {
                ::close(listener_lock_fd_);
                listener_lock_fd_ = -1;
            }
            return false;
        }
    }
    listener_key_ = key;
    CRB_LOG_INFO("rpc server: listening on {}:{}", addr, listen_port_);

    // Register built-in handlers.
    handlers_.register_handler(static_cast<uint16_t>(proto::FBMsgType_EConnectionPingRequest), handle_ping);

    return true;
}

void RpcServer::register_handler(uint16_t msg_type, HandlerFn handler)
{
    handlers_.register_handler(msg_type, std::move(handler));
}

void RpcServer::start()
{
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    CRB_LOG_INFO("rpc server: starting");
    if (transport_->rdma_listener_enabled()) {
        transport_->set_rdma_accept_handler([this](const std::shared_ptr<Connection> &conn) {
            transport_->attach_rdma_connection(
                conn, [this](Frame *frame, Connection *c) { dispatch(frame, c); },
                [this](Connection *c) {
                    if (request_client_ != nullptr)
                        request_client_->fail_all(c, RpcError::ConnectionClosed);
                });
            transport_->send_rdma_handshake(conn);
        });
        if (!transport_->listen_rdma_service(listen_addr_, listen_port_))
            CRB_LOG_WARN("rpc server: RDMA listener unavailable; TCP remains active");
    }
    transport_->start();
    // Register the listen fd with the I/O worker's epoll loop instead of
    // a dedicated acceptor thread. The worker's epoll loop is already
    // woken by I/O events, so accept is not starved under CPU contention.
    if (listen_fd_ >= 0) {
        // Make the listen socket non-blocking for edge-triggered accept.
        int lflags = fcntl(listen_fd_, F_GETFL, 0);
        fcntl(listen_fd_, F_SETFL, lflags | O_NONBLOCK);

        transport_->set_accept_handler([this](int fd) { handle_accept(fd); });
        transport_->add_listen_fd(listen_fd_);
    }
}

void RpcServer::stop()
{
    const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (!was_running && listener_key_.empty()) {
        return;
    }
    if (was_running) {
        CRB_LOG_INFO("rpc server: stopping");
    }
    // Clear the accept handler before closing the listen fd so the
    // worker doesn't call accept() on a closed fd during shutdown.
    transport_->set_accept_handler(nullptr);
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (!listener_key_.empty()) {
        std::scoped_lock lock(active_listeners_mutex);
        active_listeners.erase(listener_key_);
        listener_key_.clear();
    }
    if (listener_lock_fd_ >= 0) {
        ::close(listener_lock_fd_);
        listener_lock_fd_ = -1;
    }
    if (was_running) {
        transport_->stop();
    }
}

void RpcServer::handle_accept(int listen_fd)
{
    // Loop accept() until EAGAIN — drains the listen backlog in one
    // wakeup. Called on the I/O worker thread.
    while (running_.load(std::memory_order_relaxed)) {
        int fd = ::accept(listen_fd, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                return;
            }
            // EBADF or other error — listen fd was closed during shutdown.
            return;
        }

        // Get peer address for logging.
        struct sockaddr_in peer{};
        socklen_t          peer_len                 = sizeof(peer);
        char               peer_ip[INET_ADDRSTRLEN] = {};
        int                peer_port                = 0;
        if (::getpeername(fd, reinterpret_cast<struct sockaddr *>(&peer), &peer_len) == 0) {
            ::inet_ntop(AF_INET, &peer.sin_addr, peer_ip, sizeof(peer_ip));
            peer_port = ntohs(peer.sin_port);
        }

        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        int nodelay = transport_->tcp_nodelay() ? 1 : 0;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#ifdef __linux__
        // TCP_QUICKACK breaks the Nagle + delayed-ACK deadlock (40ms
        // stalls per round). Controlled by set_quickack() — independent
        // of Nagle. QUICKACK is not sticky — re-armed after each read.
        if (transport_->quickack()) {
            int quickack = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
        }
#endif

        auto conn = transport_->create_connection(
            fd, std::string(peer_ip) + ":" + std::to_string(peer_port),
            [this](Frame *frame, Connection *c) { dispatch(frame, c); },
            [this](Connection *c) {
                if (request_client_ != nullptr) {
                    request_client_->fail_all(c, RpcError::ConnectionClosed);
                }
            });
        transport_->send_rdma_handshake(conn);
        CRB_LOG_INFO("rpc server: connection accepted {}:{} -> conn_id={}", peer_ip, peer_port,
                     static_cast<long long>(conn->id()));
    }
}

void RpcServer::dispatch(Frame *frame, Connection *conn)
{
    uint16_t msg_type   = frame->header.msg_type;
    bool     is_one_way = (frame->header.flags & FLAG_ONE_WAY) != 0;

    // Try request dispatch first — if a handler is registered for
    // this msg_type, dispatch as a request. This ensures request
    // frames are not intercepted by on_response (which matches by
    // request_id and can't distinguish a request from its ack).
    auto handler = handlers_.get_handler(msg_type);
    if (handler) {
        if (msg_type == static_cast<uint16_t>(proto::FBMsgType_EConnectionPingRequest)) {
            cnt_response_ping().inc();
        }
        OutFrame *response = handler(frame, conn);
        if (response != nullptr) {
            // Inline path: handler returned response immediately (sync).
            transport_->submit_inline(conn, response);
        }
        // Async path: handler returned nullptr, will call submit_response
        // later. The dispatched latency is not tracked — it would require
        // per-request context to span the trampoline return → submit_response
        // call. The inline histogram covers the sync path.
        return;
    }

    // No handler for this msg_type — try response routing (ack to
    // a server-sent request). on_response consumes the frame if the
    // request_id is in the request client's pending map.
    if ((frame->header.flags & FLAG_ONE_WAY) == 0 && request_client_ != nullptr &&
        request_client_->on_response(frame->request_id, frame)) {
        return; // ack routed, frame consumed
    }

    // Unknown msg_type and not an ack — send UnknownMessage (if not
    // one-way) or drop.
    if (!is_one_way) {
        OutFrame *response = handle_unknown(frame, conn);
        if (response != nullptr) {
            transport_->submit_inline(conn, response);
        }
    }
    else {
        delete frame;
    }
}

} // namespace crowdb::rpc
