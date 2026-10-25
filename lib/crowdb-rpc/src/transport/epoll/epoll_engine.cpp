// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/epoll/epoll_engine.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>

namespace crowdb::rpc
{

EpollEngine::EpollEngine() = default;

EpollEngine::~EpollEngine()
{
    shutdown();
}

int EpollEngine::init()
{
    epoll_fd_ = ::epoll_create1(0);
    if (epoll_fd_ < 0) {
        return -1;
    }

    // eventfd for cross-thread notify.
    notify_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notify_fd_ < 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
        return -1;
    }

    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events  = EPOLLIN;
    ev.data.fd = notify_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, notify_fd_, &ev) < 0) {
        ::close(notify_fd_);
        ::close(epoll_fd_);
        notify_fd_ = -1;
        epoll_fd_  = -1;
        return -1;
    }

    // timerfd for scheduled tasks.
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ >= 0) {
        std::memset(&ev, 0, sizeof(ev));
        ev.events  = EPOLLIN;
        ev.data.fd = timer_fd_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &ev);
    }

    return 0;
}

void EpollEngine::set_oneshot(bool on)
{
    oneshot_ = on;
}

// Bit 0 of data.ptr marks write fds (buzz-cpp pattern). Connection*
// is at least 2-byte aligned, so bit 0 is always 0 for read fds.
static constexpr uint64_t CONNECTION_EVENT = uint64_t{1} << 63;

static uint64_t event_token(Connection *conn, bool write_side)
{
    return CONNECTION_EVENT | (static_cast<uint64_t>(conn->id()) << 1) | (write_side ? 1 : 0);
}

// epoll_ctl is kernel-serialized (thread-safe), so no userspace lock
// is needed. Redundant MODs are ~1µs each — cheaper than a mutex.
// Kernel tokens contain a connection ID and a read/write bit.
void EpollEngine::mod_fd(int fd, uint32_t events, Connection *conn) const
{
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events   = events | (oneshot_ ? static_cast<uint32_t>(EPOLLONESHOT) : 0U);
    ev.data.u64 = event_token(conn, false);
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
}

// MOD for write fds preserves the token's write bit.
void EpollEngine::mod_fd_write(int fd, uint32_t events, Connection *conn) const
{
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events   = events | (oneshot_ ? static_cast<uint32_t>(EPOLLONESHOT) : 0U);
    ev.data.u64 = event_token(conn, true);
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
}

void EpollEngine::add_listen_fd(int fd)
{
    listen_fd_ = fd;
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events  = EPOLLIN;
    ev.data.fd = fd; // listen socket: use fd (no Connection*)
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
}

void EpollEngine::remove_listen_fd(int fd)
{
    if (fd == listen_fd_) {
        listen_fd_ = -1;
    }
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
}

void EpollEngine::add_connection(int read_fd, int write_fd, Connection *conn)
{
    {
        std::scoped_lock lock(conn_mu_);
        connections_[read_fd] = conn;
    }
    // Register read fd with EPOLLIN (armed immediately for reading).
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN | (oneshot_ ? static_cast<uint32_t>(EPOLLONESHOT) : 0U);
    ev.data.u64 = event_token(conn, false);
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, read_fd, &ev);

    // Register write fd with no events (armed on-demand via arm_write).
    // The token's low bit distinguishes read and write readiness.
    if (write_fd >= 0 && write_fd != read_fd) {
        std::memset(&ev, 0, sizeof(ev));
        ev.events   = 0; // no events until arm_write
        ev.data.u64 = event_token(conn, true);
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, write_fd, &ev);
    }
}

void EpollEngine::remove_connection(int read_fd, int write_fd)
{
    {
        std::scoped_lock lock(conn_mu_);
        connections_.erase(read_fd);
    }
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, read_fd, nullptr);
    if (write_fd >= 0 && write_fd != read_fd) {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, write_fd, nullptr);
    }
}

void EpollEngine::arm_read(int read_fd, Connection *conn)
{
    const auto socket_use = conn->use_socket();
    if (!socket_use) {
        return;
    }

    // MOD read fd with EPOLLIN only — does not touch the write fd.
    mod_fd(read_fd, EPOLLIN, conn);
}

void EpollEngine::arm_write(int write_fd, Connection *conn)
{
    const auto socket_use = conn->use_socket();
    if (!socket_use) {
        return;
    }

    // MOD write fd with EPOLLOUT only — does not re-arm read.
    // Uses mod_fd_write to preserve the token's write bit
    // so wait() can distinguish write events from read events.
    mod_fd_write(write_fd, EPOLLOUT, conn);
}

void EpollEngine::disarm_write(int write_fd, Connection *conn)
{
    const auto socket_use = conn->use_socket();
    if (!socket_use) {
        return;
    }

    // MOD write fd with 0 events — disarms write without touching read.
    mod_fd_write(write_fd, 0, conn);
}

void EpollEngine::notify_worker()
{
    if (notify_fd_ >= 0) {
        uint64_t val = 1;
        ::write(notify_fd_, &val, sizeof(val));
    }
}

void EpollEngine::notify_stop()
{
    if (notify_fd_ >= 0) {
        stop_notified_.store(true, std::memory_order_release);
        uint64_t val = 1;
        ::write(notify_fd_, &val, sizeof(val));
    }
}

void EpollEngine::set_timer(int timeout_ms)
{
    if (timer_fd_ < 0) {
        return;
    }
    struct itimerspec its;
    std::memset(&its, 0, sizeof(its));
    if (timeout_ms > 0) {
        its.it_value.tv_sec  = timeout_ms / 1000;
        its.it_value.tv_nsec = (timeout_ms % 1000) * 1000000L;
    }
    ::timerfd_settime(timer_fd_, 0, &its, nullptr);
}

int EpollEngine::wait(EngineEvent *out_events, int max_events, int timeout_ms)
{
    struct epoll_event events[64];
    int                max = std::min(max_events, 64);

    int n = ::epoll_wait(epoll_fd_, events, max, timeout_ms);
    if (n < 0) {
        if (errno == EINTR) {
            return 0;
        }
        return -1;
    }

    int out = 0;
    for (int i = 0; i < n && out < max_events; i++) {
        const struct epoll_event &ev = events[i];

        if ((ev.data.u64 & CONNECTION_EVENT) != 0) {
            const auto id         = static_cast<int64_t>((ev.data.u64 & ~CONNECTION_EVENT) >> 1);
            const bool write_side = (ev.data.u64 & 1) != 0;
            const auto type       = (ev.events & (EPOLLERR | EPOLLHUP)) != 0U ? SocketEvent::Error
                                  : write_side                                ? SocketEvent::Writable
                                                                              : SocketEvent::Readable;
            out_events[out++]     = {
                .type = type, .fd = -1, .conn = nullptr, .connection_id = id, .write_side = write_side};
            continue;
        }

        int fd = ev.data.fd;

        if (fd == notify_fd_) {
            if (!stop_notified_.load(std::memory_order_acquire)) {
                uint64_t val;
                ::read(notify_fd_, &val, sizeof(val));
            }
            out_events[out++] = {.type = SocketEvent::Notify, .fd = -1, .conn = nullptr};
            continue;
        }
        if (fd == timer_fd_) {
            uint64_t val;
            ::read(timer_fd_, &val, sizeof(val));
            out_events[out++] = {.type = SocketEvent::Timer, .fd = -1, .conn = nullptr};
            continue;
        }
        // Listen socket: data.fd was set (not data.ptr). Must check
        // before the connection path — data.fd and data.ptr share a
        // union, so data.ptr is non-null when data.fd is set, which
        // would produce a garbage Connection* pointer.
        if (fd == listen_fd_ && listen_fd_ >= 0) {
            out_events[out++] = {.type = SocketEvent::Accept, .fd = fd, .conn = nullptr};
            continue;
        }
    }
    return out;
}

void EpollEngine::shutdown()
{
    if (notify_fd_ >= 0) {
        ::close(notify_fd_);
        notify_fd_ = -1;
    }
    if (timer_fd_ >= 0) {
        ::close(timer_fd_);
        timer_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

} // namespace crowdb::rpc
