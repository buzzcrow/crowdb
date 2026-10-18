// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/socket_transport.h"
#ifdef CROWDB_RPC_HAVE_RDMA
#    include "crowdb-rpc/transport/rdma/rdma_transport.h"
#endif

#ifdef __linux__
#    include "crowdb-rpc/transport/epoll/epoll_engine.h"
#elif defined(__APPLE__)
#    include "crowdb-rpc/transport/kqueue/kqueue_engine.h"
#endif

#include "crowdb-common/log.h"
#include "crowdb-rpc/rpc_metrics.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/transport/rdma/rdma_capability.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>

namespace crowdb::rpc
{

thread_local Worker *tl_current_worker = nullptr;

// Forward declarations — defined after Worker (shared I/O logic).
static void on_readable_impl(Connection *conn, int fd, uint8_t *recv_buf, size_t recv_buf_size,
                             std::vector<Connection *> &pending_writes);
static bool on_writable_impl(Connection *conn, int fd, TransportStats *stats);

static void close_connection_fds(SocketEngine *engine, Connection *conn, SocketTransport *transport)
{
    int read_fd  = static_cast<int>(conn->transport_handle);
    int write_fd = conn->write_fd;
    if (read_fd < 0 || !conn->claim_socket_cleanup()) {
        return;
    }

    engine->remove_connection(read_fd, write_fd);
    transport->unregister_conn(conn);
    ::close(read_fd);
    if (write_fd >= 0 && write_fd != read_fd) {
        ::close(write_fd);
    }
}

static inline uint64_t now_nano()
{
    return static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}

// ── Worker ────────────────────────────────────────────────────────

Worker::Worker(int id, SocketEngine *engine, TransportStats *stats, SocketTransport *transport)
    : id_(id),
      engine_(engine),
      stats_(stats),
      transport_(transport)
{
}

Worker::~Worker()
{
    stop();
    close_connections();
}

void Worker::close_connections()
{
    {
        std::scoped_lock lock(conns_mu_);
        for (auto &[fd, conn] : connections_) {
            (void)fd;
            conn->close();
            while (!conn->socket_cleanup_ready()) {
                std::this_thread::yield();
            }
            close_connection_fds(engine_, conn.get(), transport_);
        }
        connections_.clear();
    }
    discard_closes();
}

void Worker::request_close(std::shared_ptr<Connection> conn)
{
    enqueue_close(new CloseRequest{.connection = std::move(conn), .next = nullptr});
    engine_->notify_worker();
}

void Worker::enqueue_close(CloseRequest *request)
{
    auto *head = pending_closes_.load(std::memory_order_relaxed);
    do {
        request->next = head;
    } while (
        !pending_closes_.compare_exchange_weak(head, request, std::memory_order_release, std::memory_order_relaxed));
}

void Worker::drain_closes(ClosedConnections &closed_fds)
{
    auto *request = pending_closes_.exchange(nullptr, std::memory_order_acquire);
    while (request != nullptr) {
        auto     *next = request->next;
        auto     *conn = request->connection.get();
        const int fd   = static_cast<int>(conn->transport_handle);
        if (!conn->socket_cleanup_ready()) {
            enqueue_close(request);
            request = next;
            continue;
        }
        close_connection_fds(engine_, conn, transport_);
        closed_fds.emplace_back(fd, conn->id());
        delete request;
        request = next;
    }
}

void Worker::discard_closes()
{
    auto *request = pending_closes_.exchange(nullptr, std::memory_order_acquire);
    while (request != nullptr) {
        auto *next = request->next;
        delete request;
        request = next;
    }
}

void Worker::start()
{
    running_.store(true, std::memory_order_relaxed);
    thread_ = std::thread([this] {
        thread_id_        = std::this_thread::get_id();
        tl_current_worker = this;
        run_loop();
        tl_current_worker = nullptr;
    });
}

bool Worker::request_stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        CRB_LOG_DEBUG("worker {} stop skipped: already stopped", id_);
        return false;
    }
    CRB_LOG_INFO("worker {} stop requested", id_);
    return true;
}

void Worker::join()
{
    if (!thread_.joinable()) {
        return;
    }
    const auto join_started = std::chrono::steady_clock::now();
    CRB_LOG_INFO("worker {} joining event-loop thread", id_);
    thread_.join();
    const auto join_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - join_started);
    CRB_LOG_INFO("worker {} event-loop thread joined after {} ms", id_, join_ms.count());
}

void Worker::stop()
{
    if (request_stop()) {
        engine_->notify_stop();
    }
    join();
}

void Worker::add_connection(int read_fd, int write_fd, std::shared_ptr<Connection> conn)
{
    conn->io_engine = engine_;
    conn->io_worker = this;
    {
        std::scoped_lock lock(conns_mu_);
        connections_[read_fd] = conn;
    }
    engine_->add_connection(read_fd, write_fd, conn.get());
    // arm_read is called by add_connection (EPOLLIN armed at ADD time).
    // In ONESHOT mode, the read fd is already armed.
}

void Worker::run_loop()
{
    constexpr int MAX_EVENTS = 64;
    EngineEvent   events[MAX_EVENTS];

    // Lazily allocate the per-worker receive buffer.
    if (recv_buf_.empty()) {
        recv_buf_.resize(RECV_BUF_SIZE);
    }

    // fds closed during this iteration — collected for map erase after
    // the pending-write flush (erasing sooner would free the Connection
    // while raw pointers in pending_write_conns_ still reference it).
    ClosedConnections closed_fds;

    // Wrap the entire event loop in try/catch to prevent C++ exceptions
    // from propagating through the thread boundary (which would call
    // std::terminate and abort the process).
    try {

        while (running_.load(std::memory_order_relaxed)) {
            int n = engine_->wait(events, MAX_EVENTS, 1000); // 1s timeout
            closed_fds.clear();

            // Pin each kernel event by its non-reused connection identity. Another
            // worker may close and unregister it while this batch is in flight.
            std::array<std::shared_ptr<Connection>, MAX_EVENTS> retained;
            for (int i = 0; i < n; i++) {
                auto &ev = events[i];
                if (ev.connection_id != 0) {
                    retained[i] = transport_->lookup_connection_id(ev.connection_id);
                    if (!retained[i]) {
                        continue;
                    }
                    ev.conn = retained[i].get();
                    ev.fd   = ev.write_side ? ev.conn->write_fd : static_cast<int>(ev.conn->transport_handle);
                    if (!ev.conn->is_open()) {
                        int fd = static_cast<int>(ev.conn->transport_handle);
                        close_connection_fds(engine_, ev.conn, transport_);
                        closed_fds.emplace_back(fd, ev.connection_id);
                        continue;
                    }
                }
                switch (ev.type) {
                case SocketEvent::Notify:
                    drain_notified();
                    break;
                case SocketEvent::Timer:
                    break;
                case SocketEvent::Readable:
                    process_readable(ev, closed_fds);
                    break;
                case SocketEvent::Writable:
                    process_writable(ev, closed_fds);
                    break;
                case SocketEvent::Error:
                    process_error(ev, closed_fds);
                    break;
                case SocketEvent::Accept:
                    transport_->on_accept(ev.fd);
                    break;
                case SocketEvent::External:
                    transport_->on_external(ev.external_tag);
                    break;
                }
            }

            drain_closes(closed_fds);
            flush_responses(closed_fds);
            retire_closed(closed_fds);
        }
        CRB_LOG_INFO("worker {} event loop exited", id_);
    }
    catch (const std::exception &e) {
    }
    catch (...) {
    }
}

void Worker::drain_notified()
{
    // Cross-thread submit: drain the shared pending list
    // and batch writev. Any worker that picks up the
    // Notify can drain all pending sends.
    {
        std::vector<Connection *> pending;
        {
            std::scoped_lock lock(transport_->cross_thread_mu_);
            pending.swap(transport_->cross_thread_pending_);
            transport_->cross_thread_notified_.store(false, std::memory_order_release);
        }
        for (Connection *c : pending) {
            auto pinned = transport_->lookup_conn(c);
            if (!pinned || !*pinned) {
                continue;
            }
            c = pinned->get();
            if (!c->is_open()) {
                continue;
            }
            int wfd = c->write_fd >= 0 ? c->write_fd : static_cast<int>(c->transport_handle);
            if (!c->try_send(wfd, stats_)) {
                // Partial/EAGAIN — arm write for retry.
                engine_->arm_write(wfd, c);
            }
        }
    }
}

void Worker::process_readable(const EngineEvent &ev, ClosedConnections &closed_fds)
{
    if (ev.conn != nullptr && ev.conn->is_open()) {
        on_readable_impl(ev.conn, ev.fd, recv_buf_.data(), recv_buf_.size(), pending_write_conns_);
        if (!ev.conn->is_open()) {
            // Connection closed (EOF or fatal read error).
            // Remove from epoll and close the fd to stop
            // level-triggered re-firing on the dead fd.
            // Map erase is deferred to after the pending-write
            // flush to avoid dangling raw pointers.
            CRB_LOG_INFO("rpc transport: connection closed (read) fd={} conn_id={} peer={}", ev.fd,
                         static_cast<long long>(ev.conn->id()), ev.conn->name());
            close_connection_fds(engine_, ev.conn, transport_);
            closed_fds.emplace_back(ev.fd, ev.conn->id());
        }
        else if (engine_->oneshot()) {
            // In one-shot mode, re-arm read after processing
            // (EV_ONESHOT consumed the event). Without this, the
            // connection goes silent — no more read events fire.
            engine_->arm_read(ev.fd, ev.conn);
        }
    }
}

void Worker::process_writable(const EngineEvent &ev, ClosedConnections &closed_fds)
{
    if (ev.conn != nullptr && ev.conn->is_open()) {
        // ev.fd is the write fd (dup'd). try_send uses it.
        // Retire the delivered interest before releasing send ownership.
        // Disarming afterward can erase a concurrent producer's re-arm.
        // One-shot events have already consumed their write interest.
        if (!engine_->oneshot()) {
            engine_->disarm_write(ev.fd, ev.conn);
        }
        bool all_sent = on_writable_impl(ev.conn, ev.fd, stats_);
        if (!ev.conn->is_open()) {
            // Connection closed during write (hard error).
            CRB_LOG_INFO("rpc transport: connection closed (write) fd={} conn_id={} peer={}", ev.fd,
                         static_cast<long long>(ev.conn->id()), ev.conn->name());
            int rfd = static_cast<int>(ev.conn->transport_handle);
            close_connection_fds(engine_, ev.conn, transport_);
            closed_fds.emplace_back(rfd, ev.conn->id());
        }
        else if (!all_sent) {
            // EAGAIN/partial — re-arm write for retry.
            engine_->arm_write(ev.fd, ev.conn);
        }
    }
}

void Worker::process_error(const EngineEvent &ev, ClosedConnections &closed_fds)
{
    if (ev.conn != nullptr && ev.conn->is_open()) {
        const auto socket_use = ev.conn->use_socket();
        if (!socket_use) {
            return;
        }
        CRB_LOG_WARN("worker: socket error event fd={} conn_id={} name={}", ev.fd,
                     static_cast<long long>(ev.conn->id()), ev.conn->name());
        int       rfd          = static_cast<int>(ev.conn->transport_handle);
        int       socket_error = 0;
        socklen_t error_size   = sizeof(socket_error);
        if (::getsockopt(rfd, SOL_SOCKET, SO_ERROR, &socket_error, &error_size) == 0 && socket_error == ECONNREFUSED) {
            ev.conn->mark_connect_refused();
        }
        ev.conn->close();
        close_connection_fds(engine_, ev.conn, transport_);
        closed_fds.emplace_back(rfd, ev.conn->id());
    }
}

void Worker::flush_responses(ClosedConnections &closed_fds)
{
    // Send aggregation: after processing all events, batch writev
    // pending responses collected during on_readable. This coalesces
    // multiple responses (from multiple frames read in one read())
    // into a single writev per connection.
    if (!pending_write_conns_.empty()) {
        for (Connection *conn : pending_write_conns_) {
            if (!conn->is_open()) {
                continue;
            }
            int rfd = static_cast<int>(conn->transport_handle);
            int wfd = conn->write_fd >= 0 ? conn->write_fd : rfd;
            if (!conn->try_send(wfd, stats_)) {
                // Partial write or EAGAIN — arm write for remaining.
                engine_->arm_write(wfd, conn);
            }
            if (!conn->is_open()) {
                close_connection_fds(engine_, conn, transport_);
                closed_fds.emplace_back(rfd, conn->id());
            }
        }
        pending_write_conns_.clear();
    }
}

void Worker::retire_closed(const ClosedConnections &closed_fds)
{
    // Now safe to release closed connections — pending_write_conns_
    // has been flushed and won't access the raw pointers.
    if (!closed_fds.empty()) {
        std::scoped_lock lock(conns_mu_);
        for (const auto &[fd, id] : closed_fds) {
            auto entry = connections_.find(fd);
            if (entry != connections_.end() && entry->second->id() == id) {
                connections_.erase(entry);
            }
        }
    }
}

// ── Shared I/O logic (called by Worker::run_loop) ─────────────────
// Defined before Worker::run_loop so they're visible there. These are the
// hot-path read/write methods — the engine tells the worker *when* to
// read/write, and these do the actual I/O + parsing.

static void on_readable_impl(Connection *conn, int fd, uint8_t *recv_buf, size_t recv_buf_size,
                             std::vector<Connection *> &pending_writes)
{
    const auto socket_use = conn->use_socket();
    if (!socket_use) {
        return;
    }

    auto &parser      = conn->parser();
    auto  on_frame_cb = [conn](Frame *frame) { conn->on_frame(frame); };

    // Process bytes from recv_buf starting at offset `pos`, up to `end`.
    // Handles header+control (via feed_data) and data (direct copy to
    // data_buf_). Returns when all bytes are consumed or parser needs
    // more bytes from the socket.
    auto process_recv_bytes = [&](uint32_t pos, uint32_t end) -> bool {
        while (pos < end && parser.last_error() == FramingError::None) {
            if (parser.state() == ParseState::ReadingData) {
                // Copy data bytes from recv_buf to data_buf_.
                auto target = parser.next_read_target();
                if (target.len == 0) {
                    return false; // shouldn't happen
                }
                uint32_t avail   = end - pos;
                uint32_t to_copy = std::min(avail, target.len);
                std::memcpy(target.ptr, recv_buf + pos, to_copy);
                pos += to_copy;
                Frame *frame = parser.advance(to_copy);
                if (frame != nullptr) {
                    on_frame_cb(frame);
                }
            }
            else {
                // Header + control: feed_data stops at ReadingData.
                uint32_t consumed = parser.feed_data(recv_buf + pos, end - pos, on_frame_cb);
                pos += consumed;
                if (parser.state() == ParseState::ReadingData) {
                    continue; // loop back to copy data bytes
                }
                if (consumed == 0 && end - pos > 0) {
                    // feed_data consumed nothing but bytes remain —
                    // shouldn't happen, but break to avoid infinite loop.
                    return false;
                }
            }
        }
        return true;
    };

    // Buzz-cpp read strategy: one big read() into recv_buf pulls a TCP
    // segment (possibly multiple frames). process_recv_bytes handles
    // header+control+small data from recv_buf (memcpy, no extra syscall).
    // When recv_buf is exhausted and parser still needs data (large data
    // payload spanning multiple TCP segments), read() directly into
    // data_buf_ — no extra copy. read_calls counts only the main recv_buf
    // read (TCP segment arrivals), so raggr = frames_parsed / read_calls
    // reflects true kernel-level coalescing.
    while (true) {
        ssize_t n = ::read(fd, recv_buf, recv_buf_size);
        if (n <= 0) {
            if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                if (n < 0 && errno == ECONNREFUSED) {
                    conn->mark_connect_refused();
                }
                cnt_read_error().inc();
                conn->close();
            }
            break;
        }
        bw_read().observe(static_cast<uint64_t>(n));
#ifdef __linux__
        // Re-arm TCP_QUICKACK — the kernel resets it after sending each
        // ACK. Without this, delayed ACKs stall Nagle's send (40ms/round).
        if (conn->quickack) {
            int quickack = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
        }
#endif
        // Process all bytes in recv_buf — header+control via feed_data,
        // data via direct copy from recv_buf. Handles multiple frames.
        process_recv_bytes(0, static_cast<uint32_t>(n));
        // If parser still needs data bytes (recv_buf exhausted mid-frame),
        // read directly into data_buf_ — no extra copy for large payloads.
        while (parser.state() == ParseState::ReadingData) {
            auto target = parser.next_read_target();
            if (target.len == 0) {
                break;
            }
            ssize_t n2 = ::read(fd, target.ptr, target.len);
            if (n2 <= 0) {
                if (n2 == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                    conn->close();
                }
                goto done;
            }
            bw_read().observe(static_cast<uint64_t>(n2));
            Frame *frame = parser.advance(static_cast<uint32_t>(n2));
            if (frame != nullptr) {
                on_frame_cb(frame);
            }
        }
    }
done:
    // If on_frame enqueued responses (via submit_inline), collect this
    // connection for batch writev after all events are processed.
    if (conn->has_pending_send()) {
        pending_writes.push_back(conn);
    }
}

static bool on_writable_impl(Connection *conn, int fd, TransportStats *stats)
{
    // Drain partials (at front of iovec buffer) + queued frames.
    return conn->try_send(fd, stats);
}

// ── SocketTransport ───────────────────────────────────────────────

SocketTransport::SocketTransport(uint32_t io_engines, uint32_t io_workers, BufferPool *pool) : pool_(pool)
{
    assert(io_engines >= 1 && io_workers >= 1);
    assert(io_workers % io_engines == 0);
    if (pool_ == nullptr) {
        pool_ = new SystemBufferPool(); // own a default pool
    }
#ifdef CROWDB_RPC_HAVE_RDMA
    if (const char *enabled = std::getenv("CROWDB_RDMA_ENABLED");
        enabled != nullptr && std::strcmp(enabled, "1") == 0) {
        rdma_transport_ = std::make_unique<RdmaTransport>();
    }
#endif
    // Create N independent engines, each with M workers. When M>1, the
    // engine uses EV_ONESHOT/EPOLLONESHOT so only one worker wakes per
    // event; workers re-arm after processing. When M=1, no ONESHOT —
    // the single worker owns the engine (level-triggered fast path).
    uint32_t workers_per_engine = io_workers / io_engines;
    for (uint32_t e = 0; e < io_engines; e++) {
        auto engine = create_engine();
        engine->init();
        if (workers_per_engine > 1) {
            engine->set_oneshot(true);
        }
        SocketEngine *engine_ptr = engine.get();
        engines_.push_back(std::move(engine));
        for (uint32_t w = 0; w < workers_per_engine; w++) {
            int  worker_id = static_cast<int>((e * workers_per_engine) + w);
            auto worker    = std::make_unique<Worker>(worker_id, engine_ptr, &stats_, this);
            workers_.push_back(std::move(worker));
        }
#ifdef CROWDB_RPC_HAVE_RDMA
        if (rdma_transport_ != nullptr)
            rdma_transport_->register_events(engine_ptr);
#endif
    }
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr) {
        rdma_transport_->set_connection_handler([this](const std::shared_ptr<Connection> &conn) {
            attach_rdma_connection(conn);
            if (rdma_accept_handler_)
                rdma_accept_handler_(conn);
        });
        set_external_handler([this](uint64_t tag) { rdma_transport_->handle_external_event(tag); });
    }
#endif
}

SocketTransport::SocketTransport(uint32_t num_workers, BufferPool *pool) : SocketTransport(1, num_workers, pool)
{
}

SocketTransport::~SocketTransport()
{
    stop();
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr)
        rdma_transport_->shutdown();
#endif
}

void SocketTransport::start()
{
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr)
        rdma_transport_->start();
#endif
    for (auto &w : workers_) {
        w->start();
    }
}

void SocketTransport::stop()
{
    for (auto &w : workers_) {
        w->request_stop();
    }
    for (auto &engine : engines_) {
        engine->notify_stop();
    }
    for (auto &w : workers_) {
        w->join();
    }
    for (auto &w : workers_) {
        w->close_connections();
    }
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr)
        rdma_transport_->stop();
#endif
}

void SocketTransport::add_listen_fd(int fd)
{
    // Register the listen socket with the first engine. The engine
    // monitors it via epoll/kqueue (level-triggered, no ONESHOT) and
    // delivers Accept events to a worker thread. This avoids the
    // dedicated acceptor thread, which can be starved under CPU
    // contention (the worker's epoll loop is already being woken
    // by other I/O events).
    if (!engines_.empty()) {
        engines_[0]->add_listen_fd(fd);
    }
}

void SocketTransport::remove_listen_fd(int fd)
{
    if (!engines_.empty()) {
        engines_[0]->remove_listen_fd(fd);
    }
}

void SocketTransport::on_accept(int listen_fd)
{
    if (accept_handler_) {
        accept_handler_(listen_fd);
    }
}

void SocketTransport::shutdown()
{
    stop();
}

bool SocketTransport::submit(Connection *conn, OutFrame *frame)
{
#ifdef CROWDB_RPC_HAVE_RDMA
    if (conn != nullptr && conn->path() == ConnectionPath::Rdma && rdma_transport_ != nullptr) {
        if (!rdma_transport_->submit(conn, frame))
            return false;
        return true;
    }
#endif
    if (workers_.empty()) {
        return false;
    }
    std::shared_ptr<Connection> live_conn;
    // Worker-thread fast path: if we're on an I/O worker thread, the
    // connection is guaranteed alive (held by this worker's connections_
    // event batch). Skip the registry lookup on this pinned worker path.
    if (tl_current_worker != nullptr) {
        if (!conn->is_open()) {
            CRB_LOG_WARN("submit: closed conn on worker thread conn_id={}", static_cast<long long>(conn->id()));
            return false;
        }
    }
    else {
        // Cross-thread submit (tokio mode): look up the connection in
        // the live-connection registry to protect against stale handles.
        auto lookup = lookup_conn(conn);
        if (!lookup.has_value() || lookup.value() == nullptr) {
            CRB_LOG_WARN("submit: stale connection handle");
            return false;
        }
        // Retain ownership through enqueue, notification and direct write.
        // Close can concurrently remove the weak registry entry.
        live_conn = std::move(lookup.value());
        conn      = live_conn.get();
        if (!conn->is_open()) {
            return false;
        }
    }
    frame->create_nano = now_nano();
    if (!conn->enqueue_send(frame)) {
        // Send queue full — try the overflow queue (deferred retry).
        // The I/O worker drains overflow first in try_send(), so these
        // frames are sent before new send_queue_ frames.
        if (conn->enqueue_overflow(frame)) {
            cnt_send_queue_full().inc();
            // Arm write so the worker picks up the overflow frames.
            auto *engine = static_cast<SocketEngine *>(conn->io_engine);
            if (engine != nullptr) {
                int wfd = conn->write_fd >= 0 ? conn->write_fd : static_cast<int>(conn->transport_handle);
                engine->arm_write(wfd, conn);
            }
            return true;
        }
        // Overflow also full — reject the frame (last resort). Ownership
        // remains with the caller on every false return.
        cnt_send_queue_full().inc();
        stats_.send_queue_rejects.fetch_add(1, std::memory_order_relaxed);
        CRB_LOG_WARN("submit: enqueue_send + overflow failed (backpressure) conn_id={} name={} request_id={}",
                     static_cast<long long>(conn->id()), conn->name(),
                     static_cast<unsigned long long>(frame->request_id));
        return false;
    }

    if (event_write_) {
        // Event-write mode: notify the I/O worker to drain + writev.
        // The worker's Notify handler calls try_send, batching all
        // frames accumulated in the queue into one writev.
        auto *engine = static_cast<SocketEngine *>(conn->io_engine);
        if (engine != nullptr) {
            {
                std::scoped_lock lock(cross_thread_mu_);
                cross_thread_pending_.push_back(conn);
            }
            bool expected = false;
            if (cross_thread_notified_.compare_exchange_strong(expected, true)) {
                engine->notify_worker();
            }
        }
        return true;
    }

    // Direct-write mode (default): try writev on the caller's thread.
    // The in_send_ CAS serializes concurrent senders — only one does
    // writev; others just enqueue and return. If writev hits EAGAIN,
    // arm write on the owning engine for retry.
    int wfd = conn->write_fd >= 0 ? conn->write_fd : static_cast<int>(conn->transport_handle);
    if (!conn->try_send(wfd, &stats_)) {
        auto *engine = static_cast<SocketEngine *>(conn->io_engine);
        if (engine != nullptr) {
            engine->arm_write(wfd, conn);
        }
    }
    return true;
}

bool SocketTransport::submit_inline(Connection *conn, OutFrame *frame)
{
#ifdef CROWDB_RPC_HAVE_RDMA
    if (conn != nullptr && conn->path() == ConnectionPath::Rdma && rdma_transport_ != nullptr)
        return rdma_transport_->submit(conn, frame);
#endif
    frame->create_nano = now_nano();
    // Both paths: enqueue only. The post-event flush drains the queue
    // (retry_direct for Path A, flush_send for Path B), batching all
    // frames accumulated during the read loop into a single writev.
    // send_direct is only used by submit() for cross-thread sends.
    if (conn->enqueue_send(frame)) {
        return true;
    }
    // Send queue full — try overflow (deferred retry).
    if (conn->enqueue_overflow(frame)) {
        cnt_send_queue_full().inc();
        return true;
    }
    // Overflow also full — drop (last resort).
    cnt_send_queue_full().inc();
    stats_.send_queue_rejects.fetch_add(1, std::memory_order_relaxed);
    CRB_LOG_WARN("submit_inline: enqueue + overflow failed conn_id={} request_id={}",
                 static_cast<long long>(conn->id()), static_cast<unsigned long long>(frame->request_id));
    if (frame->control != nullptr) {
        frame->control->release();
    }
    frame->release_data();
    delete frame;
    return false;
}

Worker *SocketTransport::get_worker()
{
    if (workers_.empty()) {
        return nullptr;
    }
    size_t idx = next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
    return workers_[idx].get();
}

std::shared_ptr<Connection> SocketTransport::create_connection(int fd, const std::string &name,
                                                               Connection::OnFrameCallback on_frame,
                                                               Connection::OnCloseCallback on_close)
{
    int64_t id             = next_conn_id_.fetch_add(1, std::memory_order_relaxed);
    auto    conn           = std::make_shared<Connection>(id, name, pool_, 4 << 20, send_queue_capacity_);
    conn->transport_handle = static_cast<uint64_t>(fd);
    conn->quickack         = quickack_;
    // dup() the fd for independent read/write epoll registration (buzz-cpp
    // pattern). This allows EPOLLONESHOT on read and write to be independent
    // — arming write does not re-arm read, preventing multi-worker races.
    // dup() the fd for independent read/write epoll registration (buzz-cpp
    // pattern). This allows EPOLLONESHOT on read and write to be independent
    // — arming write does not re-arm read, preventing multi-worker races.
    int write_fd = ::dup(fd);
    if (write_fd < 0) {
        write_fd = fd; // fallback: same fd for read and write
    }
    conn->write_fd = write_fd;
    // Keep the identity registry until descriptor cleanup; a worker must
    // safely resolve an event even after an explicit caller-side close.
    conn->set_on_frame(std::move(on_frame));
    conn->set_on_close([this, on_close = std::move(on_close)](Connection *c) {
        if (auto *worker = static_cast<Worker *>(c->io_worker); worker != nullptr) {
            worker->request_close(c->shared_from_this());
        }
        if (on_close) {
            on_close(c);
        }
    });
    register_conn(conn);
    Worker *w = get_worker();
    if (w != nullptr) {
        w->add_connection(fd, write_fd, conn);
    }
    return conn;
}

void SocketTransport::register_conn(const std::shared_ptr<Connection> &conn)
{
    live_conns_.insert_or_assign(conn.get(), conn);
    event_conns_.insert_or_assign(conn->id(), conn);
}

void SocketTransport::unregister_conn(Connection *conn)
{
    live_conns_.erase(conn);
    event_conns_.erase(conn->id());
}

std::optional<std::shared_ptr<Connection>> SocketTransport::lookup_conn(Connection *conn)
{
    auto it = live_conns_.find(conn);
    if (it == live_conns_.end()) {
        return std::nullopt; // absent or already unregistered
    }
    auto pinned = it->second.lock();
    return pinned && pinned->is_open() ? pinned : std::shared_ptr<Connection>{};
}

std::shared_ptr<Connection> SocketTransport::lookup_connection_id(int64_t id) const
{
    auto entry = event_conns_.find(id);
    return entry == event_conns_.end() ? nullptr : entry->second.lock();
}

size_t SocketTransport::connection_count() const
{
    return live_conns_.size();
}

std::shared_ptr<Connection> SocketTransport::connect(const std::string &addr, int port)
{
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr) {
        if (auto rdma_conn = rdma_transport_->connect(addr, port); rdma_conn != nullptr) {
            for (auto &engine : engines_)
                rdma_transport_->register_events(engine.get());
            register_conn(rdma_conn);
            attach_rdma_connection(rdma_conn);
            return rdma_conn;
        }
        rdma_transport_->stats().fallback_count.fetch_add(1, std::memory_order_relaxed);
    }
#endif
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

    int nodelay = tcp_nodelay_ ? 1 : 0;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#ifdef __linux__
    if (quickack_) {
        int quickack = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
    }
#endif

    auto conn = create_connection(fd, addr + ":" + std::to_string(port));
    send_rdma_handshake(conn);
    CRB_LOG_INFO("rpc transport: connection initiated -> {}:{} conn_id={}", addr, port,
                 static_cast<long long>(conn->id()));
    return conn;
}

void SocketTransport::attach_rdma_connection(const std::shared_ptr<Connection> &conn,
                                             Connection::OnFrameCallback on_frame, Connection::OnCloseCallback on_close)
{
    if (conn == nullptr)
        return;
    conn->set_on_frame(std::move(on_frame));
    conn->set_on_close(std::move(on_close));
    register_conn(conn);
    if (!workers_.empty()) {
        Worker *worker  = get_worker();
        conn->io_worker = worker;
        conn->io_engine = worker == nullptr ? nullptr : worker->engine();
    }
}

void SocketTransport::set_rdma_accept_handler(RdmaAcceptHandler handler)
{
    rdma_accept_handler_ = std::move(handler);
}

bool SocketTransport::send_rdma_handshake(const std::shared_ptr<Connection> &conn)
{
    if (!conn || pool_ == nullptr)
        return false;
    RdmaEndpointCapability capability;
    const char            *enabled = std::getenv("CROWDB_RDMA_ENABLED");
    capability.rdma_available      = enabled != nullptr && std::strcmp(enabled, "1") == 0;
    capability.protocol_version    = 1;
    capability.generation          = 1;
    capability.port                = 1;
    if (const char *device = std::getenv("CROWDB_RDMA_DEVICE"); device != nullptr)
        capability.device = device;
    if (const char *gid = std::getenv("CROWDB_RDMA_GID"); gid != nullptr)
        capability.gid = gid;
    capability.data_classes = {RDMA_CONTROL_CAPACITY, RDMA_METADATA_CAPACITY, RDMA_AGGREGATE_CAPACITY,
                               RDMA_DATAPATH_CAPACITY};
    const auto wire         = serialize_rdma_handshake(capability);
    Buffer    *control      = build_raw_control(pool_, wire.data(), static_cast<uint32_t>(wire.size()));
    if (control == nullptr)
        return false;
    OutFrame *frame = build_out_frame(0, RDMA_HANDSHAKE_MSG_TYPE, control, nullptr, FLAG_ONE_WAY);
    if (frame == nullptr) {
        control->release();
        return false;
    }
    if (!submit(conn.get(), frame)) {
        control->release();
        delete frame;
        return false;
    }
    return true;
}

bool SocketTransport::listen_rdma(const std::string &addr, int port)
{
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ == nullptr || !rdma_transport_->listen(addr, port))
        return false;
    for (auto &engine : engines_)
        rdma_transport_->register_events(engine.get());
    return true;
#else
    (void)addr;
    (void)port;
    return false;
#endif
}

std::unique_ptr<SocketEngine> SocketTransport::create_engine()
{
#ifdef __linux__
    return std::make_unique<EpollEngine>();
#elif defined(__APPLE__)
    return std::make_unique<KqueueEngine>();
#else
    return nullptr;
#endif
}

} // namespace crowdb::rpc
