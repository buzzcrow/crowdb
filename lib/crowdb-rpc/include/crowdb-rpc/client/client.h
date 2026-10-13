// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/transport.h"

#include <folly/concurrency/ConcurrentHashMap.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

namespace crowdb::rpc
{

// Error codes for the RPC layer.
enum class RpcError : uint8_t {
    Ok = 0,
    ConnectionClosed,
    Timeout,
    SendQueueFull,
    ConnectionError,
    RegistrationFailed,
    AllDown,
};

// Completion callback for request-response calls. The callback receives
// the response frame (nullptr on error) and the error code.
using CompletionCallback = std::function<void(Frame *response, RpcError err)>;

// The low byte is the lifecycle state; upper bits identify a reuse generation.
// READY -> PROCESSING claims exclusive field access before completion. Only
// FREE/DONE can be reused, so delayed cancellation cannot retire a newer call.
constexpr uint64_t SLOT_FREE            = 0;
constexpr uint64_t SLOT_PENDING_READY   = 1;
constexpr uint64_t SLOT_DONE            = 2;
constexpr uint64_t SLOT_PENDING_CLAIMED = 3;
constexpr uint64_t SLOT_PROCESSING      = 4;
constexpr uint64_t SLOT_STATE_MASK      = 255;

struct CompletionSlot
{
    std::atomic<uint64_t>     state{SLOT_FREE};
    std::atomic<uint64_t>     request_id{0};
    crowdb_rpc_on_complete    cb{nullptr};
    void                     *user_data{nullptr};
    std::atomic<uint64_t>     deadline_ns{0};
    std::atomic<Connection *> conn{nullptr};
};

// Map fallback entries share an immutable callback and one atomic ownership
// claim. Iterators can outlive erasure without racing callback moves.
struct PendingCompletion
{
    CompletionCallback cb;
    std::atomic<bool>  claimed{false};

    explicit PendingCompletion(CompletionCallback callback) : cb(std::move(callback))
    {
    }
};

struct PendingEntry
{
    std::shared_ptr<PendingCompletion> completion;
    uint64_t                           deadline_ns{0};
    Connection                        *conn{nullptr};
};

// RpcClient manages request/response correlation. Each call allocates
// a monotonic request_id, inserts a callback into the pending map, and
// submits the frame. When the response arrives (via Connection::on_frame),
// on_response looks up the request_id, invokes the callback, and removes
// the entry.
//
// The pending map uses folly::ConcurrentHashMap (striped locks) when
// folly is available, falling back to std::unordered_map + mutex
// otherwise. The striped-lock map eliminates the single-mutex bottleneck
// on the response hot path — at high TPS (benchmarks, consensus), the
// I/O worker's on_response lookup no longer contends with cross-thread
// submit/timeout removal on a single lock.
class RpcClient
{
  public:
    RpcClient();
    ~RpcClient();

    // Send a request with a C ABI completion callback. Reserves a slab
    // slot by index (request_id & pool_mask) via CAS FREE→PENDING; if
    // the slot is occupied, falls back to the pending map. The callback
    // is invoked directly on the I/O worker thread when the response
    // arrives — no oneshot channel, no scheduler round-trip. The pool
    // must be sized (set_completion_pool_size) before use. If the reaper
    // is active (start_reaper), each call gets a deadline = now +
    // timeout_ns; timed-out entries are failed with Timeout. Returns
    // true when completion owns user_data, including a disconnect racing
    // submit rejection; false guarantees the callback was not invoked.
    bool send(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
              uint16_t msg_type, crowdb_rpc_on_complete cb, void *user_data);

    // Consumes buffers without reserving a response completion slot.
    bool send_one_way(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                      uint16_t msg_type);

    // Scatter/gather variant. `data_views` contains between one and
    // MAX_DATA_VIEWS immutable owners and is consumed on every return path.
    bool send_chain(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control,
                    Buffer *const *data_views, uint8_t data_view_count, uint16_t msg_type, crowdb_rpc_on_complete cb,
                    void *user_data);

    // Bounded variant for callers that must never allocate a pending-map
    // fallback entry. Returns false when the indexed slab slot is occupied.
    bool send_slab_only(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control, Buffer *data,
                        uint16_t msg_type, crowdb_rpc_on_complete cb, void *user_data);

    // Attach this caller to a connection's on_frame callback. The
    // callback tries response routing first (on_response); if the
    // frame's request_id is not in the pending map, it dispatches as
    // a request via dispatch_request. Call this once per connection
    // before sending requests through it.
    void attach(Connection *conn);

    // Try to route a frame as a response to a previously-sent request.
    // Returns true if request_id was found in the pending map (frame
    // consumed, callback invoked). Returns false if not found (frame
    // NOT consumed — caller owns it and must dispatch or delete it).
    bool on_response(uint64_t request_id, Frame *response);

    // Dispatch an incoming request frame (server→client direction) to
    // a registered handler by msg_type. If no handler is found, sends
    // UnknownMessage (if transport set) or drops the frame.
    void dispatch_request(Frame *frame, Connection *conn);

    // Register a handler for an incoming request msg_type. The handler
    // is a C callback (same type as server-side handlers) that receives
    // the request fields + conn_handle and submits the response later
    // via crowdb_rpc_server_submit_response.
    void register_handler(uint16_t msg_type, crowdb_rpc_handler_fn callback, void *user_data);

    // Clear all registered request handlers. Called during shutdown
    // to break reference cycles (Rust handler closures capture Arcs).
    void clear_handlers()
    {
        std::scoped_lock lock(handler_mu_);
        request_handlers_.clear();
    }

    // Set the transport for submitting UnknownMessage responses when
    // no handler matches an incoming request msg_type. If not set,
    // unmatched request frames are dropped.
    void set_transport(Transport *t)
    {
        transport_ = t;
    }

    // Fail pending requests. If conn is non-null, only requests sent on
    // that connection are failed (per-connection scoping for connection
    // close). If conn is null, all pending requests are failed (used by
    // shutdown / destructor).
    void fail_all(Connection *conn, RpcError err);

    // Number of pending requests (for diagnostics).
    size_t pending_count();

    // Dump all pending request IDs + deadlines to the log. For diagnostics:
    // call when the bench stops to see which requests are still in-flight
    // and how long they've been waiting.
    void dump_pending();

    // Size the callback completion pool. Must be a power of two; the
    // caller passes the max in-flight (the next power of two is used).
    // Must be called before any send(). No-op if already sized.
    void set_completion_pool_size(size_t max_in_flight);

    // Start the timeout reaper thread. Scans the slab pool + pending map
    // every scan_interval_ns for entries past their deadline (timeout_ns
    // from submit time). Timed-out entries are failed with RpcError::Timeout
    // and their slots/entries are reclaimed. Must be called after
    // set_completion_pool_size. No-op if already running.
    void start_reaper(uint64_t timeout_ns, uint64_t scan_interval_ns);

    // Stop the timeout reaper thread. Called automatically by the destructor.
    // No-op if not running.
    void stop_reaper();

  private:
    bool send_impl(Transport *transport, Connection *conn, uint64_t request_id, Buffer *control,
                   Buffer *const *data_views, uint8_t data_view_count, uint16_t msg_type, crowdb_rpc_on_complete cb,
                   void *user_data, bool slab_only);

    // Handler registry for incoming requests (server→client direction).
    // Maps msg_type → (C callback, user_data). Same trampoline pattern
    // as the server-side handler dispatch.
    std::mutex                                                             handler_mu_;
    std::unordered_map<uint16_t, std::pair<crowdb_rpc_handler_fn, void *>> request_handlers_;

    // Transport for submitting UnknownMessage responses when no handler
    // matches an incoming request msg_type. Set via set_transport.
    Transport *transport_{nullptr};

    // Slab-based completion pool (callback model). Indexed by
    // request_id & pool_mask_. Null by default (callback model opt-in).
    // unique_ptr<[]> because CompletionSlot has a non-movable atomic.
    std::unique_ptr<CompletionSlot[]> completion_pool_;
    size_t                            pool_size_{0};
    size_t                            pool_mask_{0}; // pool_size - 1 (power of two)
    // Guards the one-time pool allocation in set_completion_pool_size.
    // Only contended during init (idempotent after first call).
    std::mutex pool_mu_;

    // Pending map: oneshot call() entries (deadline=0) + slab-fallback
    // entries (deadline>0). folly::ConcurrentHashMap (striped locks)
    // eliminates the single-mutex bottleneck on the response hot path.
    folly::ConcurrentHashMap<uint64_t, PendingEntry> pending_;

    // Reaper thread: scans slab + map for timed-out entries.
    std::thread             reaper_thread_;
    std::atomic<bool>       reaper_running_{false};
    std::condition_variable reaper_cv_;
    std::mutex              reaper_mu_;
    uint64_t                reaper_interval_ns_{0};
    std::atomic<uint64_t>   default_timeout_ns_{0}; // 0 = no timeout (bench)

    // Build an OutFrame for submission. The RpcClient owns the OutFrame;
    // the transport takes it and releases buffers after send.
    static OutFrame *build_frame(uint64_t request_id, Buffer *control, Buffer *data, uint16_t msg_type, uint8_t flags);
    static OutFrame *build_frame_chain(uint64_t request_id, Buffer *control, Buffer *const *data_views,
                                       uint8_t data_view_count, uint16_t msg_type, uint8_t flags);

    // Reaper loop: scans slab pool + pending map for timed-out entries.
    void reaper_loop();

    // Compute steady-clock nanoseconds (monotonic).
    static uint64_t steady_now_ns();
};

} // namespace crowdb::rpc
