// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Legacy synchronous pool model used only by low-level regression tests.
// The production Tree uses SharedPageCache and independently owned frames.
#pragma once

#include "crowdb-common/metrics/metrics.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/maptable/page_types.h" // kInvalidPageId
#include "crowdb-tree/status.h"

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

// Forward-declare metric types for TestBufferPool handles. The metrics core
// moved to crowdb-common::metrics (R12); bridge the moved types into
// `crowdb-tree` with per-type using-declarations so existing
// `Counter*`/`Gauge*` references compile unchanged. (Not a `namespace
// crowdb::tree = crowdb::common::metrics;` alias — only the specific types are
// bridged.) The forward declarations live at the root namespace so they resolve
// to `crowdb::common::metrics::Counter` rather than nesting under
// `crowdb::tree::`.
namespace crowdb::common::metrics
{
class Counter;
class Gauge;
class LatencySummary;
class Bandwidth;
} // namespace crowdb::common::metrics

namespace crowdb::tree
{
using crowdb::common::metrics::Bandwidth;
using crowdb::common::metrics::Counter;
using crowdb::common::metrics::Gauge;
using crowdb::common::metrics::LatencySummary;

class TestBufferPool;

// RAII pin handle. Keeps a frame resident until destroyed/released. Movable,
// non-copyable. bytes() is valid for the handle's lifetime.
class TestPoolFrameRef
{
  public:
    TestPoolFrameRef() = default;

    TestPoolFrameRef(TestBufferPool *pool, uint32_t idx, uint8_t *bytes, uint64_t page_id)
        : pool_(pool),
          idx_(idx),
          bytes_(bytes),
          page_id_(page_id)
    {
    }

    ~TestPoolFrameRef();

    TestPoolFrameRef(const TestPoolFrameRef &)            = delete;
    TestPoolFrameRef &operator=(const TestPoolFrameRef &) = delete;

    TestPoolFrameRef(TestPoolFrameRef &&o) noexcept
    {
        *this = std::move(o);
    }

    TestPoolFrameRef &operator=(TestPoolFrameRef &&o) noexcept;

    [[nodiscard]] bool valid() const
    {
        return pool_ != nullptr;
    }

    [[nodiscard]] uint8_t *bytes() const
    {
        return bytes_;
    }

    [[nodiscard]] uint64_t page_id() const
    {
        return page_id_;
    }

    [[nodiscard]] uint32_t index() const
    {
        return idx_;
    }

    void release();

  private:
    TestBufferPool *pool_    = nullptr;
    uint32_t        idx_     = 0;
    uint8_t        *bytes_   = nullptr;
    uint64_t        page_id_ = kInvalidPageId;
};

class TestBufferPool
{
  public:
    struct Stats
    {
        uint64_t hits       = 0;
        uint64_t misses     = 0;
        uint64_t evictions  = 0;
        uint64_t writebacks = 0;
        uint32_t resident   = 0;
        uint32_t dirty      = 0;
        uint32_t used       = 0; // frames currently held (pinned or page_id-mapped)
        uint32_t num_frames = 0;
    };

    // capacity_bytes / page_bytes frames (>= 1). `store` is non-owning.
    TestBufferPool(size_t capacity_bytes, uint32_t page_bytes, PageStore *store);

    // pin the frame for `page_id`, demand-loading from `addr` on a miss
    // (CRC-checked).
    Status pin(uint64_t page_id, PageAddr addr, TestPoolFrameRef *out);
    // pin a fresh zeroed frame for a new page that will live at `addr`. No load.
    Status pin_new(uint64_t page_id, PageAddr addr, TestPoolFrameRef *out);

    // Acquire a fresh zeroed, anonymous frame for a freshly built base page (no
    // page_id mapping, no durable addr; dirty until a snapshot assigns one). The
    // frame is pinned-resident until release_frame so it is never evicted.
    // Returns an error (caller should fall back to a heap buffer) if no frame is
    // free. The returned `out_bytes` window is valid until
    // release_frame(*out_idx).
    Status acquire_frame(uint32_t *out_idx, uint8_t **out_bytes);
    // Return an owned frame (from acquire_frame, or a page_id-mapped base) to the
    // pool.
    void release_frame(uint32_t idx);

    void mark_dirty(uint64_t page_id);
    // Write every dirty resident frame back to its addr (no fsync; caller syncs).
    Status flush_dirty();

    [[nodiscard]] uint32_t page_bytes() const
    {
        return page_bytes_;
    }

    [[nodiscard]] Stats stats() const;

    // Wire optional metrics handles. All pointers must outlive the pool.
    void set_metrics(Counter *evictions, Counter *writebacks, Gauge *resident, Gauge *dirty,
                     LatencySummary *writeback_l, Bandwidth *page_writeback_bw)
    {
        m_evictions_         = evictions;
        m_writebacks_        = writebacks;
        m_resident_          = resident;
        m_dirty_             = dirty;
        m_writeback_l_       = writeback_l;
        m_page_writeback_bw_ = page_writeback_bw;
    }

  private:
    friend class TestPoolFrameRef;

    struct FrameMeta
    {
        enum class State : uint8_t { kReady, kLoading, kWriteback };

        uint64_t page_id = kInvalidPageId;
        PageAddr addr    = 0;
        int32_t  pin     = 0;
        uint8_t  ref     = 0;
        bool     dirty   = false;
        State    state   = State::kReady;
    };

    [[nodiscard]] uint8_t *frame_bytes(uint32_t idx)
    {
        return arena_.get() + (static_cast<size_t>(idx) * page_bytes_);
    }

    void unpin(uint32_t idx);

    // open-addressing page_id->frame index table (linear probe, backward-shift
    // erase).
    void                  ht_insert(uint64_t page_id, uint32_t idx);
    [[nodiscard]] int64_t ht_find(uint64_t page_id) const;
    void                  ht_erase(uint64_t page_id);

    // CLOCK: choose an unpinned ready victim. The caller reserves it before I/O.
    // Returns -1 if every frame is pinned or reserved. Caller holds mu_.
    [[nodiscard]] int64_t acquire_victim();
    Status                write_back_io(uint32_t idx, PageAddr addr);

    mutable std::mutex         mu_;
    std::condition_variable    state_cv_;
    std::unique_ptr<uint8_t[]> arena_;
    std::vector<FrameMeta>     frames_;
    uint32_t                   page_bytes_;
    uint32_t                   num_frames_;
    PageStore                 *store_;
    uint32_t                   clock_hand_ = 0;

    std::vector<uint64_t> ht_key_; // page_id or kInvalidPageId
    std::vector<uint32_t> ht_val_; // frame index
    size_t                ht_mask_ = 0;

    Stats stats_;

    // Optional metrics handles (null if not wired).
    Counter        *m_evictions_         = nullptr;
    Counter        *m_writebacks_        = nullptr;
    Gauge          *m_resident_          = nullptr;
    Gauge          *m_dirty_             = nullptr;
    LatencySummary *m_writeback_l_       = nullptr;
    Bandwidth      *m_page_writeback_bw_ = nullptr;
};

} // namespace crowdb::tree
