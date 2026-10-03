// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// One node per key, with prefix versions retained until their coverage is
// published. Writer admission is per batch; reader lifetime is epoch protected.
#pragma once

#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/buffer.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/memtable/skip_list.h"
#include "crowdb-tree/slice.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace crowdb::tree
{

struct mem_entry
{
    std::string key;
    buffer      cell; // materialized contiguous [header][value]
    uint64_t    slot;
};

class MemTable
{
  public:
    // `epoch` is the engine's EpochManager — used to retire unlinked nodes
    // and overwritten cell versions. Must outlive this MemTable (owned by
    // the Crowdbtree, which outlives all MemTables via shared_ptr).
    explicit MemTable(uint64_t id = 0, EpochManager *epoch = nullptr) : list_(epoch), epoch_(&list_.epoch()), id_(id)
    {
    }

    [[nodiscard]] VersionMemory memory() const
    {
        return list_.memory();
    }

    [[nodiscard]] EpochManager::Guard read_guard() const
    {
        return epoch_->enter();
    }

    [[nodiscard]] uint64_t id() const
    {
        return id_;
    }

    // Insert/replace with highest-slot-wins. Returns true if the table changed.
    // Drops the write if an existing entry has a >= slot. Also drops writes with
    // slot <= durable_floor (already in L1) unless allow_old_slots is set.
    bool upsert(Slice key, uint64_t slot, Slice cell_payload);
    bool upsert(Slice key, uint64_t slot, buffer &&cell_payload, uint64_t bound = UINT64_MAX,
                MutationStats *stats = nullptr);

    // Zero-copy apply path (R30): store a split cell — the value is borrowed
    // from a Rust `bytes::Bytes` via a kExternal buffer (no value memcpy), and
    // the 9-byte cell header is stored as `slot`/`flags` fields.
    bool upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer &&value, uint64_t bound = UINT64_MAX,
                         MutationStats *stats = nullptr);

    void set_durable_floor(uint64_t slot);

    [[nodiscard]] uint64_t durable_floor() const
    {
        return durable_floor_.load(std::memory_order_relaxed);
    }

    void set_allow_old_slots(bool v)
    {
        allow_old_slots_.store(v, std::memory_order_relaxed);
    }

    struct slot_range_t
    {
        uint64_t min   = UINT64_MAX;
        uint64_t max   = 0;
        bool     empty = true;
    };

    [[nodiscard]] slot_range_t slot_range() const
    {
        uint64_t mn = min_slot_.load(std::memory_order_relaxed);
        uint64_t mx = max_slot_.load(std::memory_order_relaxed);
        if (mn == UINT64_MAX) {
            return slot_range_t{};
        }
        return {.min = mn, .max = mx, .empty = false};
    }

    // Point lookup (lock-free, zero-copy): returns the CellVersion* for `key`,
    // or nullptr. The returned pointer is valid only while the caller's epoch
    // guard is held — a concurrent overwrite retires the old version via epoch.
    [[nodiscard]] const CellVersion *find(Slice key) const
    {
        return list_.find(key);
    }

    // Ordered cursor (lock-free, zero-copy): positioned at the first live
    // node with key > `start_after`. The cursor borrows key/cell Slices
    // directly off the node; valid only while the caller's epoch guard is held.
    [[nodiscard]] ConcurrentSkipList::Cursor cursor(Slice start_after) const
    {
        return list_.cursor(start_after);
    }

    [[nodiscard]] ConcurrentSkipList::Cursor cursor_from(Slice start_key, bool inclusive) const
    {
        return list_.cursor_from(start_key, inclusive);
    }

    [[nodiscard]] ConcurrentSkipList::Cursor cursor_reverse(Slice start_key, bool has_start_bound, bool inclusive) const
    {
        return list_.cursor_reverse(start_key, has_start_bound, inclusive);
    }

    // Prefix iteration is non-destructive; a frozen source stays readable.
    [[nodiscard]] ConcurrentSkipList::Cursor prefix_cursor(uint64_t frontier, uint64_t floor = 0) const
    {
        return list_.prefix_cursor(frontier, floor);
    }

    void prune(Slice key, uint64_t bound, MutationStats *stats = nullptr)
    {
        list_.prune(key, bound, stats);
    }
#ifdef CROWDB_TREE_TEST_UTIL
    void set_hook_for_tests(void *context, ConcurrentSkipList::TestHook hook)
    {
        list_.set_hook_for_tests(context, hook);
    }
#endif

    bool try_enter() noexcept;
    bool validate_open() noexcept;
    void leave() noexcept;
    void close() noexcept;
    void wait_frozen() const noexcept;

    [[nodiscard]] bool closed() const
    {
        return (writers_.load(std::memory_order_acquire) & kClosed) != 0;
    }

    [[nodiscard]] uint64_t writers() const
    {
        return writers_.load(std::memory_order_acquire) & ~kClosed;
    }

    // Ordered immutable copy of the current contents (for full-set paths:
    // iter_all, compare, snapshot_export). O(N) copy is correct there.
    [[nodiscard]] std::vector<mem_entry> snapshot(uint64_t frontier = UINT64_MAX) const;

    [[nodiscard]] size_t approx_bytes() const
    {
        return list_.approx_bytes();
    }

    [[nodiscard]] size_t count() const
    {
        return list_.count();
    }

    [[nodiscard]] bool empty() const
    {
        return list_.empty();
    }

    bool mark_backlog_warning()
    {
        return !backlog_warned_.exchange(true, std::memory_order_relaxed);
    }

  private:
    void update_slot_range(uint64_t slot)
    {
        // Concurrent extrema are monotonic until the whole table is retired.
        uint64_t mn = min_slot_.load(std::memory_order_relaxed);
        uint64_t mx = max_slot_.load(std::memory_order_relaxed);
        while (slot < mn && !min_slot_.compare_exchange_weak(mn, slot, std::memory_order_relaxed)) {
        }
        while (slot > mx && !max_slot_.compare_exchange_weak(mx, slot, std::memory_order_relaxed)) {
        }
    }

    static constexpr uint64_t kClosed = uint64_t{1} << 63;
    std::atomic<uint64_t>     writers_{0};
    std::atomic<bool>         backlog_warned_{false};

    ConcurrentSkipList    list_;
    EpochManager         *epoch_;
    uint64_t              id_ = 0;
    std::atomic<uint64_t> durable_floor_{0};
    std::atomic<bool>     allow_old_slots_{false};
    std::atomic<uint64_t> min_slot_{UINT64_MAX};
    std::atomic<uint64_t> max_slot_{0};
};

} // namespace crowdb::tree
