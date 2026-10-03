// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-common/metrics/latency_summary.h"
#include "crowdb-tree/memtable/generation.h"
#include "crowdb-tree/memtable/memtable.h"

namespace crowdb::tree
{
// Coverage is captured with source membership, before any L1 head is read.
// It is not a scan timestamp: live updates above this floor remain observable.
struct MemTableSource
{
    std::shared_ptr<MemTable>            owner;
    uint64_t                             floor = 0;
    std::shared_ptr<EpochManager::Guard> guard;

    MemTableSource(std::shared_ptr<MemTable> table, uint64_t covered)
        : owner(std::move(table)),
          floor(covered),
          guard(std::make_shared<EpochManager::Guard>(owner->read_guard()))
    {
    }

    const MemTableSource *operator->() const
    {
        return this;
    }

    [[nodiscard]] const CellVersion *find(Slice key) const
    {
        const auto *cv = owner->find(key);
        return cv != nullptr && cv->slot > floor ? cv : nullptr;
    }

    [[nodiscard]] ConcurrentSkipList::Cursor cursor(Slice key) const
    {
        return filter(owner->cursor(key));
    }

    [[nodiscard]] ConcurrentSkipList::Cursor cursor_from(Slice key, bool inclusive) const
    {
        return filter(owner->cursor_from(key, inclusive));
    }

    [[nodiscard]] ConcurrentSkipList::Cursor cursor_reverse(Slice key, bool bounded, bool inclusive) const
    {
        auto cur = owner->cursor_reverse(key, bounded, inclusive);
        while (cur.valid() && cur.cell_version()->slot <= floor) {
            cur = owner->cursor_reverse(cur.key(), true, false);
        }
        return cur;
    }

    [[nodiscard]] size_t count() const
    {
        size_t result = 0;
        for (auto cur = owner->prefix_cursor(UINT64_MAX, floor); cur.valid(); cur.advance()) {
            ++result;
        }
        return result;
    }

    [[nodiscard]] size_t approx_bytes() const
    {
        return owner->approx_bytes();
    }

  private:
    [[nodiscard]] ConcurrentSkipList::Cursor filter(ConcurrentSkipList::Cursor cur) const
    {
        cur.set_floor(floor);
        return cur;
    }
};

struct MemTableCounters
{
    std::atomic<uint64_t> cas_retries{0};
    std::atomic<uint64_t> overwrite{0};
    std::atomic<uint64_t> keep{0};
    std::atomic<uint64_t> merged{0};
    std::atomic<uint64_t> failed_batches{0};
    std::atomic<uint64_t> keep_gap{0};
    std::atomic<uint64_t> keep_pending{0};
    std::atomic<uint64_t> keep_boundary{0};
};

struct MemTableBatch
{
    std::shared_ptr<MemTable>                table;
    uint64_t                                 bound;
    MutationStats                            stats;
    GenerationGate::Guard                    generation;
    MemTableCounters                        *counters;
    crowdb::common::metrics::LatencySummary *copy_latency = nullptr;
    bool                                     completed    = false;

    ~MemTableBatch()
    {
        if (copy_latency != nullptr && stats.copy_count != 0) {
            copy_latency->observe_batch(stats.copy_count, stats.copy_ns, stats.copy_max_ns);
        }
        if (stats.cas_retries != 0) {
            counters->cas_retries.fetch_add(stats.cas_retries);
        }
        if (stats.overwrite != 0) {
            counters->overwrite.fetch_add(stats.overwrite);
        }
        if (stats.keep != 0) {
            counters->keep.fetch_add(stats.keep);
            counters->keep_gap.fetch_add(stats.keep_gap);
            counters->keep_pending.fetch_add(stats.keep_pending);
            counters->keep_boundary.fetch_add(stats.keep_boundary);
        }
        if (stats.merged != 0) {
            counters->merged.fetch_add(stats.merged);
        }
        if (!completed) {
            counters->failed_batches.fetch_add(1);
        }
        table->leave();
    }
};
} // namespace crowdb::tree
