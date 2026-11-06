// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/memtable.h"

#include <cassert>

namespace crowdb::tree
{
MemTable::~MemTable()
{
    if (source_attached_) {
        list_.source_ledger_->replace(source_stats(), {});
    }
}

SourceMemoryStats MemTable::source_stats() const noexcept
{
    const auto state        = writers_.load(std::memory_order_acquire);
    const auto writer_count = state & ~kClosed;
    const bool frozen       = (state & kClosed) != 0;
    return {.tables                = 1,
            .frozen_tables         = frozen ? 1U : 0U,
            .writers               = writer_count,
            .freezing_writers      = frozen ? writer_count : 0,
            .frozen_retained_bytes = frozen && writer_count == 0 ? approx_bytes() : 0,
            .records               = count(),
            .logical_bytes         = approx_bytes(),
            .memory                = memory(),
            .known                 = true};
}

void MemTable::attach_source_stats(std::shared_ptr<SourceMemoryLedger> ledger) noexcept
{
    assert(!list_.source_ledger_ && ledger && !closed() && writers() == 0);
    ledger->replace({}, source_stats());
    list_.source_ledger_ = std::move(ledger);
    source_attached_     = true;
}

void MemTable::detach_source_stats() noexcept
{
    assert(closed() && writers() == 0);
    if (source_attached_) {
        list_.source_ledger_->replace(source_stats(), {});
        // Keep the ledger pointer immutable: close/last-writer bookkeeping
        // may finish after another thread observed the drained writer state.
        // Their additive deltas still settle into this same generation.
        source_attached_ = false;
    }
}
} // namespace crowdb::tree
