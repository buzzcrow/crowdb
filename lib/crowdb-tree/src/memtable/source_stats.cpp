// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/source_stats.h"

#include <limits>

namespace crowdb::tree
{
namespace
{
uint64_t sample(const std::atomic<uint64_t> &value) noexcept
{
    const auto count = value.load(std::memory_order_relaxed);
    return count <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ? count : 0;
}
} // namespace

SourceMemoryStats &SourceMemoryStats::operator+=(const SourceMemoryStats &other) noexcept
{
    tables += other.tables;
    frozen_tables += other.frozen_tables;
    writers += other.writers;
    freezing_writers += other.freezing_writers;
    frozen_retained_bytes += other.frozen_retained_bytes;
    records += other.records;
    logical_bytes += other.logical_bytes;
    memory.history_count += other.memory.history_count;
    memory.history_bytes += other.memory.history_bytes;
    memory.descriptor_bytes += other.memory.descriptor_bytes;
    memory.node_bytes += other.memory.node_bytes;
    memory.payload_bytes += other.memory.payload_bytes;
    known = known && other.known;
    return *this;
}

SourceMemoryStats SourceMemoryLedger::snapshot() const noexcept
{
    return {
        .tables                = sample(tables_),
        .frozen_tables         = sample(frozen_tables_),
        .writers               = sample(writers_),
        .freezing_writers      = sample(freezing_writers_),
        .frozen_retained_bytes = sample(frozen_retained_bytes_),
        .records               = sample(records_),
        .logical_bytes         = sample(logical_bytes_),
        .memory =
            {
                     .history_count    = sample(history_count_),
                     .history_bytes    = sample(history_bytes_),
                     .descriptor_bytes = sample(descriptor_bytes_),
                     .node_bytes       = sample(node_bytes_),
                     .payload_bytes    = sample(payload_bytes_),
                     },
        .known = true,
    };
}

void SourceMemoryLedger::replace(const SourceMemoryStats &before, const SourceMemoryStats &after) noexcept
{
    tables_.fetch_add(after.tables - before.tables, std::memory_order_relaxed);
    frozen_tables_.fetch_add(after.frozen_tables - before.frozen_tables, std::memory_order_relaxed);
    writers_.fetch_add(after.writers - before.writers, std::memory_order_relaxed);
    freezing_writers_.fetch_add(after.freezing_writers - before.freezing_writers, std::memory_order_relaxed);
    frozen_retained_bytes_.fetch_add(after.frozen_retained_bytes - before.frozen_retained_bytes,
                                     std::memory_order_relaxed);
    records_.fetch_add(after.records - before.records, std::memory_order_relaxed);
    logical_bytes_.fetch_add(after.logical_bytes - before.logical_bytes, std::memory_order_relaxed);
    history_count_.fetch_add(after.memory.history_count - before.memory.history_count, std::memory_order_relaxed);
    history_bytes_.fetch_add(after.memory.history_bytes - before.memory.history_bytes, std::memory_order_relaxed);
    descriptor_bytes_.fetch_add(after.memory.descriptor_bytes - before.memory.descriptor_bytes,
                                std::memory_order_relaxed);
    node_bytes_.fetch_add(after.memory.node_bytes - before.memory.node_bytes, std::memory_order_relaxed);
    payload_bytes_.fetch_add(after.memory.payload_bytes - before.memory.payload_bytes, std::memory_order_relaxed);
}

void SourceMemoryLedger::writer_enter() noexcept
{
    writers_.fetch_add(1, std::memory_order_relaxed);
}

void SourceMemoryLedger::writer_leave(bool closed) noexcept
{
    writers_.fetch_sub(1, std::memory_order_relaxed);
    if (closed) {
        freezing_writers_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void SourceMemoryLedger::table_closed(uint64_t writers) noexcept
{
    frozen_tables_.fetch_add(1, std::memory_order_relaxed);
    freezing_writers_.fetch_add(writers, std::memory_order_relaxed);
}

void SourceMemoryLedger::table_frozen(uint64_t bytes) noexcept
{
    frozen_retained_bytes_.fetch_add(bytes, std::memory_order_relaxed);
}
} // namespace crowdb::tree
