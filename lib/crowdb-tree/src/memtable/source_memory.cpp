// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/skip_list.h"

#include <limits>

namespace crowdb::tree
{
namespace
{
VersionMemory version_memory(const VersionSet *versions) noexcept
{
    if (versions == nullptr) {
        return {};
    }
    return {.history_count    = versions->history.size(),
            .history_bytes    = versions->bytes - versions->current->cell.size(),
            .descriptor_bytes = sizeof(VersionSet) + (versions->history.capacity() * sizeof(versions->current)),
            .node_bytes       = 0,
            .payload_bytes    = versions->bytes + ((versions->history.size() + 1) * sizeof(CellVersion))};
}

uint64_t sample(const std::atomic<uint64_t> &counter) noexcept
{
    const auto value = counter.load(std::memory_order_relaxed);
    // A later CAS may subtract its predecessor before the earlier CAS adds
    // it. Such an independent sample is zero, never a wrapped huge capacity.
    return value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ? value : 0;
}
} // namespace

void ConcurrentSkipList::account_versions(const VersionSet *previous, const VersionSet &next) noexcept
{
    const auto before = version_memory(previous);
    const auto after  = version_memory(&next);
    // Deltas commute even when a later successful CAS finishes accounting
    // first. The independent samples converge once accepted writers drain.
    history_count_.fetch_add(after.history_count - before.history_count, std::memory_order_relaxed);
    history_bytes_.fetch_add(after.history_bytes - before.history_bytes, std::memory_order_relaxed);
    descriptor_bytes_.fetch_add(after.descriptor_bytes - before.descriptor_bytes, std::memory_order_relaxed);
    payload_bytes_.fetch_add(after.payload_bytes - before.payload_bytes, std::memory_order_relaxed);
    if (source_ledger_) {
        source_ledger_->replace({.logical_bytes = previous != nullptr ? previous->bytes : 0, .memory = before},
                                {.logical_bytes = next.bytes, .memory = after});
    }
}

void ConcurrentSkipList::account_node(const Node &node, const VersionSet &versions) noexcept
{
    node_bytes_.fetch_add(Node::alloc_size(node.height_, node.key_len_), std::memory_order_relaxed);
    account_versions(nullptr, versions);
    if (source_ledger_) {
        source_ledger_->replace({}, {.records       = 1,
                                     .logical_bytes = node.key_len_,
                                     .memory        = {.node_bytes = Node::alloc_size(node.height_, node.key_len_)}});
    }
}

VersionMemory ConcurrentSkipList::memory() const
{
    return {.history_count    = sample(history_count_),
            .history_bytes    = sample(history_bytes_),
            .descriptor_bytes = sample(descriptor_bytes_),
            .node_bytes       = sample(node_bytes_),
            .payload_bytes    = sample(payload_bytes_)};
}
} // namespace crowdb::tree
