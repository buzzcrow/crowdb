// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"

#include <atomic>
#include <cstdint>

namespace crowdb::tree
{
struct VersionMemory
{
    uint64_t history_count                           = 0;
    uint64_t history_bytes                           = 0;
    uint64_t descriptor_bytes                        = 0;
    uint64_t node_bytes                              = 0;
    uint64_t payload_bytes                           = 0;
    bool     operator==(const VersionMemory &) const = default;
};

// These are current source membership and linked-version summaries, not
// unique physical allocation bytes or retention by external views.
struct SourceMemoryStats
{
    uint64_t      tables                = 0;
    uint64_t      frozen_tables         = 0;
    uint64_t      writers               = 0;
    uint64_t      freezing_writers      = 0;
    uint64_t      frozen_retained_bytes = 0;
    uint64_t      records               = 0;
    uint64_t      logical_bytes         = 0;
    VersionMemory memory{};
    bool          known = false;

    SourceMemoryStats &operator+=(const SourceMemoryStats &other) noexcept;
    bool               operator==(const SourceMemoryStats &) const = default;
};

// One ledger per source admission generation. Mutation deltas commute and
// independent samples converge after writers drain. No source scan is needed.
class SourceMemoryLedger
{
  public:
    [[nodiscard]] SourceMemoryStats snapshot() const noexcept;
    void                            replace(const SourceMemoryStats &before, const SourceMemoryStats &after) noexcept;
    void                            writer_enter() noexcept;
    void                            writer_leave(bool closed) noexcept;
    void                            table_closed(uint64_t writers) noexcept;
    void                            table_frozen(uint64_t bytes) noexcept;

  private:
    std::atomic<uint64_t> tables_{0};
    std::atomic<uint64_t> frozen_tables_{0};
    std::atomic<uint64_t> writers_{0};
    std::atomic<uint64_t> freezing_writers_{0};
    std::atomic<uint64_t> frozen_retained_bytes_{0};
    std::atomic<uint64_t> records_{0};
    std::atomic<uint64_t> logical_bytes_{0};
    std::atomic<uint64_t> history_count_{0};
    std::atomic<uint64_t> history_bytes_{0};
    std::atomic<uint64_t> descriptor_bytes_{0};
    std::atomic<uint64_t> node_bytes_{0};
    std::atomic<uint64_t> payload_bytes_{0};
};
} // namespace crowdb::tree
