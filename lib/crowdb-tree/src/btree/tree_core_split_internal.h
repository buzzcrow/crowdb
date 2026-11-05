// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/flush_operation.h"

#include <array>
#include <functional>

namespace crowdb::tree
{
class PublishedSnapshotContext;

#ifdef CROWDB_TREE_TEST_UTIL
void set_between_split_captures_for_tests(std::function<void()> callback);
#endif

struct SplitGeneration
{
    std::shared_ptr<const PublishedTreeState> expected;
    std::shared_ptr<const PublishedSources>   inherited;
};

struct SplitSourceSession
{
    static constexpr size_t kMaxSources = 64;

    struct Interval
    {
        uint64_t floor    = 0;
        uint64_t frontier = 0;
    };

    std::array<std::shared_ptr<MemTable>, kMaxSources> tables;
    std::array<Interval, kMaxSources>                  intervals;
    size_t                                             table_count    = 0;
    size_t                                             interval_count = 0;
    SourceWriteOptions                                 limits;
    std::shared_ptr<PublishedSnapshotContext>          baseline;
    uint64_t                                           generation = 0;
    uint64_t                                           session    = 0;
    uint64_t                                           frontier   = 0;
    bool                                               active     = false;

    Status             retain(const FrozenSourceView &frozen);
    [[nodiscard]] bool covers(uint64_t floor, uint64_t end) const noexcept;
};

struct SplitHandoff
{
    detail::FlushReservations                  reservations;
    std::shared_ptr<FlushBatch>                operation;
    std::shared_ptr<SourceAdmission::Coverage> coverage;
    FlushBatch::Maintenance                    maintenance;
    uint64_t                                   source_identity = 0;
    uint64_t                                   session         = 0;
    uint64_t                                   frontier        = 0;

    ~SplitHandoff()
    {
        reservations.seal();
    }
};
} // namespace crowdb::tree
