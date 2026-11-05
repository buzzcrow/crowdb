// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/status.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace crowdb::tree
{
class TreeCore;

struct SplitOverlayLimits
{
    size_t max_records = 0;
    size_t max_bytes   = 0;
};

struct InstalledSplitOverlay
{
    uint64_t generation = 0;
    uint64_t frontier   = 0;
};

// An independent bounded delta and the child's already prepared publication.
// Preparation is a host operation; installation never waits for maintenance.
class PreparedSplitOverlay
{
    struct State;

  public:
    static Status arm(TreeCore &source, TreeCore &destination, SplitOverlayLimits limits,
                      std::shared_ptr<PreparedSplitOverlay> *out, uint64_t *start_frontier);
    Status        prepare(uint64_t replayed_prefix);
    ~PreparedSplitOverlay();
    PreparedSplitOverlay(const PreparedSplitOverlay &)            = delete;
    PreparedSplitOverlay &operator=(const PreparedSplitOverlay &) = delete;

    Status                                try_install(uint64_t frontier, InstalledSplitOverlay *out) noexcept;
    [[nodiscard]] std::optional<uint64_t> committed_frontier() const noexcept;

  private:
    explicit PreparedSplitOverlay(std::shared_ptr<State> state);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
