// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/published_read.h"
#include "crowdb-tree/btree/read_result.h"
#include "runtime/tree_operation_lifetime.h"
#include "runtime/tree_runtime_binding.h"

namespace crowdb::tree
{
class TreeCore;
struct CoreScanOptions;
class ScanPackedBuf;

// One accepted window. Idle cursor handles keep no I/O reserve or Tree
// ticket. Lower-level callback owners independently retain accepted I/O.
// Construct and destroy this host scope off runtime workers.
struct SnapshotReadWindow
{
    // Host-only guard; released after the loader and ticket, so a last Core
    // release cannot synchronously close while waiting on this same ticket.
    std::shared_ptr<TreeCore>                      host_core;
    PageLoadBudget                                 budget;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<AsyncPageStore>                backend;
    std::shared_ptr<PageLoader>                    loader;

    SnapshotReadWindow() = default;
    ~SnapshotReadWindow();
    SnapshotReadWindow(const SnapshotReadWindow &)            = delete;
    SnapshotReadWindow &operator=(const SnapshotReadWindow &) = delete;
    SnapshotReadWindow(SnapshotReadWindow &&) noexcept        = default;
    SnapshotReadWindow &operator=(SnapshotReadWindow &&other) noexcept;
    void                seal() noexcept;
};

// A captured L1 header/mapping with no live L0 catalog. Shared backends can
// detach from the Core; raw backends admit each window through a weak Core.
class PublishedSnapshotContext
{
    struct State;

  public:
    static Status capture(const std::shared_ptr<TreeCore> &core, std::shared_ptr<PublishedSnapshotContext> *out);
    [[nodiscard]] static size_t allocation_bound() noexcept;
    [[nodiscard]] static size_t window_control_bound() noexcept;
    // bytes covers the caller's operation, loader and output capacities.
    // This adds reservation-control/ticket overhead and reserves one I/O.
    Status prepare_window(size_t bytes, SnapshotReadWindow *out) const;
    Status get(Slice key, GetView *out) const;
    Status scan(CoreScanOptions options, ScanPackedBuf *out, size_t *count, bool *truncated) const;

    [[nodiscard]] const std::shared_ptr<const PublishedTreeState>   &view() const noexcept;
    [[nodiscard]] const std::shared_ptr<TreeRuntimeBinding>         &binding() const noexcept;
    [[nodiscard]] const std::shared_ptr<detail::FlushRuntime::Lane> &lane() const noexcept;
    [[nodiscard]] PageLoadLimits                                     load_limits() const noexcept;
    [[nodiscard]] PublishedReadLimits                                read_limits() const noexcept;
    [[nodiscard]] bool                                               detached() const noexcept;
    [[nodiscard]] const PageKeyRange                                &range() const noexcept;

  private:
    friend class TreeCore;
    static Status capture_admitted(const std::shared_ptr<TreeCore>           &core,
                                   std::shared_ptr<PublishedSnapshotContext> *out,
                                   std::shared_ptr<const PublishedTreeState>  captured = {});

    explicit PublishedSnapshotContext(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};

// A long-lived traversal can retain this delegate across idle windows. Its
// active loader is installed only while the serialized next operation owns
// a SnapshotReadWindow. Completed leaf/value owners never retain it.
class SnapshotPageLoader final : public PageLoader
{
  public:
    Status begin(std::shared_ptr<PageLoader> loader);
    void   end() noexcept;
    void   load_leaf(BaseRef base, LeafComplete complete) override;
    void   load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete) override;
    void   load_overflow(std::shared_ptr<const BackingRef> primary, uint64_t head, uint64_t length,
                         ValueComplete complete) override;

  private:
    std::shared_ptr<PageLoader> active_;
};
} // namespace crowdb::tree
