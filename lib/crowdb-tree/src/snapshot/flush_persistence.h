// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/tree_core_checkpoint_internal.h"
#include "snapshot/snapshot_baseline.h"

namespace crowdb::tree
{
// The durable sink for one admitted flush. Page calls are serialized by the
// flush's existing I/O window; unrelated subtree computation remains runnable.
class FlushPersistence : public std::enable_shared_from_this<FlushPersistence>
{
    struct Page;
    struct Commit;

  public:
    explicit FlushPersistence(std::shared_ptr<checkpoint_detail::Preparation> preparation, uint64_t wal_offset = 0);
    [[nodiscard]] static size_t control_allocation_bound(size_t workers) noexcept;
    [[nodiscard]] static size_t commit_control_allocation_bound() noexcept;
    void                        configure(FlushBatchOptions &options);

    void bind_io(std::shared_ptr<detail::AllocationReservation> io)
    {
        io_ = std::move(io);
    }

    void               write(PageEncodeInput input, FlushBatchOptions::ReserveOverflow reserve,
                             FlushBatchOptions::PersistComplete done);
    void               commit(std::shared_ptr<const PublishedTreeState> view, FlushBatch::MaintenanceComplete done);
    [[nodiscard]] bool selected(uint64_t pid, const PageKeyRange &range) const;
    [[nodiscard]] std::optional<CheckpointResult> result() const;

  private:
    friend struct Page;
    friend struct Commit;
    std::shared_ptr<checkpoint_detail::Preparation>                   preparation_;
    std::shared_ptr<const PublishedTreeState>                         initial_;
    std::shared_ptr<const CapturedDurablePages>                       inherited_;
    std::shared_ptr<CapturedDurablePages>                             pages_;
    CheckpointResult                                                  report_;
    bool                                                              finished_   = false;
    uint64_t                                                          wal_offset_ = 0;
    std::shared_ptr<detail::AllocationReservation>                    io_;
    std::shared_ptr<detail::AllocationReservation>                    selection_metadata_;
    std::optional<detail::ReservedVector<const SnapshotPageChange *>> pending_leaves_;
    void                                                              select_inputs();
};
} // namespace crowdb::tree
