// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_runtime_ids.h"
#include "btree/overflow_backing.h"
#include "snapshot/captured_write.h"
#include "snapshot/chunk_root_codec.h"

namespace crowdb::tree
{
struct ChunkRecoveryLimits
{
    size_t max_pages            = 1024 * 1024;
    size_t max_frame_bytes      = 1024 * 1024;
    size_t max_blob_bytes       = 2 * 1024 * 1024;
    size_t max_references       = 1024 * 1024;
    size_t max_dependency_steps = 1024 * 1024;
};

// One exact index, frame, or bounded dependency slice per backend step. The
// root is loaded first; its validated counts determine the admission bound.
// A successful result still requires RecoveredStateOperation graph validation.
class ChunkRootRecovery
{
    struct ConstructionKey
    {
    };

  public:
    ChunkRootRecovery(ConstructionKey, std::shared_ptr<detail::AllocationReservation> metadata,
                      std::shared_ptr<detail::AllocationReservation> window);
    static Status create(ChunkRootManifest root, std::shared_ptr<const ChunkBackingDomain> domain,
                         std::shared_ptr<detail::ChunkRuntimeIds> ids, ChunkRecoveryLimits limits,
                         std::shared_ptr<detail::AllocationReservation> metadata,
                         std::shared_ptr<detail::AllocationReservation> window,
                         std::shared_ptr<ChunkRootRecovery>            *out);
    [[nodiscard]] static std::optional<size_t> allocation_bound(const ChunkRootManifest &root) noexcept;
    [[nodiscard]] static std::optional<size_t> window_allocation_bound(ChunkRecoveryLimits limits) noexcept;
    Status                                     step(bool stopped = false) noexcept;

    [[nodiscard]] bool done() const noexcept
    {
        return done_;
    }

    [[nodiscard]] const Status &status() const noexcept
    {
        return status_;
    }

    [[nodiscard]] const std::shared_ptr<CapturedDurablePages> &pages() const noexcept
    {
        return pages_;
    }

    [[nodiscard]] size_t max_frame_bytes() const noexcept
    {
        return max_frame_bytes_;
    }

    [[nodiscard]] size_t max_blob_bytes() const noexcept
    {
        return max_blob_bytes_;
    }

    bool                                     retire_step() noexcept;
    std::shared_ptr<const ChunkRootManifest> take_manifest();

  private:
    struct LeafProtection
    {
        std::shared_ptr<const void>               primary;
        std::shared_ptr<const OverflowBackingSet> overflow;
    };

    Status index();
    Status frame();
    Status validate_frame(DurableCapturedFrame &descriptor, std::span<const uint8_t> words);
    Status dependencies();
    Status next_pin(detail::ChunkStageId *out);
    Status finish(Status status);
    std::shared_ptr<detail::AllocationReservation> metadata_;
    std::shared_ptr<detail::AllocationReservation> window_;
    std::shared_ptr<const ChunkBackingDomain>      domain_;
    std::shared_ptr<detail::ChunkRuntimeIds>       ids_;
    ChunkRecoveryLimits                            limits_;

    ChunkRootManifest                                         root_;
    std::shared_ptr<CapturedDurablePages>                     pages_;
    ChunkMetadataBytes                                        index_bytes_;
    detail::ReservedVector<uint8_t, AllocationKind::kIo>      blob_;
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> raw_;
    size_t                                                    max_frame_bytes_  = 0;
    size_t                                                    max_blob_bytes_   = 0;
    size_t                                                    index_            = 0;
    size_t                                                    offset_           = 0;
    uint64_t                                                  left_             = 0;
    uint64_t                                                  last_pid_         = 0;
    uint64_t                                                  references_       = 0;
    uint64_t                                                  steps_            = 0;
    bool                                                      have_pid_         = false;
    bool                                                      dependency_phase_ = false;
    bool                                                      done_             = false;
    Status                                                    status_;
    CapturedDurablePages::Frames::iterator                    leaf_;
    size_t                                                    reference_ = 0;
    uint64_t                                                  pid_       = kInvalidPageId;
    uint64_t                                                  remaining_ = 0;
    std::shared_ptr<OverflowBackingSet>                       overflow_;
};
} // namespace crowdb::tree
