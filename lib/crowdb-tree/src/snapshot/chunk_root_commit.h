// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_runtime_ids.h"
#include "snapshot/captured_write.h"
#include "snapshot/chunk_root_codec.h"

#include <set>

namespace crowdb::tree
{
// A backend-lane protocol. Each step handles at most one index pack or one
// bounded reference authority window. The caller yields between steps; no CPU task
// performs catalog waits, and an unknown mutation is queried by its exact ID.
class ChunkRootCommit
{
    struct ConstructionKey
    {
    };

  public:
    ChunkRootCommit(ConstructionKey, std::shared_ptr<detail::AllocationReservation> metadata);
    static Status create(std::shared_ptr<const CapturedDurablePages> pages,
                         std::shared_ptr<const ChunkBackingDomain> domain, std::shared_ptr<detail::ChunkRuntimeIds> ids,
                         ChunkRootIdentity identity, std::shared_ptr<detail::AllocationReservation> metadata,
                         std::shared_ptr<detail::AllocationReservation> window, std::shared_ptr<ChunkRootCommit> *out,
                         size_t                                      max_dependency_steps = 1024 * 1024,
                         std::shared_ptr<const ChunkRootManifest>    previous             = {},
                         std::shared_ptr<const CapturedDurablePages> baseline             = {});
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t frames, size_t previous_indexes = 0) noexcept;
    [[nodiscard]] static std::optional<size_t> window_allocation_bound(size_t stored_references) noexcept;
    Status                                     step(bool stop_requested = false) noexcept;

    [[nodiscard]] bool done() const noexcept
    {
        return stage_ == Stage::Done;
    }

    [[nodiscard]] bool committed() const noexcept
    {
        return committed_;
    }

    [[nodiscard]] bool needs_recovery() const noexcept
    {
        return uncertain_;
    }

    [[nodiscard]] const Status &status() const noexcept
    {
        return status_;
    }

    [[nodiscard]] const ChunkRootIdentity &identity() const noexcept
    {
        return root_.identity;
    }

    [[nodiscard]] detail::ChunkStageId candidate() const noexcept
    {
        return candidate_;
    }

    [[nodiscard]] const std::shared_ptr<const ChunkRootManifest> &manifest() const noexcept
    {
        return committed_root_;
    }

    bool retire_step() noexcept;

  private:
    enum class Stage : uint8_t {
        Index,
        Dependencies,
        Checksum,
        Begin,
        Prepare,
        Commit,
        QueryBegin,
        QueryPrepare,
        QueryCommit,
        Abort,
        QueryAbort,
        Release,
        Done
    };
    using References = std::set<detail::ChunkStageId, std::less<>, detail::ReservationAllocator<detail::ChunkStageId>>;
    Status             advance();
    Status             index();
    [[nodiscard]] bool unchanged(std::span<const DurableCapturedFrame *const> frames,
                                 const ChunkIndexReference                   &index) const;
    Status             reuse_index(const ChunkIndexReference &index, std::shared_ptr<const ChunkPackBacking> *backing);
    Status             write_index(std::span<const DurableCapturedFrame *const> frames,
                                   std::shared_ptr<const ChunkPackBacking>     *backing);
    Status             checksum();
    Status             dependencies();
    Status             begin();
    Status             prepare();
    Status             finish_candidate(bool commit);
    Status             query();
    Status             release();
    Status             fail(Status status);
    Status             finish(Status status, bool uncertain = false);
    Status             query_result(const detail::ChunkCandidateStatus &current);
    void               accepted_window();

    std::shared_ptr<detail::AllocationReservation> metadata_;
    std::shared_ptr<detail::AllocationReservation> window_;
    std::shared_ptr<const CapturedDurablePages>    pages_;
    std::shared_ptr<const ChunkBackingDomain>      domain_;
    std::shared_ptr<detail::ChunkRuntimeIds>       ids_;
    std::shared_ptr<const CapturedDurablePages>    baseline_;
    std::shared_ptr<const ChunkRootManifest>       previous_;
    std::shared_ptr<ChunkRootManifest>             pending_root_;
    std::shared_ptr<const ChunkRootManifest>       committed_root_;
    size_t                                         previous_index_ = 0;

    ChunkRootManifest                                               root_;
    ChunkMetadataBytes                                              manifest_;
    detail::ReservedVector<std::shared_ptr<const ChunkPackBacking>> index_pins_;
    References                                                      references_;
    CapturedDurablePages::Frames::const_iterator                    page_;
    References::const_iterator                                      reference_;
    detail::ChunkStageId                                            candidate_{};
    std::array<detail::ChunkStageId, detail::kMaxReferenceWindow>   pending_{};
    size_t                                                          pending_count_        = 0;
    uint64_t                                                        prepared_             = 0;
    uint64_t                                                        released_             = 0;
    uint32_t                                                        checksum_             = 0;
    size_t                                                          dependency_           = 0;
    size_t                                                          max_dependency_steps_ = 0;
    uint64_t                                                        chain_pid_            = kInvalidPageId;
    uint64_t                                                        chain_bytes_          = 0;
    Stage                                                           stage_                = Stage::Index;
    bool                                                            candidate_started_    = false;
    bool                                                            committed_            = false;
    bool                                                            uncertain_            = false;
    Status                                                          status_;
};
} // namespace crowdb::tree
