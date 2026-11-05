// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_collector.h"

#include "chunk_physical_registry.h"

namespace crowdb::tree::detail
{
size_t ChunkCollector::allocation_bound() noexcept
{
    // Active, loaded and CAS copy plus encoded old/new records coexist. The
    // cursor itself contains identities only and retains no unbounded result.
    return (4 * kMaxStagePacks * sizeof(ChunkStagePack)) + (2 * kMaxStageBytes) +
           ChunkPhysicalRegistry::pack_allocation_bound();
}

Status ChunkCollector::step(RootCatalog &catalog, ChunkTransport &transport, uint64_t tree, uint64_t epoch,
                            const std::shared_ptr<AllocationBudget> &budget, ChunkCollectionProgress *out,
                            const std::shared_ptr<ChunkPhysicalRegistry> &registry)
try {
    auto *protection = catalog.protection();
    if (tree == 0 || epoch == 0 || !budget || out == nullptr) {
        return Status::invalid_argument();
    }
    if (protection == nullptr || !transport.fixed_staging_supported()) {
        return Status::not_supported();
    }
    auto status = protection->check_read_owner();
    if (!status.ok()) {
        return status;
    }
    if (candidates_end_ && stages_end_) {
        candidates_end_ = false;
        stages_end_     = false;
        candidate_after_.reset();
        stage_after_.reset();
    }
    const bool candidate = !candidates_end_ && (next_candidate_ || stages_end_);
    status               = candidate ? candidate_step(*protection, tree, *out)
                                     : stage_step(catalog, transport, tree, epoch, budget, *out, registry);
    next_candidate_      = !candidate;
    out->more            = !(candidates_end_ && stages_end_);
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status ChunkCollector::candidate_step(ChunkProtectionCatalog &catalog, uint64_t tree, ChunkCollectionProgress &out)
{
    std::optional<ChunkCandidateStatus> record;
    auto status = candidate_active_ ? catalog.load_candidate(tree, *candidate_active_, &record)
                                    : catalog.next_candidate(tree, candidate_after_, &record);
    if (!status.ok()) {
        return status;
    }
    if (!record) {
        if (candidate_active_) {
            return Status::corruption("active candidate disappeared before reference release");
        }
        candidates_end_ = true;
        return Status::Ok();
    }
    if (!candidate_active_) {
        ++out.candidates_examined;
    }
    candidate_active_ = record->id;
    if (record->state == ChunkCandidateState::Preparing || record->state == ChunkCandidateState::Ready) {
        status = catalog.finish_candidate(tree, record->id, CT_CHUNK_CANDIDATE_RECOVER);
        if (status.ok()) {
            return status; // Query this same identity before releasing anything.
        }
        if (status.code() != Code::kUnavailable) {
            return status;
        }
        // A live writer or an unavailable proof retains its intents. Revisit
        // next sweep; it must not starve later abandoned identities.
    }
    else if ((record->state == ChunkCandidateState::Aborted || record->state == ChunkCandidateState::Reclaimed) &&
             record->released_windows < record->prepared_windows) {
        status = catalog.release_references(tree, record->id, record->released_windows);
        if (status.ok()) {
            ++out.reference_windows;
        }
        return status; // Unknown reply is resolved by load_candidate next time.
    }
    candidate_after_ = record->id;
    candidate_active_.reset();
    return Status::Ok();
}

Status ChunkCollector::stage_step(RootCatalog &catalog, ChunkTransport &transport, uint64_t tree, uint64_t epoch,
                                  const std::shared_ptr<AllocationBudget> &budget, ChunkCollectionProgress &out,
                                  const std::shared_ptr<ChunkPhysicalRegistry> &registry)
{
    std::optional<ChunkStageRecord> record{
        ChunkStageRecord{.tree_id             = 0,
                         .id                  = {},
                         .revision            = 0,
                         .chunk_high          = 0,
                         .chunk_low           = 0,
                         .capacity            = 0,
                         .manifest_generation = 0,
                         .purpose             = 0,
                         .state               = ChunkStageState::Reserved,
                         .packs = decltype(ChunkStageRecord::packs)(MetadataAllocator<ChunkStagePack>(budget))}
    };
    auto status = stage_active_ ? catalog.load_stage(tree, *stage_active_, &record)
                                : catalog.next_stage(tree, stage_after_, budget, &record);
    if (!status.ok()) {
        return status;
    }
    if (!record) {
        if (stage_active_) {
            stage_after_ = stage_active_;
            stage_active_.reset();
            pin_after_.reset();
        }
        else {
            stages_end_ = true;
        }
        return Status::Ok();
    }
    if (!stage_active_) {
        ++out.stages_examined;
        stage_active_ = record->id;
    }
    if (record->id.owner_epoch > epoch) {
        stage_after_ = record->id;
        stage_active_.reset();
        return Status::Ok();
    }
    if (record->state < ChunkStageState::Retired) {
        ChunkPinProgress progress;
        status = catalog.protection()->prune_pins(tree, record->id, pin_after_, 64, &progress);
        if (!status.ok()) {
            return status;
        }
        if (progress.examined > 64 || progress.released > progress.examined ||
            (progress.more && (!progress.after || (pin_after_ && *progress.after <= *pin_after_)))) {
            return Status::corruption("pin sweep did not advance its bounded cursor");
        }
        out.pins_examined += progress.examined;
        out.pins_released += progress.released;
        pin_after_ = progress.after;
        if (progress.more) {
            return Status::Ok();
        }
        if (record->revision == UINT64_MAX) {
            return Status::corruption();
        }
        auto retired = *record;
        ++retired.revision;
        retired.state = ChunkStageState::Retired;
        status        = catalog.compare_stage(tree, epoch, record, retired);
        if (!status.ok() && status.code() != Code::kUnavailable) {
            return status;
        }
        if (status.ok()) {
            pin_after_.reset();
            return Status::Ok(); // Durable retirement precedes physical deletion.
        }
    }
    else {
        const auto result = reclaim_retired_stage(catalog, transport, tree, epoch, record->id, budget, registry);
        if (!result.status.ok()) {
            return result.status;
        }
        if (result.stage_removed) {
            ++out.stages_removed;
            out.logical_capacity_bytes += record->capacity;
        }
    }
    stage_after_ = record->id;
    stage_active_.reset();
    pin_after_.reset();
    return Status::Ok();
}
} // namespace crowdb::tree::detail
