// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_commit.h"

#include "backend/chunk/chunk_physical_registry.h"
#include "btree/allocation_bound.h"

#include <algorithm>

namespace crowdb::tree
{
ChunkRootCommit::ChunkRootCommit(ConstructionKey /*unused*/, std::shared_ptr<detail::AllocationReservation> metadata)
    : metadata_(std::move(metadata)),
      root_(metadata_),
      manifest_(detail::ReservationAllocator<uint8_t>(metadata_)),
      index_pins_(detail::ReservationAllocator<std::shared_ptr<const ChunkPackBacking>>(metadata_)),
      references_(std::less<>{}, detail::ReservationAllocator<detail::ChunkStageId>(metadata_))
{
}

Status ChunkRootCommit::create(std::shared_ptr<const CapturedDurablePages> pages,
                               std::shared_ptr<const ChunkBackingDomain>   domain,
                               std::shared_ptr<detail::ChunkRuntimeIds> ids, ChunkRootIdentity identity,
                               std::shared_ptr<detail::AllocationReservation> metadata,
                               std::shared_ptr<detail::AllocationReservation> window,
                               std::shared_ptr<ChunkRootCommit> *out, size_t max_dependency_steps)
try {
    if (max_dependency_steps == 0 || max_dependency_steps > detail::kMaxCandidateReferences || !pages || !domain ||
        !domain->supported() || !ids || !metadata || !window || out == nullptr || *out ||
        identity.tree != domain->tree || identity.owner_epoch != domain->epoch || identity.root_generation == 0 ||
        identity.page_generation != pages->generation || identity.coverage != pages->coverage ||
        identity.root_page_id != pages->root_page_id || identity.next_page_id != pages->next_page_id ||
        (!pages->frames.empty() && pages->backend_id != domain->backend_id) ||
        !allocation_bound(pages->frames.size())) {
        return Status::invalid_argument();
    }
    auto result                   = detail::reserved_shared<ChunkRootCommit>(metadata, ConstructionKey{}, metadata);
    result->max_dependency_steps_ = max_dependency_steps;
    result->window_               = std::move(window);
    result->pages_                = std::move(pages);
    result->domain_               = std::move(domain);
    result->ids_                  = std::move(ids);
    result->root_.identity        = identity;
    result->root_.frame_count     = result->pages_->frames.size();
    const auto indexes            = (result->root_.frame_count + kChunkIndexFrameLimit - 1) / kChunkIndexFrameLimit;
    result->root_.indexes.reserve(indexes);
    result->index_pins_.reserve(indexes);
    detail::ChunkId unused;
    auto            status = result->ids_->next(PagePurpose::kPageIndex, &result->candidate_, &unused);
    if (!status.ok()) {
        return status;
    }
    result->page_ = result->pages_->frames.begin();
    *out          = std::move(result);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

std::optional<size_t> ChunkRootCommit::allocation_bound(size_t frames) noexcept
{
    using namespace detail;
    if (frames > kMaxCandidateReferences) {
        return {};
    }
    const auto indexes = (frames + kChunkIndexFrameLimit - 1) / kChunkIndexFrameLimit;
    const auto root_bytes =
        AllocationBound(kChunkRootHeaderBytes + 4).add_product(indexes, kChunkRootEntryBytes).value();
    if (!root_bytes || *root_bytes > kMaxCandidateManifestBytes || indexes > kMaxCandidateReferences - frames) {
        return {};
    }
    return AllocationBound(shared_allocation_bound<ChunkRootCommit>())
        .add(*root_bytes)

        .add_product(frames + indexes, sizeof(ChunkStageId) + (8 * sizeof(void *)))
        .add_product(indexes, sizeof(ChunkIndexReference) + sizeof(std::shared_ptr<const ChunkPackBacking>) +
                                  chunk_backing_allocation_bound())
        .value();
}

std::optional<size_t> ChunkRootCommit::window_allocation_bound(size_t stored_references) noexcept
{
    const auto refs  = std::min(stored_references, kChunkIndexFrameLimit * kChunkIndexReferencesPerFrame);
    const auto bytes = kChunkIndexHeaderBytes + 4 + (kChunkIndexFrameLimit * kChunkIndexRecordBytes) + (16 * refs);
    return detail::AllocationBound(bytes).add(detail::ChunkFixedPackStore::write_allocation_bound(bytes)).value();
}

bool ChunkRootCommit::retire_step() noexcept
{
    for (size_t i = 0; i < 64 && !references_.empty(); ++i) {
        references_.erase(references_.begin());
    }
    for (size_t i = 0; i < 64 && !index_pins_.empty(); ++i) {
        index_pins_.pop_back();
    }
    return references_.empty() && index_pins_.empty();
}

Status ChunkRootCommit::step(bool stop_requested) noexcept
try {
    if (done()) {
        return status_.without_message();
    }
    if (stop_requested && stage_ != Stage::QueryCommit && stage_ != Stage::QueryBegin &&
        stage_ != Stage::QueryPrepare && stage_ != Stage::Abort && stage_ != Stage::QueryAbort &&
        stage_ != Stage::Release) {
        return fail(Status::unavailable("Chunk checkpoint stopped before root commit"));
    }
    return advance();
}
catch (const std::bad_alloc &) {
    return fail(Status::resource_exhausted());
}
catch (...) {
    return fail(Status::internal_error());
}

Status ChunkRootCommit::advance()
{
    switch (stage_) {
    case Stage::Index:
        return index();
    case Stage::Dependencies:
        return dependencies();
    case Stage::Checksum:
        return checksum();
    case Stage::Begin:
        return begin();
    case Stage::Prepare:
        return prepare();
    case Stage::Commit:
        return finish_candidate(true);
    case Stage::Abort:
        return finish_candidate(false);
    case Stage::QueryBegin:
    case Stage::QueryPrepare:
    case Stage::QueryCommit:
    case Stage::QueryAbort:
        return query();
    case Stage::Release:
        return release();
    case Stage::Done:
        return status_.without_message();
    }
    return finish(Status::internal_error());
}

Status ChunkRootCommit::fail(Status status)
{
    if (status_.ok()) {
        status_ = std::move(status);
    }
    if (stage_ == Stage::QueryCommit || stage_ == Stage::QueryBegin || stage_ == Stage::QueryPrepare ||
        stage_ == Stage::QueryAbort || stage_ == Stage::Abort || stage_ == Stage::Release) {
        return finish(status_.without_message(), true);
    }
    if (candidate_started_) {
        stage_ = Stage::Abort;
        return Status::Ok();
    }
    return finish(status_.without_message());
}

Status ChunkRootCommit::finish(Status status, bool uncertain)
{
    status_    = std::move(status);
    uncertain_ = uncertain;
    stage_     = Stage::Done;
    return status_.without_message();
}
} // namespace crowdb::tree
