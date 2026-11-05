// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_recovery.h"

#include "backend/chunk/chunk_physical_registry.h"
#include "btree/allocation_bound.h"

namespace crowdb::tree
{
ChunkRootRecovery::ChunkRootRecovery(ConstructionKey /*unused*/,
                                     std::shared_ptr<detail::AllocationReservation> metadata,
                                     std::shared_ptr<detail::AllocationReservation> window)
    : metadata_(std::move(metadata)),
      window_(std::move(window)),
      root_(metadata_),
      index_bytes_(detail::ReservationAllocator<uint8_t>(window_)),
      blob_(detail::ReservationAllocator<uint8_t, AllocationKind::kIo>(window_)),
      raw_(detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(window_))
{
}

std::optional<size_t> ChunkRootRecovery::allocation_bound(const ChunkRootManifest &root) noexcept
{
    using namespace detail;
    constexpr size_t node_links = 4 * sizeof(void *);
    return AllocationBound(shared_allocation_bound<ChunkRootRecovery>() +
                           shared_allocation_bound<CapturedDurablePages>())
        .add(chunk_pin_validation_bound())
        .add_product(root.indexes.size(), sizeof(ChunkIndexReference) + chunk_backing_allocation_bound())
        .add_product(root.frame_count,
                     sizeof(CapturedDurablePages::Entry) + node_links + (2 * shared_allocation_bound<BackingRef>()) +
                         chunk_backing_allocation_bound() + shared_allocation_bound<StoredOverflowReferences>() +
                         shared_allocation_bound<OverflowBackingSet>() + shared_allocation_bound<LeafProtection>())
        .add_product(root.stored_references, sizeof(StoredOverflowReference))
        .add_product(root.dependency_entries, sizeof(OverflowBackingSet::Entry) + node_links)
        .value();
}

std::optional<size_t> ChunkRootRecovery::window_allocation_bound(ChunkRecoveryLimits limits) noexcept
{
    using namespace detail;
    return AllocationBound(kChunkIndexWindowBytes)
        .add(limits.max_blob_bytes)
        .add(limits.max_frame_bytes)
        .add(chunk_backing_allocation_bound())
        .add(chunk_pin_validation_bound())
        .add(ChunkFixedPackStore::read_into_allocation_bound(std::max(kChunkIndexWindowBytes, limits.max_blob_bytes)))
        .value();
}

Status ChunkRootRecovery::create(ChunkRootManifest root, std::shared_ptr<const ChunkBackingDomain> domain,
                                 std::shared_ptr<detail::ChunkRuntimeIds> ids, ChunkRecoveryLimits limits,
                                 std::shared_ptr<detail::AllocationReservation> metadata,
                                 std::shared_ptr<detail::AllocationReservation> window,
                                 std::shared_ptr<ChunkRootRecovery>            *out)
try {
    if (!domain || !domain->supported() || !ids || !metadata || !window || out == nullptr || *out ||
        root.identity.tree != domain->tree || root.identity.root_generation == 0 ||
        limits.max_frame_bytes <= kFrameHeaderSize + kFrameTrailerSize || limits.max_blob_bytes < kDurableBlobHeader) {
        return Status::invalid_argument();
    }
    if (root.frame_count > limits.max_pages || root.stored_references > limits.max_references ||
        root.dependency_entries > limits.max_dependency_steps) {
        return Status::resource_exhausted();
    }
    auto result          = detail::reserved_shared<ChunkRootRecovery>(metadata, ConstructionKey{}, metadata, window);
    result->domain_      = std::move(domain);
    result->ids_         = std::move(ids);
    result->limits_      = limits;
    result->root_        = std::move(root);
    result->pages_       = detail::reserved_shared<CapturedDurablePages>(metadata, metadata);
    const auto &identity = result->root_.identity;
    auto       &pages    = *result->pages_;
    pages.root_page_id   = identity.root_page_id;
    pages.next_page_id   = identity.next_page_id;
    pages.coverage       = identity.coverage;
    pages.generation     = identity.page_generation;
    pages.backend_id     = result->domain_->backend_id;
    // Fixed capacities eliminate vector growth overlap across later windows.
    result->index_bytes_.reserve(kChunkIndexWindowBytes);
    result->blob_.reserve(limits.max_blob_bytes);
    result->raw_.reserve(limits.max_frame_bytes);
    *out = std::move(result);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status ChunkRootRecovery::next_pin(detail::ChunkStageId *out)
{
    detail::ChunkId unused;
    return ids_->next(PagePurpose::kBtreePage, out, &unused);
}

Status ChunkRootRecovery::step(bool stopped) noexcept
try {
    if (done_) {
        return status_;
    }
    if (stopped) {
        return finish(Status::unavailable());
    }
    auto status = domain_->catalog->protection()->check_read_owner();
    if (status.ok()) {
        if (dependency_phase_) {
            status = dependencies();
        }
        else if (left_ != 0) {
            status = frame();
        }
        else {
            status = index();
        }
    }
    if (!status.ok()) {
        return finish(std::move(status));
    }
    return status;
}
catch (const std::bad_alloc &) {
    return finish(Status::resource_exhausted());
}
catch (...) {
    return finish(Status::internal_error());
}

Status ChunkRootRecovery::finish(Status status)
{
    status_ = std::move(status);
    done_   = true;
    if (!status_.ok()) {
        pages_.reset();
    }
    return status_;
}

bool ChunkRootRecovery::retire_step() noexcept
{
    // The catalog owns its own bounded retirement; no synchronous map walk.
    pages_.reset();
    return true;
}
} // namespace crowdb::tree
