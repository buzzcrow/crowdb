// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_runtime_page_store.h"

#include "btree/tree_core.h"
#include "chunk_frame.h"
#include "chunk_physical_registry.h"
#include "chunk_pin_release.h"

#include <bit>

namespace crowdb::tree::detail
{
ChunkRuntimePageStore::ChunkRuntimePageStore(ChunkRuntimeOptions options, std::shared_ptr<RootCatalog> catalog,
                                             std::shared_ptr<ChunkTransport>          transport,
                                             const std::shared_ptr<AllocationBudget> &budget)
    : ChunkRuntimePageStore(options, std::move(catalog), std::move(transport),
                            AllocationLease(budget, sizeof(ChunkRuntimePageStore), AllocationKind::kMetadata))
{
}

ChunkRuntimePageStore::ChunkRuntimePageStore(ChunkRuntimeOptions options, std::shared_ptr<RootCatalog> catalog,
                                             std::shared_ptr<ChunkTransport> transport, AllocationLease allocation,
                                             const void *host_domain)
    : allocation_(std::move(allocation)),
      host_domain_(host_domain),
      options_(options),
      catalog_(std::move(catalog)),
      transport_(std::move(transport))
{
}

PageStore::RuntimeFactory ChunkRuntimePageStore::runtime_factory() const noexcept
{
    return &TreeCore::open_chunk;
}

bool ChunkRuntimePageStore::supported() const noexcept
{
    return options_.tree != 0 && options_.owner_epoch != 0 && std::has_single_bit(options_.iu_size) &&
           options_.iu_size <= kMaxChunkPackBytes && options_.mirrors != 0 && options_.mirrors <= kMaxMirrorCopies &&
           options_.max_chunk_bytes <= kMaxChunkPackBytes && options_.max_chunk_bytes >= options_.iu_size && catalog_ &&
           catalog_->stage_schema_supported() && catalog_->protection() != nullptr && catalog_->pin_release_queue() &&
           transport_ && transport_->fixed_staging_supported();
}

Status ChunkRuntimePageStore::make_domain(uint64_t identity, const std::shared_ptr<AllocationBudget> &budget,
                                          std::shared_ptr<const ChunkBackingDomain> *out) const
try {
    if (!supported()) {
        return Status::not_supported("Chunk runtime needs fixed packs, protection "
                                     "authority and a host release service");
    }
    if (!budget || identity == 0 || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    if (!physical_registry_) {
        auto status = transport_->physical_registry(options_.tree, budget, &physical_registry_);
        if (!status.ok()) {
            return status;
        }
    }
    else if (!physical_registry_->uses_queue(budget->retirement_queue().get())) {
        return Status::invalid_argument("Chunk PageStore belongs to another host retirement domain");
    }
    auto domain               = std::allocate_shared<ChunkBackingDomain>(MetadataAllocator<ChunkBackingDomain>(budget));
    domain->backend_id        = identity;
    domain->tree              = options_.tree;
    domain->epoch             = options_.owner_epoch;
    domain->mirrors           = options_.mirrors;
    domain->max_chunk_bytes   = options_.max_chunk_bytes;
    domain->catalog           = catalog_;
    domain->transport         = transport_;
    domain->physical_registry = physical_registry_;
    *out                      = std::move(domain);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status ChunkRuntimePageStore::prepare_writer(const std::shared_ptr<AllocationBudget> &budget)
{
    if (writer_session_) {
        return Status::Ok();
    }
    std::shared_ptr<ChunkPinOwner> session;
    auto                           status =
        catalog_->pin_release_queue()->prepare_writer(budget, catalog_, options_.tree, options_.owner_epoch, &session);
    if (!status.ok()) {
        return status;
    }
    if (!catalog_->claim_runtime_writer()) {
        return Status::unavailable();
    }
    session->arm();
    writer_session_ = std::move(session);
    return Status::Ok();
}

std::shared_ptr<ChunkPinOwner> ChunkRuntimePageStore::take_writer() noexcept
{
    return std::move(writer_session_);
}

Status ChunkRuntimePageStore::write_at(uint64_t /*offset*/, const uint8_t * /*data*/, size_t /*length*/)
{
    return Status::not_supported("typed Chunk runtime has no mutable byte-address namespace");
}

Status ChunkRuntimePageStore::read_at(uint64_t /*offset*/, uint8_t * /*data*/, size_t /*length*/) const
{
    return Status::not_supported("typed Chunk runtime reads require exact pack identities");
}

Status ChunkRuntimePageStore::sync()
{
    return Status::not_supported("Chunk durability requires an exact root authority commit");
}

void ChunkRuntimePageStore::request_wal_offset(uint64_t value) noexcept
{
    auto previous = wal_requested_.load(std::memory_order_relaxed);
    while (previous < value && !wal_requested_.compare_exchange_weak(previous, value, std::memory_order_release)) {
    }
}

void ChunkRuntimePageStore::committed(uint64_t generation, uint64_t wal_offset) noexcept
{
    // Serialized root recovery/publication supplies the already verified pair.
    wal_committed_.store(wal_offset, std::memory_order_release);
    generation_.store(generation, std::memory_order_release);
    request_wal_offset(wal_offset);
}
} // namespace crowdb::tree::detail
