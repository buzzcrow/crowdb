// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_backing.h"

#include "btree/allocation_bound.h"
#include "chunk_frame.h"
#include "chunk_physical_registry.h"

#include <algorithm>

namespace crowdb::tree
{
static Status read_chunk_pack(const ChunkPackBacking &backing, std::span<uint8_t> output,
                              const std::shared_ptr<AllocationBudget> &budget);
static Status write_chunk_pack(ChunkPackBacking &backing, std::span<const uint8_t> input,
                               const std::shared_ptr<AllocationBudget> &budget);

namespace
{
Status validate_pin_pack(const ChunkBackingDomain &domain, const detail::ChunkFixedPackRef &pack,
                         const std::shared_ptr<AllocationBudget> &metadata)
{
    std::optional<detail::ChunkStageRecord> record(detail::ChunkStageRecord{
        .tree_id             = 0,
        .id                  = {},
        .revision            = 0,
        .chunk_high          = 0,
        .chunk_low           = 0,
        .capacity            = 0,
        .manifest_generation = 0,
        .purpose             = 0,
        .state               = detail::ChunkStageState::Reserved,
        .packs = decltype(detail::ChunkStageRecord::packs)(MetadataAllocator<detail::ChunkStagePack>(metadata))});
    auto                                    status = domain.catalog->load_stage(domain.tree, pack.stage, &record);
    if (!status.ok()) {
        return status;
    }
    if (!record || record->tree_id != domain.tree || record->id != pack.stage ||
        record->chunk_high != pack.chunk.high || record->chunk_low != pack.chunk.low ||
        record->purpose != static_cast<uint32_t>(pack.purpose) || pack.offset > record->capacity ||
        detail::chunk_framed_length(pack.length) > record->capacity - pack.offset ||
        std::none_of(record->packs.begin(), record->packs.end(), [&](const auto &stored) {
            return stored.offset == pack.offset && stored.length == pack.length && stored.checksum == pack.checksum;
        })) {
        return Status::corruption("Chunk pack is not an exact member of its protected stage");
    }
    return record->state == detail::ChunkStageState::Reloadable || record->state == detail::ChunkStageState::Referenced
             ? Status::Ok()
             : Status::unavailable("Chunk stage no longer admits readers");
}
} // namespace

size_t chunk_pin_validation_bound() noexcept
{
    return 2 * detail::kMaxStagePacks * sizeof(detail::ChunkStagePack);
}

Status prepare_chunk_backing(const std::shared_ptr<const ChunkBackingDomain> &domain, detail::ChunkStageId stage,
                             detail::ChunkId chunk, uint32_t length, PagePurpose purpose,
                             const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<ChunkPackBacking> *out)
try {
    if (!domain || !domain->supported() || !metadata || out == nullptr || *out || stage.owner_epoch != domain->epoch ||
        detail::chunk_purpose(chunk) != purpose || detail::chunk_framed_length(length) > domain->max_chunk_bytes ||
        !detail::ChunkFixedPackStore::write_allocation_bound(length)) {
        return Status::invalid_argument();
    }
    auto result   = std::allocate_shared<ChunkPackBacking>(MetadataAllocator<ChunkPackBacking>(metadata));
    result->read  = &read_chunk_pack;
    result->write = &write_chunk_pack;
    auto status   = domain->catalog->pin_release_queue()->prepare(metadata, domain->catalog, domain->tree, stage, stage,
                                                                  &result->pin);
    if (!status.ok()) {
        return status;
    }
    result->physical_budget = metadata;
    result->domain          = domain;
    result->pack = {.stage = stage, .chunk = chunk, .offset = 0, .length = length, .checksum = 0, .purpose = purpose};
    *out         = std::move(result);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status pin_chunk_backing(const std::shared_ptr<const ChunkBackingDomain> &domain, detail::ChunkFixedPackRef pack,
                         detail::ChunkStageId pin, const std::shared_ptr<AllocationBudget> &metadata,
                         std::shared_ptr<const ChunkPackBacking> *out)
try {
    if (!domain || !domain->supported() || !metadata || out == nullptr || *out || pin.owner_epoch != domain->epoch) {
        return Status::invalid_argument();
    }
    auto result    = std::allocate_shared<ChunkPackBacking>(MetadataAllocator<ChunkPackBacking>(metadata));
    result->read   = &read_chunk_pack;
    result->write  = &write_chunk_pack;
    result->domain = domain;
    result->pack   = pack;
    auto status    = domain->catalog->pin_release_queue()->prepare(metadata, domain->catalog, domain->tree, pack.stage,
                                                                   pin, &result->pin);
    if (!status.ok()) {
        return status;
    }
    if (!chunk_backing_matches(*result, domain->backend_id, pack.length)) {
        return Status::corruption("checkpoint pack identity is invalid");
    }
    auto *protection = domain->catalog->protection();
    status           = protection->check_read_owner();
    if (!status.ok()) {
        return status;
    }
    status = validate_pin_pack(*domain, pack, metadata);
    if (!status.ok()) {
        return status;
    }
    result->pin->arm();
    status = protection->change_pin(domain->tree, pack.stage, pin, true);
    if (status.ok()) {
        status = protection->check_read_owner();
    }
    if (status.ok() && domain->physical_registry) {
        auto registry = domain->physical_registry;
        status        = registry->prepare(pack.stage, pack.chunk, metadata, &result->physical);
        detail::ChunkAllocationIdentity receipt;
        if (status.ok()) {
            status = domain->transport->inspect_fixed_allocation(pack.chunk, pack.stage.owner_epoch, &receipt);
        }
        if (status.ok()) {
            status = registry->observe(result->physical, receipt, metadata);
        }
        if (status.ok()) {
            result->physical_metrics = ChunkPhysicalRegistry::extent(result->physical);
        }
    }
    if (status.ok()) {
        *out = std::move(result);
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

static Status read_chunk_pack(const ChunkPackBacking &backing, std::span<uint8_t> output,
                              const std::shared_ptr<AllocationBudget> &budget)
{
    if (!backing.domain || !chunk_backing_matches(backing, backing.domain->backend_id, output.size())) {
        return Status::corruption("read does not name one owned Chunk pack");
    }
    const auto &domain     = *backing.domain;
    auto       *protection = domain.catalog->protection();
    auto        status     = protection->check_read_owner();
    if (!status.ok()) {
        return status;
    }
    detail::ChunkFixedPackStore store(domain.tree, domain.epoch, domain.mirrors, domain.catalog, domain.transport);
    status = store.read_into(backing.pack, budget, output);
    if (status.ok()) {
        status = protection->check_read_owner();
    }
    return status;
}

static Status write_chunk_pack(ChunkPackBacking &backing, std::span<const uint8_t> input,
                               const std::shared_ptr<AllocationBudget> &budget)
{
    if (!backing.domain || !chunk_backing_matches(backing, backing.domain->backend_id, input.size()) ||
        backing.pack.stage.owner_epoch != backing.domain->epoch) {
        return Status::invalid_argument("write does not name one reserved Chunk pack");
    }
    const auto &domain = *backing.domain;
    auto        status = domain.catalog->protection()->check_read_owner();
    if (!status.ok()) {
        return status;
    }
    backing.pin->arm();
    detail::ChunkFixedPackStore store(domain.tree, domain.epoch, domain.mirrors, domain.catalog, domain.transport,
                                      domain.physical_registry);
    detail::ChunkFixedPackRef   result;
    status = store.write(backing.pack.stage, backing.pack.chunk, backing.pack.purpose, input, budget, &result,
                         &backing.physical, backing.physical_budget);
    if (status.ok()) {
        status = domain.catalog->protection()->check_read_owner();
    }
    backing.physical_metrics = ChunkPhysicalRegistry::extent(backing.physical);
    if (status.ok()) {
        backing.pack = result;
    }
    return status;
}
} // namespace crowdb::tree
