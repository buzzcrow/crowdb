// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_stage_gc.h"

#include "chunk_physical_registry.h"

namespace crowdb::tree::detail
{
ChunkStageGcResult reclaim_retired_stage(RootCatalog &catalog, ChunkTransport &transport, uint64_t tree, uint64_t epoch,
                                         ChunkStageId id, const std::shared_ptr<AllocationBudget> &budget,
                                         const std::shared_ptr<ChunkPhysicalRegistry> &registry)
try {
    ChunkStageGcResult result;
    if (!catalog.stage_schema_supported() || !transport.fixed_staging_supported()) {
        result.status = Status::not_supported();
        return result;
    }
    if (epoch < id.owner_epoch) {
        result.status = Status::unavailable("stage belongs to a newer writer");
        return result;
    }
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
    result.status = catalog.load_stage(tree, id, &record);
    if (!result.status.ok() || !record) {
        return result;
    }
    if (record->state == ChunkStageState::Retired) {
        if (record->revision == UINT64_MAX) {
            result.status = Status::corruption();
            return result;
        }
        auto deleting = *record;
        ++deleting.revision;
        deleting.state = ChunkStageState::Deleting;
        result.status  = catalog.compare_stage(tree, epoch, record, deleting);
        if (!result.status.ok()) {
            return result;
        }
        record = std::move(deleting);
    }
    if (record->state == ChunkStageState::Deleting) {
        ChunkAllocationIdentity physical;
        result.status = transport.inspect_fixed_allocation({record->chunk_high, record->chunk_low},
                                                           record->id.owner_epoch, &physical);
        if (!result.status.ok() && result.status.code() != Code::kNotFound) {
            return result;
        }
        std::optional<ChunkAllocationIdentity> receipt;
        if (result.status.ok()) {
            receipt = physical;
            if (registry) {
                std::shared_ptr<ChunkPhysicalPack> observed;
                result.status =
                    registry->prepare(record->id, {record->chunk_high, record->chunk_low}, budget, &observed);
                if (result.status.ok()) {
                    result.status = registry->observe(observed, physical, budget);
                }
                if (!result.status.ok()) {
                    return result;
                }
            }
        }
        result.status = transport.drain_fixed_chunk({record->chunk_high, record->chunk_low});
        if (!result.status.ok() && result.status.code() != Code::kNotFound) {
            return result;
        }
        // A completed earlier delete can make the drain lookup absent. The
        // durable Deleting authority still requires an idempotent delete ack.
        result.status = transport.delete_chunk({record->chunk_high, record->chunk_low});
        if (!result.status.ok()) {
            return result;
        }
        result.deletion_acknowledged = true;
        if (registry) {
            result.status = registry->deleted(record->id, {record->chunk_high, record->chunk_low}, receipt, budget);
            if (!result.status.ok()) {
                return result;
            }
        }
        if (record->revision == UINT64_MAX) {
            result.status = Status::corruption();
            return result;
        }
        auto reclaimed = *record;
        ++reclaimed.revision;
        reclaimed.state = ChunkStageState::Reclaimed;
        result.status   = catalog.compare_stage(tree, epoch, record, reclaimed);
        if (!result.status.ok()) {
            return result;
        }
        record = std::move(reclaimed);
    }
    if (record->state != ChunkStageState::Reclaimed) {
        result.status = Status::unavailable("stage still protects a live or unresolved pack");
        return result;
    }
    // A persisted Reclaimed record is itself proof of an acknowledged physical
    // delete, including recovery after the final metadata erase was interrupted.
    if (registry) {
        result.status = registry->deleted(record->id, {record->chunk_high, record->chunk_low}, {}, budget);
        if (!result.status.ok()) {
            return result;
        }
    }
    result.status        = catalog.compare_stage(tree, epoch, record, {});
    result.stage_removed = result.status.ok();
    return result;
}
catch (const std::bad_alloc &) {
    return {.status = Status::resource_exhausted()};
}
} // namespace crowdb::tree::detail
