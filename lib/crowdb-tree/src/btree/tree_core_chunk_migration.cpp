// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_backend_state.h"
#include "backend/chunk/legacy_chunk_recovery.h"
#include "btree/tree_core.h"
#include "snapshot/published_byte_export.h"
#include "snapshot/runtime_snapshot_import.h"

namespace crowdb::tree
{
namespace
{
Status copy_snapshot(const std::shared_ptr<TreeCore> &source, const std::shared_ptr<TreeCore> &destination)
{
    std::shared_ptr<PublishedByteExport> exported;
    const auto                          &range = source->config().key_range;
    const auto format = range.is_bounded() ? snapshot_format::kPortable : snapshot_format::kNative;
    auto       status = PublishedByteExport::create(source, format, kSnapshotChunkBytes, &exported, range);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<RuntimeSnapshotImport> imported;
    status    = RuntimeSnapshotImport::create(*destination, &imported);
    bool done = false;
    while (status.ok() && !done) {
        SnapshotChunk chunk;
        status = exported->next(&chunk, &done);
        if (status.ok()) {
            status = imported->feed(Slice(reinterpret_cast<const char *>(chunk.bytes.data()), chunk.bytes.size()));
        }
    }
    uint64_t coverage = 0;
    if (status.ok()) {
        status = imported->finish(&coverage);
    }
    if (status.ok() && coverage != source->durable_coverage()) {
        return Status::corruption("legacy Chunk migration changed the durable coverage");
    }
    return status;
}
} // namespace

Status TreeCore::migrate_chunk(const Config &config, uint64_t expected_generation, std::shared_ptr<TreeCore> *out)
{
    if (out == nullptr || *out || expected_generation == 0 || expected_generation == UINT64_MAX) {
        return Status::invalid_argument();
    }
    std::shared_ptr<detail::LegacyChunkReadStore> legacy;
    std::shared_ptr<TreeCore>                     source;
    std::shared_ptr<TreeCore>                     destination;
    Status                                        status;
    try {
        status = detail::recover_legacy_chunk_source(config, expected_generation, &legacy, &source);
        if (status.ok() && !source) {
            status = open_chunk(config, &destination);
            if (status.ok()) {
                status = legacy->release_pin();
            }
            if (status.ok()) {
                *out = std::move(destination);
            }
            return status;
        }
        if (status.ok()) {
            status = legacy->prepare_migration();
        }
        if (status.ok()) {
            status = open_chunk_migration(config, legacy, source->snapshot_sequence(), &destination);
        }
        if (status.ok()) {
            status = copy_snapshot(source, destination);
        }
        // The completed copy owns every imported frame; close source reads before
        // taking the checkpoint's complete write window.
        source.reset();
        if (status.ok()) {
            status = destination->checkpoint();
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    catch (...) {
        status = Status::internal_error();
    }
    // Terminal cleanup is also required when allocation or a host callback
    // throws. A confirmed unknown durable commit retains the same pin.
    source.reset();
    const bool uncertain = destination && destination->chunk_backend()->needs_recovery.load(std::memory_order_acquire);
    if (legacy && !uncertain) {
        const auto released = legacy->release_pin();
        if (status.ok()) {
            status = released;
        }
    }
    if (status.ok()) {
        *out = std::move(destination);
    }
    return status;
}

Status Crowdbtree::migrate_legacy_chunk(const Config &opt, uint64_t expected_generation,
                                        std::unique_ptr<Crowdbtree> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    std::shared_ptr<TreeCore> core;
    auto                      status = TreeCore::migrate_chunk(opt, expected_generation, &core);
    if (status.ok()) {
        out->reset(new Crowdbtree(opt, std::move(core)));
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (...) {
    return Status::internal_error();
}
} // namespace crowdb::tree
