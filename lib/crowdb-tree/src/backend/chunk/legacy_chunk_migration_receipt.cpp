// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "legacy_chunk_read_store.h"
#include "snapshot/chunk_root_codec.h"

namespace crowdb::tree::detail
{
Status LegacyChunkReadStore::prepare_migration()
{
    if (!pinned_ || !verified_) {
        return Status::invalid_argument();
    }
    return catalog_->prepare_legacy_migration(tree_, layout_.generation(), manifest_);
}

Status LegacyChunkReadStore::resolve_migration(uint64_t generation, bool *committed)
{
    if (committed == nullptr || generation == 0 || generation == UINT64_MAX || !catalog_ ||
        catalog_->protection() == nullptr || !metadata_) {
        return Status::invalid_argument();
    }
    *committed  = false;
    auto status = catalog_->protection()->check_read_owner();
    if (status.ok()) {
        status = catalog_->protection()->load_root_owned(tree_, 0, &manifest_);
    }
    if (!status.ok()) {
        return status;
    }
    if (manifest_.size() >= 4 && frame_u32(manifest_.data(), 0) == kChunkManifestFormat) {
        return Status::Ok();
    }
    {
        ChunkRootManifest root{metadata_};
        status = decode_chunk_root(manifest_, tree_, 0, &root);
        if (!status.ok()) {
            return status;
        }
        if (root.identity.root_generation <= generation || root.identity.owner_epoch > epoch_) {
            return Status::unavailable("current Chunk root predates the requested migration");
        }
    }
    ChunkRootBytes receipt{MetadataAllocator<uint8_t>(metadata_)};
    status = catalog_->load_legacy_migration_owned(tree_, generation, &receipt);
    if (!status.ok()) {
        return status;
    }
    manifest_ = std::move(receipt);
    status    = LegacyChunkLayout::parse(manifest_, tree_, epoch_, &layout_);
    if (!status.ok() || layout_.generation() != generation) {
        return Status::corruption("legacy Chunk migration receipt identity differs");
    }
    // The immutable receipt predates root CAS. A newer typed root confirms
    // the data copy, while the original generation pin is released idempotently.
    pinned_    = true;
    *committed = true;
    return Status::Ok();
}
} // namespace crowdb::tree::detail
