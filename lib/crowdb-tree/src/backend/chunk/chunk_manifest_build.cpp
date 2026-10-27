// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_catalog_codec.h"
#include "chunk_frame.h"
#include "chunk_page_store.h"
#include "crowdb-protocol/frame.h"

#include <algorithm>
#include <chrono>
#include <limits>

namespace crowdb::tree::detail
{
namespace
{
uint64_t monotonic_nanos()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

uint64_t monotonic_millis()
{
    return monotonic_nanos() / 1'000'000;
}
} // namespace

Status ChunkPageStore::build_manifest(uint64_t expected_generation, std::shared_ptr<ChunkManifest> *out,
                                      uint64_t *new_pack_bytes, ChunkCancellation cancellation)
{
    *new_pack_bytes = 0;
    if (expected_generation == std::numeric_limits<uint64_t>::max()) {
        return Status::resource_exhausted("chunk manifest generation is exhausted");
    }
    Status active_status = refresh_active_chunk();
    if (!active_status.ok()) {
        return active_status;
    }
    if (!orphan_reference_segments_.empty()) {
        catalog_->discard_reference_segments(config_.tree_id, orphan_reference_segments_);
        orphan_reference_segments_.clear();
    }
    auto manifest               = std::make_shared<ChunkManifest>();
    manifest->format_version    = kChunkManifestFormat;
    manifest->tree_id           = config_.tree_id;
    manifest->generation        = expected_generation + 1;
    manifest->owner_epoch       = config_.owner_epoch;
    manifest->logical_size      = staged_.size();
    manifest->published_at_ms   = monotonic_millis();
    manifest->wal_replay_offset = wal_replay_offset_.load(std::memory_order_acquire);
    auto reuse_base             = reuse_base_manifest();
    if (reuse_base != nullptr) {
        const RootCatalog &reuse_catalog =
            reuse_base == inherited_manifest_ && inherited_catalog_ != nullptr ? *inherited_catalog_ : *catalog_;
        Status status = validate_manifest(*reuse_base, reuse_catalog);
        if (!status.ok()) {
            return status;
        }
    }
    uint64_t offset = 0;
    while (offset < staged_.size()) {
        if (cancellation.cancelled()) {
            return Status::unavailable("chunk manifest build cancelled");
        }
        const size_t length  = staged_pack_length(offset, reuse_base.get());
        const auto   purpose = staged_purposes_.at(offset);
        if (reuse_base != nullptr && !range_has_pack(*reuse_base, offset, length) &&
            !range_was_written(offset, length)) {
            offset += length;
            continue;
        }
        if (reuse_base != nullptr) {
            const ChunkPagePack *reused =
                find_reusable_pack(*reuse_base, offset, static_cast<uint32_t>(length), cancellation);
            if (reused != nullptr && chunk_purpose(reused->ref.chunk_id) == purpose) {
                manifest->packs.push_back({
                    .owner_tree_id  = reused->owner_tree_id,
                    .ordinal        = manifest->packs.size(),
                    .logical_offset = offset,
                    .ref            = reused->ref,
                    .reused         = true,
                });
                ++manifest->packs_reused;
                manifest->pack_bytes_reused += length;
                offset += length;
                continue;
            }
        }
        if (!active_chunk_id_.empty() &&
            (chunk_purpose(active_chunk_id_) != purpose || length > config_.max_chunk_bytes - active_chunk_bytes_ ||
             chunk_framed_length(length) > chunk_allocation_bytes() - active_chunk_cursor_)) {
            Status seal_status = transport_->seal_chunk(active_chunk_id_, config_.owner_epoch, active_chunk_cursor_);
            if (!seal_status.ok()) {
                return seal_status;
            }
            active_chunk_id_     = {};
            active_chunk_bytes_  = 0;
            active_chunk_cursor_ = 0;
        }
        if (active_chunk_id_.empty()) {
            const uint64_t packs_per_chunk = (config_.max_chunk_bytes + config_.pack_bytes - 1) / config_.pack_bytes;
            const uint64_t physical_pack_bytes = chunk_framed_length(config_.pack_bytes);
            if (physical_pack_bytes == 0) {
                return Status::resource_exhausted("chunk page framing exceeds address space");
            }
            if (packs_per_chunk > std::numeric_limits<uint64_t>::max() / physical_pack_bytes) {
                return Status::resource_exhausted("chunk page framing exceeds address space");
            }
            Status allocate_status = transport_->allocate_mirror_chunk(packs_per_chunk * physical_pack_bytes,
                                                                       config_.owner_epoch, &active_chunk_id_, purpose);
            if (!allocate_status.ok()) {
                return allocate_status;
            }
        }

        Status capacity_status =
            transport_->ensure_capacity(active_chunk_id_, active_chunk_cursor_ + chunk_framed_length(length));
        if (!capacity_status.ok()) {
            return capacity_status;
        }
        ChunkPagePack pack;
        pack.owner_tree_id                   = config_.tree_id;
        pack.ordinal                         = manifest->packs.size();
        pack.logical_offset                  = offset;
        pack.ref.chunk_id                    = active_chunk_id_;
        pack.ref.offset                      = active_chunk_cursor_;
        pack.ref.length                      = static_cast<uint32_t>(length);
        const size_t         physical_length = chunk_framed_length(length);
        std::vector<uint8_t> framed;
        if (physical_length == 0 ||
            crowdb::protocol::encode_frames(chunk_page_frame_magic(purpose),
                                            {.high = active_chunk_id_.high, .low = active_chunk_id_.low},
                                            std::span<const uint8_t>(staged_.data() + offset, length),
                                            monotonic_millis(), &framed) != crowdb::protocol::FrameError::Ok) {
            return Status::resource_exhausted("chunk page frame encoding failed");
        }
        *new_pack_bytes += length;
        for (uint32_t mirror = 0; mirror < config_.mirror_copies; ++mirror) {
            bool written = false;
            for (uint32_t attempt = 0; attempt <= config_.mirror_retry_limit; ++attempt) {
                if (cancellation.cancelled()) {
                    return Status::unavailable("chunk manifest build cancelled");
                }
                mirror_write_attempts_.fetch_add(1, std::memory_order_relaxed);
                if ((mirror_write_failure_mask_.load(std::memory_order_acquire) & (1U << mirror)) == 0) {
                    const uint64_t started = monotonic_nanos();
                    Status write_status    = transport_->write_mirror(active_chunk_id_, mirror, active_chunk_cursor_,
                                                                      framed.data(), framed.size());
                    diskio_operations_.fetch_add(1, std::memory_order_relaxed);
                    diskio_latency_ns_.fetch_add(monotonic_nanos() - started, std::memory_order_relaxed);
                    if (write_status.ok()) {
                        written = true;
                        break;
                    }
                }
                mirror_write_failures_.fetch_add(1, std::memory_order_relaxed);
            }
            if (!written) {
                return Status::unavailable("chunk mirror write unavailable after bounded retries");
            }
        }
        if (cancellation.cancelled()) {
            return Status::unavailable("chunk manifest build cancelled");
        }
        Status advance_status =
            transport_->advance_write(active_chunk_id_, active_chunk_cursor_, active_chunk_cursor_ + physical_length);
        if (!advance_status.ok()) {
            return advance_status;
        }
        active_chunk_bytes_ += length;
        active_chunk_cursor_ += physical_length;
        manifest->packs.push_back(std::move(pack));
        offset += length;
    }
    Status segment_status = persist_reference_segments(manifest.get(), reuse_base.get());
    if (!segment_status.ok()) {
        return segment_status;
    }
    manifest->checksum = manifest_checksum(*manifest);
    *out               = std::move(manifest);
    return Status::Ok();
}

} // namespace crowdb::tree::detail
