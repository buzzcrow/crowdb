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

Status ChunkPageStore::materialize_ownership(uint64_t *bytes_written, bool *complete)
{
    if (bytes_written == nullptr || complete == nullptr) {
        return Status::invalid_argument("chunk materialization requires output counters");
    }
    *bytes_written = 0;
    *complete      = false;
    auto fail      = [this](Status status) {
        materialization_failures_.fetch_add(1, std::memory_order_relaxed);
        return status;
    };
    if (staged_initialized_) {
        return fail(Status::unavailable("chunk materialization cannot race staged tree writes"));
    }
    auto base = catalog_->load(config_.tree_id);
    if (base == nullptr) {
        *complete = true;
        materialization_passes_.fetch_add(1, std::memory_order_relaxed);
        return Status::Ok();
    }
    Status base_status = validate_manifest(*base, *catalog_);
    if (!base_status.ok()) {
        return fail(std::move(base_status));
    }
    if (base->generation == std::numeric_limits<uint64_t>::max()) {
        return fail(Status::resource_exhausted("chunk manifest generation is exhausted"));
    }

    auto manifest               = std::make_shared<ChunkManifest>();
    manifest->format_version    = kChunkManifestFormat;
    manifest->tree_id           = config_.tree_id;
    manifest->generation        = base->generation + 1;
    manifest->owner_epoch       = config_.owner_epoch;
    manifest->logical_size      = base->logical_size;
    manifest->published_at_ms   = monotonic_millis();
    manifest->wal_replay_offset = base->wal_replay_offset;
    manifest->packs.reserve(base->packs.size());

    uint64_t copied_bytes   = 0;
    bool     shared_remains = false;
    bool     removed_packs  = false;
    for (const ChunkPagePack &base_pack : base->packs) {
        materialization_scan_bytes_.fetch_add(base_pack.ref.length, std::memory_order_relaxed);
        if (!materialization_live_extents_.empty() && !pack_is_live(base_pack)) {
            removed_packs = true;
            continue;
        }
        const bool shared        = base_pack.owner_tree_id != config_.tree_id;
        const bool within_budget = base_pack.ref.length <= config_.materialization_bytes_per_pass - copied_bytes;
        if (!shared || !within_budget) {
            ChunkPagePack reused = base_pack;
            reused.ordinal       = manifest->packs.size();
            reused.owner_tree_id = base_pack.owner_tree_id;
            reused.reused        = true;
            manifest->packs.push_back(reused);
            ++manifest->packs_reused;
            manifest->pack_bytes_reused += reused.ref.length;
            shared_remains |= shared;
            continue;
        }

        std::shared_ptr<const std::vector<uint8_t>> bytes;
        Status                                      read_status = read_pack(base_pack.ref, &bytes, {});
        if (!read_status.ok()) {
            orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
            return fail(std::move(read_status));
        }
        const auto   purpose         = chunk_purpose(base_pack.ref.chunk_id);
        const size_t physical_length = chunk_framed_length(bytes->size());
        if (!active_chunk_id_.empty() && (chunk_purpose(active_chunk_id_) != purpose ||
                                          base_pack.ref.length > config_.max_chunk_bytes - active_chunk_bytes_ ||
                                          physical_length > chunk_allocation_bytes() - active_chunk_cursor_)) {
            Status seal_status = transport_->seal_chunk(active_chunk_id_, config_.owner_epoch, active_chunk_cursor_);
            if (!seal_status.ok()) {
                orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
                active_chunk_id_     = {};
                active_chunk_bytes_  = 0;
                active_chunk_cursor_ = 0;
                return fail(std::move(seal_status));
            }
            active_chunk_id_     = {};
            active_chunk_bytes_  = 0;
            active_chunk_cursor_ = 0;
        }
        if (active_chunk_id_.empty()) {
            const uint64_t packs_per_chunk = (config_.max_chunk_bytes + config_.pack_bytes - 1) / config_.pack_bytes;
            const uint64_t physical_pack_bytes = chunk_framed_length(config_.pack_bytes);
            if (physical_pack_bytes == 0) {
                orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
                return fail(Status::resource_exhausted("chunk materialization framing exceeds address space"));
            }
            if (packs_per_chunk > std::numeric_limits<uint64_t>::max() / physical_pack_bytes) {
                orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
                return fail(Status::resource_exhausted("chunk materialization framing exceeds address space"));
            }
            Status allocate_status = transport_->allocate_mirror_chunk(packs_per_chunk * physical_pack_bytes,
                                                                       config_.owner_epoch, &active_chunk_id_, purpose);
            if (!allocate_status.ok()) {
                orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
                return fail(std::move(allocate_status));
            }
        }

        Status capacity_status = transport_->ensure_capacity(active_chunk_id_, active_chunk_cursor_ + physical_length);
        if (!capacity_status.ok()) {
            orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
            return fail(std::move(capacity_status));
        }
        std::vector<uint8_t> framed;
        if (physical_length == 0 ||
            crowdb::protocol::encode_frames(chunk_page_frame_magic(purpose),
                                            {.high = active_chunk_id_.high, .low = active_chunk_id_.low}, *bytes,
                                            monotonic_millis(), &framed) != crowdb::protocol::FrameError::Ok) {
            orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
            return fail(Status::resource_exhausted("chunk materialization frame encoding failed"));
        }
        for (uint32_t mirror = 0; mirror < config_.mirror_copies; ++mirror) {
            bool written = false;
            for (uint32_t attempt = 0; attempt <= config_.mirror_retry_limit; ++attempt) {
                mirror_write_attempts_.fetch_add(1, std::memory_order_relaxed);
                if ((mirror_write_failure_mask_.load(std::memory_order_acquire) & (1U << mirror)) == 0) {
                    const uint64_t started    = monotonic_nanos();
                    const Status write_status = transport_->write_mirror(active_chunk_id_, mirror, active_chunk_cursor_,
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
                orphan_bytes_.fetch_add(copied_bytes + base_pack.ref.length, std::memory_order_relaxed);
                return fail(
                    Status::unavailable("chunk materialization mirror write unavailable after bounded retries"));
            }
        }
        Status advance_status =
            transport_->advance_write(active_chunk_id_, active_chunk_cursor_, active_chunk_cursor_ + physical_length);
        if (!advance_status.ok()) {
            orphan_bytes_.fetch_add(copied_bytes + base_pack.ref.length, std::memory_order_relaxed);
            active_chunk_id_     = {};
            active_chunk_bytes_  = 0;
            active_chunk_cursor_ = 0;
            return fail(std::move(advance_status));
        }
        ChunkPagePack copied = base_pack;
        copied.ordinal       = manifest->packs.size();
        copied.owner_tree_id = config_.tree_id;
        copied.ref.chunk_id  = active_chunk_id_;
        copied.ref.offset    = active_chunk_cursor_;
        copied.reused        = false;
        manifest->packs.push_back(std::move(copied));
        active_chunk_bytes_ += base_pack.ref.length;
        active_chunk_cursor_ += physical_length;
        copied_bytes += base_pack.ref.length;
    }
    if (copied_bytes == 0 && !removed_packs) {
        *complete = !shared_remains;
        materialization_passes_.fetch_add(1, std::memory_order_relaxed);
        return Status::Ok();
    }

    Status segment_status = persist_reference_segments(manifest.get(), base.get());
    if (!segment_status.ok()) {
        orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
        return fail(std::move(segment_status));
    }
    materialized_metadata_segments_.fetch_add(
        std::count_if(manifest->reference_segments.begin(), manifest->reference_segments.end(),
                      [](const ChunkReferenceSegment &segment) { return !segment.reused; }),
        std::memory_order_relaxed);
    manifest->checksum       = manifest_checksum(*manifest);
    Status validation_status = validate_manifest(*manifest, *catalog_);
    if (!validation_status.ok()) {
        orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
        auto ids = reference_segment_ids(*manifest);
        orphan_reference_segments_.insert(orphan_reference_segments_.end(), ids.begin(), ids.end());
        return fail(std::move(validation_status));
    }
    const uint64_t publish_started = monotonic_nanos();
    Status         publish_status = catalog_->publish(config_.tree_id, base->generation, config_.owner_epoch, manifest);
    const uint64_t publish_elapsed = monotonic_nanos() - publish_started;
    manifest_publication_latency_ns_.fetch_add(publish_elapsed, std::memory_order_relaxed);
    rpc_latency_ns_.fetch_add(publish_elapsed, std::memory_order_relaxed);
    rpc_operations_.fetch_add(1, std::memory_order_relaxed);
    if (!publish_status.ok()) {
        orphan_bytes_.fetch_add(copied_bytes, std::memory_order_relaxed);
        auto ids = reference_segment_ids(*manifest);
        orphan_reference_segments_.insert(orphan_reference_segments_.end(), ids.begin(), ids.end());
        return fail(std::move(publish_status));
    }
    generations_published_.fetch_add(1, std::memory_order_relaxed);
    packs_written_.fetch_add(manifest->packs.size() - manifest->packs_reused, std::memory_order_relaxed);
    pack_bytes_written_.fetch_add(copied_bytes, std::memory_order_relaxed);
    packs_reused_.fetch_add(manifest->packs_reused, std::memory_order_relaxed);
    pack_bytes_reused_.fetch_add(manifest->pack_bytes_reused, std::memory_order_relaxed);
    materialization_passes_.fetch_add(1, std::memory_order_relaxed);
    materialization_packs_written_.fetch_add(manifest->packs.size() - manifest->packs_reused,
                                             std::memory_order_relaxed);
    materialization_bytes_written_.fetch_add(copied_bytes, std::memory_order_relaxed);
    cached_layout_.store(manifest, std::memory_order_release);
    bootstrap_layout_.store(nullptr, std::memory_order_release);
    layout_valid_until_ms_.store(monotonic_millis() + config_.layout_validity_ms, std::memory_order_release);
    *bytes_written = copied_bytes;
    *complete      = !shared_remains;
    materialization_live_extents_.clear();
    return Status::Ok();
}

} // namespace crowdb::tree::detail
