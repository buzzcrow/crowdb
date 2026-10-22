// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_frame.h"
#include "chunk_page_store.h"
#include "crowdb-common/log.h"

#include <chrono>

namespace crowdb::tree::detail
{
namespace
{
uint64_t monotonic_nanos()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
} // namespace

Status ChunkPageStore::read_pack(const ChunkPageRef &ref, std::shared_ptr<const std::vector<uint8_t>> *out,
                                 ChunkCancellation cancellation) const
{
    if (cancellation.cancelled()) {
        return Status::unavailable("chunk page read cancelled");
    }
    const bool use_cache = cancellation.cancelled_id != nullptr;
    auto       cached    = use_cache ? cached_pack_ : nullptr;
    if (cached != nullptr && cached->ref.chunk_id == ref.chunk_id && cached->ref.offset == ref.offset &&
        cached->ref.length == ref.length) {
        if (cancellation.cancelled()) {
            return Status::unavailable("chunk page read cancelled");
        }
        *out = cached->bytes;
        coalesced_reads_.fetch_add(1, std::memory_order_relaxed);
        coalesced_read_bytes_.fetch_add(ref.length, std::memory_order_relaxed);
        return Status::Ok();
    }
    pack_reads_.fetch_add(1, std::memory_order_relaxed);
    const size_t physical_length = chunk_framed_length(ref.length);
    if (physical_length == 0) {
        return Status::corruption("chunk page frame length overflows");
    }
    auto framed           = std::make_shared<std::vector<uint8_t>>(physical_length);
    bool mirror_responded = false;
    for (uint32_t mirror = 0; mirror < config_.mirror_copies; ++mirror) {
        if (cancellation.cancelled()) {
            return Status::unavailable("chunk page read cancelled");
        }
        const uint64_t started = monotonic_nanos();
        Status read_status = transport_->read_mirror(ref.chunk_id, mirror, ref.offset, framed->data(), framed->size());
        diskio_operations_.fetch_add(1, std::memory_order_relaxed);
        diskio_latency_ns_.fetch_add(monotonic_nanos() - started, std::memory_order_relaxed);
        if (!read_status.ok()) {
            CRB_LOG_ERROR("chunk page store tree={} mirror={} read failed: chunk={}:{} offset={} length={} status={}",
                          config_.tree_id, mirror, ref.chunk_id.high, ref.chunk_id.low, ref.offset, physical_length,
                          read_status.to_string());
            continue;
        }
        mirror_responded       = true;
        auto       payload     = std::make_shared<std::vector<uint8_t>>();
        const auto frame_error = crowdb::protocol::parse_frames(
            *framed, {.high = ref.chunk_id.high, .low = ref.chunk_id.low},
            chunk_page_frame_magic(chunk_purpose(ref.chunk_id)), ref.length, payload.get());
        if (frame_error == crowdb::protocol::FrameError::Ok) {
            auto entry = std::make_shared<CachedPack>(CachedPack{.ref = ref, .bytes = payload});
            if (use_cache) {
                cached_pack_ = std::move(entry);
            }
            *out = std::move(payload);
            return cancellation.cancelled() ? Status::unavailable("chunk page read cancelled") : Status::Ok();
        }
        CRB_LOG_ERROR("chunk page store tree={} mirror={} frame verification failed: chunk={}:{} offset={} length={}",
                      config_.tree_id, mirror, ref.chunk_id.high, ref.chunk_id.low, ref.offset, physical_length);
    }
    return mirror_responded ? Status::corruption("chunk page frame verification failed on every mirror")
                            : Status::unavailable("chunk page pack mirrors are unavailable");
}

} // namespace crowdb::tree::detail
