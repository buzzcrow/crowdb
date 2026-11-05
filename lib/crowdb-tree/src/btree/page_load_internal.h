// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/chunk/chunk_fixed_pack.h"
#include "btree/page_load.h"
#include "crowdb-common/log.h"

namespace crowdb::tree
{
struct AsyncStorePageLoader::State
{
    std::shared_ptr<AsyncPageStore>             store;
    std::shared_ptr<detail::FlushRuntime>       runtime;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    PageLoadLimits                              limits;
    OverflowBackingResolver                     overflow;
    PageLoadBudget                              budget;
    std::atomic<size_t>                         inflight{0};
};

inline std::optional<size_t> chunk_load_window_bound(const PageLoadLimits &limits) noexcept
{
    return limits.max_chunk_blob_bytes == 0
             ? std::optional<size_t>{0}
             : detail::ChunkFixedPackStore::read_into_allocation_bound(limits.max_chunk_blob_bytes);
}

inline bool chunk_load_allowed(const BackingRef &backing, const PageLoadLimits &limits) noexcept
{
    return backing.storage != BackingStorage::kChunkPack || backing.length <= limits.max_chunk_blob_bytes;
}

using LoadBytes = detail::ReservedVector<uint8_t, AllocationKind::kScratch>;

Status decode_backing_frame(std::span<const uint8_t> blob, const BackingRef &backing, const PageLoadLimits &limits,
                            const std::shared_ptr<detail::AllocationReservation> &metadata, LoadBytes *out);

[[nodiscard]] std::optional<size_t> decoded_leaf_allocation_bound(const PageLoadLimits &limits) noexcept;
[[nodiscard]] std::optional<size_t> overflow_read_allocation_bound(const PageLoadLimits &limits, size_t workers,
                                                                   size_t retained_values, size_t value_bytes) noexcept;

// The charged storage supplies direct-I/O alignment itself, so a Block read
// does not allocate an unaccounted second copy in the backend adapter.
class AlignedLoadBytes
{
  public:
    static constexpr size_t kAlignment = 4096;

    explicit AlignedLoadBytes(detail::ReservationAllocator<uint8_t> allocator)
        : storage_(detail::ReservationAllocator<uint8_t, AllocationKind::kIo>(allocator.owner()))
    {
    }

    void resize(size_t length)
    {
        if (length > SIZE_MAX - (kAlignment - 1)) {
            throw std::bad_alloc();
        }
        const auto required = length + kAlignment - 1;
        if (required > storage_.size()) {
            // Release an old overflow window before growing; vector growth
            // must not retain two windows or geometrically exceed the bound.
            storage_ = decltype(storage_)(storage_.get_allocator());
            storage_.resize(required);
        }
        void *data      = storage_.data();
        auto  available = storage_.size();
        auto *aligned   = std::align(kAlignment, length, data, available);
        bytes_          = {static_cast<uint8_t *>(aligned), length};
    }

    [[nodiscard]] uint8_t *data() noexcept
    {
        return bytes_.data();
    }

    [[nodiscard]] const uint8_t *data() const noexcept
    {
        return bytes_.data();
    }

    [[nodiscard]] size_t size() const noexcept
    {
        return bytes_.size();
    }

    [[nodiscard]] size_t capacity() const noexcept
    {
        return storage_.capacity();
    }

  private:
    detail::ReservedVector<uint8_t, AllocationKind::kIo> storage_;
    std::span<uint8_t>                                   bytes_;
};

template <class Callback, class Result>
void complete_page_load(Callback &callback, Status status, Result result) noexcept
{
    try {
        callback(std::move(status), std::move(result));
    }
    catch (...) {
        CRB_LOG_ERROR("page load receiver threw after terminal");
    }
}

inline bool acquire_page_load(std::atomic<size_t> &inflight, size_t maximum)
{
    size_t current = inflight.load(std::memory_order_relaxed);
    while (current < maximum) {
        if (inflight.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}
} // namespace crowdb::tree
