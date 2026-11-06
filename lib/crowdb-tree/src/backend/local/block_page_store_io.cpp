// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "block_account.h"
#include "block_directory.h"
#include "btree/allocation_account.h"
#include "btree/tree_work_counters.h"
#include "crowdb-tree/backend/block_page_store.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>

namespace crowdb::tree
{
namespace
{
// O_DIRECT's buffer-address alignment requirement is filesystem/device
// specific but 4096 (a common page/sector size) satisfies virtually every
// real device; safe to over-align a bit beyond iu_size_ when iu_size_ is
// smaller (e.g. a caller-supplied 512 on a regular file).
constexpr size_t kDirectBufAlign = 4096;

size_t align_up(size_t v, size_t a)
{
    return (v + a - 1) / a * a;
}

// RAII aligned scratch buffer (posix_memalign), used for the bounce path
// when a caller's offset/length/buffer isn't already O_DIRECT-aligned.
class AlignedBuffer
{
  public:
    AlignedBuffer(size_t len, size_t align, std::shared_ptr<AllocationBudget> budget) : budget_(std::move(budget))
    {
        size_t a = align > kDirectBufAlign ? align : kDirectBufAlign;
        len_     = len == 0 ? a : len;
        if (budget_ && !budget_->claim(len_)) {
            throw std::bad_alloc();
        }
        if (::posix_memalign(reinterpret_cast<void **>(&ptr_), a, len == 0 ? a : len) != 0) {
            ptr_ = nullptr;
            if (budget_) {
                budget_->release(len_);
            }
        }
        len_ = ptr_ != nullptr ? len_ : 0;
        if (ptr_ != nullptr && budget_) {
            budget_->record_allocation(AllocationKind::kIo, len_, true);
        }
    }

    ~AlignedBuffer()
    {
        std::free(ptr_);
        if (ptr_ != nullptr && budget_) {
            budget_->record_allocation(AllocationKind::kIo, len_, false);
            budget_->release(len_);
        }
    }

    AlignedBuffer(const AlignedBuffer &)            = delete;
    AlignedBuffer &operator=(const AlignedBuffer &) = delete;

    [[nodiscard]] uint8_t *data() const
    {
        return ptr_;
    }

    [[nodiscard]] bool ok() const
    {
        return ptr_ != nullptr;
    }

  private:
    std::shared_ptr<AllocationBudget> budget_;
    uint8_t                          *ptr_ = nullptr;
    size_t                            len_ = 0;
};
} // namespace

Status BlockPageStore::write_at_extents(uint64_t off, const uint8_t *buf, size_t len,
                                        const std::shared_ptr<AllocationBudget> &budget)
{
    auto prepared = ensure_extents(off, len, budget);
    if (!prepared.ok()) {
        return prepared;
    }

    // Split write across extent boundaries
    size_t   done = 0;
    uint64_t cur  = off;
    while (done < len) {
        uint64_t local = 0;
        auto     file  = file_for_offset(cur, &local, budget);
        if (!file) {
            return Status::io_error("BlockPageStore: write extent unavailable");
        }
        uint64_t avail = block_size_ - local;
        size_t   chunk = std::min(static_cast<uint64_t>(len - done), avail);

        // For IU=1, write directly. For IU>1, use the bounce path via medium.
        Status s;
        if (iu_size_ <= 1) {
            s = file->pwrite_at(local, buf + done, chunk);
        }
        else {
            // Use the same alignment logic as single-medium mode
            bool aligned = (local % iu_size_ == 0) && (chunk % iu_size_ == 0) &&
                           (reinterpret_cast<uintptr_t>(buf + done) % kDirectBufAlign == 0);
            if (aligned) {
                s = file->pwrite_at(local, buf + done, chunk);
            }
            else {
                uint64_t      start = (local / iu_size_) * iu_size_;
                uint64_t      e     = align_up(local + chunk, iu_size_);
                size_t        span  = e - start;
                AlignedBuffer scratch(span, iu_size_, budget);
                if (!scratch.ok()) {
                    return Status::io_error("BlockPageStore: aligned scratch allocation failed");
                }
                size_t got = 0;
                s          = file->pread_partial(start, scratch.data(), span, &got);
                if (s.ok()) {
                    if (got < span) {
                        std::memset(scratch.data() + got, 0, span - got);
                    }
                    std::memcpy(scratch.data() + (local - start), buf + done, chunk);
                    record_work(budget, TreeWorkCounter::BackendBounceCopyBytes, chunk);
                    s = file->pwrite_at(start, scratch.data(), span);
                }
            }
        }
        if (!s.ok()) {
            return s;
        }
        done += chunk;
        cur += chunk;
    }
    return Status::Ok();
}

Status BlockPageStore::read_at_extents(uint64_t off, uint8_t *buf, size_t len,
                                       const std::shared_ptr<AllocationBudget> &budget) const
{
    size_t   done = 0;
    uint64_t cur  = off;
    while (done < len) {
        uint64_t local = 0;
        auto     file  = file_for_offset(cur, &local, budget);
        if (!file) {
            return Status::not_found("BlockPageStore: read extent unavailable");
        }
        uint64_t avail = block_size_ - local;
        size_t   chunk = std::min(static_cast<uint64_t>(len - done), avail);

        Status s;
        if (iu_size_ <= 1) {
            s = file->pread_at(local, buf + done, chunk);
        }
        else {
            bool aligned = (local % iu_size_ == 0) && (chunk % iu_size_ == 0) &&
                           (reinterpret_cast<uintptr_t>(buf + done) % kDirectBufAlign == 0);
            if (aligned) {
                s = file->pread_at(local, buf + done, chunk);
            }
            else {
                uint64_t      start = (local / iu_size_) * iu_size_;
                uint64_t      e     = align_up(local + chunk, iu_size_);
                size_t        span  = e - start;
                AlignedBuffer scratch(span, iu_size_, budget);
                if (!scratch.ok()) {
                    return Status::io_error("BlockPageStore: aligned scratch allocation failed");
                }
                size_t got = 0;
                s          = file->pread_partial(start, scratch.data(), span, &got);
                if (s.ok()) {
                    if (got < (local - start) + chunk) {
                        return Status::not_found("BlockPageStore: read past end");
                    }
                    std::memcpy(buf + done, scratch.data() + (local - start), chunk);
                    record_work(budget, TreeWorkCounter::BackendBounceCopyBytes, chunk);
                }
            }
        }
        if (!s.ok()) {
            return s;
        }
        done += chunk;
        cur += chunk;
    }
    return Status::Ok();
}

Status BlockPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    return write_at(off, buf, len, allocations_);
}

Status BlockPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len,
                                const std::shared_ptr<AllocationBudget> &budget)
try {
    if (!budget) {
        return write_at(off, buf, len);
    }
    if (!detail::valid_block_budget(*this, budget)) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }
    if (len == 0) {
        return Status::Ok();
    }
    if (off > UINT64_MAX - len) {
        return Status::invalid_argument("BlockPageStore: byte range overflow");
    }

    // Array-of-blocks mode
    if (blocks_) {
        return write_at_extents(off, buf, len, budget);
    }

    // Single-medium mode
    // IU=1 (byte-aligned): no alignment checks, no bounce buffer.
    if (iu_size_ <= 1) {
        return medium_->pwrite_at(off, buf, len);
    }

    bool aligned =
        (off % iu_size_ == 0) && (len % iu_size_ == 0) && (reinterpret_cast<uintptr_t>(buf) % kDirectBufAlign == 0);
    if (aligned) {
        return medium_->pwrite_at(off, buf, len);
    }

    // Bounce through an IU-aligned scratch span covering [off, off+len):
    // read-modify-write, since O_DIRECT can't write a sub-IU or misaligned
    // range directly.
    uint64_t      start = (off / iu_size_) * iu_size_;
    uint64_t      end   = align_up(off + len, iu_size_);
    size_t        span  = end - start;
    AlignedBuffer scratch(span, iu_size_, budget);
    if (!scratch.ok()) {
        return Status::io_error("BlockPageStore: aligned scratch allocation failed");
    }
    size_t got = 0;
    Status rs  = medium_->pread_partial(start, scratch.data(), span, &got);
    if (!rs.ok()) {
        return rs;
    }
    if (got < span) {
        // Genuinely new territory past the current end (growing the
        // store) rather than a read failure -- zero-fill the unread tail.
        std::memset(scratch.data() + got, 0, span - got);
    }
    std::memcpy(scratch.data() + (off - start), buf, len);
    record_work(budget, TreeWorkCounter::BackendBounceCopyBytes, len);
    return medium_->pwrite_at(start, scratch.data(), span);
}

catch (const std::bad_alloc &) {
    return Status::resource_exhausted("backend metadata allocation unavailable");
}

Status BlockPageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    return read_at(off, buf, len, allocations_);
}

Status BlockPageStore::read_at(uint64_t off, uint8_t *buf, size_t len,
                               const std::shared_ptr<AllocationBudget> &budget) const
try {
    if (!budget) {
        return read_at(off, buf, len);
    }
    if (!detail::valid_block_budget(*this, budget)) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }
    if (len == 0) {
        return Status::Ok();
    }
    if (off > UINT64_MAX - len) {
        return Status::invalid_argument("BlockPageStore: byte range overflow");
    }

    // Array-of-blocks mode
    if (blocks_) {
        return read_at_extents(off, buf, len, budget);
    }

    // Single-medium mode
    // IU=1 (byte-aligned): no alignment checks, no bounce buffer.
    if (iu_size_ <= 1) {
        return medium_->pread_at(off, buf, len);
    }

    bool aligned =
        (off % iu_size_ == 0) && (len % iu_size_ == 0) && (reinterpret_cast<uintptr_t>(buf) % kDirectBufAlign == 0);
    if (aligned) {
        return medium_->pread_at(off, buf, len);
    }

    uint64_t      start = (off / iu_size_) * iu_size_;
    uint64_t      end   = align_up(off + len, iu_size_);
    size_t        span  = end - start;
    AlignedBuffer scratch(span, iu_size_, budget);
    if (!scratch.ok()) {
        return Status::io_error("BlockPageStore: aligned scratch allocation failed");
    }
    size_t got = 0;
    Status rs  = medium_->pread_partial(start, scratch.data(), span, &got);
    if (!rs.ok()) {
        return rs;
    }
    if (got < (off - start) + len) {
        return Status::not_found("BlockPageStore: read past end");
    }
    std::memcpy(buf, scratch.data() + (off - start), len);
    record_work(budget, TreeWorkCounter::BackendBounceCopyBytes, len);
    return Status::Ok();
}

catch (const std::bad_alloc &) {
    return Status::resource_exhausted("backend metadata allocation unavailable");
}

} // namespace crowdb::tree
