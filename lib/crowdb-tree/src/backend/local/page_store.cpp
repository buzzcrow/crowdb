// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Backend-neutral in-memory page-store implementation.

#include "crowdb-tree/backend/page_store.h"

#include "memory_backing_test.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <unordered_map>

namespace crowdb::tree
{
#ifdef CROWDB_TREE_TEST_UTIL
namespace
{
thread_local std::optional<size_t> new_pages_before_failure;
}

void set_memory_backing_failure_after_for_tests(std::optional<size_t> pages) noexcept
{
    new_pages_before_failure = pages;
}
#endif

// ── MemPageStore ──────────────────────────────────────────────────

struct MemPageStore::Data
{
    static constexpr size_t kPageBytes = MemPageStore::kBackingPageBytes;
    using Page                         = std::array<uint8_t, kPageBytes>;
    std::unordered_map<uint64_t, std::unique_ptr<Page>> pages;
    uint64_t                                            bytes = 0;

    void prepare(uint64_t off, size_t len)
    {
        std::unordered_map<uint64_t, std::unique_ptr<Page>> prepared;
        const auto                                          last = (off + len - 1) / kPageBytes;
        for (auto index = off / kPageBytes; index <= last; ++index) {
            if (!pages.contains(index)) {
#ifdef CROWDB_TREE_TEST_UTIL
                if (new_pages_before_failure) {
                    if (*new_pages_before_failure == 0) {
                        throw std::bad_alloc();
                    }
                    --*new_pages_before_failure;
                }
#endif
                prepared.emplace(index, std::make_unique<Page>());
            }
        }
        if (!prepared.empty()) {
            if (prepared.size() > pages.max_size() - pages.size()) {
                throw std::bad_alloc();
            }
            // Reserve the index before transferring nodes. Failure releases
            // every new data page while leaving the stored bytes unchanged.
            const auto required = pages.size() + prepared.size();
            if (pages.empty() || required > pages.bucket_count()) {
                const auto grown =
                    pages.bucket_count() <= pages.max_size() / 2 ? 2 * pages.bucket_count() : pages.max_size();
                pages.reserve(std::max(required, grown));
            }
            pages.merge(prepared);
        }
    }

    void write(uint64_t off, const uint8_t *buf, size_t len)
    {
        while (len != 0) {
            const auto offset = static_cast<size_t>(off % kPageBytes);
            const auto count  = std::min(len, kPageBytes - offset);
            std::memcpy(pages.at(off / kPageBytes)->data() + offset, buf, count);
            off += count;
            buf += count;
            len -= count;
        }
        bytes = std::max(bytes, off);
    }

    void read(uint64_t off, uint8_t *buf, size_t len) const
    {
        while (len != 0) {
            const auto offset = static_cast<size_t>(off % kPageBytes);
            const auto count  = std::min(len, kPageBytes - offset);
            const auto page   = pages.find(off / kPageBytes);
            if (page == pages.end()) {
                std::memset(buf, 0, count);
            }
            else {
                std::memcpy(buf, page->second->data() + offset, count);
            }
            off += count;
            buf += count;
            len -= count;
        }
    }
};

MemPageStore::MemPageStore(uint32_t iu_size) : data_(std::make_unique<Data>()), iu_size_(iu_size)
{
}

MemPageStore::~MemPageStore() = default;

Status MemPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
try {
    if (len == 0) {
        return Status::Ok();
    }
    if (buf == nullptr || off > std::numeric_limits<uint64_t>::max() - len) {
        return Status::invalid_argument("memory write has a null input or overflowing range");
    }
    std::scoped_lock lk(mu_);
    // Grow only the written pages. Existing data never moves, and sparse holes
    // do not become physical allocations during checkpoint address growth.
    data_->prepare(off, len);
    data_->write(off, buf, len);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("memory backing page allocation failed");
}

Status MemPageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    std::scoped_lock lk(mu_);
    if (off > data_->bytes || len > data_->bytes - off) {
        return Status::io_error("MemPageStore: read past end");
    }
    if (len != 0 && buf == nullptr) {
        return Status::invalid_argument("memory read has a null output");
    }
    data_->read(off, buf, len);
    return Status::Ok();
}

uint64_t MemPageStore::size() const
{
    std::scoped_lock lk(mu_);
    return data_->bytes;
}

size_t MemPageStore::resident_bytes() const
{
    std::scoped_lock lk(mu_);
    return data_->pages.size() * Data::kPageBytes;
}

bool MemPageStore::release_backing_page(uint64_t index)
{
    std::scoped_lock lk(mu_);
    return data_->pages.erase(index) != 0;
}

} // namespace crowdb::tree
