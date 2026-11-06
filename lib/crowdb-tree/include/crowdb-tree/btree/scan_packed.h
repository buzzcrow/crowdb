// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/slice.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <utility>

namespace crowdb::tree
{

// Growing byte buffer for scan's packed wire format, using std::malloc /
// std::realloc so the raw pointer can be transferred across the FFI and
// freed by ct_free_buf's std::free (R57: zero-copy scan result staging).
// The wire format per entry is:
//   [u32 klen][key][u64 slot][u8 tombstone][u32 vlen][value]
class ScanPackedBuf
{
  public:
    ScanPackedBuf() = default;

    explicit ScanPackedBuf(std::shared_ptr<AllocationBudget> budget) : budget_(std::move(budget))
    {
    }

    ~ScanPackedBuf()
    {
        std::free(data_);
    }

    ScanPackedBuf(const ScanPackedBuf &)            = delete;
    ScanPackedBuf &operator=(const ScanPackedBuf &) = delete;

    ScanPackedBuf(ScanPackedBuf &&o) noexcept
        : budget_(std::move(o.budget_)),
          charge_(std::move(o.charge_)),
          data_(o.data_),
          size_(o.size_),
          cap_(o.cap_)
    {
        o.data_ = nullptr;
        o.size_ = 0;
        o.cap_  = 0;
    }

    ScanPackedBuf &operator=(ScanPackedBuf &&o) noexcept
    {
        if (this != &o) {
            std::free(data_);
            charge_ = std::move(o.charge_);
            budget_ = std::move(o.budget_);
            data_   = o.data_;
            size_   = o.size_;
            cap_    = o.cap_;
            o.data_ = nullptr;
            o.size_ = 0;
            o.cap_  = 0;
        }
        return *this;
    }

    void reserve(size_t n)
    {
        if (n <= cap_) {
            return;
        }
        grow(n);
    }

    void pack_u32(uint32_t v)
    {
        ensure(4);
        for (int i = 0; i < 4; ++i) {
            data_[size_++] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
        }
    }

    void pack_u64(uint64_t v)
    {
        ensure(8);
        for (int i = 0; i < 8; ++i) {
            data_[size_++] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
        }
    }

    void push_back(uint8_t b)
    {
        ensure(1);
        data_[size_++] = b;
    }

    void append(const char *p, size_t n)
    {
        if (n == 0) {
            return;
        }
        ensure(n);
        std::memcpy(data_ + size_, p, n);
        size_ += n;
    }

    void append(const uint8_t *p, size_t n)
    {
        append(reinterpret_cast<const char *>(p), n);
    }

    void append(Slice s)
    {
        append(s.data(), s.size());
    }

    [[nodiscard]] size_t size() const
    {
        return size_;
    }

    [[nodiscard]] size_t capacity() const
    {
        return cap_;
    }

    [[nodiscard]] const uint8_t *data() const
    {
        return data_;
    }

    // Transfer ownership of the internal buffer to the caller. The
    // ScanPackedBuf no longer owns it and will not free it. The caller
    // must std::free the returned pointer.
    [[nodiscard]] uint8_t *release()
    {
        if (budget_) {
            // The legacy raw-buffer ABI transfers an application-owned copy.
            // Budgeted consumers keep this object instead of using release().
            auto *copy = static_cast<uint8_t *>(std::malloc(size_));
            if (copy == nullptr && size_ != 0) {
                throw std::bad_alloc();
            }
            if (size_ != 0) {
                std::memcpy(copy, data_, size_);
            }
            std::free(data_);
            data_ = nullptr;
            size_ = cap_ = 0;
            charge_.reset();
            return copy;
        }
        uint8_t *p = data_;
        data_      = nullptr;
        size_      = 0;
        cap_       = 0;
        return p;
    }

  private:
    void ensure(size_t need)
    {
        if (need > std::numeric_limits<size_t>::max() - size_) {
            throw std::bad_array_new_length();
        }
        if (size_ + need > cap_) {
            grow(size_ + need);
        }
    }

    void grow(size_t min_cap)
    {
        size_t new_cap = budget_ ? min_cap : 256;
        while (new_cap < min_cap) {
            if (new_cap > std::numeric_limits<size_t>::max() / 2) {
                new_cap = min_cap;
                break;
            }
            new_cap *= 2;
        }
        AllocationLease charge(budget_, new_cap, AllocationKind::kPayload);
        auto           *p = static_cast<uint8_t *>(std::malloc(new_cap));
        if (p == nullptr) {
            throw std::bad_alloc();
        }
        if (size_ != 0) {
            std::memcpy(p, data_, size_);
        }
        std::free(data_);
        charge_ = std::move(charge);
        data_   = p;
        cap_    = new_cap;
    }

    std::shared_ptr<AllocationBudget> budget_;
    AllocationLease                   charge_;
    uint8_t                          *data_ = nullptr;
    size_t                            size_ = 0;
    size_t                            cap_  = 0;
};

} // namespace crowdb::tree
