// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "crowdb-tree/slice.h"

#include <array>
#include <compare>
#include <string>

namespace crowdb::tree
{
using KeyArenaStorage = std::basic_string<char, std::char_traits<char>, detail::ReservationAllocator<char>>;

// Immutable boundaries and continuation keys can cross workers and old views.
// Short keys live inside their containing object; long copies share storage.
// Borrowed slices remain valid until their owning key is moved or destroyed.
class OwnedKey
{
    struct Storage;

  public:
    OwnedKey() = default;
    OwnedKey(Slice key, const std::shared_ptr<AllocationBudget> &metadata = {});

    OwnedKey(const std::string &key) : OwnedKey(Slice(key))
    {
    }

    OwnedKey(const char *key) : OwnedKey(Slice(key))
    {
    }

    OwnedKey(const OwnedKey &other) noexcept;
    OwnedKey &operator=(const OwnedKey &other) noexcept;
    OwnedKey(OwnedKey &&other) noexcept;
    OwnedKey &operator=(OwnedKey &&other) noexcept;
    ~OwnedKey();

    // Per-key fixed allocation overhead; add the key bytes separately.
    [[nodiscard]] static size_t allocation_overhead() noexcept;

    [[nodiscard]] operator Slice() const noexcept
    {
        return {data(), size_};
    }

    [[nodiscard]] size_t size() const noexcept
    {
        return size_;
    }

    [[nodiscard]] const char *data() const noexcept
    {
        if (size_ == 0) {
            return nullptr;
        }
        return is_heap() ? contents_.heap.data : contents_.inline_bytes.data();
    }

    [[nodiscard]] bool operator==(const OwnedKey &other) const noexcept
    {
        return Slice(*this) == Slice(other);
    }

    [[nodiscard]] std::strong_ordering operator<=>(const OwnedKey &other) const noexcept
    {
        return Slice(*this).compare(Slice(other)) <=> 0;
    }

    [[nodiscard]] bool operator==(const std::string &other) const noexcept
    {
        return Slice(*this) == Slice(other);
    }

    [[nodiscard]] std::strong_ordering operator<=>(const std::string &other) const noexcept
    {
        return Slice(*this).compare(Slice(other)) <=> 0;
    }

    [[nodiscard]] bool operator==(const char *other) const noexcept
    {
        return Slice(*this) == Slice(other);
    }

    [[nodiscard]] std::strong_ordering operator<=>(const char *other) const noexcept
    {
        return Slice(*this).compare(Slice(other)) <=> 0;
    }

  private:
    static constexpr size_t inline_capacity = 24;

    struct Heap
    {
        std::shared_ptr<const Storage> owner;
        const char                    *data = nullptr;
    };

    union Contents {
        std::array<char, inline_capacity> inline_bytes;
        Heap                              heap;

        Contents() : inline_bytes{}
        {
        }

        ~Contents()
        {
        }
    } contents_;

    size_t size_ = 0;

    [[nodiscard]] bool is_heap() const noexcept
    {
        return size_ > inline_capacity;
    }
};
} // namespace crowdb::tree
