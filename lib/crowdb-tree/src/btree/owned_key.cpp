// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "owned_key.h"

#include "tree_work_counters.h"

#include <cstring>
#include <vector>

namespace crowdb::tree
{
struct OwnedKey::Storage
{
    using Allocator = detail::ReservationAllocator<char>;

    std::vector<char, Allocator> bytes;

    Storage(Slice key, const std::shared_ptr<AllocationBudget> &metadata) : bytes(Allocator(metadata))
    {
        bytes.assign(key.data(), key.data() + key.size());
        record_work(metadata, TreeWorkCounter::KeyCopyBytes, key.size());
    }

    [[nodiscard]] const char *data() const noexcept
    {
        return bytes.data();
    }
};

OwnedKey::OwnedKey(Slice key, const std::shared_ptr<AllocationBudget> &metadata)
{
    if (key.size() <= inline_capacity) {
        if (!key.empty()) {
            std::memcpy(contents_.inline_bytes.data(), key.data(), key.size());
        }
        record_work(metadata, TreeWorkCounter::KeyCopyBytes, key.size());
    }
    else {
        auto        owner = detail::reserved_shared<Storage>(metadata, key, metadata);
        const auto *data  = owner->data();
        std::construct_at(&contents_.heap, Heap{.owner = std::move(owner), .data = data});
    }
    size_ = key.size();
}

OwnedKey::OwnedKey(const OwnedKey &other) noexcept : size_(other.size_)
{
    if (is_heap()) {
        std::construct_at(&contents_.heap, other.contents_.heap);
    }
    else {
        contents_.inline_bytes = other.contents_.inline_bytes;
    }
}

OwnedKey::OwnedKey(OwnedKey &&other) noexcept : size_(other.size_)
{
    if (is_heap()) {
        std::construct_at(&contents_.heap, std::move(other.contents_.heap));
        std::destroy_at(&other.contents_.heap);
        std::construct_at(&other.contents_.inline_bytes);
    }
    else {
        contents_.inline_bytes = other.contents_.inline_bytes;
    }
    other.size_ = 0;
}

OwnedKey &OwnedKey::operator=(const OwnedKey &other) noexcept
{
    if (this != &other) {
        OwnedKey copy(other);
        *this = std::move(copy);
    }
    return *this;
}

OwnedKey &OwnedKey::operator=(OwnedKey &&other) noexcept
{
    if (this != &other) {
        this->~OwnedKey();
        std::construct_at(this, std::move(other));
    }
    return *this;
}

OwnedKey::~OwnedKey()
{
    if (is_heap()) {
        std::destroy_at(&contents_.heap);
    }
}

size_t OwnedKey::allocation_overhead() noexcept
{
    return detail::shared_allocation_bound<Storage>() + 1;
}
} // namespace crowdb::tree
