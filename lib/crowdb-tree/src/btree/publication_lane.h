// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace crowdb::tree
{
// Fixed publication window reserved before a Tree accepts proposals. Requests
// already own their metadata; this lane only bounds their simultaneous ingress.
class PublicationLane
{
  public:
    [[nodiscard]] size_t capacity() const noexcept
    {
        return capacity_;
    }

  private:
    friend class PublicationCoordinator;

    PublicationLane(uint64_t publisher, size_t capacity) : publisher_(publisher), capacity_(capacity)
    {
    }

    const uint64_t      publisher_;
    const size_t        capacity_;
    std::atomic<size_t> queued_{0};
};
} // namespace crowdb::tree
