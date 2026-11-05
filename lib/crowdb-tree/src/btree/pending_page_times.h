// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>
#include <map>
#include <memory_resource>

namespace crowdb::tree
{
// Coordinator-owned membership. Map nodes also form a chronological list, so
// the oldest pending timestamp needs no scan or additional index allocation.
class PendingPageTimes
{
    struct Entry
    {
        uint64_t since_ns = 0;
        Entry   *previous = nullptr;
        Entry   *next     = nullptr;
    };

  public:
    explicit PendingPageTimes(std::pmr::memory_resource *resource);
    PendingPageTimes(const PendingPageTimes &)                                   = delete;
    PendingPageTimes                        &operator=(const PendingPageTimes &) = delete;
    bool                                     insert(uint64_t pid, uint64_t since_ns);
    void                                     erase(uint64_t pid) noexcept;
    void                                     erase_first() noexcept;
    void                                     swap(PendingPageTimes &other) noexcept;
    [[nodiscard]] bool                       contains(uint64_t pid) const;
    [[nodiscard]] bool                       empty() const noexcept;
    [[nodiscard]] size_t                     size() const noexcept;
    [[nodiscard]] uint64_t                   oldest_since_ns() const noexcept;
    [[nodiscard]] std::pmr::memory_resource *resource() const noexcept;

  private:
    void                           unlink(Entry &entry) noexcept;
    std::pmr::map<uint64_t, Entry> pages_;
    Entry                         *head_ = nullptr;
    Entry                         *tail_ = nullptr;
};
} // namespace crowdb::tree
