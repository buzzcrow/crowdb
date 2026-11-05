// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "pending_page_times.h"

#include <cassert>
#include <utility>

namespace crowdb::tree
{
PendingPageTimes::PendingPageTimes(std::pmr::memory_resource *resource) : pages_(resource)
{
}

bool PendingPageTimes::insert(uint64_t pid, uint64_t since_ns)
{
    const auto [position, inserted] = pages_.try_emplace(pid);
    if (!inserted) {
        return false;
    }
    auto &entry = position->second;
    assert(tail_ == nullptr || tail_->since_ns <= since_ns);
    entry.since_ns = since_ns;
    entry.previous = tail_;
    if (tail_ != nullptr) {
        tail_->next = &entry;
    }
    else {
        head_ = &entry;
    }
    tail_ = &entry;
    return true;
}

void PendingPageTimes::unlink(Entry &entry) noexcept
{
    if (entry.previous != nullptr) {
        entry.previous->next = entry.next;
    }
    else {
        head_ = entry.next;
    }
    if (entry.next != nullptr) {
        entry.next->previous = entry.previous;
    }
    else {
        tail_ = entry.previous;
    }
}

void PendingPageTimes::erase(uint64_t pid) noexcept
{
    const auto position = pages_.find(pid);
    if (position != pages_.end()) {
        unlink(position->second);
        pages_.erase(position);
    }
}

void PendingPageTimes::erase_first() noexcept
{
    if (!pages_.empty()) {
        unlink(pages_.begin()->second);
        pages_.erase(pages_.begin());
    }
}

void PendingPageTimes::swap(PendingPageTimes &other) noexcept
{
    pages_.swap(other.pages_);
    std::swap(head_, other.head_);
    std::swap(tail_, other.tail_);
}

bool PendingPageTimes::contains(uint64_t pid) const
{
    return pages_.contains(pid);
}

bool PendingPageTimes::empty() const noexcept
{
    return pages_.empty();
}

size_t PendingPageTimes::size() const noexcept
{
    return pages_.size();
}

uint64_t PendingPageTimes::oldest_since_ns() const noexcept
{
    return head_ == nullptr ? 0 : head_->since_ns;
}

std::pmr::memory_resource *PendingPageTimes::resource() const noexcept
{
    return pages_.get_allocator().resource();
}
} // namespace crowdb::tree
