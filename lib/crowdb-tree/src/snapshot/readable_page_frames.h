// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/budget_memory_resource.h"
#include "crowdb-tree/maptable/mapping_geometry.h"
#include "snapshot/readable_page_frame.h"

#include <map>

namespace crowdb::tree
{
// The durable mapping uses the same segment boundaries in memory and on disk.
// Copying a catalog shares segment owners; the first mutation copies that
// segment alone. A segment's allocations keep their original credit owners.
class ReadablePageFrames
{
    struct Segment;
    using Segments = std::pmr::map<uint64_t, std::shared_ptr<Segment>>;

    template <bool Const> class Position
    {
        friend class ReadablePageFrames;
        template <bool> friend class Position;
        using Owner = std::conditional_t<Const, const ReadablePageFrames, ReadablePageFrames>;

        Position(Owner *owner, uint64_t pid) : owner_(owner), pid_(pid)
        {
        }

        Owner   *owner_ = nullptr;
        uint64_t pid_   = UINT64_MAX;

      public:
        using iterator_category = std::forward_iterator_tag;
        using value_type        = std::pair<const uint64_t, DurableCapturedFrame>;
        using difference_type   = std::ptrdiff_t;
        using reference         = std::conditional_t<Const, const value_type &, value_type &>;
        using pointer           = std::conditional_t<Const, const value_type *, value_type *>;
        Position()              = default;

        template <bool Other>
            requires(Const && !Other)
        Position(const Position<Other> &other) : owner_(other.owner_),
                                                 pid_(other.pid_)
        {
        }

        reference operator*() const
        {
            return owner_->entry(pid_);
        }

        pointer operator->() const
        {
            return &**this;
        }

        Position &operator++()
        {
            pid_ = owner_->next(pid_);
            return *this;
        }

        Position operator++(int)
        {
            auto old = *this;
            ++*this;
            return old;
        }

        template <bool Other> bool operator==(const Position<Other> &other) const
        {
            return owner_ == other.owner_ && pid_ == other.pid_;
        }
    };

  public:
    using value_type     = std::pair<const uint64_t, DurableCapturedFrame>;
    using iterator       = Position<false>;
    using const_iterator = Position<true>;
    explicit ReadablePageFrames(std::shared_ptr<detail::AllocationReservation> metadata = {});
    ~ReadablePageFrames();
    ReadablePageFrames(const ReadablePageFrames &source);
    ReadablePageFrames &operator=(const ReadablePageFrames &source);
    void                inherit(const ReadablePageFrames &source);
    void                budget(std::shared_ptr<detail::AllocationReservation> metadata);
    [[nodiscard]] bool  same_segment(const ReadablePageFrames &other, uint64_t segment) const noexcept;
    [[nodiscard]] std::pair<size_t, size_t> primary_counts(uint64_t segment) const;
    void                                    refresh_counts();
    [[nodiscard]] TreeSummary               shape() const;

    [[nodiscard]] size_t segment_count() const noexcept
    {
        return segments_.size();
    }

    bool retire_step() noexcept;

    [[nodiscard]] size_t size() const noexcept
    {
        return size_;
    }

    [[nodiscard]] bool empty() const noexcept
    {
        return size_ == 0;
    }

    iterator begin()
    {
        return lower_bound(0);
    }

    [[nodiscard]] const_iterator begin() const
    {
        return lower_bound(0);
    }

    iterator end()
    {
        return {this, UINT64_MAX};
    }

    [[nodiscard]] const_iterator end() const
    {
        return {this, UINT64_MAX};
    }

    iterator lower_bound(uint64_t pid)
    {
        return {this, lower(pid)};
    }

    [[nodiscard]] const_iterator lower_bound(uint64_t pid) const
    {
        return {this, lower(pid)};
    }

    iterator find(uint64_t pid)
    {
        return {this, contains(pid) ? pid : UINT64_MAX};
    }

    [[nodiscard]] const_iterator find(uint64_t pid) const
    {
        return {this, contains(pid) ? pid : UINT64_MAX};
    }

    [[nodiscard]] bool contains(uint64_t pid) const;

    DurableCapturedFrame &at(uint64_t pid)
    {
        return entry(pid).second;
    }

    [[nodiscard]] const DurableCapturedFrame &at(uint64_t pid) const
    {
        return entry(pid).second;
    }

    std::pair<iterator, bool> emplace(uint64_t pid, DurableCapturedFrame frame);
    std::pair<iterator, bool> insert_or_assign(uint64_t pid, DurableCapturedFrame frame);

    template <class Iterator> void insert(Iterator first, Iterator last)
    {
        for (; first != last; ++first) {
            emplace(first->first, first->second);
        }
    }

    size_t erase(uint64_t pid);

    iterator erase(iterator position)
    {
        auto pid = position.pid_;
        ++position;
        erase(pid);
        return position;
    }

    [[nodiscard]] static size_t segment_allocation_bound() noexcept;

  private:
    Segment                                       &mutable_segment(uint64_t segment);
    value_type                                    &entry(uint64_t pid);
    [[nodiscard]] const value_type                &entry(uint64_t pid) const;
    [[nodiscard]] uint64_t                         next(uint64_t pid) const;
    [[nodiscard]] uint64_t                         lower(uint64_t pid) const;
    [[nodiscard]] TreeSummary                      segment_shape(uint64_t segment) const;
    detail::BudgetMemoryResource                   resource_;
    Segments                                       segments_;
    std::shared_ptr<detail::AllocationReservation> metadata_;
    size_t                                         size_ = 0;
};
} // namespace crowdb::tree
