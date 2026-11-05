// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "page_cache_residency.h"

#include <map>

namespace crowdb::tree
{
// Preparation may run concurrently with cache completion. Allocation binds to
// the calling thread; every node retains its own credit for later reclamation.
class PageCacheNodeResource final : public std::pmr::memory_resource
{
  public:
    class Scope
    {
      public:
        explicit Scope(std::shared_ptr<detail::AllocationReservation> budget);
        ~Scope();
        Scope(const Scope &)            = delete;
        Scope &operator=(const Scope &) = delete;

      private:
        std::shared_ptr<detail::AllocationReservation> previous_;
    };

  private:
    void              *do_allocate(size_t bytes, size_t alignment) override;
    void               do_deallocate(void *pointer, size_t bytes, size_t alignment) override;
    [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override;
};

struct PageCacheResidency::State : std::enable_shared_from_this<State>
{
    struct Page;
    struct Base;

    struct Link
    {
        PageCacheKey key;
        Page        *page     = nullptr;
        Base        *base     = nullptr;
        Link        *previous = nullptr;
        Link        *next     = nullptr;
    };

    struct Page
    {
        explicit Page(const std::shared_ptr<detail::AllocationReservation> &metadata)
            : links(detail::ReservationAllocator<Link>(metadata))
        {
        }

        detail::ReservedVector<Link> links;
        size_t                       cold = 0;
    };

    struct Base
    {
        Link    *views = nullptr;
        uint64_t ready = 0;
    };

    using Pages = std::pmr::map<uint64_t, std::shared_ptr<Page>>;
    using Bases = std::pmr::map<PageCacheKey, std::shared_ptr<Base>>;
    enum class Kind : uint8_t { Mapping, Ready, Evict };

    struct Command
    {
        Kind     kind;
        Command *next    = nullptr;
        void    *context = nullptr;
    };

    explicit State(std::shared_ptr<TreeResourceMetrics> resources);
    static void prepare_page(Update &update, uint64_t pid, const std::shared_ptr<const PublishedPage> &replacement,
                             const std::shared_ptr<detail::AllocationReservation> &metadata);
    void        enqueue(Command *command) noexcept;
    bool        enter() noexcept;
    void        leave() noexcept;
    static void discard(Command *command) noexcept;
    void        wake() noexcept;
    void        drain() noexcept;
    void        apply_command(Command *command) noexcept;
    static void step(void *context) noexcept;
    void        apply(Update &update) noexcept;
    void        apply(Ready &ready, bool present) noexcept;
    void        unlink(Page &page) noexcept;
    void        attach(Page &page) noexcept;
    void        change(Base &base, bool present) noexcept;
    void        proxy(bool added) noexcept;
    void        erase_unused(const PageCacheKey &key) noexcept;

    PageCacheNodeResource                           allocator;
    Pages                                           pages{&allocator};
    Bases                                           bases{&allocator};
    const std::shared_ptr<TreeResourceMetrics>      metrics;
    std::atomic<Command *>                          incoming{nullptr};
    static constexpr uint64_t                       kClosed = uint64_t{1} << 63;
    std::atomic<uint64_t>                           producers{0};
    std::atomic<bool>                               clear_requested{false};
    std::shared_ptr<detail::FlushRuntime::Progress> progress;
    Command                                        *reversing = nullptr;
    Command                                        *ordered   = nullptr;
    std::atomic<uint64_t>                           proxies{0};
};

class PageCacheResidency::Update
{
  public:
    bool retire_step() noexcept
    {
        size_t steps = 0;
        for (; steps < 64 && !pages.empty(); ++steps) {
            pages.erase(pages.begin());
        }
        for (; steps < 64 && !bases.empty(); ++steps) {
            bases.erase(bases.begin());
        }
        return pages.empty() && bases.empty();
    }

    explicit Update(std::shared_ptr<State> state)
        : state(std::move(state)),
          pages(&this->state->allocator),
          bases(&this->state->allocator)
    {
        command.context = this;
    }

    std::shared_ptr<State>  state;
    State::Pages            pages;
    State::Bases            bases;
    State::Command          command{.kind = State::Kind::Mapping};
    std::shared_ptr<Update> queued_owner;
    std::shared_ptr<Update> following;
    bool                    reset = false;
};

class PageCacheResidency::Ready
{
  public:
    explicit Ready(std::shared_ptr<State> state) : state(std::move(state)), bases(&this->state->allocator)
    {
        added.context = removed.context = this;
    }

    std::shared_ptr<State> state;
    State::Bases           bases;
    PageCacheKey           key;
    State::Command         added{.kind = State::Kind::Ready};
    State::Command         removed{.kind = State::Kind::Evict};
    std::shared_ptr<Ready> added_owner;
    std::shared_ptr<Ready> removed_owner;
};
} // namespace crowdb::tree
