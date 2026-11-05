// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_operation_lifetime.h"

#include "btree/allocation_bound.h"

#include <stdexcept>

namespace crowdb::tree
{
struct TreeOperationLifetime::State
{
    static constexpr uint64_t kClosed = uint64_t{1} << 63;

    State(size_t limit, std::shared_ptr<const void> owner) : maximum(limit), host_owner(std::move(owner))
    {
    }

    void release() noexcept
    {
        active.fetch_sub(1, std::memory_order_acq_rel);
        active.notify_all();
    }

    const size_t                maximum;
    std::shared_ptr<const void> host_owner;
    std::atomic<uint64_t>       active{0};
    std::atomic<uint64_t>       denied{0};
};

TreeOperationLifetime::TreeOperationLifetime(size_t                                                maximum,
                                             const std::shared_ptr<detail::AllocationReservation> &metadata,
                                             std::shared_ptr<const void>                           host_owner)
{
    if (maximum == 0 || maximum >= State::kClosed) {
        throw std::invalid_argument("invalid Tree operation capacity");
    }
    state_ = detail::reserved_shared<State>(metadata, maximum, std::move(host_owner));
}

TreeOperationLifetime::~TreeOperationLifetime()
{
    close();
}

Status TreeOperationLifetime::acquire(const std::shared_ptr<detail::AllocationReservation> &metadata,
                                      std::shared_ptr<Ticket>                              *out)
{
    return acquire_impl(metadata, out, false);
}

Status TreeOperationLifetime::try_acquire(const std::shared_ptr<detail::AllocationReservation> &metadata,
                                          std::shared_ptr<Ticket>                              *out)
{
    return acquire_impl(metadata, out, true);
}

Status TreeOperationLifetime::acquire_impl(const std::shared_ptr<detail::AllocationReservation> &metadata,
                                           std::shared_ptr<Ticket> *out, bool temporary_capacity)
{
    if (out == nullptr || *out) {
        return Status::invalid_argument("operation ticket output must be empty");
    }
    auto active = state_->active.load(std::memory_order_acquire);
    for (;;) {
        if ((active & State::kClosed) != 0) {
            return Status::unavailable("Tree operation admission closed");
        }
        if (active >= state_->maximum) {
            state_->denied.fetch_add(1, std::memory_order_relaxed);
            return temporary_capacity ? Status::unavailable("Tree operation capacity is occupied")
                                      : Status::resource_exhausted("Tree operation capacity exhausted");
        }
        if (state_->active.compare_exchange_weak(active, active + 1, std::memory_order_acq_rel)) {
            break;
        }
    }
    bool constructed = false;
    try {
        *out = detail::reserved_construct<Ticket>(metadata, [&](Ticket *storage) {
            ::new (static_cast<void *>(storage)) Ticket(state_);
            constructed = true;
        });
    }
    catch (const std::bad_alloc &) {
        // shared_ptr destroys an already constructed Ticket if allocating its
        // control block fails; only a failed object allocation needs rollback.
        if (!constructed) {
            state_->release();
        }
        return Status::resource_exhausted("Tree operation ticket allocation failed");
    }
    return Status::Ok();
}

TreeOperationLifetime::Ticket::~Ticket()
{
    state_->release();
}

std::shared_ptr<AsyncPageStore> TreeOperationLifetime::Ticket::backend(AsyncPageStore *store)
{
    return store == nullptr ? nullptr : std::shared_ptr<AsyncPageStore>(shared_from_this(), store);
}

size_t TreeOperationLifetime::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<State>();
}

size_t TreeOperationLifetime::ticket_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<Ticket>();
}

size_t TreeOperationLifetime::outstanding() const noexcept
{
    return state_->active.load(std::memory_order_acquire) & ~State::kClosed;
}

ResourceUsage TreeOperationLifetime::resource_usage() const noexcept
{
    return {
        .limit = state_->maximum, .used = outstanding(), .denied_tree = state_->denied.load(std::memory_order_relaxed)};
}

bool TreeOperationLifetime::closed() const noexcept
{
    return (state_->active.load(std::memory_order_acquire) & State::kClosed) != 0;
}

void TreeOperationLifetime::close() noexcept
{
    state_->active.fetch_or(State::kClosed, std::memory_order_acq_rel);
}

void TreeOperationLifetime::close_and_drain() noexcept
{
    close();
    drain_accepted();
}

void TreeOperationLifetime::drain_accepted() noexcept
{
    auto active = state_->active.load(std::memory_order_acquire);
    while ((active & ~State::kClosed) != 0) {
        state_->active.wait(active, std::memory_order_acquire);
        active = state_->active.load(std::memory_order_acquire);
    }
}
} // namespace crowdb::tree
