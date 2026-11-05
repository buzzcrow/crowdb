// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "block_account.h"

#include "block_async_io.h"
#include "block_directory.h"
#include "btree/allocation_account.h"
#include "btree/allocation_account_grant.h"
#include "btree/allocation_bound.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "crowdb-tree/epoch.h"

#include <climits>

namespace crowdb::tree::detail
{
struct BlockAccountAccess
{
    static std::optional<size_t> operation_bound(const BlockPageStore &store, BackendOperationLimits limits) noexcept;

    static Status create_grant(const BlockPageStore &store, ResourceBudget::Credit credit,
                               std::shared_ptr<AllocationBudget> *out)
    {
        std::shared_ptr<BackendAllocationGrant> grant;
        auto status = BackendAllocationGrant::create(store.allocations_, std::move(credit), &grant);
        if (status.ok()) {
            *out = std::move(grant);
        }
        return status;
    }

    static std::shared_ptr<AllocationBudget> selected(const BlockPageStore                    &store,
                                                      const std::shared_ptr<AllocationBudget> &budget)
    {
        if (!valid(store, budget)) {
            return nullptr;
        }
        return budget ? budget : store.allocations_;
    }

    static bool valid(const BlockPageStore &store, const std::shared_ptr<AllocationBudget> &budget) noexcept
    {
        if (!budget || budget == store.allocations_) {
            return true;
        }
        const auto *grant = dynamic_cast<const BackendAllocationGrant *>(budget.get());
        return grant != nullptr && grant->account() == store.allocations_;
    }

    static Status bind(BlockPageStore &store, const std::shared_ptr<ResourceBudget::Account> &account, size_t workers,
                       BackendOpenBudget *out)
    {
        // Recovery only reads existing extents. Each backend worker and the
        // opening host thread may need one previously unseen epoch participant.
        const auto growth = AllocationBound(EpochManager::participant_allocation_bytes())
                                .add_product(workers, EpochManager::participant_allocation_bytes())
                                .value();
        if (!growth) {
            return Status::resource_exhausted("backend startup metadata bound overflow");
        }
        auto status = store.allocations_->rebind(account);
        if (!status.ok()) {
            return status;
        }
        out->owned_bytes  = store.allocations_->owned_bytes();
        out->growth_bytes = store.blocks_ ? *growth : 0;
        return Status::Ok();
    }
};

std::shared_ptr<AllocationBudget> block_allocation_budget(const BlockPageStore                    &store,
                                                          const std::shared_ptr<AllocationBudget> &budget)
{
    return BlockAccountAccess::selected(store, budget);
}

bool valid_block_budget(const BlockPageStore &store, const std::shared_ptr<AllocationBudget> &budget) noexcept
{
    return BlockAccountAccess::valid(store, budget);
}

std::optional<size_t> BlockAccountAccess::operation_bound(const BlockPageStore  &store,
                                                          BackendOperationLimits limits) noexcept
{
    const auto unit     = std::max<size_t>(1, store.iu_size_);
    const auto max_part = (static_cast<size_t>(INT_MAX) / unit) * unit;
    if (max_part == 0 || limits.workers == 0) {
        return {};
    }
    const auto part_bytes  = store.blocks_ ? std::min<uint64_t>(store.block_size_, max_part) : max_part;
    const auto parts       = AllocationBound(limits.max_io_bytes / part_bytes).add(2).value();
    const auto new_extents = store.blocks_ && parts ? AllocationBound().add_product(limits.write_calls, *parts).value()
                                                    : std::optional<size_t>{0};
    if (!parts || !new_extents) {
        return {};
    }
    const auto tail_count = AllocationBound(limits.workers).add(1).value();
    if (!tail_count) {
        return {};
    }
    auto bound  = AllocationBound(BackendAllocationGrant::allocation_bound())
                      .add_product(*tail_count, EpochManager::participant_allocation_bytes())
                      .add_product(*new_extents, BlockDirectory::extent_allocation_bound())
                      .add_product(2, store.dir_.size() + 65);
    auto window = AllocationBound(limits.max_io_bytes).add_product(2, unit);
#ifdef CROWDB_HAVE_LIBURING
    if (limits.asynchronous) {
        window = AllocationBound();
        window.add(block_io_allocation_bound(limits.max_io_bytes, *parts));
        if (limits.barriers) {
            const auto targets = AllocationBound(store.blocks_ ? store.blocks_->count() : 1).add(*new_extents).value();
            window.add(targets ? block_sync_allocation_bound(*targets) : std::nullopt);
        }
        // Completed asynchronous callbacks can retain paid controls while
        // the next CPU worker starts another admitted I/O window.
        return window.value() ? bound.add_product(*tail_count, *window.value()).value() : std::nullopt;
    }
#endif
    // Blocking adapters free their scratch span before invoking completion.
    return bound.add(window.value()).value();
}

std::optional<size_t> backend_operation_bound(PageStore *backend, BackendOperationLimits limits) noexcept
{
    auto *store = dynamic_cast<BlockPageStore *>(backend);
    return store != nullptr ? BlockAccountAccess::operation_bound(*store, limits) : std::optional<size_t>{0};
}

Status reserve_backend_operation(PageStore *backend, const std::shared_ptr<ResourceBudget::Account> &account,
                                 BackendOperationLimits limits, std::shared_ptr<AllocationBudget> *out)
{
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    auto *store = dynamic_cast<BlockPageStore *>(backend);
    if (store == nullptr) {
        return Status::Ok();
    }
    const auto bound = BlockAccountAccess::operation_bound(*store, limits);
    if (!bound) {
        return Status::resource_exhausted("backend operation bound overflow");
    }
    auto credit = ResourceBudget::try_reserve(account, BudgetClass::kRouting, *bound);
    if (!credit) {
        return Status::resource_exhausted("backend operation routing credit unavailable");
    }
    return bind_backend_operation(backend, limits, std::move(*credit), out);
}

Status bind_backend_operation(PageStore *backend, BackendOperationLimits limits, ResourceBudget::Credit credit,
                              std::shared_ptr<AllocationBudget> *out)
{
    if (out == nullptr || *out || credit.allocated()) {
        return Status::invalid_argument("backend binding requires an unused operation grant");
    }
    auto *store = dynamic_cast<BlockPageStore *>(backend);
    if (store == nullptr) {
        return credit.amount() == 0 ? Status::Ok()
                                    : Status::invalid_argument("external backend has no local operation grant");
    }
    const auto bound = BlockAccountAccess::operation_bound(*store, limits);
    if (!bound || credit.amount() < *bound) {
        return Status::resource_exhausted("backend grant is smaller than the complete operation bound");
    }
    return BlockAccountAccess::create_grant(*store, std::move(credit), out);
}

uint64_t backend_size(PageStore &store, const std::shared_ptr<AllocationBudget> &budget)
{
    auto *block = dynamic_cast<BlockPageStore *>(&store);
    return block != nullptr ? block->size(budget) : store.size();
}

void seal_backend_budget(const std::shared_ptr<AllocationBudget> &budget) noexcept
{
    auto *grant = dynamic_cast<BackendAllocationGrant *>(budget.get());
    if (grant != nullptr) {
        grant->seal();
    }
}

Status bind_block_account(PageStore *store, const std::shared_ptr<ResourceBudget::Account> &account,
                          size_t backend_workers, BackendOpenBudget *out)
{
    if (store == nullptr || !account || backend_workers == 0 || out == nullptr) {
        return Status::invalid_argument("backend account binding requires an owned startup domain");
    }
    *out        = {};
    auto *block = dynamic_cast<BlockPageStore *>(store);
    return block != nullptr ? BlockAccountAccess::bind(*block, account, backend_workers, out) : Status::Ok();
}
} // namespace crowdb::tree::detail
