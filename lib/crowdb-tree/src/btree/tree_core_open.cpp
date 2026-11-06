// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/block_account.h"
#include "backend/local/local_backend_state.h"
#include "backend/local/scheduled_page_store.h"
#include "btree/allocation_bound.h"
#include "btree/backing_identity.h"
#include "btree/tree_core.h"
#include "btree/tree_core_geometry.h"
#include "crowdb-tree/backend/page_store.h"
#include "snapshot/local_recovery.h"
#include "snapshot/operation_wait.h"
#include "snapshot/recovered_state.h"

#include <algorithm>
#include <bit>

namespace crowdb::tree
{
namespace
{
struct OpenCredits
{
    PageLoadBudget budget;

    ~OpenCredits()
    {
        detail::seal_backend_budget(budget.backend);
        if (budget.reservation()) {
            budget.reservation()->seal();
        }
        if (budget.io) {
            budget.io->seal();
        }
    }
};

struct OpenLifetime
{
    OpenLifetime(const std::shared_ptr<TreeRuntimeBinding> &binding, std::shared_ptr<const void> owner)
        : lifetime(1, binding->lifecycle_budget(), std::move(owner)),
          binding(binding)
    {
    }

    ~OpenLifetime()
    {
        store.reset();
        ticket.reset();
        lifetime.close_and_drain();
        binding->drain();
    }

    TreeOperationLifetime                          lifetime;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<AsyncPageStore>                store;
};

Status reserve_open(const std::shared_ptr<TreeRuntimeBinding> &binding, const detail::BackendOpenBudget &backend,
                    OpenCredits *credits, std::optional<size_t> recovery_bound)
{
    const auto &options = binding->options();
    if (options.limits.routing_bytes <= options.lifecycle_bytes || options.limits.io_operations == 0) {
        return Status::resource_exhausted();
    }
    auto available = options.limits.routing_bytes - options.lifecycle_bytes;
    for (const auto retained : {backend.owned_bytes, backend.growth_bytes}) {
        if (retained >= available) {
            return Status::resource_exhausted("backend metadata leaves no recovery workspace");
        }
        available -= retained;
    }
    available =
        std::min(available, detail::ResourceBudget::available(binding->account(), detail::BudgetClass::kRouting));
    if (recovery_bound) {
        if (*recovery_bound > available) {
            return Status::resource_exhausted("empty recovery exceeds Tree routing limit");
        }
        available = *recovery_bound;
    }
    auto bytes = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kRouting, available);
    auto io    = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kIo, 1);
    if (!bytes || !io) {
        return Status::resource_exhausted("recovery routing or IO credit unavailable");
    }
    credits->budget.bytes = detail::AllocationReservation::create(std::move(*bytes));
    credits->budget.io =
        detail::reserved_shared<detail::AllocationReservation>(credits->budget.reservation(), std::move(*io));
    return Status::Ok();
}

Status recover_open(const Config &config, const std::shared_ptr<TreeRuntimeBinding> &binding,
                    const std::shared_ptr<LocalBackendState> &local, PageLoadBudget budget,
                    std::shared_ptr<const PublishedTreeState> *initial)
{
    OpenLifetime lifetime(binding, local);
    auto         status = lifetime.lifetime.acquire(binding->lifecycle_budget(), &lifetime.ticket);
    if (!status.ok()) {
        return status;
    }
    lifetime.store = lifetime.ticket->backend(local->store.get());
    LocalRecoveryLimits limits;
    limits.max_frame_bytes = std::max(limits.max_frame_bytes, physical_frame_capacity(config));
    limits.max_blob_bytes  = round_up_to_iu(limits.max_frame_bytes + kDurableBlobHeader, config.page_store->iu_size());
    ExtentLedgerLimits domain{.backend_id   = detail::allocate_backend_identity(),
                              .region_begin = snapshot::region_base_for(config.page_store->iu_size()),
                              .file_size    = detail::backend_size(*config.page_store, budget.backend),
                              .iu_size      = config.page_store->iu_size()};
    if (domain.backend_id == 0) {
        return Status::resource_exhausted();
    }
    std::shared_ptr<LocalRecoveryOperation> recovery;
    status = LocalRecoveryOperation::create(lifetime.store, binding->cpu(), binding->lane(), domain, limits, budget,
                                            &recovery);
    if (!status.ok()) {
        return status;
    }
    status = detail::wait_snapshot_operation(recovery);
    if (!status.ok()) {
        return status;
    }
    local->ledger      = recovery->result().ledger;
    local->checkpoints = recovery->result().registry;
    local->inherited   = recovery->result().latest;
    if (!local->inherited) {
        initial->reset();
        return Status::Ok();
    }
    PageLoadLimits loads;
    loads.max_blob_bytes  = limits.max_blob_bytes;
    loads.max_frame_bytes = limits.max_frame_bytes;
    auto loader =
        detail::reserved_shared<AsyncStorePageLoader>(budget.reservation(), lifetime.store, binding->cpu(),
                                                      binding->lane(), loads, OverflowBackingResolver{}, budget);
    std::shared_ptr<RecoveredStateOperation> recovered;
    status = RecoveredStateOperation::create(local->inherited, loader, binding->cpu(), binding->lane(), {},
                                             budget.reservation(), &recovered);
    if (!status.ok()) {
        return status;
    }
    status = detail::wait_snapshot_operation(recovered);
    if (status.ok()) {
        *initial = recovered->result().state;
    }
    return status;
}
} // namespace

Status TreeCore::open(const Config &config, std::shared_ptr<TreeCore> *out)
try {
    if (out == nullptr || !config.runtime || config.page_store == nullptr || config.page_store->iu_size() == 0 ||
        config.frame_bytes == 0 || config.frame_bytes % config.page_store->iu_size() != 0) {
        return Status::invalid_argument();
    }
    if (const auto factory = config.page_store->runtime_factory()) {
        return factory(config, out);
    }
    if ((config.async_page_store != nullptr && !config.async_page_store->write_completion_is_reloadable()) ||
        config.page_store->has_shared_ownership()) {
        return Status::not_supported("runtime local recovery requires an "
                                     "independently reloadable local backend");
    }
    auto status = config.key_range.validate();
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<TreeRuntimeBinding> binding;
    status = TreeRuntimeBinding::create(config.runtime, config.runtime_binding, &binding);
    if (!status.ok()) {
        return status;
    }
    auto local = detail::reserved_shared<LocalBackendState>(binding->lifecycle_budget());
    status     = ScheduledPageStore::create(config.page_store, config.async_page_store, config.backend_owner, binding,
                                            binding->lifecycle_budget(), binding->backend_lane(), &local->store);
    if (!status.ok()) {
        return status;
    }
    status =
        LocalBackendDomain::claim(config.page_store, config.backend_owner, binding->lifecycle_budget(), &local->domain);
    if (!status.ok()) {
        return status;
    }
    detail::BackendOpenBudget backend_metadata;
    status = detail::bind_block_account(config.page_store, binding->account(),
                                        config.runtime->options().backend_workers, &backend_metadata);
    if (!status.ok()) {
        return status;
    }
    OpenCredits                          credits;
    const detail::BackendOperationLimits backend_limits{
        .max_io_bytes = round_up_to_iu(std::max<size_t>(1024 * 1024, config.frame_bytes) + kDurableBlobHeader,
                                       config.page_store->iu_size()),
        .write_calls  = 0,
        .workers      = config.runtime->options().workers + config.runtime->options().backend_workers,
        .barriers     = false};
    const auto backend_bound = detail::backend_operation_bound(local->store.get(), backend_limits);
    if (!backend_bound) {
        return Status::resource_exhausted("backend recovery bound overflow");
    }
    status = detail::reserve_backend_operation(local->store.get(), binding->account(), backend_limits,
                                               &credits.budget.backend);
    if (!status.ok()) {
        return status;
    }
    backend_metadata.growth_bytes = *backend_bound;
    std::optional<size_t> recovery_bound;
    // Domain ownership excludes a concurrent writer throughout this preflight
    // and normal recovery. A shorter file cannot contain either commit anchor.
    if (detail::backend_size(*config.page_store, credits.budget.backend) <
        snapshot::superblock_slot_bytes(config.page_store->iu_size())) {
        LocalRecoveryLimits limits;
        limits.max_frame_bytes = std::max(limits.max_frame_bytes, physical_frame_capacity(config));
        limits.max_blob_bytes =
            round_up_to_iu(limits.max_frame_bytes + kDurableBlobHeader, config.page_store->iu_size());
        recovery_bound = detail::AllocationBound()
                             .add(LocalRecoveryOperation::empty_allocation_bound(limits, config.page_store->iu_size()))
                             .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                             .value();
        if (!recovery_bound) {
            return Status::resource_exhausted("empty recovery bound overflow");
        }
    }
    status = reserve_open(binding, backend_metadata, &credits, recovery_bound);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<const PublishedTreeState> initial;
    status = recover_open(config, binding, local, credits.budget, &initial);
    if (!status.ok()) {
        return status;
    }
    credits.budget.reservation()->seal();
    credits.budget.io->seal();
    return create(config, binding, {.initial = std::move(initial), .local = std::move(local)}, out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (const std::exception &) {
    return Status::internal_error();
}
} // namespace crowdb::tree
