// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_snapshot.h"

#include "backend/local/block_account.h"
#include "backend/local/local_backend_state.h"
#include "backend/local/scheduled_page_store.h"
#include "btree/allocation_bound.h"
#include "btree/tree_core.h"

namespace crowdb::tree
{
namespace
{
struct SnapshotBackendOwner
{
    std::shared_ptr<TreeRuntimeBinding> binding;
    std::shared_ptr<LocalBackendDomain> domain;
    std::shared_ptr<const void>         backend;
};
} // namespace

struct PublishedSnapshotContext::State
{
    ~State()
    {
        if (lifetime) {
            lifetime->close_and_drain();
            lifetime.reset();
        }
        // A terminal callback may precede its CPU job's final owner release.
        // Snapshot contexts are host-owned, so the binding outlives that tail.
        if (lane) {
            lane->drain();
        }
        if (backend_lane) {
            backend_lane->drain();
        }
    }

    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<const PublishedTreeState>      view;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<detail::FlushRuntime::Lane>    lane;
    std::shared_ptr<SnapshotBackendOwner>          owner;
    std::shared_ptr<detail::FlushRuntime::Lane>    backend_lane;
    std::shared_ptr<AsyncPageStore>                scheduled;
    detail::ReservedUnique<TreeOperationLifetime>  lifetime;
    std::weak_ptr<TreeCore>                        core;
    std::shared_ptr<TreeWorkMetrics>               work_metrics;
    AsyncPageStore                                *backend = nullptr;
    PageLoadLimits                                 loads;
    PublishedReadLimits                            reads;
    PageKeyRange                                   range;
    bool                                           detached = false;
};

SnapshotReadWindow::~SnapshotReadWindow()
{
    seal();
}

SnapshotReadWindow &SnapshotReadWindow::operator=(SnapshotReadWindow &&other) noexcept
{
    if (this != &other) {
        seal();
        loader.reset();
        backend.reset();
        ticket.reset();
        budget = {};
        host_core.reset();
        host_core = std::move(other.host_core);
        budget    = std::move(other.budget);
        ticket    = std::move(other.ticket);
        backend   = std::move(other.backend);
        loader    = std::move(other.loader);
    }
    return *this;
}

void SnapshotReadWindow::seal() noexcept
{
    detail::seal_backend_budget(budget.backend);
    if (budget.io) {
        budget.io->seal();
    }
    if (!budget.account) {
        if (auto reserved = budget.reservation()) {
            reserved->seal();
        }
    }
}

size_t PublishedSnapshotContext::allocation_bound() noexcept
{
    using namespace detail;
    return shared_allocation_bound<State>() + shared_allocation_bound<PublishedSnapshotContext>() +
           shared_allocation_bound<AllocationReservation>() + shared_allocation_bound<PublishedTreeState>() +
           PublishedSources::allocation_bound() + shared_allocation_bound<FlushRuntime::Lane>() +
           shared_allocation_bound<SnapshotBackendOwner>() + sizeof(TreeOperationLifetime) +
           TreeOperationLifetime::allocation_bound() + TreeOperationLifetime::ticket_allocation_bound();
}

size_t PublishedSnapshotContext::window_control_bound() noexcept
{
    return (2 * detail::shared_allocation_bound<detail::AllocationReservation>()) +
           TreeOperationLifetime::ticket_allocation_bound();
}

Status PublishedSnapshotContext::capture(const std::shared_ptr<TreeCore>           &core,
                                         std::shared_ptr<PublishedSnapshotContext> *out)
{
    return core ? core->capture_snapshot(out) : Status::invalid_argument("snapshot capture requires an open Core");
}

Status PublishedSnapshotContext::capture_admitted(const std::shared_ptr<TreeCore>           &core,
                                                  std::shared_ptr<PublishedSnapshotContext> *out,
                                                  std::shared_ptr<const PublishedTreeState>  captured)
try {
    using namespace detail;
    if (!core || out == nullptr || *out) {
        return Status::invalid_argument("snapshot capture requires an open Core and empty output");
    }
    auto binding = core->binding();
    auto bound   = AllocationBound(allocation_bound())
                       .add(ScheduledPageStore::allocation_bound(binding->options().limits.io_operations))
                       .add(shared_allocation_bound<FlushRuntime::Lane>())
                       .add_product(2, OwnedKey::allocation_overhead() + 1)
                       .add(core->config().key_range.start() ? core->config().key_range.start()->size() : 0)
                       .add(core->config().key_range.end() ? core->config().key_range.end()->size() : 0)
                       .value();
    if (!bound) {
        return Status::resource_exhausted("snapshot backend control bound overflow");
    }
    auto credit = ResourceBudget::try_reserve(binding->account(), BudgetClass::kRouting, *bound);
    if (!credit) {
        return Status::resource_exhausted("snapshot context reserve unavailable");
    }
    auto                                           metadata = AllocationReservation::create(std::move(*credit));
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto                                           status = core->operation_lifetime().acquire(metadata, &ticket);
    if (!status.ok()) {
        return status;
    }
    auto state      = reserved_shared<State>(metadata);
    state->metadata = metadata;
    if (core->config().key_range.start()) {
        state->range.lower = OwnedKey(Slice(*core->config().key_range.start()), metadata);
    }
    if (core->config().key_range.end()) {
        state->range.upper = OwnedKey(Slice(*core->config().key_range.end()), metadata);
    }
    state->work_metrics = core->publisher().work_metrics();
    state->binding      = std::move(binding);
    state->lane         = reserved_shared<FlushRuntime::Lane>(metadata, state->binding->options().cpu_queue_limit);
    if (!captured) {
        captured = core->publisher().capture_complete();
    }
    auto view                  = reserved_shared<PublishedTreeState>(metadata, *captured);
    auto sources               = reserved_shared<PublishedSources>(metadata, metadata);
    sources->floor             = view->coverage;
    view->sources              = std::move(sources);
    state->view                = std::move(view);
    state->loads               = core->load_limits();
    state->loads.max_inflight  = 1;
    state->reads               = core->read_limits();
    state->reads.max_memtables = 1;
    state->backend             = core->config().async_page_store;
    state->detached            = (state->backend == nullptr && core->config().page_store == nullptr) ||
                                 static_cast<bool>(core->config().backend_owner);
    if (state->detached) {
        state->owner          = reserved_shared<SnapshotBackendOwner>(metadata);
        state->owner->binding = state->binding;
        state->owner->backend = core->config().backend_owner;
        if (const auto &local = core->local_backend()) {
            state->owner->domain = local->domain;
        }
        if (core->config().page_store != nullptr || state->backend != nullptr) {
            state->backend_lane =
                reserved_shared<FlushRuntime::Lane>(metadata, state->binding->options().backend_queue_limit);
            status = ScheduledPageStore::create(core->config().page_store, state->backend, core->config().backend_owner,
                                                state->binding, metadata, state->backend_lane, &state->scheduled);
            if (!status.ok()) {
                return status;
            }
            state->backend = state->scheduled.get();
        }
        state->lifetime = reserved_unique<TreeOperationLifetime>(metadata, state->binding->options().cpu_queue_limit,
                                                                 metadata, state->owner);
    }
    else {
        state->core = core;
    }
    *out = reserved_construct<PublishedSnapshotContext>(metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) PublishedSnapshotContext(std::move(state));
    });
    metadata->seal();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot context allocation failed");
}

Status PublishedSnapshotContext::prepare_window(size_t bytes, SnapshotReadWindow *out) const
try {
    using namespace detail;
    if (out == nullptr || out->host_core || out->budget.bytes || out->loader || out->ticket) {
        return Status::invalid_argument("snapshot read window output must be empty");
    }
    auto bound = AllocationBound(bytes).add(window_control_bound()).value();
    if (!bound) {
        return Status::resource_exhausted("snapshot window bound overflow");
    }
    auto credit = ResourceBudget::try_reserve(state_->binding->account(), BudgetClass::kRouting, *bound);
    auto io     = ResourceBudget::try_reserve(state_->binding->account(), BudgetClass::kIo, 1);
    if (!credit || !io) {
        return Status::resource_exhausted("snapshot read window reserve unavailable");
    }
    SnapshotReadWindow window;
    window.budget.bytes        = AllocationReservation::create(std::move(*credit));
    window.budget.io           = reserved_shared<AllocationReservation>(window.budget.reservation(), std::move(*io));
    window.budget.work_metrics = state_->work_metrics;
    window.budget.cause        = TreeWorkCause::Snapshot;
    return finish_window(std::move(window), out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot read window allocation failed");
}

Status PublishedSnapshotContext::prepare_window(SnapshotReadWindow *out) const
try {
    using namespace detail;
    if (out == nullptr || out->host_core || out->budget.bytes || out->loader || out->ticket) {
        return Status::invalid_argument("snapshot read window output must be empty");
    }
    SnapshotReadWindow window;
    window.budget.bytes        = state_->reads.metadata;
    window.budget.account      = state_->binding->account();
    window.budget.work_metrics = state_->work_metrics;
    window.budget.cause        = TreeWorkCause::Snapshot;
    return finish_window(std::move(window), out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot read window allocation failed");
}

Status PublishedSnapshotContext::finish_window(SnapshotReadWindow window, SnapshotReadWindow *out) const
try {
    using namespace detail;
    Status          status;
    AsyncPageStore *backend = state_->backend;
    if (state_->detached) {
        status = state_->lifetime->acquire(window.budget.bytes, &window.ticket);
    }
    else {
        window.host_core = state_->core.lock();
        if (window.host_core) {
            status = window.host_core->acquire_snapshot_window(window.budget.bytes, &window.ticket, &backend);
        }
        else {
            status = Status::unavailable("snapshot source Tree closed");
        }
    }
    if (!status.ok()) {
        return status;
    }
    window.backend = window.ticket->backend(backend);
    window.loader =
        reserved_shared<AsyncStorePageLoader>(window.budget.bytes, window.backend, state_->binding->cpu(), state_->lane,
                                              state_->loads, OverflowBackingResolver{}, window.budget);
    *out = std::move(window);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot read window allocation failed");
}

const std::shared_ptr<const PublishedTreeState> &PublishedSnapshotContext::view() const noexcept
{
    return state_->view;
}

const std::shared_ptr<TreeRuntimeBinding> &PublishedSnapshotContext::binding() const noexcept
{
    return state_->binding;
}

const std::shared_ptr<detail::FlushRuntime::Lane> &PublishedSnapshotContext::lane() const noexcept
{
    return state_->lane;
}

PageLoadLimits PublishedSnapshotContext::load_limits() const noexcept
{
    return state_->loads;
}

PublishedReadLimits PublishedSnapshotContext::read_limits() const noexcept
{
    auto       limits          = state_->reads;
    const auto shape           = state_->view->mapping.copy_bounds();
    limits.page_metadata_bytes = shape.max_leaf_bytes;
    return limits;
}

const PageKeyRange &PublishedSnapshotContext::range() const noexcept
{
    return state_->range;
}

bool PublishedSnapshotContext::detached() const noexcept
{
    return state_->detached;
}
} // namespace crowdb::tree
