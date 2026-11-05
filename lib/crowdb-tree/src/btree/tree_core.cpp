// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/checkpoint_backend.h"
#include "backend/local/local_backend_state.h"
#include "backend/local/scheduled_page_store.h"
#include "btree/allocation_bound.h"
#include "runtime/tree_tuning_c_api.h"
#include "tree_apply_operation.h"
#include "tree_core_geometry.h"
#include "tree_core_internal.h"

#include <algorithm>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
class EmptyBackend final : public AsyncPageStore
{
  public:
    uint64_t submit_read(PageAddr /*addr*/, void * /*buf*/, size_t /*len*/, AsyncCompletion done) override
    {
        done.complete(Status::unavailable("in-memory Tree has no durable backing"));
        return 0;
    }

    uint64_t submit_write(PageAddr /*addr*/, const void * /*buf*/, size_t /*len*/, AsyncCompletion done) override
    {
        done.complete(Status::unavailable("in-memory Tree has no durable backing"));
        return 0;
    }

    Status submit_fsync(AsyncCompletion done) override
    {
        done.complete(Status::unavailable("in-memory Tree has no durable backing"));
        return Status::Ok();
    }

    void cancel(uint64_t /*op_id*/) override
    {
    }
};

FlushBatchOptions flush_options(const Config &config, const TreeRuntimeBinding &binding, size_t max_key_size)
{
    FlushBatchOptions batch;
    batch.parallelism =
        std::max<size_t>(1, std::min(binding.cpu()->worker_count(), binding.options().limits.io_operations));
    if (config.execution.flush_parallelism != 0) {
        batch.parallelism = config.execution.flush_parallelism;
    }
    batch.run_limits         = {.max_entries     = config.execution.run_max_entries,
                                .max_key_bytes   = std::max(config.execution.run_max_key_bytes, max_key_size),
                                .key_block_bytes = config.execution.run_key_block_bytes};
    batch.max_run_bytes      = config.execution.run_max_bytes;
    batch.install_run_window = config.execution.install_run_window;
    return batch;
}

Status configure_maintenance(const Config &config, PageMaintenanceOperationOptions *maintenance)
{
    maintenance->visits_per_step = config.execution.maintenance_visits_per_step;
    maintenance->max_visits      = config.execution.maintenance_max_visits;
    maintenance->max_candidates  = config.execution.maintenance_max_candidates;
    maintenance->max_steps       = config.execution.maintenance_max_steps;
    auto &pages                  = maintenance->publication.pages;
    auto  status                 = configure_leaf_geometry(config, &pages);
    if (!status.ok()) {
        return status;
    }
    pages.inline_value_bytes = config.max_inline_value != 0 ? config.max_inline_value : config.frame_bytes / 4;
    pages.max_metadata_bytes = config.execution.maintenance_metadata_bytes;
    // A split window scans at most one primary frame of minimum-size records.
    // Its byte-aware outputs may span several ranges. Even an empty-key,
    // empty-value record needs a slot and cell header; larger logical inputs
    // continue in later steps without reserving a tree-sized merge window.
    pages.max_work_entries =
        std::min(pages.max_work_entries,
                 (config.frame_bytes - kFrameHeaderSize - kFrameTrailerSize) / (kLeafSlotSize + kCellHeaderSize));
    maintenance->publication.max_inner_children = config.inner_max_keys + 1ULL;
    maintenance->publication.max_inner_bytes    = pages.primary_page_bytes;
    maintenance->publication.inner_merge_keys =
        config.inner_merge_keys != 0 ? config.inner_merge_keys : std::max<uint32_t>(1, config.inner_max_keys / 4);
    return Status::Ok();
}

} // namespace

std::shared_ptr<PublishedTreeState>
TreeCore::State::empty_state(const std::shared_ptr<detail::AllocationReservation> &credit)
{
    auto leaf                      = detail::reserved_shared<PageState>(credit, credit);
    auto page                      = detail::reserved_shared<PublishedPage>(credit);
    page->leaf                     = std::move(leaf);
    page->metrics.page_size_known  = true;
    page->metrics.kv_known         = true;
    auto                   initial = detail::reserved_shared<PublishedTreeState>(credit, credit);
    const PageReplacement  replacement{.page_id = 0, .expected = {}, .replacement = page};
    MappingView::CopyStats copied;
    initial->mapping      = initial->mapping.replace(std::span(&replacement, 1), copied, credit);
    initial->root_page_id = 0;
    initial->next_page_id = 1;
    (void)apply_page_metrics(initial->stats, nullptr, &page->metrics);
    initial->stats.height = 1;
    return initial;
}

TreeCore::State::State(Config options, std::shared_ptr<TreeRuntimeBinding> host)
    : config(std::move(options)),
      binding(std::move(host))
{
}

TreeCore::State::~State()
{
    if (cache) {
        cache->close_and_drain();
    }
    // State teardown is a host boundary. Drop roots before waiting for this
    // account's accepted retirement work; immutable external views may remain.
    source_rotation.reset();
    flush.reset();
    publisher.reset();
    sources.reset();
    cache.reset();
    // Lifetime state also owns the backend. Release it before the final drain
    // so registry and ledger roots cannot enqueue retirement after that drain.
    replay_lifetime.reset();
    apply_lifetime.reset();
    lifetime.reset();
    replay_waiters.reset();
    replay_controls.reset();
    local.reset();
    chunk.reset();
    backend.reset();
    binding->drain();
}

Status TreeCore::State::initialize(TreeCoreBootstrap bootstrap)
{
    const auto credit        = binding->lifecycle_budget();
    const auto source_bytes  = SourceAdmission::allocation_bound();
    auto       source_credit = source_bytes ? detail::ResourceBudget::try_reserve(
                                                  binding->account(), detail::BudgetClass::kAdmittedWriter, *source_bytes)
                                            : std::nullopt;
    if (!source_credit) {
        return Status::resource_exhausted("Tree source lifecycle reserve unavailable");
    }
    source_lifecycle = detail::AllocationReservation::create(std::move(*source_credit));
    auto initial     = bootstrap.initial ? std::move(bootstrap.initial) : empty_state(credit);
    if (initial->mapping.empty() && initial->root_page_id == kInvalidPageId) {
        auto empty        = detail::reserved_shared<PublishedTreeState>(credit, *empty_state(credit));
        empty->coverage   = initial->coverage;
        empty->generation = initial->generation;
        auto source       = detail::reserved_shared<PublishedSources>(credit, credit);
        source->floor     = initial->coverage;
        empty->sources    = std::move(source);
        initial           = std::move(empty);
    }
    local = std::move(bootstrap.local);
    chunk = std::move(bootstrap.chunk);
    if (local) {
        backend = local->store;
    }
    else if (chunk) {
        backend = chunk->store;
    }
    else if (config.async_page_store != nullptr || config.page_store != nullptr) {
        auto status = ScheduledPageStore::create(config.page_store, config.async_page_store, config.backend_owner,
                                                 binding, credit, binding->backend_lane(), &backend);
        if (!status.ok()) {
            return status;
        }
    }
    else {
        backend = detail::reserved_shared<EmptyBackend>(credit);
    }
    auto status = ApplyFrontier::create(initial->coverage, ApplyFrontier::kMaxWindow, credit, &frontier);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<const void> lifetime_owner = binding;
    if (local) {
        lifetime_owner = local;
    }
    else if (chunk) {
        lifetime_owner = chunk;
    }
    lifetime        = detail::reserved_unique<TreeOperationLifetime>(credit, binding->options().cpu_queue_limit, credit,
                                                                     lifetime_owner);
    apply_lifetime  = detail::reserved_unique<TreeOperationLifetime>(credit, binding->options().cpu_queue_limit, credit,
                                                                     lifetime_owner);
    replay_lifetime = detail::reserved_unique<TreeOperationLifetime>(credit, binding->options().cpu_queue_limit + 2,
                                                                     credit, lifetime_owner);
    const auto replay_bytes = TreeApplyOperation::allocation_bound(TreeApplyOperation::kReplayCallerControlBytes);
    if (!replay_bytes) {
        return Status::resource_exhausted("replay control bound overflow");
    }
    status =
        ReplayControls::create(binding->account(), binding->options().cpu_queue_limit, *replay_bytes, &replay_controls);
    if (!status.ok()) {
        return status;
    }
    status = ApplyWaiters::create(binding->account(), credit, &replay_waiters);
    if (!status.ok()) {
        return status;
    }
    status = ApplyProgress::create((2 * binding->options().cpu_queue_limit) + 2, credit, &apply_progress);
    if (!status.ok()) {
        return status;
    }
    status = ApplyDispatcher::create(binding->cpu(), binding->lane(), 16, credit, &apply_dispatcher);
    if (!status.ok()) {
        return status;
    }
    SourceWriteOptions writers{.account = binding->account()};
    writers.max_entries       = config.execution.source_max_entries;
    writers.max_key_bytes     = config.execution.source_max_key_bytes;
    writers.max_gap_entries   = config.execution.source_max_gap_entries;
    writers.max_gap_key_bytes = config.execution.source_max_gap_key_bytes;
    writers.max_key_size      = config.max_key_size != 0 ? config.max_key_size : config.frame_bytes / 2;
    sources   = detail::reserved_unique<SourceAdmission>(credit, initial->coverage, initial->generation, writers,
                                                         source_lifecycle);
    resources = binding->resource_metrics();
    work      = binding->work_metrics();
    const PageCacheLimits cache_limits{.max_pages =
                                           std::max<size_t>(1, std::min(config.execution.cache_max_pages,
                                                                        config.buffer_pool_bytes / config.frame_bytes)),
                                       .max_waiters        = binding->options().cpu_queue_limit,
                                       .max_resident_bytes = config.buffer_pool_bytes};
    cache = detail::reserved_shared<SharedPageCache>(credit, cache_limits, credit, resources, binding->cpu().get(),
                                                     binding->lane());
    const auto publication_bytes = detail::AllocationBound()
                                       .add(PublicationCoordinator::allocation_bound(sources->catalog()->tables.size(),
                                                                                     64, &initial->mapping, true))
                                       .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                                       .value();
    if (!publication_bytes) {
        return Status::resource_exhausted("initial publication metadata bound overflow");
    }
    auto publication_credit =
        detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kRouting, *publication_bytes);
    if (!publication_credit) {
        return Status::resource_exhausted("initial publication metadata reserve unavailable");
    }
    // Recovered page roles scale with the mapping, independently of the fixed
    // core lifecycle. Keep the prepaid participant headroom for later readers.
    auto publication_metadata = detail::AllocationReservation::create(std::move(*publication_credit));
    publisher                 = detail::reserved_unique<PublicationCoordinator>(
        publication_metadata, binding->options().cpu_queue_limit, initial->generation, work,
        std::pmr::get_default_resource(), cache,
        PublicationBootstrap{
            .metadata = publication_metadata, .initial = initial, .source_catalog = sources->catalog()});
    loads.max_frame_bytes     = config.frame_bytes;
    loads.max_blob_bytes      = static_cast<size_t>(config.frame_bytes) + 64;
    loads.max_inner_children  = config.inner_max_keys + 1ULL;
    loads.max_inner_key_bytes = config.frame_bytes;
    loads.max_leaf_key_bytes  = config.frame_bytes;
    if (local && local->inherited) {
        for (const auto &[pid, frame] : local->inherited->frames) {
            (void)pid;
            loads.max_frame_bytes = std::max<size_t>(loads.max_frame_bytes, frame.frame_bytes);
            loads.max_blob_bytes  = std::max<size_t>(loads.max_blob_bytes, frame.backing->length);
        }
        loads.max_inner_key_bytes = loads.max_frame_bytes;
        loads.max_leaf_key_bytes  = loads.max_frame_bytes;
        local->inherited.reset();
    }
    if (local) {
        const auto unit      = local->ledger->limits().iu_size;
        loads.max_blob_bytes = ((loads.max_blob_bytes + unit - 1) / unit) * unit;
    }
    if (chunk) {
        loads.max_frame_bytes      = std::max(loads.max_frame_bytes, chunk->max_frame_bytes);
        loads.max_blob_bytes       = std::max(loads.max_blob_bytes, chunk->max_blob_bytes);
        loads.max_blob_bytes       = round_up_to_iu(loads.max_blob_bytes, chunk->iu_size);
        loads.max_chunk_blob_bytes = loads.max_blob_bytes;
        loads.max_inner_key_bytes  = loads.max_frame_bytes;
        loads.max_leaf_key_bytes   = loads.max_frame_bytes;
    }
    reads.max_key_bytes = writers.max_key_size;
    auto batch          = flush_options(config, *binding, writers.max_key_size);
    preparation = {.runtime = binding->cpu(), .store = backend, .loads = loads, .maintenance = {}, .cache = cache};
    preparation.cpu_progress = apply_dispatcher;
    status                   = publisher->prepare_lane(batch.parallelism + 1, credit, &preparation.publication);
    if (!status.ok()) {
        return status;
    }
    status = configure_maintenance(config, &preparation.maintenance);
    if (!status.ok()) {
        return status;
    }
    flush = detail::reserved_unique<FlushController>(credit, *publisher, *binding->cpu(), *sources,
                                                     frontier->contiguous(), binding->lane(), batch,
                                                     FlushBatch::Maintenance{}, credit, apply_progress);
    return Status::Ok();
}

TreeCore::TreeCore(detail::ReservedUnique<State> state) : state_(std::move(state))
{
    state_->source_rotation = detail::reserved_shared<SourceRotation>(
        state_->binding->lifecycle_budget(), *state_->sources, *state_->publisher, state_->apply_dispatcher,
        state_->apply_progress, state_->binding->account(),
        SourceRotationOptions{.bytes         = state_->config.memtable_flush_bytes,
                              .entries       = state_->config.memtable_flush_entries,
                              .tables        = state_->config.max_memtable_count,
                              .source_window = std::min(state_->reads.max_memtables, state_->flush->source_window())});
    state_->source_rotation->set_flush(
        [this](std::shared_ptr<FlushOperation> *out) { return prepare_flush(out); },
        [this](const PublishedTreeState &view, const SourceCatalog &catalog, WriteFootprint sealed) {
            return state_->flush->check_rotation(state_->preparation, view, catalog, sealed);
        });
    state_->source_rotation->set_async_flush(*state_->flush, [this](FlushPreparationOptions *out) {
        State::Ingress admission(*state_);
        if (admission.owner == nullptr) {
            return Status::unavailable("Tree ingress closed");
        }
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        auto status = state_->lifetime->acquire(binding()->lifecycle_budget(), &ticket);
        if (!status.ok()) {
            return status;
        }
        *out       = state_->preparation;
        out->store = ticket->backend(async_backend());
        return Status::Ok();
    });
}

Status TreeCore::create(Config config, std::shared_ptr<TreeRuntimeBinding> binding, TreeCoreBootstrap bootstrap,
                        std::shared_ptr<TreeCore> *out)
try {
    if (!binding || out == nullptr || *out || !config.runtime || config.frame_bytes < 512 ||
        config.buffer_pool_bytes < config.frame_bytes || config.inner_max_keys == 0) {
        return Status::invalid_argument("Tree core requires runtime, bounded frames and empty output");
    }
    if (config.page_store != nullptr && !bootstrap.local && !bootstrap.chunk) {
        return Status::invalid_argument("runtime local PageStore requires recovery and an allocation domain");
    }
    auto execution_status = validate_tree_execution(config.execution);
    if (!execution_status.ok()) {
        return execution_status;
    }
    if (config.execution.flush_parallelism > binding->cpu()->worker_count() ||
        config.execution.flush_parallelism > binding->options().limits.io_operations) {
        return Status::invalid_argument("flush_parallelism exceeds CPU workers or Tree I/O permits");
    }
    const auto credit = binding->lifecycle_budget();
    auto       state  = detail::reserved_unique<State>(credit, std::move(config), std::move(binding));
    auto       status = state->initialize(std::move(bootstrap));
    if (!status.ok()) {
        return status;
    }
    *out = detail::reserved_construct<TreeCore>(
        credit, [&](auto *storage) { ::new (static_cast<void *>(storage)) TreeCore(std::move(state)); });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("Tree core lifecycle credit exhausted");
}
catch (const std::invalid_argument &error) {
    return Status::invalid_argument(error.what());
}

TreeCore::~TreeCore()
{
    close_and_drain();
}
} // namespace crowdb::tree
