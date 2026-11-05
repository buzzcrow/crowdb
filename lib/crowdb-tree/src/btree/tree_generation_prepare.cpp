// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "tree_generation_lease_internal.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
Status reserve_generation(const std::shared_ptr<TreeRuntimeBinding> &binding, const MappingView &mapping, bool empty,
                          TreeGenerationPreparation &out, size_t source_count)
{
    const auto publication = PublicationCoordinator::replacement_allocation_bound(mapping, source_count);
    auto       bytes       = detail::AllocationBound(publication.value_or(SIZE_MAX))
                                 .add(sizeof(TreeGenerationPreparation))
                                 .add(detail::shared_allocation_bound<TreeGenerationLease>())
                                 .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                                 .add(detail::shared_allocation_bound<PublishedTreeState>());
    if (empty) {
        bytes.add(MappingView::path_copy_bound())
            .add(PageState::allocation_bound())
            .add(detail::shared_allocation_bound<PublishedPage>())
            .add(2 * detail::shared_allocation_bound<PublishedTreeState>())
            .add(PublishedSources::allocation_bound())
            .add(PageCacheResidency::page_allocation_bound(0) + 256);
    }
    const auto bound        = bytes.value();
    const auto source_bound = SourceAdmission::allocation_bound();
    const auto flush_bound  = detail::AllocationBound(FlushController::allocation_bound())
                                  .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                                  .value();
    if (!bound || !source_bound || !flush_bound) {
        return Status::resource_exhausted("generation preparation bound overflow");
    }
    auto metadata = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kReclaim, *bound);
    auto source =
        detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kAdmittedWriter, *source_bound);
    auto flush = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kReclaim, *flush_bound);
    if (!metadata || !source || !flush) {
        return Status::resource_exhausted("generation preparation credit unavailable");
    }
    out.metadata         = detail::AllocationReservation::create(std::move(*metadata));
    out.source_lifecycle = detail::AllocationReservation::create(std::move(*source));
    // Publication preparation is sealed after installation. The controller
    // instead reuses its bounded pause control until this generation closes.
    out.flush_lifecycle = detail::AllocationReservation::create(std::move(*flush));
    return Status::Ok();
}

FlushBatchOptions flush_options(const Config &config, const TreeRuntimeBinding &binding)
{
    FlushBatchOptions batch;
    batch.parallelism =
        std::max<size_t>(1, std::min(binding.cpu()->worker_count(), binding.options().limits.io_operations));
    const auto max_key = config.max_key_size != 0 ? config.max_key_size : config.frame_bytes / 2;
    batch.run_limits   = {
        .max_entries = 128, .max_key_bytes = std::max<size_t>(256 * 1024, max_key), .key_block_bytes = 1024};
    return batch;
}
} // namespace

Status TreeGenerationLease::prepare(TreeCore &owner, std::shared_ptr<const PublishedTreeState> candidate,
                                    const SplitGeneration *split, std::shared_ptr<TreeGenerationLease> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument("generation preparation requires an empty output");
    }
    const bool empty_candidate = candidate && candidate->mapping.empty() && candidate->root_page_id == kInvalidPageId;
    TreeGenerationPreparation prepared;
    prepared.owner = &owner;
    {
        TreeCore::State::Ingress admission(*owner.state_);
        if (admission.owner == nullptr) {
            return Status::unavailable("Tree ingress closed or replacing");
        }
        PublishedSources::Inheritance inherited_range;
        if (split != nullptr && split->inherited) {
            const auto &range = split->inherited->inheritance;
            if (!range.has_value()) {
                return Status::invalid_argument("split generation requires its inheritance frontier");
            }
            inherited_range             = *range;
            inherited_range.local_count = 1;
        }
        const auto current = owner.state_->publisher->capture();
        if (split != nullptr && (current != split->expected || owner.state_->frontier->max_seen() > current->coverage ||
                                 current->sources->inheritance || current->pending_handoff ||
                                 current->flush_admission_owner != 0 || current->checkpoint_inflight)) {
            return Status::unavailable("split destination changed before baseline preparation");
        }
        if (current->generation == UINT64_MAX) {
            return Status::resource_exhausted("Tree generation exhausted");
        }
        prepared.previous_generation = current->generation;
        prepared.expected            = split != nullptr ? split->expected : nullptr;
        auto status                  = reserve_generation(
            owner.binding(), candidate ? candidate->mapping : MappingView{}, !candidate || empty_candidate, prepared,
            1 + (split != nullptr && split->inherited ? split->inherited->tables.size() : 0));
        if (!status.ok()) {
            return status;
        }
        auto next = candidate && !empty_candidate
                      ? detail::reserved_shared<PublishedTreeState>(prepared.metadata, *candidate)
                      : TreeCore::State::empty_state(prepared.metadata);
        if (empty_candidate) {
            next->coverage = candidate->coverage;
        }
        next->generation  = current->generation + 1;
        prepared.coverage = next->coverage;
        prepared.frontier = split != nullptr && split->inherited ? inherited_range.frontier : next->coverage;
        SourceWriteOptions writers{.account = owner.binding()->account()};
        writers.max_key_size =
            owner.config().max_key_size != 0 ? owner.config().max_key_size : owner.config().frame_bytes / 2;
        prepared.sources = detail::reserved_unique<SourceAdmission>(
            prepared.source_lifecycle, next->coverage, next->generation, writers, prepared.source_lifecycle);
        prepared.flush = detail::reserved_unique<FlushController>(
            prepared.flush_lifecycle, *owner.state_->publisher, *owner.binding()->cpu(), *prepared.sources,
            owner.state_->frontier->contiguous(), owner.binding()->lane(),
            flush_options(owner.config(), *owner.binding()), FlushBatch::Maintenance{}, prepared.flush_lifecycle,
            owner.state_->apply_progress);
        std::shared_ptr<PublishedSources> inherited;
        if (split != nullptr && split->inherited) {
            inherited         = detail::reserved_shared<PublishedSources>(prepared.metadata, prepared.metadata);
            inherited->floor  = next->coverage;
            inherited->memory = prepared.sources->catalog()->memory;
            inherited->tables.reserve(1 + split->inherited->tables.size());
            inherited->tables.push_back(prepared.sources->catalog()->active);
            inherited->tables.insert(inherited->tables.end(), split->inherited->tables.begin(),
                                     split->inherited->tables.end());
            inherited->inheritance = inherited_range;
        }
        prepared.inherited = inherited;
        status             = owner.state_->publisher->prepare_replacement(
            {.metadata = prepared.metadata, .initial = std::move(next), .source_catalog = prepared.sources->catalog()},
            &prepared.publication, std::move(inherited));
        if (!status.ok()) {
            return status;
        }
    }
    auto metadata = prepared.metadata;
    auto state    = detail::reserved_unique<TreeGenerationPreparation>(metadata, std::move(prepared));
    *out          = detail::reserved_construct<TreeGenerationLease>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) TreeGenerationLease(std::move(state)); });
    metadata->seal();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("generation preparation allocation failed");
}
} // namespace crowdb::tree
