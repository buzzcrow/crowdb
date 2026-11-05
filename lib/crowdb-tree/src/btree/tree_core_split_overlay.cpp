// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_internal.h"
#include "btree/tree_core_split_wait.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
Status TreeCore::State::install_split_sources(const SplitSourceSession &session, uint64_t identity, uint64_t end)
{
    const auto view = publisher->capture();
    if (view->sources->inheritance) {
        const auto &inherited = *view->sources->inheritance;
        return inherited.owner_identity == identity && inherited.session == session.session && inherited.frontier == end
                 ? Status::Ok()
                 : Status::invalid_argument("another split prefix is already installed");
    }
    if (end < view->coverage || !session.covers(view->coverage, end)) {
        return Status::invalid_argument("retained split sources do not cover the destination prefix");
    }
    if (end == view->coverage) {
        return Status::Ok();
    }
    if (view->pending_handoff || view->flush_admission_owner != 0 || view->checkpoint_inflight) {
        return Status::unavailable("split installation waits for destination maintenance");
    }
    // The destination's sequence namespace starts at the inherited prefix.
    // Its own writes may begin once this installation returns.
    if (frontier->max_seen() > view->coverage) {
        return Status::invalid_argument("split overlay requires an unwritten destination prefix");
    }
    const auto count = view->sources->tables.size() + session.table_count;
    if (count > reads.max_memtables) {
        return Status::resource_exhausted("split source catalog exceeds the destination read window");
    }
    const auto bytes = detail::AllocationBound(PublicationCoordinator::empty_commit_reserve_bytes())
                           .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                           .add(PublishedSources::allocation_bound())
                           .add_product(count, sizeof(std::shared_ptr<MemTable>))
                           .value();
    if (!bytes) {
        return Status::resource_exhausted("split catalog bound overflow");
    }
    auto reserved = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kHandoff, *bytes);
    if (!reserved) {
        return Status::unavailable("split catalog admission unavailable");
    }
    auto credit  = detail::AllocationReservation::create(std::move(*reserved));
    auto next    = detail::reserved_shared<PublishedSources>(credit, credit);
    next->floor  = view->coverage;
    next->memory = view->sources->memory;
    next->tables.reserve(count);
    next->tables.assign(view->sources->tables.begin(), view->sources->tables.end());
    for (size_t i = 0; i < session.table_count; ++i) {
        next->tables.push_back(session.tables[i]);
    }
    next->inheritance               = PublishedSources::Inheritance{.owner_identity = identity,
                                                                    .session        = session.session,
                                                                    .frontier       = end,
                                                                    .local_count    = view->sources->tables.size()};
    next->inheritance->memory.known = true;
    for (size_t i = 0; i < session.table_count; ++i) {
        next->inheritance->memory += session.tables[i]->source_stats();
    }
    auto request                = detail::reserved_shared<PublicationRequest>(credit, credit);
    request->transaction.record = detail::reserved_shared<InstallRecord>(
        credit,
        InstallToken{.generation = view->generation, .batch = session.session, .range = UINT64_MAX - 4, .sequence = 0});
    request->transaction.expected_commit_seq = view->commit_seq;
    request->transaction.expected_sources    = view->sources;
    request->transaction.sources             = std::move(next);
    detail::SplitWait completion;
    request->on_done = [&completion](PublicationResult result) { completion.finish(std::move(result.status)); };
    if (!publisher->try_submit(request)) {
        return Status::unavailable("split publication queue unavailable");
    }
    completion.wait();
    credit->seal();
    if (completion.status.ok()) {
        frontier->force_advance(end);
    }
    return completion.status;
}

Status TreeCore::install_split_overlay(TreeCore &source, uint64_t journal_frontier)
try {
    if (&source == this || config().runtime != source.config().runtime) {
        return Status::invalid_argument("split trees must be distinct and share their host runtime");
    }
    State::Ingress source_ingress(*source.state_);
    if (source_ingress.owner == nullptr) {
        return Status::unavailable("split source ingress closed");
    }
    detail::SplitGate source_gate(source.state_->split_busy);
    detail::SplitGate child_gate(state_->split_busy);
    if (!source_gate.held() || !child_gate.held()) {
        return Status::unavailable("split administration is busy");
    }
    return install_split_overlay_guarded(source, journal_frontier);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("split overlay allocation failed");
}

Status TreeCore::install_split_overlay_guarded(TreeCore &source, uint64_t journal_frontier)
{
    if (journal_frontier > source.contiguous_slot()) {
        return Status::invalid_argument("split prefix exceeds source applied frontier");
    }
    if (!source.state_->split_sources || source.state_->split_sources->frontier < journal_frontier) {
        if (source.state_->split_sources && source.state_->split_sources->active) {
            return Status::invalid_argument("split prefix exceeds active captured view");
        }
        auto status = source.state_->capture_split_sources(source, journal_frontier);
        if (!status.ok()) {
            return status;
        }
    }
    const auto &session = *source.state_->split_sources;
    if (session.frontier < journal_frontier) {
        return Status::unavailable("retained split sources do not reach the requested prefix");
    }
    const auto current = capture();
    if (!current->sources->inheritance && session.baseline && current->coverage < session.baseline->view()->coverage) {
        return install_split_baseline(session, source.state_->sources->identity(), journal_frontier);
    }
    if (!state_->begin_replacement()) {
        return Status::unavailable("split destination ingress closed or replacing");
    }

    struct Release
    {
        State &state;

        ~Release()
        {
            state.finish_replacement();
        }
    } release{*state_};

    state_->apply_lifetime->drain_accepted();
    state_->replay_lifetime->drain_accepted();
    const auto drained = capture();
    if (drained->sources->inheritance) {
        return state_->install_split_sources(*source.state_->split_sources, source.state_->sources->identity(),
                                             journal_frontier);
    }
    if (drained->flush_admission_owner != 0 || drained->pending_handoff) {
        return Status::unavailable("split destination has an unfinished handoff");
    }
    state_->flush->release_pending();
    state_->lifetime->drain_accepted();
    state_->binding->drain();
    if (closed()) {
        return Status::unavailable("split destination closed during drain");
    }
    return state_->install_split_sources(*source.state_->split_sources, source.state_->sources->identity(),
                                         journal_frontier);
}

Status TreeCore::clear_split_overlay(TreeCore &source)
{
    if (&source == this) {
        return Status::invalid_argument("split source must differ from destination");
    }
    State::Ingress ingress(*state_);
    State::Ingress source_ingress(*source.state_);
    if (ingress.owner == nullptr || source_ingress.owner == nullptr) {
        return Status::unavailable("split ingress closed");
    }
    detail::SplitGate gate(state_->split_busy);
    if (!gate.held()) {
        return Status::unavailable("split administration is busy");
    }
    const auto view = capture();
    if (view->sources->inheritance) {
        return view->sources->inheritance->owner_identity != source.state_->sources->identity()
                 ? Status::invalid_argument("split overlay source does not match")
                 : Status::unavailable("split prefix publication is incomplete");
    }
    if (state_->split_handoff && state_->split_handoff->source_identity != source.state_->sources->identity()) {
        return Status::invalid_argument("split overlay source does not match");
    }
    state_->split_handoff.reset();
    return Status::Ok();
}
} // namespace crowdb::tree
