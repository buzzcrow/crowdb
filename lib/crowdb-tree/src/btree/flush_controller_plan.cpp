// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "apply_progress.h"
#include "flush_controller_internal.h"

namespace crowdb::tree
{
Status FlushController::prepare_or_plan(const FlushPreparationOptions   &preparation,
                                        std::shared_ptr<FlushOperation> *existing, FlushReservePlan *plan)
{
    if (existing == nullptr || *existing || plan == nullptr || plan->view ||
        preparation.runtime.get() != &state_->runtime || !state_->lane) {
        return Status::invalid_argument("flush planning requires empty outputs and the bound runtime");
    }
    if (!state_->try_enter()) {
        return Status::unavailable("flush controller closed or preparing");
    }

    struct Release
    {
        State &state;

        ~Release()
        {
            state.leave();
        }
    } release{*state_};

    if (state_->has_pending()) {
        *existing = state_->pending;
        return Status::Ok();
    }
    const auto sources = state_->sources.catalog();
    const auto view    = state_->publisher.capture();
    if (view->flush_admission_owner != 0 || view->pending_handoff || view->checkpoint_inflight) {
        return Status::unavailable("flush waits for the current publication owner");
    }
    auto status = state_->prepare_covered(preparation, view, sources, existing);
    if (!status.ok() || *existing) {
        return status;
    }
    status = state_->pause_source(*sources);
    if (!status.ok()) {
        return status;
    }
    auto                       pause = std::move(state_->pause);
    detail::FlushReserveBounds bounds;
    status = detail::flush_reserve_bounds(state_->publisher, *view, *sources, preparation,
                                          State::control_allocation_bound(), state_->options, &bounds, pause->bound());
    if (!status.ok()) {
        return status;
    }
    *plan = {.view = view, .sources = sources, .bounds = bounds, .pause = std::move(pause)};
    return Status::Ok();
}

Status FlushController::prepare_prepaid(const FlushPreparationOptions &preparation, const FlushReservePlan &plan,
                                        detail::FlushCredits credits, std::shared_ptr<FlushOperation> *out)
{
    return prepare_impl({}, out, {}, &preparation, &plan, &credits);
}

Status FlushController::check_rotation(const FlushPreparationOptions &preparation, const PublishedTreeState &view,
                                       const SourceCatalog &catalog, WriteFootprint sealed) const
{
    detail::FlushReserveBounds bounds;
    auto                       status =
        detail::flush_reserve_bounds(state_->publisher, view, catalog, preparation, State::control_allocation_bound(),
                                     state_->options, &bounds, sealed, true);
    if (!status.ok()) {
        return status;
    }
    for (size_t i = 0; i < bounds.amounts.size(); ++i) {
        if (bounds.amounts[i] > detail::ResourceBudget::capacity(catalog.active_budget->options().account,
                                                                 detail::FlushReserveBounds::categories[i])) {
            return Status::resource_exhausted("source rotation would exceed a complete flush partition");
        }
    }
    return Status::Ok();
}

} // namespace crowdb::tree
