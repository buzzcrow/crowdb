// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "source_pause.h"

#include "source_admission.h"

namespace crowdb::tree
{
SourcePause::~SourcePause()
{
    if (owns_) {
        const bool changed = table_->closed() || budget_->resume();
        if ((changed || notify_) && progress_) {
            progress_->advance();
        }
    }
}

Status SourcePause::create(const SourceCatalog &catalog, const std::shared_ptr<detail::AllocationReservation> &metadata,
                           std::shared_ptr<ApplyProgress> progress, std::shared_ptr<SourcePause> *out)
try {
    if (!catalog.active_budget || out == nullptr || *out) {
        return Status::invalid_argument("source pause requires a budgeted active source and empty output");
    }
    auto owner =
        detail::reserved_shared<SourcePause>(metadata, catalog.active, catalog.active_budget, std::move(progress));
    owner->self_ = owner;
    owner->owns_ = true;
    if (!catalog.active_budget->pause(&SourcePause::complete, owner.get())) {
        owner->owns_ = false;
        owner->self_.reset();
        return Status::unavailable("source already has an admission pause owner");
    }
    *out = std::move(owner);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("source pause control credit exhausted");
}

void SourcePause::complete(void *context) noexcept
{
    auto &owner    = *static_cast<SourcePause *>(context);
    auto  retained = std::move(owner.self_);
    owner.ready_.store(true, std::memory_order_release);
}

bool SourcePause::matches(const SourceCatalog &catalog) const noexcept
{
    return table_ == catalog.active && budget_ == catalog.active_budget;
}
} // namespace crowdb::tree
