// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "source_write_budget.h"

namespace crowdb::tree
{
Status SourceWriteReservation::bind_prepaid(const SourceWriteOptions &options, WriteFootprint footprint,
                                            detail::BudgetClass category, detail::ResourceBudget::Credit credit,
                                            SourceWriteReservation *out)
{
    const auto bound   = SourceWriteBudget::allocation_bound(options, footprint);
    const bool gap     = category == detail::BudgetClass::kGapRepair;
    const auto largest = footprint.max_key_size == 0 ? footprint.key_bytes : footprint.max_key_size;
    if (out == nullptr || out->valid() || footprint.entries == 0 || !bound ||
        (category != detail::BudgetClass::kAdmittedWriter && !gap) ||
        footprint.entries > (gap ? options.max_gap_entries : options.max_entries) ||
        footprint.key_bytes > (gap ? options.max_gap_key_bytes : options.max_key_bytes) ||
        largest > footprint.key_bytes || largest > options.max_key_size ||
        !credit.belongs_to(options.account, category) || credit.allocated() || credit.amount() < *bound) {
        return Status::invalid_argument("writer binding requires its complete unused partition grant");
    }
    out->credit_    = detail::AllocationReservation::create(std::move(credit));
    out->options_   = options;
    out->footprint_ = footprint;
    out->category_  = category;
    return Status::Ok();
}
} // namespace crowdb::tree
