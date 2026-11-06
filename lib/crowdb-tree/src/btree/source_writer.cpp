// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/external_allocation.h"
#include "source_admission.h"

namespace crowdb::tree
{
SourceAdmission::Writer::~Writer()
{
    release();
}

SourceAdmission::Writer::Writer(Writer &&other) noexcept
{
    *this = std::move(other);
}

SourceAdmission::Writer &SourceAdmission::Writer::operator=(Writer &&other) noexcept
{
    if (this != &other) {
        release();
        observation_   = std::move(other.observation_);
        table_         = std::move(other.table_);
        bound_         = other.bound_;
        source_budget_ = std::move(other.source_budget_);
        credit_        = std::move(other.credit_);
        remaining_     = std::exchange(other.remaining_, {});
        planned_       = std::exchange(other.planned_, {});
        category_      = other.category_;
    }
    return *this;
}

void SourceAdmission::Writer::release() noexcept
{
    planned_ = {};
    if (source_budget_) {
        source_budget_->return_unused(std::exchange(remaining_, {}), category_);
        source_budget_.reset();
    }
    if (credit_) {
        credit_->seal();
        credit_.reset();
    }
    if (auto table = std::move(table_)) {
        table->leave();
    }
    observation_ = {};
}

Status SourceAdmission::Writer::upsert(Slice key, uint64_t slot, buffer value, bool *changed)
{
    return apply(key, slot, 0, std::move(value), false, changed);
}

Status SourceAdmission::Writer::upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer value, bool *changed)
{
    return apply(key, slot, flags, std::move(value), true, changed);
}

Status SourceAdmission::Writer::apply(Slice key, uint64_t slot, uint8_t flags, buffer value, bool split, bool *changed)
try {
    if (!table_ || planned_.entries != 0) {
        return Status::invalid_argument("writer permit is no longer active");
    }
    size_t capacity = 0;
    if (credit_) {
        if (split) {
            const auto *allocation = value.external_allocation();
            if (allocation == nullptr || !allocation->charged_to(credit_)) {
                if (value.size() > SIZE_MAX - kCellHeaderSize) {
                    return Status::invalid_argument("external value capacity overflow");
                }
                capacity = value.size() + kCellHeaderSize;
                capacity = capacity <= buffer::kInlineCap ? 0 : capacity;
            }
        }
        else {
            capacity = value.owned() ? value.capacity() : value.size();
            if (value.inlined() || (!value.owned() && capacity <= buffer::kInlineCap)) {
                capacity = 0;
            }
        }
        if (remaining_.entries == 0 || key.size() > remaining_.key_bytes || capacity > remaining_.payload_bytes ||
            key.size() > source_budget_->options().max_key_size ||
            (remaining_.max_key_size != 0 && key.size() > remaining_.max_key_size)) {
            return Status::invalid_argument("write exceeds its admitted footprint");
        }
    }
    const bool applied = split ? table_->upsert_external(key, slot, flags, std::move(value), bound_, nullptr, credit_)
                               : table_->upsert(key, slot, std::move(value), bound_, nullptr, credit_);
    if (credit_) {
        --remaining_.entries;
        remaining_.key_bytes -= key.size();
        remaining_.payload_bytes -= capacity;
    }
    if (changed != nullptr) {
        *changed = applied;
    }
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("admitted writer allocation failed");
}
} // namespace crowdb::tree
