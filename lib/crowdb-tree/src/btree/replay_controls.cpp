// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "replay_controls.h"

#include "allocation_bound.h"

#include <cassert>

namespace crowdb::tree
{
struct ReplayControls::Slot
{
    explicit Slot(std::shared_ptr<detail::AllocationReservation> value)
        : metadata(std::move(value)),
          available(metadata->available())
    {
    }

    ~Slot()
    {
        metadata->seal();
    }

    std::shared_ptr<detail::AllocationReservation> metadata;
    const size_t                                   available;
    std::atomic<bool>                              busy{false};
};

ReplayControls::ReplayControls(std::shared_ptr<detail::AllocationReservation> metadata)
    : metadata_(std::move(metadata)),
      slots_(decltype(slots_)::allocator_type(metadata_))
{
}

Status ReplayControls::create(const std::shared_ptr<detail::ResourceBudget::Account> &account, size_t ordinary_slots,
                              size_t control_bytes, std::shared_ptr<ReplayControls> *out)
{
    if (out == nullptr || *out || ordinary_slots == 0 || ordinary_slots > SIZE_MAX - 2) {
        return Status::invalid_argument("replay controls require a finite ordinary window");
    }
    const auto count = ordinary_slots + 2;
    const auto bound = detail::AllocationBound(detail::shared_allocation_bound<ReplayControls>())
                           .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                           .add_product(count, detail::shared_allocation_bound<Slot>() + sizeof(std::shared_ptr<Slot>))
                           .add_product(count, control_bytes)
                           .value();
    auto       paid =
        bound ? detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, *bound) : std::nullopt;
    if (!paid) {
        return Status::resource_exhausted("replay control window cannot be prepaid");
    }
    auto metadata = detail::AllocationReservation::create(std::move(*paid));
    auto controls = detail::reserved_construct<ReplayControls>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) ReplayControls(metadata); });
    controls->slots_.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        auto credit = metadata->take_reserved(control_bytes);
        if (!credit) {
            return Status::internal_error("prepaid replay control split failed");
        }
        auto slot_budget = detail::AllocationReservation::create(std::move(*credit));
        controls->slots_.push_back(detail::reserved_shared<Slot>(metadata, std::move(slot_budget)));
    }
    metadata->seal();
    *out = std::move(controls);
    return Status::Ok();
}

ReplayControls::Lease ReplayControls::acquire(bool current_gap) noexcept
{
    const auto begin = current_gap ? size_t{0} : size_t{2};
    const auto end   = slots_.size();
    for (size_t index = begin; index < end; ++index) {
        const auto &slot     = slots_[index];
        bool        expected = false;
        if (!slot->busy.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            continue;
        }
        // Completed foreign result handles may still own control allocations.
        // Do not reuse this slot until their actual storage has been freed.
        if (slot->metadata->available() != slot->available) {
            slot->busy.store(false, std::memory_order_release);
            continue;
        }
        return Lease(slot);
    }
    return {};
}

ReplayControls::Lease::~Lease()
{
    if (slot_) {
        slot_->busy.store(false, std::memory_order_release);
    }
}

ReplayControls::Lease::Lease(Lease &&other) noexcept : slot_(std::move(other.slot_))
{
}

ReplayControls::Lease &ReplayControls::Lease::operator=(Lease &&other) noexcept
{
    if (this != &other) {
        if (slot_) {
            slot_->busy.store(false, std::memory_order_release);
        }
        slot_ = std::move(other.slot_);
    }
    return *this;
}

const std::shared_ptr<detail::AllocationReservation> &ReplayControls::Lease::budget() const noexcept
{
    assert(slot_);
    return slot_->metadata;
}
} // namespace crowdb::tree
