// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/local_checkpoint.h"

namespace crowdb::tree
{
Status LocalCheckpointRegistry::create(std::shared_ptr<ExtentLedger>                             ledger,
                                       std::shared_ptr<detail::AllocationReservation>            metadata,
                                       std::array<std::shared_ptr<const ProtectedCheckpoint>, 2> recovered,
                                       std::shared_ptr<LocalCheckpointRegistry>                 *out)
{
    if (!ledger || out == nullptr || ledger->limits().iu_size == 0 ||
        ledger->limits().region_begin < snapshot::region_base_for(ledger->limits().iu_size) ||
        (metadata && metadata->category() == detail::BudgetClass::kIo)) {
        return Status::invalid_argument();
    }
    try {
        auto registry = detail::reserved_construct<LocalCheckpointRegistry>(
            metadata, [](auto *storage) { ::new (static_cast<void *>(storage)) LocalCheckpointRegistry; });
        registry->ledger_ = std::move(ledger);
        for (size_t slot = 0; slot < recovered.size(); ++slot) {
            const auto &image = recovered[slot];
            if (!image) {
                continue;
            }
            const auto &anchor = image->anchor;
            if (!image->pages || anchor.snapshot_seq == 0 || (anchor.snapshot_seq % 2 == 0 ? 1U : 0U) != slot ||
                anchor.magic != snapshot::kAnchorMagic || anchor.format_version != snapshot::kFormatVersion ||
                anchor.last_applied_slot != image->pages->coverage ||
                (!image->pages->frames.empty() && image->pages->backend_id != registry->ledger_->limits().backend_id)) {
                return Status::invalid_argument();
            }
            if (anchor.snapshot_seq > registry->sequence_.load()) {
                registry->sequence_.store(anchor.snapshot_seq);
                registry->coverage_.store(anchor.last_applied_slot);
                registry->generation_ = image->pages->generation;
            }
        }
        registry->anchors_ = std::move(recovered);
        *out               = std::move(registry);
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

Status LocalCheckpointRegistry::begin(uint64_t coverage, uint64_t generation, uint64_t *sequence) noexcept
{
    if (busy_.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable();
    }
    if (uncertain_.load(std::memory_order_acquire) || generation < generation_ ||
        (generation == generation_ && coverage < coverage_.load()) || sequence_.load() == UINT64_MAX) {
        busy_.store(false, std::memory_order_release);
        return Status::unavailable();
    }
    *sequence = sequence_.load() + 1;
    return Status::Ok();
}

void LocalCheckpointRegistry::touched(std::shared_ptr<const ProtectedCheckpoint> candidate) noexcept
{
    candidate_ = std::move(candidate);
    uncertain_.store(true, std::memory_order_release);
}

void LocalCheckpointRegistry::finish(std::shared_ptr<const ProtectedCheckpoint> candidate, bool committed) noexcept
{
    if (committed) {
        const auto &anchor                             = candidate->anchor;
        generation_                                    = candidate->pages->generation;
        anchors_[anchor.snapshot_seq % 2 == 0 ? 1 : 0] = std::move(candidate);
        sequence_.store(anchor.snapshot_seq, std::memory_order_release);
        coverage_.store(anchor.last_applied_slot, std::memory_order_release);
        candidate_.reset();
        uncertain_.store(false, std::memory_order_release);
    }
    busy_.store(false, std::memory_order_release);
}
} // namespace crowdb::tree
