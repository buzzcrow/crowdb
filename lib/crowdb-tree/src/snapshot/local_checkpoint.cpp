// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "snapshot/local_checkpoint_internal.h"

#include <algorithm>
#include <bit>
#include <cassert>

namespace crowdb::tree
{
LocalCheckpointOperation::State::Window::Window(size_t size, size_t alignment,
                                                const std::shared_ptr<detail::AllocationReservation> &budget)
    : storage(size + alignment - 1, detail::ReservationAllocator<uint8_t, AllocationKind::kIo>(budget))
{
    void *data     = storage.data();
    auto  capacity = storage.size();
    auto *aligned  = std::align(alignment, size, data, capacity);
    assert(aligned != nullptr);
    bytes = {static_cast<uint8_t *>(aligned), size};
}

LocalCheckpointOperation::State::State(PageLoadBudget credits)
    : budget(std::move(credits)),
      directory(detail::ReservationAllocator<DirEntry>(budget.bytes))
{
}

Status LocalCheckpointOperation::create(std::shared_ptr<const CapturedDurablePages> pages,
                                        std::shared_ptr<LocalCheckpointRegistry>    registry,
                                        std::shared_ptr<AsyncPageStore>             store,
                                        std::shared_ptr<detail::FlushRuntime>       runtime,
                                        std::shared_ptr<detail::FlushRuntime::Lane> lane, LocalCheckpointLimits limits,
                                        PageLoadBudget budget, std::shared_ptr<LocalCheckpointOperation> *out)
{
    if (!pages || !registry || !store || !runtime || !lane || out == nullptr || limits.max_segments == 0 ||
        limits.max_segments > kMaxMappingSegments || static_cast<bool>(budget.bytes) != static_cast<bool>(budget.io) ||
        (budget.bytes &&
         ((budget.bytes->category() != detail::BudgetClass::kRouting &&
           budget.bytes->category() != detail::BudgetClass::kReclaim) ||
          budget.io->category() != detail::BudgetClass::kIo || budget.bytes->account() != budget.io->account()))) {
        return Status::invalid_argument();
    }
    if (!store->write_completion_is_reloadable()) {
        return Status::not_supported();
    }
    try {
        auto state          = detail::reserved_shared<State>(budget.bytes, budget);
        state->limits       = limits;
        state->registry     = std::move(registry);
        state->store        = std::move(store);
        state->runtime      = std::move(runtime);
        state->lane         = std::move(lane);
        state->image        = detail::reserved_shared<ProtectedCheckpoint>(budget.bytes, budget.bytes);
        state->image->pages = std::move(pages);
        *out                = detail::reserved_construct<LocalCheckpointOperation>(budget.bytes, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) LocalCheckpointOperation(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

std::optional<size_t> LocalCheckpointOperation::allocation_bound(size_t segments, uint32_t iu) noexcept
{
    using namespace detail;
    if (iu == 0 || segments > kMaxMappingSegments) {
        return {};
    }
    const auto bytes = round_up_to_iu(segment_image_encoded_size(kMappingSegmentSize), iu);
    return AllocationBound(shared_allocation_bound<State>() + shared_allocation_bound<LocalCheckpointOperation>() +
                           ProtectedCheckpoint::allocation_bound() + shared_allocation_bound<State::Window>() +
                           shared_allocation_bound<State::IoStep>())
        .add(std::max<uint64_t>(bytes, snapshot::superblock_slot_bytes(iu)))
        .add(std::bit_ceil(std::max<size_t>(4096, iu)) - 1)
        .add_product(3 * (segments + 1), sizeof(DirEntry) + sizeof(CheckpointMetadata::Segment))
        .add_product(segments + 1, ExtentLedger::reservation_metadata_bound())
        .value();
}

detail::CallbackSender LocalCheckpointOperation::sender() noexcept
{
    return {this, &LocalCheckpointOperation::submit};
}

const LocalCheckpointResult &LocalCheckpointOperation::result() const noexcept
{
    return state_->report;
}

void LocalCheckpointOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void LocalCheckpointOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<LocalCheckpointOperation *>(source)->state_->start(complete, context);
}

void LocalCheckpointOperation::State::start(detail::CallbackComplete callback, void *argument) noexcept
{
    uint8_t idle = 0;
    if (!lifecycle.compare_exchange_strong(idle, 1, std::memory_order_acq_rel)) {
        callback(argument, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    complete = callback;
    context  = argument;
    lifecycle.store(2, std::memory_order_release);
    wake();
}

void LocalCheckpointOperation::State::begin()
{
    const auto    &pages      = *image->pages;
    constexpr auto page_limit = kMappingSegmentSize * kMaxMappingSegments;
    if (pages.next_page_id > page_limit ||
        (pages.backend_id != registry->ledger_->limits().backend_id && !pages.frames.empty()) ||
        (pages.frames.empty() ? pages.root_page_id != kInvalidPageId : !pages.frames.contains(pages.root_page_id))) {
        finish(Status::invalid_argument());
        return;
    }
    auto status = registry->begin(pages.coverage, pages.generation, &image->anchor.snapshot_seq);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    admitted            = true;
    const auto sequence = registry->sequence_.load(std::memory_order_relaxed);
    previous            = sequence == 0 ? nullptr : registry->anchors_[sequence % 2 == 0 ? 1 : 0];
    iu                  = registry->ledger_->limits().iu_size;
    segment_bytes       = round_up_to_iu(segment_image_encoded_size(kMappingSegmentSize), iu);
    window = detail::reserved_shared<Window>(budget.bytes,
                                             std::max<uint64_t>(segment_bytes, snapshot::superblock_slot_bytes(iu)),
                                             std::bit_ceil(std::max<size_t>(4096, iu)), budget.bytes);
    auto &anchor             = image->anchor;
    anchor.magic             = snapshot::kAnchorMagic;
    anchor.format_version    = snapshot::kFormatVersion;
    anchor.root_page_id      = pages.root_page_id;
    anchor.last_applied_slot = pages.coverage;
    anchor.next_page_id      = pages.next_page_id;
    anchor.segment_slots     = kMappingSegmentSize;
    position                 = pages.frames.begin();
    stage                    = Stage::Segment;
    wake();
}

void LocalCheckpointOperation::State::finish(Status status) noexcept
{
    if (lifecycle.exchange(3, std::memory_order_acq_rel) == 3) {
        return;
    }
    const bool cancelled = !anchor_touched && stopped.load(std::memory_order_acquire);
    report.status        = cancelled ? Status::unavailable() : std::move(status);
    if (admitted) {
        registry->finish(image, report.status.ok() && !unchanged);
    }
    if (report.status.ok()) {
        report.checkpoint = unchanged ? std::move(previous) : std::move(image);
    }
    image.reset();
    previous.reset();
    window.reset();
    reserved.owner.reset();
    store.reset();
    auto signal = report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (cancelled) {
        signal = detail::CallbackSignal::kStopped;
    }
    complete(context, signal, report.status.without_message());
}
} // namespace crowdb::tree
