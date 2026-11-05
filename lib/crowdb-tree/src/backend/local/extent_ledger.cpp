// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "crowdb-common/log.h"
#include "extent_ledger_internal.h"

#include <cassert>

namespace crowdb::tree
{
size_t ExtentLedger::creation_metadata_bound() noexcept
{
    return detail::shared_allocation_bound<State>() + detail::shared_allocation_bound<ExtentLedger>() + (2 * 256);
}

size_t ExtentLedger::reservation_metadata_bound() noexcept
{
    // Includes four red/black-tree nodes, their charged headers/alignment and
    // separate record/control allocations on the supported standard libraries.
    return detail::shared_allocation_bound<State::Request>() + detail::shared_allocation_bound<State::Record>() +
           (4 * 256);
}

ExtentLedger::State::State(ExtentLedgerLimits bounds, const std::shared_ptr<detail::AllocationReservation> &metadata)
    : limits(bounds),
      append(bounds.file_size),
      address_high_water(bounds.file_size)
{
    detail::BudgetMemoryResource::Scope credit(resource, metadata);
    if (append > limits.region_begin) {
        ranges.emplace(limits.region_begin, Range{.length = append - limits.region_begin, .record = nullptr});
        free.emplace(append - limits.region_begin, limits.region_begin);
    }
}

ExtentLedger::State::~State()
{
    assert(protected_ranges.load() == 0 && incoming.load() == nullptr && excluded == nullptr);
}

Status ExtentLedger::create(ExtentLedgerLimits limits, std::shared_ptr<detail::AllocationReservation> metadata,
                            std::shared_ptr<ExtentLedger> *out)
{
    if (out == nullptr || limits.backend_id == 0 || limits.generation == 0 || limits.iu_size == 0 ||
        limits.region_begin % limits.iu_size != 0 || limits.file_size % limits.iu_size != 0 ||
        limits.file_size < limits.region_begin || limits.file_size > limits.address_limit || limits.max_ranges == 0 ||
        limits.queue_capacity == 0 || (metadata && metadata->category() == detail::BudgetClass::kIo)) {
        return Status::invalid_argument();
    }
    try {
        auto state = detail::reserved_shared<State>(metadata, limits, metadata);
        *out       = detail::reserved_construct<ExtentLedger>(
            metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) ExtentLedger(std::move(state)); });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

Status ExtentLedger::recover(uint64_t address, uint64_t length, std::shared_ptr<detail::AllocationReservation> metadata,
                             Complete complete)
{
    return state_->submit(State::Action::Recover, address, length, std::move(metadata), std::move(complete));
}

Status ExtentLedger::activate(std::shared_ptr<detail::AllocationReservation> metadata, Complete complete)
{
    return state_->submit(State::Action::Activate, 0, 0, std::move(metadata), std::move(complete));
}

Status ExtentLedger::reserve(uint64_t length, std::shared_ptr<detail::AllocationReservation> metadata,
                             Complete complete)
{
    return state_->submit(State::Action::Reserve, 0, length, std::move(metadata), std::move(complete));
}

Status ExtentLedger::retain(uint64_t address, uint64_t length, std::shared_ptr<detail::AllocationReservation> metadata,
                            Complete complete)
{
    return state_->submit(State::Action::Retain, address, length, std::move(metadata), std::move(complete));
}

Status ExtentLedger::claim_free(uint64_t address, uint64_t length,
                                std::shared_ptr<detail::AllocationReservation> metadata, Complete complete)
{
    return state_->submit(State::Action::ClaimFree, address, length, std::move(metadata), std::move(complete));
}

Status ExtentLedger::exclude_allocations(uint64_t address, uint64_t length,
                                         std::shared_ptr<detail::AllocationReservation> metadata, Complete complete)
{
    return state_->submit(State::Action::Exclude, address, length, std::move(metadata), std::move(complete));
}

ExtentLedgerStats ExtentLedger::stats() const noexcept
{
    return {.address_high_water   = state_->address_high_water.load(std::memory_order_acquire),
            .protected_bytes      = state_->protected_bytes.load(std::memory_order_relaxed),
            .protected_ranges     = state_->protected_ranges.load(std::memory_order_relaxed),
            .reused_allocations   = state_->reused_allocations.load(std::memory_order_relaxed),
            .appended_allocations = state_->appended_allocations.load(std::memory_order_relaxed)};
}

const ExtentLedgerLimits &ExtentLedger::limits() const noexcept
{
    return state_->limits;
}

void ExtentLedger::close() noexcept
{
    state_->closed.store(true, std::memory_order_release);
}

Status ExtentLedger::State::submit(Action action, uint64_t address, uint64_t length,
                                   std::shared_ptr<detail::AllocationReservation> metadata, Complete complete)
{
    if (!complete || (metadata && metadata->category() == detail::BudgetClass::kIo)) {
        return Status::invalid_argument();
    }
    if (closed.load(std::memory_order_acquire)) {
        return Status::unavailable();
    }
    std::shared_ptr<Request> request;
    try {
        request           = detail::reserved_shared<Request>(metadata);
        request->command  = {.action = action, .context = request.get()};
        request->address  = address;
        request->length   = length;
        request->metadata = std::move(metadata);
        request->complete = std::move(complete);
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    auto count = queued.load(std::memory_order_relaxed);
    do {
        if (count >= limits.queue_capacity) {
            return Status::resource_exhausted();
        }
    } while (!queued.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel));
    request->ledger       = shared_from_this();
    request->queued_owner = request;
    enqueue(&request->command);
    return Status::Ok();
}

void ExtentLedger::State::enqueue(Command *command) noexcept
{
    auto self = weak_from_this().lock();
    assert(self);
    auto *head = incoming.load(std::memory_order_relaxed);
    do {
        command->next = head;
    } while (!incoming.compare_exchange_weak(head, command, std::memory_order_release, std::memory_order_relaxed));
    if (!running.exchange(true, std::memory_order_acq_rel)) {
        drain();
    }
}

void ExtentLedger::State::drain() noexcept
{
    for (;;) {
        auto    *list    = incoming.exchange(nullptr, std::memory_order_acquire);
        Command *ordered = nullptr;
        while (list != nullptr) {
            auto *next = list->next;
            list->next = ordered;
            ordered    = list;
            list       = next;
        }
        while (ordered != nullptr) {
            auto *next = ordered->next;
            if (ordered->action == Action::Release) {
                release(static_cast<Record *>(ordered->context));
            }
            else {
                process(static_cast<Request *>(ordered->context));
            }
            ordered = next;
        }
        (void)running.exchange(false, std::memory_order_acq_rel);
        if (incoming.load(std::memory_order_acquire) == nullptr || running.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
    }
}

void ExtentLedger::State::process(Request *request) noexcept
{
    auto               owner = std::move(request->queued_owner);
    ExtentLedgerResult result;
    try {
        detail::BudgetMemoryResource::Scope credit(resource, request->metadata);
        result = apply(*request);
    }
    catch (const std::bad_alloc &) {
        result.status = Status::resource_exhausted();
    }
    catch (...) {
        result.status = Status::internal_error();
    }
    queued.fetch_sub(1, std::memory_order_release);
    try {
        request->complete(std::move(result));
    }
    catch (...) {
        CRB_LOG_ERROR("extent receiver threw; remaining allocation/release commands will continue");
    }
}

ProtectedExtent ExtentLedger::State::describe(Record &record, std::shared_ptr<const void> owner)
{
    auto metrics = record.exclusive ? nullptr : std::shared_ptr<BackingMetricExtent>(owner, &record.metrics);
    return {.backend_id       = record.ledger->limits.backend_id,
            .generation       = record.ledger->limits.generation,
            .identity         = record.identity,
            .address          = record.address,
            .length           = record.length,
            .owner            = std::move(owner),
            .physical_metrics = std::move(metrics)};
}

} // namespace crowdb::tree
