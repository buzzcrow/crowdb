// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "apply_progress.h"
#include "flush_controller_internal.h"
#include "flush_controller_test.h"
#include "flush_operation_internal.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
std::atomic<uint64_t> next_admission_owner{1};
#ifdef CROWDB_TREE_TEST_UTIL
thread_local FlushPrepareHook prepare_hook_for_tests    = nullptr;
thread_local void            *prepare_context_for_tests = nullptr;
#endif
} // namespace

#ifdef CROWDB_TREE_TEST_UTIL
void FlushController::set_handoff_wait_for_tests(std::function<bool(uint64_t, std::function<void()>)> callback)
{
    state_->options.handoff_wait_for_tests = std::move(callback);
}

void set_flush_prepare_hook_for_tests(FlushPrepareHook hook, void *context) noexcept
{
    prepare_hook_for_tests    = hook;
    prepare_context_for_tests = context;
}
#endif

size_t FlushController::source_window() const noexcept
{
    return state_->options.max_sources;
}

bool FlushController::State::try_enter() noexcept
{
    if (closed.load(std::memory_order_acquire)) {
        return false;
    }
    if (!preparing.test_and_set(std::memory_order_acquire)) {
#ifdef CROWDB_TREE_TEST_UTIL
        if (prepare_hook_for_tests != nullptr) {
            prepare_hook_for_tests(prepare_context_for_tests, true);
        }
#endif
        return true;
    }
    preparing_waiter.store(true, std::memory_order_release);
#ifdef CROWDB_TREE_TEST_UTIL
    if (prepare_hook_for_tests != nullptr) {
        prepare_hook_for_tests(prepare_context_for_tests, false);
    }
#endif
    // Covers the holder releasing just before this failed entrant registered.
    // Only an observed release is progress; uncontended planning is not.
    if (!preparing.test(std::memory_order_acquire) && progress) {
        progress->advance();
    }
    return false;
}

void FlushController::State::leave() noexcept
{
    preparing.clear(std::memory_order_release);
    if (preparing_waiter.exchange(false, std::memory_order_acq_rel) && progress) {
        progress->advance();
    }
}

bool FlushController::State::has_pending() const noexcept
{
    return pending && !pending->state_->complete_success.load(std::memory_order_acquire) &&
           !pending->state_->replaceable.load(std::memory_order_acquire);
}

size_t FlushController::State::control_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<FlushOperation::State>() +
           detail::shared_allocation_bound<FlushOperation>() +
           (4 * PublicationCoordinator::empty_commit_reserve_bytes()) +
           PublicationCoordinator::complete_view_reserve_bytes();
}

FlushController::FlushController(PublicationCoordinator &publisher, detail::FlushRuntime &runtime,
                                 SourceAdmission &sources, const std::atomic<uint64_t> &contiguous,
                                 std::shared_ptr<detail::FlushRuntime::Lane> lane, FlushBatchOptions options,
                                 FlushBatch::Maintenance                        maintenance,
                                 std::shared_ptr<detail::AllocationReservation> lifecycle,
                                 std::shared_ptr<ApplyProgress>                 progress)
    : state_(detail::reserved_unique<State>(lifecycle, publisher, runtime, sources, contiguous, std::move(lane),
                                            std::move(options), std::move(maintenance)))
{
    state_->progress  = std::move(progress);
    state_->lifecycle = std::move(lifecycle);
}

size_t FlushController::allocation_bound() noexcept
{
    return sizeof(FlushController) + sizeof(State) + detail::shared_allocation_bound<SourcePause>();
}

FlushController::~FlushController()
{
    close();
}

Status FlushController::prepare(std::shared_ptr<detail::AllocationReservation> metadata,
                                std::shared_ptr<FlushOperation> *out, std::optional<uint64_t> expected_commit_seq)
{
    return prepare_impl(std::move(metadata), out, expected_commit_seq, nullptr);
}

Status FlushController::prepare(const FlushPreparationOptions &preparation, std::shared_ptr<FlushOperation> *out)
{
    return prepare_impl({}, out, {}, &preparation);
}

Status FlushController::prepare(const FlushPreparationOptions &preparation, std::shared_ptr<FlushOperation> *out,
                                bool *blocked)
{
    return prepare_impl({}, out, {}, &preparation, nullptr, nullptr, blocked);
}

Status FlushController::prepare_impl(std::shared_ptr<detail::AllocationReservation> metadata,
                                     std::shared_ptr<FlushOperation> *out, std::optional<uint64_t> expected_commit_seq,
                                     const FlushPreparationOptions *preparation, const FlushReservePlan *plan,
                                     detail::FlushCredits *credits, bool *blocked)
{
    if (blocked != nullptr) {
        *blocked = false;
    }
    if (out == nullptr || (!metadata && preparation == nullptr) || !state_->lane ||
        (preparation != nullptr && preparation->runtime.get() != &state_->runtime)) {
        return Status::invalid_argument("flush needs output, credit and lane");
    }
    if (!state_->try_enter()) {
        if (blocked != nullptr) {
            *blocked = !state_->closed.load(std::memory_order_acquire);
        }
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
        *out = state_->pending;
        return Status::Ok();
    }
    state_->pending.reset();
    try {
        const auto source_view = state_->sources.catalog();
        if (source_view->active_budget) {
            const auto &limits = source_view->active_budget->options();
            const auto  largest_key =
                std::min(limits.max_key_size, std::max(limits.max_key_bytes, limits.max_gap_key_bytes));
            if (largest_key > state_->options.run_limits.max_key_bytes) {
                return Status::invalid_argument("flush key window is smaller than the admitted source key limit");
            }
        }
        const auto current = state_->publisher.capture();
        if (current->flush_admission_owner != 0 || current->pending_handoff || current->checkpoint_inflight) {
            if (blocked != nullptr) {
                *blocked = true;
            }
            return Status::unavailable("flush waits for the current publication owner");
        }
        if (plan != nullptr && (credits == nullptr || !plan->view || !plan->sources || plan->view != current ||
                                plan->sources != source_view)) {
            return Status::unavailable("flush reserve plan changed before binding");
        }
        if (preparation != nullptr && plan == nullptr) {
            std::shared_ptr<FlushOperation> covered;
            auto status = state_->prepare_covered(*preparation, current, source_view, &covered);
            if (!status.ok() || covered) {
                if (covered) {
                    *out = std::move(covered);
                }
                return status;
            }
        }
        if (preparation != nullptr) {
            if (plan != nullptr) {
                if (!plan->pause || !plan->pause->ready() || !plan->pause->matches(*source_view)) {
                    return Status::unavailable("flush plan lost its stable admission bound");
                }
            }
            else {
                auto status = state_->pause_source(*source_view);
                if (!status.ok()) {
                    if (blocked != nullptr) {
                        *blocked = status.code() == Code::kUnavailable;
                    }
                    return status;
                }
            }
        }

        struct ReleasePause
        {
            State &state;

            ~ReleasePause()
            {
                state.pause.reset();
            }
        } release_pause{*state_};

        auto                      options     = state_->options;
        auto                      maintenance = state_->maintenance;
        detail::FlushReservations reservations;
        if (preparation != nullptr) {
            auto status =
                plan != nullptr
                    ? detail::bind_flush_reservations(
                          state_->publisher, *preparation, source_view->active_budget->options().account, state_->lane,
                          plan->bounds, std::move(*credits), &options, &maintenance, &reservations)
                    : detail::prepare_flush_reservations(state_->publisher, *current, *source_view, *preparation,
                                                         State::control_allocation_bound(), state_->lane, &options,
                                                         &maintenance, &reservations, state_->pause->bound());
            if (!status.ok()) {
                return status;
            }
            metadata            = reservations.handoff;
            expected_commit_seq = current->commit_seq;
        }
        auto operation = detail::reserved_shared<FlushOperation::State>(
            metadata, state_->publisher, state_->runtime, state_->sources, state_->contiguous, state_->lane,
            std::move(options), std::move(maintenance));
        operation->reservations              = std::move(reservations);
        operation->progress                  = state_->progress;
        operation->expected_catalog          = preparation != nullptr ? source_view : nullptr;
        operation->source_pause              = plan != nullptr ? plan->pause : state_->pause;
        operation->report.floor              = current->coverage;
        operation->report.frontier           = current->coverage;
        operation->report.published_coverage = current->coverage;
        operation->metadata                  = std::move(metadata);
        operation->expected_commit_seq       = expected_commit_seq.value_or(current->commit_seq);
        operation->admission_owner           = next_admission_owner.fetch_add(1, std::memory_order_relaxed);
        operation->admission_record          = detail::reserved_shared<InstallRecord>(
            operation->metadata, InstallToken{.generation = current->generation,
                                              .batch      = operation->admission_owner,
                                              .range      = UINT64_MAX - 3,
                                              .sequence   = 0});
        auto release_credit = operation->metadata->take_reserved(PublicationCoordinator::empty_commit_reserve_bytes());
        if (!release_credit) {
            return Status::resource_exhausted("flush needs independent admission release credit");
        }
        operation->release_metadata =
            detail::reserved_shared<detail::AllocationReservation>(operation->metadata, std::move(*release_credit));
        operation->release_request =
            detail::reserved_shared<PublicationRequest>(operation->metadata, operation->metadata);
        operation->release_request->transaction = {
            .record   = detail::reserved_shared<InstallRecord>(operation->metadata,
                                                               InstallToken{.generation = current->generation,
                                                                            .batch      = operation->admission_owner,
                                                                            .range      = UINT64_MAX - 3,
                                                                            .sequence   = 1}
                                                               ),
            .metadata = operation->release_metadata,
            .flush_admission =
                FlushAdmissionPublication{.action = FlushAdmissionAction::Release, .owner = operation->admission_owner},
            .flush_admission_owner = operation->admission_owner
        };
        state_->pending = detail::reserved_construct<FlushOperation>(operation->metadata, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) FlushOperation(std::move(operation));
        });
        *out            = state_->pending;
        return Status::Ok();
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted("flush operation allocation failed");
    }
}

void FlushController::close() noexcept
{
    state_->closed.store(true, std::memory_order_release);
    if (state_->progress) {
        state_->progress->advance();
    }
}

void FlushController::release_pending() noexcept
{
    state_->pending.reset();
    state_->pause.reset();
}

} // namespace crowdb::tree
