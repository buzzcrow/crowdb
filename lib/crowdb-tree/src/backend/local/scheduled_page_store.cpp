// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "scheduled_page_store.h"

#include "btree/allocation_bound.h"
#include "btree/tree_metrics.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/backend/block_page_store.h"

namespace crowdb::tree
{
struct ScheduledPageStore::State
{
    PageStore                                     *sync  = nullptr;
    AsyncPageStore                                *async = nullptr;
    std::shared_ptr<const void>                    backend_owner;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<detail::FlushRuntime::Lane>    lane;
    std::shared_ptr<detail::AllocationReservation> jobs;
    std::atomic<size_t>                            active{0};

    ~State()
    {
        if (jobs) {
            jobs->seal();
        }
    }
};

struct ScheduledPageStore::Job
{
    std::shared_ptr<State>           state;
    std::shared_ptr<Job>             queued_owner;
    AsyncCompletion                  consumer;
    detail::InlineFunction<Status()> step;
    PageAddr                         address     = 0;
    void                            *read        = nullptr;
    const void                      *write       = nullptr;
    size_t                           length      = 0;
    PagePurpose                      purpose     = PagePurpose::kBtreePage;
    size_t                           read_bytes  = 0;
    size_t                           write_bytes = 0;
    bool                             io_started  = false;
    std::atomic<bool>                completed{false};
    bool                             admitted = false;
    ApplyDispatcher::Node            node{&Job::execute, this};

    ~Job()
    {
        if (admitted) {
            finish(Status::unavailable("backend dropped its accepted completion"));
        }
    }

    void finish(Status status, bool callback = false) noexcept
    {
        if (completed.exchange(true, std::memory_order_acq_rel)) {
            if (callback) {
                record_work(state->binding->work_metrics(), TreeWorkCounter::StaleCallbacks, 1, consumer.cause);
            }
            return;
        }
        if (io_started) {
            const auto &metrics = state->binding->work_metrics();
            if (read_bytes != 0) {
                record_work(metrics, status.ok() ? TreeWorkCounter::ReadBytes : TreeWorkCounter::ReadErrors,
                            status.ok() ? read_bytes : 1, consumer.cause);
            }
            if (write_bytes != 0) {
                record_work(metrics, status.ok() ? TreeWorkCounter::WriteBytes : TreeWorkCounter::WriteErrors,
                            status.ok() ? write_bytes : 1, consumer.cause);
            }
        }
        state->active.fetch_sub(1, std::memory_order_acq_rel);
        try {
            consumer.complete(std::move(status));
        }
        catch (...) {
            CRB_LOG_ERROR("backend completion receiver threw after terminal");
        }
    }

    static void completed_io(void *context, Status status)
    {
        static_cast<Job *>(context)->finish(std::move(status), true);
    }

    static void execute(void *context) noexcept
    {
        auto *job  = static_cast<Job *>(context);
        auto  self = std::move(job->queued_owner);
        try {
            job->io_started = true;
            record_work(job->state->binding->work_metrics(), TreeWorkCounter::ReadOperations,
                        static_cast<uint64_t>(job->read_bytes != 0), job->consumer.cause);
            record_work(job->state->binding->work_metrics(), TreeWorkCounter::WriteOperations,
                        static_cast<uint64_t>(job->write_bytes != 0), job->consumer.cause);
            if (job->step) {
                job->finish(job->step());
                return;
            }
            if (job->state->async != nullptr) {
                AsyncCompletion callback{.context     = job,
                                         .complete_fn = &completed_io,
                                         .owner       = self,
                                         .allocations = job->consumer.allocations,
                                         .cause       = job->consumer.cause};
                if (job->read != nullptr) {
                    job->state->async->submit_read(job->address, job->read, job->length, std::move(callback));
                }
                else if (job->write != nullptr) {
                    job->state->async->submit_typed_write(job->purpose, job->address, job->write, job->length,
                                                          std::move(callback));
                }
                else {
                    auto status = job->state->async->submit_fsync(std::move(callback));
                    if (!status.ok()) {
                        job->finish(std::move(status));
                    }
                }
                return;
            }
            if (job->state->sync == nullptr) {
                job->finish(Status::unavailable("backend owner has no synchronous store"));
                return;
            }
            Status status;
            auto  *block = dynamic_cast<BlockPageStore *>(job->state->sync);
            if (block != nullptr) {
                const auto &budget = job->consumer.allocations;
                if (job->read != nullptr) {
                    status = block->read_at(job->address, static_cast<uint8_t *>(job->read), job->length, budget);
                }
                else if (job->write != nullptr) {
                    status =
                        block->write_at(job->address, static_cast<const uint8_t *>(job->write), job->length, budget);
                }
                else {
                    status = block->sync(budget);
                }
                job->finish(std::move(status));
                return;
            }
            if (job->read != nullptr) {
                status = job->state->sync->read_at(job->address, static_cast<uint8_t *>(job->read), job->length);
            }
            else if (job->write != nullptr) {
                status = job->state->sync->write_typed_at(job->purpose, job->address,
                                                          static_cast<const uint8_t *>(job->write), job->length);
            }
            else {
                status = job->state->sync->sync();
            }
            job->finish(std::move(status));
        }
        catch (const std::bad_alloc &) {
            job->finish(Status::resource_exhausted());
        }
        catch (...) {
            job->finish(Status::io_error("backend submission threw"));
        }
    }
};

std::optional<size_t> ScheduledPageStore::allocation_bound(size_t operations) noexcept
{
    using namespace detail;
    return AllocationBound(shared_allocation_bound<State>() + shared_allocation_bound<ScheduledPageStore>() +
                           shared_allocation_bound<AllocationReservation>())
        .add_product(operations, shared_allocation_bound<Job>())
        .add(2 * shared_allocation_bound<Job>())
        .value();
}

Status ScheduledPageStore::create(PageStore *sync, AsyncPageStore *async, std::shared_ptr<const void> backend_owner,
                                  std::shared_ptr<TreeRuntimeBinding>                   binding,
                                  const std::shared_ptr<detail::AllocationReservation> &metadata,
                                  std::shared_ptr<detail::FlushRuntime::Lane>           lane,
                                  std::shared_ptr<AsyncPageStore>                      *out)
try {
    if ((sync == nullptr && async == nullptr) || !binding || !metadata || !lane || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    const auto bound = detail::AllocationBound(detail::shared_allocation_bound<detail::AllocationReservation>())
                           .add_product(binding->options().limits.io_operations, detail::shared_allocation_bound<Job>())
                           .add(2 * detail::shared_allocation_bound<Job>())
                           .value();
    if (!bound) {
        return Status::resource_exhausted();
    }
    auto credit = metadata->take_reserved(*bound);
    if (!credit) {
        return Status::resource_exhausted();
    }
    auto state           = detail::reserved_shared<State>(metadata);
    state->jobs          = detail::AllocationReservation::create(std::move(*credit));
    state->lane          = std::move(lane);
    state->sync          = sync;
    state->async         = async;
    state->backend_owner = std::move(backend_owner);
    state->binding       = std::move(binding);
    *out                 = detail::reserved_construct<ScheduledPageStore>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) ScheduledPageStore(std::move(state)); });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

PageStore *ScheduledPageStore::synchronous_store() const noexcept
{
    return state_->sync;
}

bool ScheduledPageStore::uses_async_transport() const noexcept
{
    return state_->async != nullptr;
}

size_t ScheduledPageStore::allocation_workers() const noexcept
{
    return std::max(state_->binding->cpu()->worker_count(), state_->binding->backend_cpu()->worker_count());
}

bool ScheduledPageStore::write_completion_is_reloadable() const noexcept
{
    return state_->async != nullptr ? state_->async->write_completion_is_reloadable()
                                    : !state_->sync->has_shared_ownership();
}

Status ScheduledPageStore::submit(PageAddr address, void *read, const void *write, size_t length, PagePurpose purpose,
                                  AsyncCompletion complete, detail::InlineFunction<Status()> step, size_t read_bytes,
                                  size_t write_bytes)
try {
    if (!complete || (length != 0 && read == nullptr && write == nullptr) || address > UINT64_MAX - length) {
        return Status::invalid_argument();
    }
    using namespace detail;
    auto job         = reserved_shared<Job>(state_->jobs);
    job->state       = state_;
    job->step        = std::move(step);
    job->consumer    = std::move(complete);
    job->address     = address;
    job->read        = read;
    job->write       = write;
    job->length      = length;
    job->read_bytes  = read != nullptr ? length : read_bytes;
    job->write_bytes = write != nullptr ? length : write_bytes;
    job->purpose     = purpose;
    auto active      = state_->active.load(std::memory_order_relaxed);
    do {
        if (active >= state_->binding->options().limits.io_operations) {
            record_work(state_->binding->work_metrics(), TreeWorkCounter::AdmissionRejected, 1, job->consumer.cause);
            return Status::resource_exhausted();
        }
    } while (!state_->active.compare_exchange_weak(active, active + 1, std::memory_order_acq_rel));
    job->admitted     = true;
    job->queued_owner = job;
    auto status       = state_->binding->backend_cpu()->try_submit(state_->lane, &Job::execute, job.get());
    if (!status.ok()) {
        state_->binding->backend_progress()->request(job->node, job);
    }
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

uint64_t ScheduledPageStore::submit_read(PageAddr address, void *buffer, size_t length, AsyncCompletion complete)
{
    if (length == 0) {
        complete.complete(Status::Ok());
        return 0;
    }
    auto status = submit(address, buffer, nullptr, length, PagePurpose::kBtreePage, complete);
    if (!status.ok()) {
        complete.complete(std::move(status));
    }
    return 0;
}

uint64_t ScheduledPageStore::submit_write(PageAddr address, const void *buffer, size_t length, AsyncCompletion complete)
{
    return submit_typed_write(PagePurpose::kBtreePage, address, buffer, length, std::move(complete));
}

uint64_t ScheduledPageStore::submit_typed_write(PagePurpose purpose, PageAddr address, const void *buffer,
                                                size_t length, AsyncCompletion complete)
{
    if (length == 0) {
        complete.complete(Status::Ok());
        return 0;
    }
    auto status = submit(address, nullptr, buffer, length, purpose, complete);
    if (!status.ok()) {
        complete.complete(std::move(status));
    }
    return 0;
}

Status ScheduledPageStore::submit_fsync(AsyncCompletion complete)
{
    return submit(0, nullptr, nullptr, 0, PagePurpose::kBtreePage, std::move(complete));
}

Status ScheduledPageStore::submit_step(detail::InlineFunction<Status()> step, AsyncCompletion complete,
                                       size_t read_bytes, size_t write_bytes)
{
    if (!step || (read_bytes != 0 && write_bytes != 0)) {
        return Status::invalid_argument();
    }
    return submit(0, nullptr, nullptr, 0, PagePurpose::kBtreePage, std::move(complete), std::move(step), read_bytes,
                  write_bytes);
}

void ScheduledPageStore::cancel(uint64_t /*operation*/)
{
    // Accepted operations keep their owners until real terminal or transport
    // disposal. Cancellation of a consumer never releases in-flight I/O early.
}
} // namespace crowdb::tree
