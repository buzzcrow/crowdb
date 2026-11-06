// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "apply_dispatcher.h"
#include "backend/backing_io.h"
#include "page_load.h"
#include "resource_observation.h"
#include "tree_metrics.h"

namespace crowdb::tree
{
inline std::shared_ptr<detail::AllocationReservation> page_load_io_credit(const PageLoadBudget &budget)
{
    if (budget.io || !budget.account) {
        return budget.io;
    }
    auto io = detail::ResourceBudget::try_reserve(budget.account, detail::BudgetClass::kIo, 1);
    if (!io) {
        throw std::bad_alloc();
    }
    return detail::reserved_shared<detail::AllocationReservation>(budget.bytes, std::move(*io));
}

struct PageLoadProgress
{
    PageLoadProgress(const PageLoadBudget &budget, uint64_t started_at_ns)
        : io_credit(page_load_io_credit(budget), 1),
          metrics(budget.work_metrics, TreeStage::PageLoadPipeline, started_at_ns),
          backend_budget(budget.backend),
          cpu_progress(budget.cpu_progress),
          cause(budget.cause),
          cpu_node(&PageLoadProgress::run_ready, this)
    {
    }

    ~PageLoadProgress()
    {
        release_progress();
    }

    PageLoadProgress(const PageLoadProgress &)            = delete;
    PageLoadProgress &operator=(const PageLoadProgress &) = delete;

    AllocationLease                   io_credit;
    TreeMetricsOperation              metrics;
    std::shared_ptr<AllocationBudget> backend_budget;
    std::shared_ptr<void>             loader_owner;
    std::shared_ptr<ApplyDispatcher>  cpu_progress;
    TreeWorkCause                     cause;
    ApplyDispatcher::Node             cpu_node;
    detail::FlushRuntime::Task        cpu_task    = nullptr;
    void                             *cpu_context = nullptr;

    static void run_ready(void *context) noexcept
    {
        auto &self = *static_cast<PageLoadProgress *>(context);
        self.cpu_task(self.cpu_context);
    }

    std::atomic<size_t> *inflight = nullptr;
    std::atomic<bool>    io_pending{false};
    std::atomic<bool>    progress_released{false};

    void release_progress() noexcept
    {
        if (!progress_released.exchange(true, std::memory_order_acq_rel)) {
            io_credit.reset();
            if (inflight != nullptr) {
                inflight->fetch_sub(1, std::memory_order_release);
            }
        }
    }

    void release_if_idle() noexcept
    {
        if (!io_pending.load(std::memory_order_acquire)) {
            release_progress();
        }
    }
};

inline void record_page_load_terminal(const PageLoadBudget &budget, uint64_t started_at_ns,
                                      const Status &status) noexcept
{
    TreeMetricsOperation metrics(budget.work_metrics, TreeStage::PageLoadPipeline, started_at_ns);
    (void)metrics.finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
}

// One immutable callback identity per read, including every overflow step.
// Decoding waits for submit_read to return, so inline success followed by an
// exception cannot publish a result or start another read before rejection.
struct PageReadTicket
{
    PageReadTicket(std::shared_ptr<const void> lifetime, void *operation, AsyncCompleteFn callback,
                   const std::shared_ptr<AllocationBudget> &metadata, size_t bytes)
        : resources(metadata, TreeResourceMetric::ReadInflightBytes, bytes),
          owner(std::move(lifetime)),
          context(operation),
          complete(callback)
    {
    }

    ResourceObservation         resources;
    std::shared_ptr<const void> owner;
    void                       *context;
    AsyncCompleteFn             complete;
    Status                      status;
    std::atomic<bool>           callback_claimed{false};
    std::atomic<unsigned>       events{0};

    static void completed(void *context, Status result)
    {
        auto *ticket = static_cast<PageReadTicket *>(context);
        if (ticket->callback_claimed.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        ticket->resources.reset();
        ticket->status = std::move(result);
        if ((ticket->events.fetch_or(1, std::memory_order_acq_rel) & 2U) != 0) {
            ticket->complete(ticket->context, std::move(ticket->status));
        }
    }

    void submitted()
    {
        if ((events.fetch_or(2, std::memory_order_acq_rel) & 1U) != 0) {
            complete(context, std::move(status));
        }
    }
};

template <class Load>
void queue_page_load(const std::shared_ptr<Load> &operation, detail::FlushRuntime::Task task) noexcept
{
    operation->queued_owner = operation;
    Status status;
    try {
        status = operation->runtime->try_submit(operation->lane, task, operation.get());
    }
    catch (...) {
        status = Status::resource_exhausted();
    }
    if (!status.ok() && operation->cpu_progress) {
        operation->cpu_task    = task;
        operation->cpu_context = operation.get();
        operation->cpu_progress->request(operation->cpu_node, operation);
        return;
    }
    if (!status.ok()) {
        operation->queued_owner.reset();
        operation->finish(std::move(status));
    }
}

template <class Load> void page_read_completed(void *context, Status status) noexcept
{
    auto operation = static_cast<Load *>(context)->shared_from_this();
    operation->io_pending.store(false, std::memory_order_release);
    if (operation->terminal.load(std::memory_order_acquire)) {
        operation->release_progress();
    }
    else if (!status.ok()) {
        operation->finish(std::move(status));
    }
    else {
        queue_page_load(operation, &Load::decode);
    }
}

template <class Load>
void submit_page_read(const std::shared_ptr<Load> &operation, std::shared_ptr<const BackingRef> backing, void *buffer,
                      size_t bytes)
{
    auto ticket = detail::reserved_shared<PageReadTicket>(operation->metadata, operation, operation.get(),
                                                          &page_read_completed<Load>, operation->metadata,
                                                          operation->blob.capacity());
    operation->io_pending.store(true, std::memory_order_release);
    try {
        submit_backing_read(*operation->store, std::move(backing), buffer, bytes, operation->metadata,
                            {.context     = ticket.get(),
                             .complete_fn = &PageReadTicket::completed,
                             .owner       = ticket,
                             .allocations = operation->backend_budget,
                             .cause       = operation->cause});
    }
    catch (const std::bad_alloc &) {
        operation->finish(Status::resource_exhausted());
    }
    catch (...) {
        operation->finish(Status::io_error());
    }
    // On rejection, finish reported the error but retained the operation's
    // progress credit. A late callback or final ticket destruction releases
    // it; neither path may decode or report a second terminal.
    ticket->submitted();
}
} // namespace crowdb::tree
