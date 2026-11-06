// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_subtree.h"

#include "page_load.h"
#include "snapshot/flush_persistence.h"

#include <algorithm>
#include <limits>

namespace crowdb::tree
{
// stdexec joins a runtime-sized child set as a balanced tree of when_all
// operations. The storage belongs to the parent until every child terminates.
struct FlushBatch::State::Subtree::Children
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        Children *owner;

        void set_value() noexcept
        {
            owner->finish(Status::Ok());
        }

        void set_error(Status error) noexcept
        {
            owner->finish(std::move(error));
        }

        void set_error(std::exception_ptr /*error*/) noexcept
        {
            owner->finish(Status::internal_error());
        }

        void set_stopped() noexcept
        {
            owner->finish(Status::unavailable("flush child stopped"));
        }
    };

    using Sender =
        decltype(stdexec::when_all(std::declval<detail::CallbackSender>(), std::declval<detail::CallbackSender>()));
    using Operation = stdexec::connect_result_t<Sender, Receiver>;

    Children(Subtree &parent, size_t first, size_t last) : parent(parent), first(first), last(last)
    {
    }

    detail::CallbackSender sender()
    {
        return {this, +[](void *raw, detail::CallbackComplete done, void *context) {
                    static_cast<Children *>(raw)->start(done, context);
                }};
    }

    void start(detail::CallbackComplete callback, void *argument) noexcept
    {
        try {
            done    = callback;
            context = argument;
            while (first < last && !parent.children[first]) {
                ++first;
            }
            while (first < last && !parent.children[last - 1]) {
                --last;
            }
            if (first == last) {
                finish(Status::Ok());
                return;
            }
            if (last - first == 1) {
                parent.children[first]->start(done, context);
                return;
            }
            const auto middle = first + ((last - first) / 2);
            left              = detail::reserved_unique<Children>(parent.owner.metadata, parent, first, middle);
            right             = detail::reserved_unique<Children>(parent.owner.metadata, parent, middle, last);
            operation         = detail::reserved_construct<Operation>(parent.owner.metadata, [&](Operation *storage) {
                ::new (static_cast<void *>(storage))
                    Operation(stdexec::connect(stdexec::when_all(left->sender(), right->sender()), Receiver{this}));
            });
            stdexec::start(*operation);
        }
        catch (const std::bad_alloc &) {
            finish(Status::resource_exhausted("flush child join allocation failed"));
        }
        catch (const std::exception &) {
            finish(Status::internal_error("flush child join failed"));
        }
    }

    void finish(Status status) noexcept
    {
        done(context, status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError, std::move(status));
    }

    Subtree                         &parent;
    size_t                           first;
    size_t                           last;
    detail::CallbackComplete         done    = nullptr;
    void                            *context = nullptr;
    detail::ReservedUnique<Children> left;
    detail::ReservedUnique<Children> right;
    std::shared_ptr<Operation>       operation;
};

FlushBatch::State::Subtree::Subtree(State &state, uint64_t page_id, PageKeyRange range, bool selected)
    : owner(state),
      pid(page_id),
      bounds(std::move(range)),
      active(selected),
      final_pages(detail::ReservationAllocator<FinalPage>(state.metadata)),
      expected(state.captured->mapping.get(page_id)),
      output(state.metadata),
      children(detail::ReservationAllocator<std::shared_ptr<Subtree>>(state.metadata))
{
}

FlushBatch::State::Subtree::~Subtree() = default;

detail::CallbackSender FlushBatch::State::Subtree::sender() noexcept
{
    return {this, +[](void *raw, detail::CallbackComplete done, void *context) {
                static_cast<Subtree *>(raw)->start(done, context);
            }};
}

void FlushBatch::State::Subtree::start(detail::CallbackComplete callback, void *argument)
{
    done    = callback;
    context = argument;
    try {
        if ((finished && status.ok()) || !active) {
            if (output.pids.empty()) {
                output.pids.push_back(pid);
            }
            complete(Status::Ok());
            return;
        }
        finished = false;
        status   = Status::Ok();
        if (expected && expected->level == 0) {
            auto *head = owner.leaf_waiters.load(std::memory_order_relaxed);
            do {
                leaf_next = head;
            } while (!owner.leaf_waiters.compare_exchange_weak(head, this, std::memory_order_release,
                                                               std::memory_order_relaxed));
            owner.start_leaves();
        }
        else {
            schedule();
        }
    }
    catch (const std::bad_alloc &) {
        complete(Status::resource_exhausted("flush task allocation failed"));
    }
    catch (const std::exception &) {
        complete(Status::internal_error("flush task failed"));
    }
}

void FlushBatch::State::Subtree::schedule() noexcept
{
    const auto result = owner.runtime.try_submit(owner.lane, &Subtree::run, this);
    if (result.ok()) {
        return;
    }
    if (owner.options.cpu_progress) {
        owner.options.cpu_progress->request(cpu_node, owner.weak_from_this().lock());
    }
    else {
        complete(result);
    }
}

void FlushBatch::State::Subtree::run(void *raw) noexcept
{
    auto &self = *static_cast<Subtree *>(raw);
    // Terminal callbacks may release the connected operation, including us.
    auto          lifetime = self.owner.weak_from_this().lock();
    DispatchScope scope(self.owner);
    try {
        self.step();
    }
    catch (const std::bad_alloc &) {
        self.complete(Status::resource_exhausted("flush subtree allocation failed"));
    }
    catch (const std::exception &) {
        self.complete(Status::internal_error("flush subtree failed"));
    }
}

void FlushBatch::State::Subtree::complete(Status result) noexcept
{
    auto       lifetime = owner.weak_from_this().lock();
    auto      &state    = owner;
    const bool release  = std::exchange(leaf_permit, false);
    status              = std::move(result);
    if (layout_metrics) {
        (void)layout_metrics->finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
    }
    finished          = true;
    const auto signal = status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    done(context, signal, status);
    if (release) {
        state.active_leaves.fetch_sub(1, std::memory_order_acq_rel);
        state.start_leaves();
    }
}

size_t FlushBatch::State::Subtree::fork_bytes() noexcept
{
    return (2 * sizeof(Children)) + detail::shared_allocation_bound<Children::Operation>();
}

void FlushBatch::State::start_leaves() noexcept
{
    auto count = active_leaves.load(std::memory_order_relaxed);
    while (count < options.parallelism) {
        if (!active_leaves.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel)) {
            continue;
        }
        auto *next = leaf_waiters.load(std::memory_order_acquire);
        while (next != nullptr &&
               !leaf_waiters.compare_exchange_weak(next, next->leaf_next, std::memory_order_acq_rel)) {
        }
        if (next != nullptr) {
            next->leaf_permit = true;
            next->schedule();
        }
        else {
            active_leaves.fetch_sub(1, std::memory_order_acq_rel);
            // An enqueuer either takes this permit or observes our idle edge.
            if (leaf_waiters.load(std::memory_order_acquire) == nullptr) {
                return;
            }
        }
        count = active_leaves.load(std::memory_order_relaxed);
    }
}

void FlushBatch::State::Subtree::step()
{
    if (!status.ok()) {
        complete(status);
        return;
    }
    if ((owner.lifecycle.load(std::memory_order_acquire) & kStopped) != 0) {
        complete(Status::unavailable("flush stopped during subtree work"));
        return;
    }
    if (!expected) {
        complete(Status::corruption("flush child is absent from mapping"));
        return;
    }
    if (!active) {
        if (output.pids.empty()) {
            output.pids.push_back(pid);
        }
        complete(Status::Ok());
        return;
    }
    if (joined) {
        persist_next();
        return;
    }
    if (expected->level == 0) {
        leaf();
        return;
    }
    if (children_ready) {
        join();
        return;
    }
    fork();
}

void FlushBatch::State::Subtree::fork()
{
    if (!inner) {
        inner = expected->inner;
    }
    if (!inner) {
        if (!expected->inner_backing || !owner.options.loader) {
            complete(Status::unavailable("flush index requires an asynchronous loader"));
            return;
        }
        request_load();
        return;
    }
    if (inner->children.empty() || inner->children.size() != inner->separators.size() + 1) {
        complete(Status::corruption("invalid flush index shape"));
        return;
    }
    if (children.empty()) {
        children.reserve(inner->children.size());
        for (size_t i = 0; i < inner->children.size(); ++i) {
            auto selected = bounds;
            if (i != 0) {
                selected.lower = inner->separators[i - 1];
            }
            if (i < inner->separators.size()) {
                selected.upper = inner->separators[i];
            }
            auto       first   = owner.first_input(selected.lower_slice());
            const bool touched = (first && selected.contains(Slice(*first))) ||
                                 (owner.options.persistent && owner.options.persistence &&
                                  owner.options.persistence->selected(inner->children[i], selected));
            if (!touched) {
                children.push_back(nullptr);
                continue;
            }
            auto child = detail::reserved_shared<Subtree>(owner.metadata, owner, inner->children[i],
                                                          std::move(selected), touched);
            if (!child->expected || child->expected->level + 1 != expected->level) {
                complete(Status::corruption("flush child level disagrees with parent"));
                return;
            }
            children.push_back(std::move(child));
        }
    }
    child_join = detail::reserved_unique<Children>(owner.metadata, *this, 0, children.size());
    // Callback transports feed stdexec's when_all. The continuation executes
    // only after all direct children, including accepted failures, have joined.
    child_join->start(
        +[](void *raw, detail::CallbackSignal signal, Status result) {
            auto &self          = *static_cast<Subtree *>(raw);
            self.status         = signal == detail::CallbackSignal::kValue ? Status::Ok() : std::move(result);
            self.children_ready = true;
            self.schedule();
        },
        this);
}

void FlushBatch::State::Subtree::reset_work()
{
#ifdef CROWDB_TREE_TEST_UTIL
    handoff_test_waited = false;
#endif
    finished       = false;
    status         = Status::Ok();
    children_ready = false;
    joined         = false;
    persist_at     = 0;
    final_pages.clear();
    child_join.reset();
    layout.reset();
    layout_metrics.reset();
    builder.reset();
    loaded_leaf.reset();
    next_base = 0;
    output.pids.clear();
    output.separators.clear();
    for (auto &child : children) {
        if (child) {
            child->reset_work();
        }
    }
}

} // namespace crowdb::tree
