// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/block_account.h"
#include "btree/local_compaction_internal.h"

namespace crowdb::tree
{
void LocalCompactionOperation::State::Receiver::set_value() noexcept
{
    set_error(Status::Ok());
}

void LocalCompactionOperation::State::Receiver::set_error(Status status) noexcept
{
    auto retained = std::move(owner);
    retained->child_done(std::move(status));
}

void LocalCompactionOperation::State::Receiver::set_stopped() noexcept
{
    set_error(Status::unavailable());
}

void LocalCompactionOperation::State::child_done(Status status) noexcept
{
    child_status = std::move(status);
    child_ready.store(true, std::memory_order_release);
    wake();
}

void LocalCompactionOperation::State::checkpoint()
{
    auto options                            = checkpoint_detail::defaults(*core);
    options.cause                           = TreeWorkCause::GarbageCollection;
    options.metadata.retire_previous_anchor = true;
    options.write.frames.exclusion = {.address = candidate.exclusion.address, .length = candidate.exclusion.length};
    const auto status              = core->prepare_checkpoint(options, &child, ticket);
    if (!status.ok()) {
        finish(status.without_message());
        return;
    }
    phase = candidate.exclusion.owner ? Phase::kCheckpoint : Phase::kRetireAnchor;
    child_ready.store(false, std::memory_order_release);
    attempt.emplace(child->sender(), weak_from_this().lock());
    stdexec::start(attempt->connected);
}

void LocalCompactionOperation::State::pump()
{
    if (phase == Phase::kDone) {
        return;
    }
    if (phase == Phase::kBegin) {
        if (stopped.load(std::memory_order_acquire)) {
            finish(Status::unavailable());
            return;
        }
        if (store == nullptr) {
            finish(Status::Ok());
            return;
        }
        if (local->checkpoints->needs_anchor_retirement()) {
            checkpoint();
            return;
        }
        phase = Phase::kSelected;
        backend_progress->request(backend_node, weak_from_this().lock());
        return;
    }
    if ((phase == Phase::kCheckpoint || phase == Phase::kRetireAnchor) && stopped.load(std::memory_order_acquire)) {
        child->request_stop();
    }
    if (!child_ready.load(std::memory_order_acquire)) {
        return;
    }
    if (phase == Phase::kCheckpoint) {
        report.pages_relocated += child->result().written_frames;
        report.bytes_relocated += child->result().written_bytes;
        attempt.reset();
        child.reset();
        ++rotations;
    }
    else if (phase == Phase::kRetireAnchor) {
        attempt.reset();
        child.reset();
    }
    if (!child_status.ok()) {
        finish(child_status.without_message());
        return;
    }
    if (stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable());
        return;
    }
    if (phase == Phase::kRetireAnchor) {
        phase = Phase::kSelected;
        child_ready.store(false, std::memory_order_release);
        backend_progress->request(backend_node, weak_from_this().lock());
        return;
    }
    if (phase == Phase::kCleaned || !candidate.exclusion.owner) {
        finish(Status::Ok());
        return;
    }
    if (rotations < 2) {
        checkpoint();
        return;
    }
    phase = Phase::kCleaned;
    child_ready.store(false, std::memory_order_release);
    backend_progress->request(backend_node, weak_from_this().lock());
}

void LocalCompactionOperation::State::backend_step()
{
    // Retirement drains are host/backend waits. The CPU lane remains free to
    // execute the paid destructor continuations they depend on.
    core->publisher().reclaim();
    core->binding()->account()->drain_retirements();
    if (phase == Phase::kSelected) {
        const LocalBlockGcLimits limits;
        auto                     freed =
            collect_free_local_blocks(*store, *local->ledger, local->gc_cursor.load(), limits, credit, backend);
        report.blocks_deleted += freed.deleted_blocks;
        child_status = freed.status;
        if (!child_status.ok()) {
            return;
        }
        candidate = select_sparse_local_block(*store, *local->ledger, local->gc_cursor.load(), credit,
                                              options.free_threshold, backend);
        local->gc_cursor.store(candidate.next_block);
        child_status = candidate.status.code() == Code::kNotFound ? Status::Ok() : candidate.status.without_message();
        if (candidate.exclusion.owner) {
            report.blocks_selected = 1;
        }
    }
    else {
        core->cache()->evict();
        core->publisher().reclaim();
        core->binding()->account()->drain_retirements();
        const LocalBlockGcLimits limits{.max_blocks = 1, .max_deleted_bytes = store->block_size()};
        auto                     freed = collect_free_local_blocks(
            *store, *local->ledger, candidate.exclusion.address / store->block_size(), limits, credit, backend);
        report.blocks_deleted += freed.deleted_blocks;
        child_status = freed.status;
    }
}
} // namespace crowdb::tree
