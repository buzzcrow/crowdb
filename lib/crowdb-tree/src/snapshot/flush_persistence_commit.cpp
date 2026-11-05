// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/checkpoint_backend.h"
#include "btree/allocation_bound.h"
#include "crowdb-common/log.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/primary_backing.h"

#include <cassert>

namespace crowdb::tree
{
struct FlushPersistence::Commit : std::enable_shared_from_this<Commit>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<Commit> owner;

        void set_value() noexcept
        {
            owner->resume(Status::Ok());
        }

        void set_error(Status status) noexcept
        {
            owner->resume(std::move(status));
        }

        void set_stopped() noexcept
        {
            owner->resume(Status::unavailable());
        }
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<Commit> owner)
            : connected(stdexec::connect(sender, Receiver{std::move(owner)}))
        {
        }

        Connected connected;
    };

    std::shared_ptr<FlushPersistence>           owner;
    std::shared_ptr<const PublishedTreeState>   view;
    FlushBatch::MaintenanceComplete             done;
    CheckpointWork                              work;
    std::shared_ptr<const CapturedDurablePages> pages;
    std::shared_ptr<LocalCheckpointOperation>   local;
    std::shared_ptr<BackendCheckpoint>          chunk;
    std::optional<Attempt>                      attempt;
    Status                                      status;
    ApplyDispatcher::Node                       node{&Commit::run, this};

    void start()
    {
        auto &p      = *owner->preparation_;
        auto  source = owner->pages_ ? std::shared_ptr<const CapturedDurablePages>(owner->pages_) : owner->inherited_;
        CapturedWritePlan plan;
        plan.segments = source ? source->frames.segment_count() : 0;
        auto extra    = detail::AllocationBound(detail::shared_allocation_bound<CapturedDurablePages>())
                            .add_product(plan.segments + 1, 128)
                            .add_product(std::min(plan.segments, view->snapshot_changes.size()),
                                         ReadablePageFrames::segment_allocation_bound());
        if (p.core->chunk_backend()) {
            extra.add(p.core->chunk_backend()->checkpoint_allocation_bound(
                source ? source->frames.size() : 0, source ? source->stored_reference_count : 0));
        }
        auto bound = extra.value();
        if (!bound) {
            finish(Status::resource_exhausted("flush metadata bound overflow"));
            return;
        }
        auto result = p.work(*view, &plan, &work, *bound, owner->io_);
        if (!result.ok()) {
            reconcile(std::move(result));
            return;
        }
        if (!source || source->coverage != view->coverage || source->generation != view->generation ||
            source->root_page_id != view->root_page_id || source->next_page_id != view->next_page_id || owner->pages_) {
            if (!owner->pages_) {
                owner->pages_ = detail::reserved_shared<CapturedDurablePages>(work.budget.bytes, work.budget.bytes);
                if (source) {
                    owner->pages_->frames.inherit(source->frames);
                    owner->pages_->overflow_counts_known  = source->overflow_counts_known;
                    owner->pages_->stored_reference_count = source->stored_reference_count;
                }
            }
            auto &catalog = *owner->pages_;
            catalog.frames.budget(work.budget.bytes);
            result = catalog.initialize_overflow_counts();
            if (!result.ok()) {
                reconcile(std::move(result));
                return;
            }

            struct Deletions
            {
                CapturedDurablePages *pages;
                Status                status;
            } deletions{.pages = &catalog, .status = Status::Ok()};

            uint64_t next = 0;
            while (deletions.status.ok() && !view->snapshot_changes.visit_from(
                                                next, 64, &deletions, +[](void *raw, const SnapshotPageChange &change) {
                                                    auto &d = *static_cast<Deletions *>(raw);
                                                    if (d.status.ok() && !change.after.present) {
                                                        d.status = d.pages->erase_primary(change.page_id);
                                                    }
                                                })) {
            }
            if (!deletions.status.ok()) {
                reconcile(std::move(deletions.status));
                return;
            }
            catalog.coverage     = view->coverage;
            catalog.generation   = view->generation;
            catalog.root_page_id = view->root_page_id;
            catalog.next_page_id = view->next_page_id;
            catalog.backend_id   = p.core->local_backend() ? p.core->local_backend()->ledger->limits().backend_id
                                                           : p.core->chunk_backend()->backend_identity();
            catalog.frames.refresh_counts();
            pages = owner->pages_;
            source.reset();
        }
        else {
            pages = std::move(source);
        }
        const auto shape              = pages->frames.shape();
        owner->report_.primary_frames = shape.reachable_leaf_pages + shape.reachable_inner_pages;
        owner->report_.reused_frames  = pages->frames.size() >= owner->report_.written_frames
                                          ? pages->frames.size() - owner->report_.written_frames
                                          : 0;
        owner->pending_leaves_.reset();
        owner->selection_metadata_.reset();
        owner->initial_.reset();
        owner->report_.captured_coverage = view->coverage;
        if (p.core->chunk_backend()) {
            result = p.core->chunk_backend()->prepare_checkpoint(
                {.pages                = pages,
                 .view                 = *view,
                 .wal_offset           = owner->wal_offset_,
                 .max_dependency_steps = p.options.write.max_dependency_steps,
                 .store                = p.ticket->backend(p.core->async_backend()),
                 .runtime              = p.core->binding()->cpu(),
                 .lane                 = p.core->binding()->lane(),
                 .budget               = work.budget,
                 .owner                = owner},
                &chunk);
        }
        else {
            auto metadata                   = p.options.metadata;
            metadata.exclusion              = p.options.write.frames.exclusion;
            metadata.retire_previous_anchor = metadata.retire_previous_anchor || metadata.exclusion.length != 0;
            result                          = LocalCheckpointOperation::create(
                pages, p.core->local_backend()->checkpoints, p.ticket->backend(p.core->async_backend()),
                p.core->binding()->cpu(), p.core->binding()->lane(), metadata, work.budget, &local);
        }
        if (!result.ok()) {
            reconcile(std::move(result));
            return;
        }
        attempt.emplace(chunk ? chunk->sender() : local->sender(), shared_from_this());
        stdexec::start(attempt->connected);
    }

    void resume(Status result) noexcept
    {
        status = std::move(result);
        owner->preparation_->cpu_progress->request(node, weak_from_this().lock());
    }

    static void run(void *raw) noexcept
    {
        auto self = static_cast<Commit *>(raw)->weak_from_this().lock();
        assert(self);
        try {
            self->written();
        }
        catch (const std::bad_alloc &) {
            self->fail(Status::resource_exhausted("flush commit reservation exhausted"));
        }
        catch (...) {
            self->fail(Status::internal_error("flush commit failed"));
        }
    }

    void fail(Status error) noexcept
    {
        try {
            reconcile(std::move(error));
        }
        catch (...) {
            finish(Status::internal_error());
        }
    }

    void written()
    {
        attempt.reset();
        if (chunk) {
            chunk->completed();
            if (chunk->committed()) {
                status = Status::Ok();
            }
        }
        if (status.ok()) {
            owner->report_.durable_committed = true;
            if (auto *diagnostics = owner->preparation_->options.diagnostics) {
                diagnostics->written_frames.store(owner->report_.written_frames, std::memory_order_release);
                diagnostics->total_written_frames.fetch_add(owner->report_.written_frames, std::memory_order_relaxed);
            }
            owner->report_.writeback_completed = true;
            if (local) {
                pages = local->result().checkpoint->pages;
            }
        }
        reconcile(std::move(status));
    }

    void reconcile(Status result)
    {
        auto &p       = *owner->preparation_;
        auto &report  = owner->report_;
        report.status = std::move(result);
        std::shared_ptr<SnapshotBaseline> baseline;
        if (report.durable_committed) {
            report.durable_coverage     = p.core->durable_coverage();
            report.snapshot_sequence    = p.core->snapshot_sequence();
            baseline                    = detail::reserved_shared<SnapshotBaseline>(p.finish);
            baseline->generation        = view->generation;
            baseline->capture_token     = report.checkpoint_token;
            baseline->coverage          = report.durable_coverage;
            baseline->snapshot_sequence = report.snapshot_sequence;
            baseline->backend_id        = pages->backend_id;
            baseline->gc_floor          = view->gc_floor;
            baseline->wal_offset        = p.core->chunk_backend() ? owner->wal_offset_ : 0;
            baseline->pages             = pages;
        }
        auto request                = detail::reserved_shared<PublicationRequest>(p.finish, p.finish);
        request->transaction.record = detail::reserved_shared<InstallRecord>(
            p.finish, InstallToken{.generation = view->generation, .sequence = report.checkpoint_token});
        request->transaction.checkpoint = {.action   = report.durable_committed ? CheckpointAction::Commit
                                                                                : CheckpointAction::Abort,
                                           .token    = report.checkpoint_token,
                                           .baseline = std::move(baseline)};
        if (report.durable_committed) {
            const auto root = view->mapping.get(view->root_page_id);
            if (root && root->primary && root->primary->frame.summary &&
                root->primary->frame.summary->live_counts_known) {
                auto        summary                  = pages->frames.shape();
                const auto &live                     = *root->primary->frame.summary;
                summary.covered_slot                 = view->coverage;
                summary.live_kv                      = live.live_keys;
                summary.live_key_bytes               = live.live_key_bytes;
                summary.live_value_bytes             = live.live_value_bytes;
                summary.live_logical_bytes           = live.live_key_bytes + live.live_value_bytes;
                request->transaction.summary         = summary;
                request->transaction.expected_l1_seq = view->l1_seq;
            }
        }
        request->on_done = [self = shared_from_this()](PublicationResult published) {
            self->finish(published.status.ok() ? self->owner->report_.status : std::move(published.status));
        };
        if (!p.core->publisher().try_submit_checkpoint_completion(request)) {
            finish(Status::unavailable("flush completion publication unavailable"));
        }
    }

    void finish(Status result) noexcept
    {
        owner->report_.status = result;
        owner->finished_      = true;
        owner->pages_.reset();
        owner->inherited_.reset();
        owner->initial_.reset();
        if (result.ok() && owner->report_.durable_committed) {
            checkpoint_detail::schedule_free_blocks(*owner->preparation_->core, owner->preparation_->ticket);
        }
        try {
            done(std::move(result), 0);
        }
        catch (...) {
            CRB_LOG_ERROR("persistent flush completion receiver threw");
        }
    }
};

void FlushPersistence::commit(std::shared_ptr<const PublishedTreeState> view, FlushBatch::MaintenanceComplete done)
try {
    auto commit   = detail::reserved_shared<Commit>(preparation_->control);
    commit->owner = shared_from_this();
    commit->view  = std::move(view);
    commit->done  = done;
    commit->start();
}
catch (const std::bad_alloc &) {
    done(Status::resource_exhausted("flush metadata control reserve exhausted"), 0);
}

size_t FlushPersistence::commit_control_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<Commit>();
}
} // namespace crowdb::tree
