// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/checkpoint_backend.h"
#include "backend/local/extent_writeback.h"
#include "btree/allocation_bound.h"
#include "btree/overflow_backing.h"
#include "btree/writeback_rebase.h"
#include "crowdb-common/log.h"
#include "snapshot/captured_plan.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/primary_backing.h"

#include <cassert>

namespace crowdb::tree
{
struct FlushPersistence::Page : std::enable_shared_from_this<Page>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<Page> owner;

        void set_value() noexcept
        {
            owner->resume(Status::Ok());
        }

        void set_error(Status error) noexcept
        {
            owner->resume(std::move(error));
        }

        void set_stopped() noexcept
        {
            owner->resume(Status::unavailable());
        }
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<Page> page)
            : connected(stdexec::connect(sender, Receiver{std::move(page)}))
        {
        }

        Connected connected;
    };

    std::shared_ptr<FlushPersistence>             owner;
    PageEncodeInput                               input;
    FlushBatchOptions::ReserveOverflow            reserve;
    FlushBatchOptions::PersistComplete            done;
    CheckpointWork                                work;
    CapturedWriteLimits                           limits;
    std::shared_ptr<CapturedFrameStream>          stream;
    detail::ReservedUnique<CapturedLayoutPlanner> planner;
    std::shared_ptr<CapturedWriteOperation>       write;
    std::optional<Attempt>                        attempt;
    enum class Stage : uint8_t { Plan, Dependencies, Write };
    Stage                 stage = Stage::Plan;
    Status                status;
    ApplyDispatcher::Node node{&Page::run, this};

    void start()
    {
        auto &preparation              = *owner->preparation_;
        limits                         = preparation.options.write;
        limits.frames.max_depth        = 1;
        limits.frames.reuse_backend_id = preparation.core->local_backend()
                                           ? preparation.core->local_backend()->ledger->limits().backend_id
                                           : preparation.core->chunk_backend()->backend_identity();
        auto result                    = preparation.work(*owner->initial_, nullptr, &work, 0, owner->io_);
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        limits.frames.encode.metadata = work.budget.bytes;
        limits.frames.cpu_progress    = work.budget.cpu_progress;
        limits.frames.plan_only       = true;
        planner                       = detail::reserved_unique<CapturedLayoutPlanner>(
            work.budget.bytes, input.next_page_id, limits.frames.reuse_backend_id, limits,
            preparation.options.metadata.max_segments, owner->inherited_, work.budget.bytes);
        result = CapturedFrameStream::create(input, work.loader, preparation.core->binding()->cpu(),
                                             preparation.core->binding()->lane(), limits.frames, {}, &stream);
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        begin(stream->sender());
    }

    void begin(detail::CallbackSender sender)
    {
        attempt.emplace(sender, shared_from_this());
        stdexec::start(attempt->connected);
    }

    void resume(Status result) noexcept
    {
        status = std::move(result);
        owner->preparation_->cpu_progress->request(node, weak_from_this().lock());
    }

    static void run(void *raw) noexcept
    {
        auto self = static_cast<Page *>(raw)->weak_from_this().lock();
        assert(self);
        try {
            self->step();
        }
        catch (const std::bad_alloc &) {
            self->finish(Status::resource_exhausted("flush page reservation exhausted"));
        }
        catch (...) {
            self->finish(Status::internal_error("flush page persistence failed"));
        }
    }

    void step()
    {
        attempt.reset();
        if (!status.ok()) {
            finish(status);
            return;
        }
        if (stage == Stage::Write) {
            written();
            return;
        }
        if (stage == Stage::Plan) {
            if (!stream->result().plan) {
                finish(Status::corruption("final page produced no layout"));
                return;
            }
            auto result = planner->accept(*stream->result().plan);
            if (!result.ok()) {
                finish(std::move(result));
                return;
            }
            stage = Stage::Dependencies;
        }
        auto result = planner->step();
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        if (planner->pending()) {
            resume(Status::Ok());
            return;
        }
        result = planner->finish();
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        auto plan = planner->result();
        stream.reset();
        planner.reset();
        work                = {};
        auto  &preparation  = *owner->preparation_;
        size_t old_segments = 1;
        if (input.page->primary && input.page->primary->frame.backing && input.page->primary->frame.backing->overflow) {
            uint64_t previous = UINT64_MAX;
            for (const auto &[pid, frame] : input.page->primary->frame.backing->overflow->pages) {
                (void)frame;
                if (pid / kMappingSegmentSize != previous) {
                    ++old_segments;
                    previous = pid / kMappingSegmentSize;
                }
            }
        }
        const auto inherited_segments = owner->inherited_ ? owner->inherited_->frames.segment_count() : 0;
        const auto extra              = ReadablePageFrames::segment_allocation_bound();
        auto       bound =
            detail::AllocationBound()
                .add_product(
                    plan.segments + old_segments +
                        (owner->inherited_ && !owner->inherited_->overflow_counts_known ? inherited_segments : 0),
                    extra)
                .add_product(inherited_segments + plan.segments + 1, 128)
                .add_product(plan.frame_count_upper, 256)
                .add(detail::shared_allocation_bound<CapturedDurablePages>())
                .value();
        if (!bound) {
            finish(Status::resource_exhausted("durable mapping segment bound overflow"));
            return;
        }
        result = preparation.work(*owner->initial_, &plan, &work, *bound, owner->io_);
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        limits.frames.encode.metadata = work.budget.bytes;
        limits.frames.plan_only       = false;
        result = CapturedFrameStream::create(input, work.loader, preparation.core->binding()->cpu(),
                                             preparation.core->binding()->lane(), limits.frames, reserve, &stream);
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        auto pages          = detail::reserved_shared<CapturedDurablePages>(work.budget.bytes, work.budget.bytes);
        pages->coverage     = input.coverage;
        pages->generation   = owner->initial_->generation;
        pages->root_page_id = input.page_id;
        pages->next_page_id = input.next_page_id;
        auto chunk          = preparation.core->chunk_backend();
        result              = CapturedWriteOperation::create(
            stream, pages, preparation.ticket->backend(preparation.core->async_backend()),
            preparation.core->binding()->cpu(), preparation.core->binding()->lane(), limits, work.budget,
            chunk ? chunk->reserve(work.budget.bytes)
                  : extent_write_reservation(preparation.core->local_backend()->ledger, work.budget.bytes),
            owner->pages_ ? owner->pages_ : owner->inherited_, &write);
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        stage = Stage::Write;
        begin(write->sender());
    }

    void written()
    {
        const auto &result = write->result();
        const auto  found  = result.pages->frames.find(input.page_id);
        if (found == result.pages->frames.end()) {
            finish(Status::corruption("flush page has no durable receipt"));
            return;
        }
        auto receipt = found->second;
        if (input.children_summary && receipt.summary) {
            receipt.summary->oldest_tombstone_slot = input.children_summary->oldest_tombstone_slot;
            receipt.summary->live_counts_known     = input.children_summary->live_counts_known;
            receipt.summary->live_keys             = input.children_summary->live_keys;
            receipt.summary->live_key_bytes        = input.children_summary->live_key_bytes;
            receipt.summary->live_value_bytes      = input.children_summary->live_value_bytes;
        }
        std::shared_ptr<const PublishedPage> replacement;
        auto status = build_writeback_rebase(input.page, input.page, receipt, input.coverage, work.budget.bytes,
                                             &replacement, limits.frames.encode.max_sources, input.gc_floor);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        // The catalog receives only final page receipts and their protected raw
        // dependencies. Structural deletions are applied at the root join.
        if (!owner->pages_) {
            owner->pages_ = detail::reserved_shared<CapturedDurablePages>(work.budget.bytes, work.budget.bytes);
            if (owner->inherited_) {
                owner->pages_->frames.inherit(owner->inherited_->frames);
                owner->pages_->overflow_counts_known  = owner->inherited_->overflow_counts_known;
                owner->pages_->stored_reference_count = owner->inherited_->stored_reference_count;
            }
            auto initialized = owner->pages_->initialize_overflow_counts();
            if (!initialized.ok()) {
                finish(std::move(initialized));
                return;
            }
        }
        owner->pages_->frames.budget(work.budget.bytes);
        status = owner->pages_->replace_primary(*result.pages);
        if (status.ok()) {
            owner->pages_->frames.at(input.page_id).summary = receipt.summary;
        }
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        owner->report_.written_frames += result.written_frames;
        owner->report_.written_bytes += result.written_bytes;
        owner->report_.reused_frames += result.reused_frames;
        ++owner->report_.primary_frames;
        ++owner->report_.rebased_pages;
        done(Status::Ok(), std::move(replacement));
    }

    void finish(Status result) noexcept
    {
        if (!result.ok()) {
            CRB_LOG_ERROR("persistent page {} failed: {}", input.page_id, result.to_string());
        }
        try {
            done(std::move(result), {});
        }
        catch (...) {
            CRB_LOG_ERROR("persistent page receiver threw");
        }
    }
};

void FlushPersistence::write(PageEncodeInput input, FlushBatchOptions::ReserveOverflow reserve,
                             FlushBatchOptions::PersistComplete done)
try {
    const auto  backend = preparation_->core->local_backend()
                            ? preparation_->core->local_backend()->ledger->limits().backend_id
                            : preparation_->core->chunk_backend()->backend_identity();
    const auto &limits  = preparation_->options.write.frames;
    const bool  same_summary =
        !input.children_summary ||
        (input.page->primary && input.page->primary->frame.summary &&
         input.page->primary->frame.summary->live_counts_known == input.children_summary->live_counts_known &&
         input.page->primary->frame.summary->live_keys == input.children_summary->live_keys &&
         input.page->primary->frame.summary->live_key_bytes == input.children_summary->live_key_bytes &&
         input.page->primary->frame.summary->live_value_bytes == input.children_summary->live_value_bytes &&
         input.page->primary->frame.summary->oldest_tombstone_slot == input.children_summary->oldest_tombstone_slot);
    if (same_summary && limits.exclusion.length == 0 && inherited_ && input.page->primary &&
        inherited_->frames.contains(input.page_id) &&
        input.page->primary->frame.backing == inherited_->frames.at(input.page_id).backing &&
        !limits.exclusion.overlaps(*inherited_->frames.at(input.page_id).backing) &&
        (input.page->leaf ? can_reuse_primary(*input.page, input.page_id, input.context.range, input.gc_floor, backend,
                                              limits.encode.frame_bytes)
                          : input.context.inner_fences &&
                                can_reuse_inner_primary(*input.page, input.page_id, *input.context.inner_fences,
                                                        backend, limits.encode.frame_bytes))) {
        done(Status::Ok(), input.page);
        return;
    }
    auto page     = detail::reserved_shared<Page>(preparation_->control);
    page->owner   = shared_from_this();
    page->input   = std::move(input);
    page->reserve = std::move(reserve);
    page->done    = done;
    page->start();
}
catch (const std::bad_alloc &) {
    done(Status::resource_exhausted("flush page control reserve exhausted"), {});
}

size_t FlushPersistence::control_allocation_bound(size_t workers) noexcept
{
    return detail::shared_allocation_bound<FlushPersistence>() + commit_control_allocation_bound() +
           ((workers + 2) * detail::shared_allocation_bound<Page>());
}
} // namespace crowdb::tree
