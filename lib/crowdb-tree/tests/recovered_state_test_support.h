// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "inner_load_test_support.h"
#include "load_budget_test_support.h"
#include "snapshot/recovered_state.h"
#include "snapshot/stored_overflow.h"

namespace crowdb::tree::test_read
{
struct TestRecoveredGraph
{
    LoadCredits                                 credits;
    std::shared_ptr<detail::FlushRuntime>       runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    std::shared_ptr<detail::FlushRuntime::Lane> lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<TestInnerStore>             store   = std::make_shared<TestInnerStore>();
    std::shared_ptr<CapturedDurablePages>       pages =
        detail::reserved_shared<CapturedDurablePages>(credits.credits.reservation(), credits.credits.reservation());
    std::shared_ptr<AsyncStorePageLoader> loader = std::make_shared<AsyncStorePageLoader>(
        store, runtime, lane, PageLoadLimits{}, OverflowBackingResolver{}, credits.credits);
    std::shared_ptr<RecoveredStateOperation> operation;

    TestRecoveredGraph()
    {
        pages->root_page_id = 0;
        pages->coverage     = 10;
        pages->generation   = 3;
        pages->backend_id   = 9;
    }

    void add(uint64_t pid, const std::vector<uint8_t> &raw)
    {
        ASSERT_TRUE(frame_validate(raw.data(), raw.size()));
        auto &blob = store->blobs[pid];
        encode_durable_page(raw.data(), raw.size(), compress_algo::kNone, &blob);
        auto backing        = std::make_shared<BackingRef>();
        backing->backend_id = 9;
        backing->address    = pid;
        backing->length     = blob.size();
        backing->owner      = std::make_shared<int>(1);
        DurableCapturedFrame frame{.page_id     = pid,
                                   .type        = frame_page_type(raw.data()),
                                   .frame_bytes = static_cast<uint32_t>(raw.size()),
                                   .backing     = backing};
        if (frame.type == page_type::kLeafBase) {
            ASSERT_TRUE(capture_stored_overflow(LeafFrameView(raw.data(), raw.size()), 4096,
                                                credits.credits.reservation(), &frame.stored_overflow)
                            .ok());
        }
        else if (frame.type == page_type::kOverflowFrame) {
            const OverflowFrameView overflow(raw.data(), raw.size());
            frame.next_overflow  = overflow.next_page_id();
            frame.overflow_bytes = overflow.chunk_len();
        }
        pages->frames.insert_or_assign(pid, std::move(frame));
        pages->next_page_id = std::max(pages->next_page_id, pid + 1);
    }

    void leaf(uint64_t pid, std::initializer_list<Record> records, uint64_t sibling = kInvalidPageId)
    {
        std::vector<uint8_t> raw(4096);
        LeafFrameBuilder     builder(raw.data(), raw.size());
        for (const auto &record : records) {
            const auto cell =
                encode_cell_buf(record.slot, record.tombstone ? OpKind::kDelete : OpKind::kPut, Slice(record.value));
            ASSERT_TRUE(builder.try_append_sorted(record.key, Slice(cell)));
        }
        builder.finish(pid, sibling);
        add(pid, raw);
    }

    void index(uint64_t pid, std::vector<uint64_t> children, std::initializer_list<Slice> keys,
               std::optional<std::pair<uint64_t, uint64_t>> fences = {})
    {
        std::vector<uint8_t> raw(4096);
        ASSERT_TRUE(inner_frame_build(raw.data(), raw.size(), pid, children, std::vector<Slice>(keys)));
        if (fences) {
            frame_set_inner_fence_pages(raw.data(), raw.size(), fences->first, fences->second);
        }
        add(pid, raw);
    }

    void balanced()
    {
        index(0, {1, 2}, {"m"}, std::pair{1, 2});
        leaf(1,
             {
                 {.key = "a", .slot = 2, .value = "one"},
                 {.key = "d", .slot = 3, .value = {}, .tombstone = true}
        },
             2);
        leaf(2, {
                    {.key = "z", .slot = 4, .value = "two"}
        });
    }

    Status prepare(RecoveredStateLimits limits = {})
    {
        return RecoveredStateOperation::create(pages, loader, runtime, lane, limits, credits.credits.reservation(),
                                               &operation);
    }

    void run(Completion *done) const
    {
        auto connected = stdexec::connect(operation->sender(), Receiver{done});
        stdexec::start(connected);
        runtime->drain();
    }

    void complete(detail::CallbackSender sender) const
    {
        Completion done;
        auto       connected = stdexec::connect(sender, Receiver{&done});
        stdexec::start(connected);
        runtime->drain();
        EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
        EXPECT_EQ(done.errors.load(), 0U);
    }
};
} // namespace crowdb::tree::test_read
