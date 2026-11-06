// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/local/extent_writeback.h"
#include "extent_ledger_test_support.h"
#include "page_writeback_test_support.h"
#include "snapshot/captured_write.h"

namespace crowdb::tree::test_write
{
inline std::shared_ptr<PublishedTreeState> captured_leaf(std::shared_ptr<const PageState> leaf)
{
    auto result          = std::make_shared<PublishedTreeState>(*view(std::move(leaf), {}, 10));
    result->next_page_id = 1;
    return result;
}

struct TestCapturedWrite
{
    LoadCredits                                 credits;
    std::shared_ptr<detail::FlushRuntime>       runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    std::shared_ptr<detail::FlushRuntime::Lane> lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<TestWriteStore>             store   = std::make_shared<TestWriteStore>();
    std::shared_ptr<ExtentLedger>               ledger;
    std::shared_ptr<CapturedWriteOperation>     operation;
    uint64_t                                    next_overflow = 1;

    TestCapturedWrite()
    {
        EXPECT_TRUE(ExtentLedger::create({.backend_id = 9}, credits.credits.reservation(), &ledger).ok());
        EXPECT_TRUE(await_ledger([&](auto done) {
                        return ledger->activate(credits.credits.reservation(), std::move(done));
                    }).status.ok());
    }

    Status prepare(std::shared_ptr<const PublishedTreeState>   captured,
                   std::shared_ptr<const CapturedDurablePages> inherited = {},
                   CapturedWriteLimits limits = {.frames = {.encode = {.frame_bytes = 512, .inline_value_bytes = 16}},
                                                 .write  = {.max_frame_bytes = 512}})
    {
        next_overflow = captured->next_page_id;
        return CapturedWriteOperation::create(
            std::move(captured), {}, store, runtime, lane, std::move(limits), credits.credits,
            [this](uint64_t count, ReservedOverflowIds *out) {
                *out = {.first = next_overflow, .count = count, .owner = ledger};
                next_overflow += count;
                return Status::Ok();
            },
            extent_write_reservation(ledger, credits.credits.reservation()), std::move(inherited), &operation);
    }

    std::vector<uint8_t> read(const DurableCapturedFrame &frame)
    {
        std::vector<uint8_t> blob(frame.backing->length);
        std::vector<uint8_t> raw(frame.frame_bytes);
        EXPECT_TRUE(store->device->read_at(frame.backing->address, blob.data(), blob.size()).ok());
        EXPECT_TRUE(decode_durable_page(blob.data(), blob.size(), raw.data(), raw.size()).ok());
        EXPECT_TRUE(frame_validate(raw.data(), raw.size()));
        EXPECT_EQ(frame_u64(raw.data(), fh::kSelfpage_id), frame.page_id);
        return raw;
    }

    void run(Completion *done)
    {
        auto connected = stdexec::connect(operation->sender(), Receiver{.done = done});
        stdexec::start(connected);
        runtime->drain();
    }
};
} // namespace crowdb::tree::test_write
