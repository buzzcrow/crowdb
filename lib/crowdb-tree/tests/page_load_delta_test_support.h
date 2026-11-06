// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/compressor.h"
#include "published_read_test_support.h"

#include <cstring>

namespace crowdb::tree::test_read
{
class TestDeltaStore final : public AsyncPageStore
{
  public:
    std::vector<uint8_t> blob;
    bool                 deferred = false;
    AsyncCompletion      pending;

    uint64_t submit_read(PageAddr /*address*/, void *destination, size_t length, AsyncCompletion complete) override
    {
        EXPECT_EQ(length, blob.size());
        std::memcpy(destination, blob.data(), std::min(length, blob.size()));
        if (deferred) {
            pending = std::move(complete);
        }
        else {
            complete.complete(Status::Ok());
        }
        return 1;
    }

    uint64_t submit_write(PageAddr /*address*/, const void * /*source*/, size_t /*length*/,
                          AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return 0;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return Status::not_supported();
    }

    void cancel(uint64_t /*id*/) override
    {
    }
};

struct TestDeltaLeaf
{
    std::shared_ptr<detail::FlushRuntime>       runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    std::shared_ptr<detail::FlushRuntime::Lane> lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<TestDeltaStore>             store   = std::make_shared<TestDeltaStore>();
    std::vector<uint8_t>                        frame   = std::vector<uint8_t>(4096);
    BaseRef                                     reference;

    explicit TestDeltaLeaf(bool overflow = false)
    {
        LeafFrameBuilder builder(frame.data(), static_cast<uint32_t>(frame.size()));
        for (const auto &record : std::initializer_list<Record>{
                 {.key = "a", .slot = 1, .value = "old"},
                 {.key = "b", .slot = 4, .value = "base-newer"},
                 {.key = "c", .slot = 2, .value = "deleted"},
                 {.key = "tie", .slot = 7, .value = "base-tie"},
                 {.key = "zero", .slot = 0, .value = {}, .tombstone = true}
        }) {
            const auto cell =
                encode_cell_buf(record.slot, record.tombstone ? OpKind::kDelete : OpKind::kPut, Slice(record.value));
            EXPECT_TRUE(builder.try_append_sorted(record.key, Slice(cell)));
        }
        builder.finish(17, 23);
        std::vector<leaf_entry> deltas;
        for (const auto &record : std::initializer_list<Record>{
                 {.key = "c", .slot = 3, .value = {}, .tombstone = true},
                 {.key = "a", .slot = 6, .value = "delta"},
                 {.key = "new", .slot = 5, .value = "first"},
                 {.key = "new", .slot = 5, .value = "second"},
                 {.key = "b", .slot = 3, .value = "stale"},
                 {.key = "tie", .slot = 7, .value = "delta-tie"},
                 {.key = "new", .slot = 4, .value = "older"}
        }) {
            deltas.push_back({.key  = record.key,
                              .cell = encode_cell_buf(record.slot, record.tombstone ? OpKind::kDelete : OpKind::kPut,
                                                      Slice(record.value))});
        }
        if (overflow) {
            deltas.push_back({.key = "overflow", .cell = encode_overflow_cell_buf(7, 41, 16384)});
        }
        std::vector<uint8_t> output(frame.size());
        EXPECT_TRUE(leaf_frame_append_deltas(frame.data(), static_cast<uint32_t>(frame.size()), deltas, output.data()));
        frame.swap(output);
        auto backing   = std::make_shared<BackingRef>();
        backing->owner = std::make_shared<int>(1);
        reference      = {.identity = 1, .max_slot = 7, .backing = backing};
        encode();
    }

    void encode()
    {
        EXPECT_TRUE(frame_validate(frame.data(), static_cast<uint32_t>(frame.size())));
        encode_durable_page(frame.data(), static_cast<uint32_t>(frame.size()), compress_algo::kNone, &store->blob);
        auto backing      = std::make_shared<BackingRef>(*reference.backing);
        backing->length   = store->blob.size();
        reference.backing = std::move(backing);
    }

    std::shared_ptr<AsyncStorePageLoader> loader(PageLoadLimits limits = {}, PageLoadBudget budget = {})
    {
        return std::make_shared<AsyncStorePageLoader>(store, runtime, lane, limits, OverflowBackingResolver{}, budget);
    }

    void complete(detail::CallbackSender sender) const
    {
        Completion done;
        auto       operation = stdexec::connect(sender, Receiver{&done});
        stdexec::start(operation);
        runtime->drain();
        EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
        EXPECT_EQ(done.errors.load(), 0U);
    }
};
} // namespace crowdb::tree::test_read
