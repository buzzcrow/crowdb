// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "local_checkpoint_test_support.h"
#include "snapshot/local_recovery.h"

namespace crowdb::tree::test_write
{
class TestRecoveryStore final : public AsyncPageStore
{
  public:
    std::shared_ptr<PageStore> device;
    size_t                     reads           = 0;
    size_t                     defer_at        = 0;
    size_t                     fail_at         = 0;
    size_t                     throw_at        = 0;
    size_t                     inline_throw_at = 0;
    AsyncCompletion            pending;
    AsyncCompletion            retained_completion;
    bool                       retain_completed = false;
    PageAddr                   address          = 0;
    uint8_t                   *output           = nullptr;
    size_t                     bytes            = 0;

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

    uint64_t submit_read(PageAddr at, void *out, size_t size, AsyncCompletion callback) override
    {
        ++reads;
        EXPECT_FALSE(pending);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(out) % 4096, 0U);
        address = at;
        output  = static_cast<uint8_t *>(out);
        bytes   = size;
        pending = std::move(callback);
        if (reads == throw_at) {
            throw std::runtime_error("unknown read acceptance");
        }
        if (reads != defer_at) {
            finish();
        }
        if (reads == inline_throw_at) {
            throw std::runtime_error("read completed before submission failure");
        }
        return reads;
    }

    uint64_t submit_write(PageAddr at, const void *data, size_t size, AsyncCompletion callback) override
    {
        callback.complete(device->write_at(at, static_cast<const uint8_t *>(data), size));
        return 0;
    }

    Status submit_fsync(AsyncCompletion callback) override
    {
        auto status = device->sync();
        callback.complete(status);
        return status;
    }

    void cancel(uint64_t /*id*/) override
    {
    }

    void finish()
    {
        auto callback = std::exchange(pending, {});
        ASSERT_TRUE(callback);
        if (retain_completed) {
            retained_completion = callback;
        }
        callback.complete(reads == fail_at ? Status::io_error() : device->read_at(address, output, bytes));
    }
};

struct TestRecovery
{
    TestCheckpoint                                    original;
    LoadCredits                                       credits;
    std::shared_ptr<TestRecoveryStore>                store = std::make_shared<TestRecoveryStore>();
    std::shared_ptr<LocalRecoveryOperation>           operation;
    snapshot::CommitAnchor                            anchor;
    std::map<uint64_t, std::pair<uint64_t, uint64_t>> locations;

    TestRecovery()
    {
        store->device = original.store->device;
    }

    void checkpoint(std::string value = "v")
    {
        ASSERT_TRUE(original.prepare(original.materialize(std::move(value))).ok());
        Completion done;
        original.run(&done);
        ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
        const auto image = original.operation->result().checkpoint;
        anchor           = image->anchor;
        locations.clear();
        for (const auto &[pid, frame] : image->pages->frames) {
            locations.emplace(pid, std::pair(frame.backing->address, frame.backing->length));
        }
    }

    Status prepare(LocalRecoveryLimits limits = {})
    {
        // Simulate process ownership ending before opening a new allocation
        // incarnation. Only durable bytes and plain locations survive.
        original.operation.reset();
        original.source.operation.reset();
        original.registry.reset();
        original.source.ledger.reset();
        return LocalRecoveryOperation::create(store, original.source.runtime, original.source.lane,
                                              {.backend_id = 9, .generation = 2, .file_size = store->device->size()},
                                              limits, credits.credits, &operation);
    }

    void run(Completion *done)
    {
        auto connected = stdexec::connect(operation->sender(), Receiver{.done = done});
        stdexec::start(connected);
        original.source.runtime->drain();
    }
};
} // namespace crowdb::tree::test_write
