// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_writeback.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "load_budget_test_support.h"
#include "published_read_test_support.h"

#include <stdexcept>

namespace crowdb::tree::test_write
{
using namespace test_read;

class TestWriteStore final : public AsyncPageStore
{
  public:
    std::shared_ptr<PageStore> device              = std::make_shared<MemPageStore>(4096);
    bool                       reloadable          = true;
    bool                       deferred            = false;
    bool                       throw_after_capture = false;
    size_t                     writes              = 0;
    PagePurpose                purpose             = PagePurpose::kInvalid;
    uint64_t                   address             = 0;
    const uint8_t             *data                = nullptr;
    size_t                     bytes               = 0;
    AsyncCompletion            pending;

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return reloadable;
    }

    uint64_t submit_read(PageAddr addr, void *out, size_t size, AsyncCompletion complete) override
    {
        complete.complete(device->read_at(addr, static_cast<uint8_t *>(out), size));
        return 0;
    }

    uint64_t submit_typed_write(PagePurpose kind, PageAddr addr, const void *input, size_t size,
                                AsyncCompletion complete) override
    {
        purpose = kind;
        return submit_write(addr, input, size, std::move(complete));
    }

    uint64_t submit_write(PageAddr addr, const void *input, size_t size, AsyncCompletion complete) override
    {
        ++writes;
        address = addr;
        data    = static_cast<const uint8_t *>(input);
        bytes   = size;
        EXPECT_EQ(reinterpret_cast<uintptr_t>(input) % 4096, 0U);
        EXPECT_EQ(size % 4096, 0U);
        pending = std::move(complete);
        if (throw_after_capture) {
            throw std::runtime_error("unknown transport acceptance");
        }
        if (!deferred) {
            finish();
        }
        return writes;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        auto status = device->sync();
        complete.complete(status);
        return status;
    }

    void cancel(uint64_t /*id*/) override
    {
    }

    void finish(Status status = Status::Ok())
    {
        auto complete = std::exchange(pending, {});
        ASSERT_TRUE(complete);
        if (status.ok()) {
            status = device->write_at(address, data, bytes);
        }
        complete.complete(std::move(status));
    }
};

inline PageWriteInput write_input(size_t bytes = 4096, bool overflow = false)
{
    auto             owner = std::make_shared<std::vector<uint8_t>>(bytes);
    LeafFrameBuilder builder(owner->data(), bytes);
    const auto       cell = overflow ? encode_overflow_cell(3, 11, 1000) : encode_cell(3, OpKind::kPut, "value");
    EXPECT_TRUE(builder.try_append_sorted("a", Slice(cell)));
    builder.finish(7, kInvalidPageId);
    return {
        .info                = {.page_id = 7, .type = page_type::kLeafBase, .bytes = static_cast<uint32_t>(bytes)},
        .frame               = *owner,
        .owner               = owner,
        .reload_dependencies = {}
    };
}

struct TestWriter
{
    LoadCredits                                 credits;
    std::shared_ptr<detail::FlushRuntime>       runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    std::shared_ptr<detail::FlushRuntime::Lane> lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<TestWriteStore>             store   = std::make_shared<TestWriteStore>();
    std::shared_ptr<PageWriteOperation>         operation;
    std::shared_ptr<int>                        extent       = std::make_shared<int>(1);
    uint64_t                                    next_address = 8192;

    Status prepare(PageWriteInput input = write_input(), PageWriteLimits limits = {})
    {
        return PageWriteOperation::create(
            store, runtime, lane, std::move(input), limits, credits.credits,
            [this](uint64_t length, PageWriteOperation::ExtentComplete done) {
                ReservedWriteExtent reserved{.backend_id       = 1,
                                             .address          = next_address,
                                             .length           = length,
                                             .content_id       = 10,
                                             .generation       = 2,
                                             .owner            = extent,
                                             .physical_metrics = {},
                                             .chunk            = {}};
                next_address += length;
                done(Status::Ok(), std::move(reserved));
            },
            &operation);
    }
};

} // namespace crowdb::tree::test_write
