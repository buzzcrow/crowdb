// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/compressor.h"
#include "published_read_test_support.h"

#include <cstring>

namespace crowdb::tree::test_read
{
class TestOverflowStore final : public AsyncPageStore
{
  public:
    std::map<uint64_t, std::vector<uint8_t>> blobs;
    std::deque<AsyncCompletion>              pending;
    std::atomic<size_t>                      reads{0};
    bool                                     deferred = false;

    uint64_t submit_read(PageAddr address, void *destination, size_t length, AsyncCompletion complete) override
    {
        const auto &blob = blobs.at(address);
        EXPECT_EQ(length, blob.size());
        std::memcpy(destination, blob.data(), std::min(length, blob.size()));
        const uint64_t id = reads.fetch_add(1) + 1;
        if (deferred) {
            pending.push_back(complete);
        }
        else {
            complete.complete(Status::Ok());
        }
        return id;
    }

    uint64_t submit_write(PageAddr /*addr*/, const void * /*buf*/, size_t /*len*/, AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return 0;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return Status::not_supported();
    }

    void cancel(uint64_t /*op_id*/) override
    {
    }

    void finish(Status status = Status::Ok())
    {
        ASSERT_FALSE(pending.empty());
        const auto callback = pending.front();
        pending.pop_front();
        callback.complete(std::move(status));
    }
};

using TestBackingMap = std::map<uint64_t, std::shared_ptr<const BackingRef>>;

inline void add_overflow(const std::shared_ptr<TestOverflowStore> &store, TestBackingMap *captured, uint64_t id,
                         uint64_t next, Slice value)
{
    const std::unique_ptr<OverflowBase> frame(OverflowBase::build(next, value.bytes(), value.size()));
    std::vector<uint8_t>                raw(frame->frame(), frame->frame() + frame->page_bytes());
    frame_put_u64(raw.data(), fh::kSelfpage_id, id);
    frame_restamp_crc(raw.data(), raw.size());
    auto &blob = store->blobs[id];
    encode_durable_page(raw.data(), raw.size(), compress_algo::kNone, &blob);
    auto backing     = std::make_shared<BackingRef>();
    backing->address = id;
    backing->length  = blob.size();
    backing->owner   = std::make_shared<int>(1);
    captured->emplace(id, std::move(backing));
}

inline OverflowBackingResolver resolver(std::shared_ptr<const TestBackingMap> captured)
{
    return [captured = std::move(captured)](uint64_t id, std::shared_ptr<const BackingRef> *out) {
        const auto at = captured->find(id);
        if (at == captured->end()) {
            return Status::not_found("captured overflow PID absent");
        }
        *out = at->second;
        return Status::Ok();
    };
}

inline std::shared_ptr<const PageState> overflow_leaf()
{
    auto           result = std::make_shared<PageState>();
    OverlayBuilder builder({});
    auto           owner = std::make_shared<CellVersion>(encode_overflow_cell_buf(5, 1, 10), 5, kFlagOverflow);
    EXPECT_TRUE(builder.append("key", std::move(owner)).ok());
    std::shared_ptr<const OverlayBatch> overlay;
    EXPECT_TRUE(builder.finish(1, &overlay).ok());
    result->overlays.push_back(std::move(overlay));
    return result;
}
} // namespace crowdb::tree::test_read
