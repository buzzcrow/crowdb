// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/published_route.h"
#include "crowdb-tree/maptable/compressor.h"
#include "published_read_test_support.h"

#include <cstring>

namespace crowdb::tree::test_read
{
class TestInnerStore final : public AsyncPageStore
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

inline std::shared_ptr<InnerPageState> inner(std::vector<std::string> separators, std::vector<uint64_t> children)
{
    auto result = std::make_shared<InnerPageState>();
    result->separators.assign(separators.begin(), separators.end());
    result->children.assign(children.begin(), children.end());
    return result;
}

inline std::shared_ptr<const BackingRef> inner_backing(const std::shared_ptr<TestInnerStore> &store, uint64_t id,
                                                       const InnerPageState &state)
{
    std::vector<std::string> separators;
    for (const auto &key : state.separators) {
        separators.push_back(Slice(key).to_string());
    }
    const std::vector<uint64_t>      children(state.children.begin(), state.children.end());
    const std::unique_ptr<InnerBase> frame(InnerBase::build(separators, children));
    auto                            &blob = store->blobs[id];
    encode_durable_page(frame->frame(), frame->page_bytes(), compress_algo::kNone, &blob);
    auto backing     = std::make_shared<BackingRef>();
    backing->address = id;
    backing->length  = blob.size();
    backing->owner   = std::make_shared<int>(1);
    return backing;
}

class TestInnerLoader final : public PageLoader
{
  public:
    bool                                                      deferred = false;
    size_t                                                    calls    = 0;
    std::map<uint64_t, std::shared_ptr<const InnerPageState>> states;
    std::deque<InnerComplete>                                 pending;
    std::deque<uint64_t>                                      ids;

    void load_leaf(BaseRef /*base*/, LeafComplete complete) override
    {
        complete(Status::not_supported(), {});
    }

    void load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete) override
    {
        ++calls;
        if (deferred) {
            pending.push_back(std::move(complete));
            ids.push_back(backing->address);
        }
        else {
            complete(Status::Ok(), states.at(backing->address));
        }
    }

    void finish(Status status = Status::Ok())
    {
        ASSERT_FALSE(pending.empty());
        auto       callback = std::move(pending.front());
        const auto id       = ids.front();
        pending.pop_front();
        ids.pop_front();
        callback(std::move(status), states.at(id));
    }
};

struct TestColdTree
{
    std::shared_ptr<TestInnerStore>     store  = std::make_shared<TestInnerStore>();
    std::shared_ptr<TestInnerLoader>    loader = std::make_shared<TestInnerLoader>();
    std::shared_ptr<PublishedTreeState> view   = std::make_shared<PublishedTreeState>();

    TestColdTree()
    {
        loader->states[0] = inner({"m"}, {1, 2});
        loader->states[1] = inner({"g"}, {3, 4});
        loader->states[2] = inner({"t"}, {5, 6});
        std::vector<PageReplacement> changes;
        changes.reserve(7);
        for (uint64_t id = 0; id < 3; ++id) {
            auto index           = std::make_shared<PublishedPage>();
            index->level         = id == 0 ? 2 : 1;
            index->inner_backing = inner_backing(store, id, *loader->states.at(id));
            changes.push_back({.page_id = id, .expected = {}, .replacement = std::move(index)});
        }
        changes.push_back({
            .page_id  = 3,
            .expected = {},
            .replacement =
                published(page({{.key = "", .value = "empty"}, {.key = "a", .value = "a"}},
              {.upper = "g"}
              ))
        });
        changes.push_back({
            .page_id     = 4,
            .expected    = {},
            .replacement = published(
                page({{.key = "g", .value = "g"}, {.key = "l", .value = "l"}},
                 {.lower = "g", .upper = "m"}
                 ))
        });
        changes.push_back({
            .page_id     = 5,
            .expected    = {},
            .replacement = published(
                page({{.key = "m", .value = "m"}, {.key = "n", .value = "n"}},
                 {.lower = "m", .upper = "t"}
                 ))
        });
        changes.push_back({
            .page_id     = 6,
            .expected    = {},
            .replacement = published(page({{.key = "t", .value = "t"}, {.key = "z", .value = "z"}},
                 {.lower = "t"}
                 ))
        });
        MappingView::CopyStats copied;
        view->mapping      = view->mapping.replace(changes, copied);
        view->root_page_id = 0;
    }
};
} // namespace crowdb::tree::test_read
