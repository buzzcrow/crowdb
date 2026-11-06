// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/published_read.h"

#include <gtest/gtest.h>

#include <atomic>
#include <deque>
#include <map>

namespace crowdb::tree::test_read
{
struct Record
{
    std::string key;
    uint64_t    slot = 1;
    std::string value;
    bool        tombstone = false;
};

inline std::shared_ptr<const PageState> page(std::initializer_list<Record> records, PageKeyRange range = {})
{
    auto result   = std::make_shared<PageState>();
    result->range = std::move(range);
    OverlayBuilder builder({});
    for (const auto &record : records) {
        const auto kind = record.tombstone ? OpKind::kDelete : OpKind::kPut;
        auto owner = std::make_shared<CellVersion>(encode_cell_buf(record.slot, kind, Slice(record.value)), record.slot,
                                                   record.tombstone ? kFlagTombstone : 0);
        EXPECT_TRUE(builder.append(Slice(record.key), std::move(owner)).ok());
    }
    std::shared_ptr<const OverlayBatch> overlay;
    EXPECT_TRUE(builder.finish(1, &overlay).ok());
    result->overlays.push_back(std::move(overlay));
    return result;
}

inline std::shared_ptr<const LeafBase> base(std::initializer_list<Record> records)
{
    std::vector<leaf_entry> entries;
    entries.reserve(records.size());
    for (const auto &record : records) {
        entries.push_back({.key  = record.key,
                           .cell = encode_cell_buf(record.slot, record.tombstone ? OpKind::kDelete : OpKind::kPut,
                                                   Slice(record.value))});
    }
    return std::shared_ptr<const LeafBase>(LeafBase::build(entries));
}

inline std::shared_ptr<PublishedPage> published(std::shared_ptr<const PageState> leaf)
{
    auto result           = std::make_shared<PublishedPage>();
    result->leaf          = std::move(leaf);
    result->metrics.dirty = true;
    return result;
}

inline std::shared_ptr<const PublishedTreeState>
view(std::shared_ptr<const PageState> leaf, std::vector<std::shared_ptr<MemTable>> tables = {}, uint64_t floor = 0)
{
    auto result  = std::make_shared<PublishedTreeState>();
    auto sources = std::make_shared<PublishedSources>();
    sources->tables.assign(tables.begin(), tables.end());
    sources->floor       = floor;
    result->sources      = std::move(sources);
    result->coverage     = floor;
    result->root_page_id = 0;
    MappingView::CopyStats               copied;
    const std::array<PageReplacement, 1> changes{
        {{.page_id = 0, .expected = {}, .replacement = published(std::move(leaf))}}};
    result->mapping = result->mapping.replace(changes, copied);
    return result;
}

inline BaseRef cold(uint64_t identity, std::optional<uint64_t> max_slot = std::nullopt)
{
    auto backing    = std::make_shared<BackingRef>();
    backing->length = 4096;
    backing->owner  = std::make_shared<int>(1);
    return {.identity = identity, .max_slot = max_slot, .backing = std::move(backing)};
}

class TestLoader final : public PageLoader
{
  public:
    bool                                                deferred = false;
    size_t                                              calls    = 0;
    std::map<uint64_t, std::shared_ptr<const LeafBase>> frames;
    std::deque<LeafComplete>                            pending;
    std::deque<uint64_t>                                identities;

    void load_leaf(BaseRef requested, LeafComplete complete) override
    {
        ++calls;
        if (deferred) {
            pending.push_back(std::move(complete));
            identities.push_back(requested.identity);
        }
        else {
            complete(Status::Ok(), frames.at(requested.identity));
        }
    }

    void finish(Status status = Status::Ok())
    {
        ASSERT_FALSE(pending.empty());
        auto       complete = std::move(pending.front());
        const auto id       = identities.front();
        pending.pop_front();
        identities.pop_front();
        complete(std::move(status), frames.at(id));
    }
};

struct Completion
{
    std::atomic<size_t> values{0};
    std::atomic<size_t> errors{0};
    std::atomic<size_t> stopped{0};
    Status              status;
};

struct Receiver
{
    using receiver_concept = stdexec::receiver_tag;
    Completion *done;

    void set_value() const noexcept
    {
        done->values.fetch_add(1);
    }

    void set_error(Status status) const noexcept
    {
        done->status = std::move(status);
        done->errors.fetch_add(1);
    }

    void set_stopped() const noexcept
    {
        done->stopped.fetch_add(1);
    }
};

inline void complete_inline(detail::CallbackSender sender)
{
    Completion done;
    auto       operation = stdexec::connect(sender, Receiver{.done = &done});
    stdexec::start(operation);
    EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_EQ(done.errors.load(), 0U);
}

inline std::vector<PublishedScanEntry> collect(const std::shared_ptr<PublishedScan> &scan)
{
    std::vector<PublishedScanEntry> result;
    for (size_t window = 0; window < 1000; ++window) {
        complete_inline(scan->next_sender());
        if (!scan->result().status.ok()) {
            return result;
        }
        result.insert(result.end(), scan->result().entries.begin(), scan->result().entries.end());
        if (scan->result().done) {
            return result;
        }
    }
    ADD_FAILURE() << "bounded scan did not terminate";
    return result;
}
} // namespace crowdb::tree::test_read
