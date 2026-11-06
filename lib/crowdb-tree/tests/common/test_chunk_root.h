// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "common/test_root_transport.h"
#include "load_budget_test_support.h"
#include "snapshot/chunk_root_commit.h"

#include <map>

namespace crowdb::tree::test_chunk
{
struct TestRootBackend
{
    test_read::LoadCredits                   credits;
    std::shared_ptr<TestRootCatalog>         catalog = std::make_shared<TestRootCatalog>(credits.credits.reservation());
    std::shared_ptr<TestRootTransport>       transport = std::make_shared<TestRootTransport>(catalog);
    std::shared_ptr<ChunkBackingDomain>      domain    = std::make_shared<ChunkBackingDomain>();
    std::shared_ptr<detail::ChunkRuntimeIds> ids =
        std::make_shared<detail::ChunkRuntimeIds>(1, 23, uint64_t{45} << 32U);

    explicit TestRootBackend(size_t pin_capacity = 256)
    {
        EXPECT_TRUE(detail::ChunkPinReleaseQueue::create(
                        credits.credits.reservation(), pin_capacity, std::make_shared<int>(1), [](void *) noexcept {},
                        nullptr, &catalog->queue)
                        .ok());
        domain->backend_id = 17;
        domain->tree       = 19;
        domain->epoch      = 1;
        domain->mirrors    = 2;
        domain->catalog    = catalog;
        domain->transport  = transport;
    }

    ~TestRootBackend()
    {
        catalog->queue->stop_admission();
        (void)catalog->queue->pump(catalog->queue->snapshot().pending);
        EXPECT_TRUE(catalog->queue->drained());
        catalog->records.clear();
    }

    std::shared_ptr<CapturedDurablePages> pages(size_t count)
    {
        auto pages =
            detail::reserved_shared<CapturedDurablePages>(credits.credits.reservation(), credits.credits.reservation());
        pages->generation   = 3;
        pages->coverage     = 15;
        pages->root_page_id = count != 0 ? 0 : kInvalidPageId;
        pages->next_page_id = count;
        pages->backend_id   = 17;
        for (uint64_t i = 0; i < count; ++i) {
            detail::ChunkStageId stage;
            detail::ChunkId      chunk;
            EXPECT_TRUE(ids->next(PagePurpose::kBtreePage, &stage, &chunk).ok());
            std::shared_ptr<ChunkPackBacking> pack;
            EXPECT_TRUE(prepare_chunk_backing(domain, stage, chunk, 8192, PagePurpose::kBtreePage,
                                              credits.credits.reservation(), &pack)
                            .ok());
            std::array<uint8_t, 8192> raw{};
            EXPECT_TRUE(write_chunk_backing(*pack, raw, credits.credits.reservation()).ok());
            auto backing            = detail::reserved_shared<BackingRef>(credits.credits.reservation());
            backing->backend_id     = 17;
            backing->length         = 8192;
            backing->owner          = pack;
            backing->physical_owner = pack;
            backing->storage        = BackingStorage::kChunkPack;
            backing->chunk          = std::move(pack);
            auto refs               = detail::reserved_shared<StoredOverflowReferences>(
                credits.credits.reservation(),
                detail::ReservationAllocator<StoredOverflowReference>(credits.credits.reservation()));
            pages->frames.emplace(i, DurableCapturedFrame{.page_id         = i,
                                                          .type            = page_type::kLeafBase,
                                                          .frame_bytes     = 4096,
                                                          .logical_bytes   = 4132,
                                                          .next_overflow   = kInvalidPageId,
                                                          .overflow_bytes  = 0,
                                                          .backing         = std::move(backing),
                                                          .stored_overflow = std::move(refs),
                                                          .summary         = {}});
        }
        return pages;
    }

    [[nodiscard]] std::shared_ptr<ChunkRootCommit>
    commit(const std::shared_ptr<const CapturedDurablePages> &pages) const
    {
        ChunkRootIdentity                identity{.tree              = 19,
                                                  .root_generation   = 7,
                                                  .owner_epoch       = 1,
                                                  .snapshot_sequence = 4,
                                                  .page_generation   = pages->generation,
                                                  .coverage          = pages->coverage,
                                                  .gc_floor          = 8,
                                                  .root_page_id      = pages->root_page_id,
                                                  .next_page_id      = pages->next_page_id,
                                                  .wal_replay_offset = 4096};
        std::shared_ptr<ChunkRootCommit> result;
        EXPECT_TRUE(ChunkRootCommit::create(pages, domain, ids, identity, credits.credits.reservation(),
                                            credits.credits.reservation(), &result)
                        .ok());
        return result;
    }
};
} // namespace crowdb::tree::test_chunk
