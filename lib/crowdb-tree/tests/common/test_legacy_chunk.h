// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/legacy_chunk_read_store.h"
#include "common/test_chunk_root.h"

#include <filesystem>
#include <fstream>

namespace crowdb::tree::test_chunk
{
class TestLegacyCatalog final : public TestRootCatalog
{
  public:
    explicit TestLegacyCatalog(std::shared_ptr<AllocationBudget> budget)
        : TestRootCatalog(budget),
          storage_(std::move(budget))
    {
    }

    [[nodiscard]] const std::shared_ptr<AllocationBudget> &storage() const
    {
        return storage_;
    }

    std::shared_ptr<AllocationBudget>                             storage_;
    std::map<std::pair<uint64_t, uint64_t>, std::vector<uint8_t>> segments;
    std::map<std::pair<uint64_t, uint64_t>, uint64_t>             generation_pins;

    std::map<uint64_t, std::vector<uint8_t>> migrations;

    [[nodiscard]] std::shared_ptr<TestLegacyCatalog> next_legacy_owner() const
    {
        auto next                = std::make_shared<TestLegacyCatalog>(storage());
        next->queue              = queue;
        next->records            = records;
        next->candidate          = candidate;
        next->manifest           = manifest;
        next->committed_manifest = committed_manifest;
        next->retained_roots     = retained_roots;
        next->current_generation = current_generation;
        next->checksum           = checksum;
        next->expected_checksum  = expected_checksum;
        next->segments           = segments;
        next->generation_pins    = generation_pins;
        next->migrations         = migrations;
        return next;
    }

    Status prepare_legacy_migration(uint64_t tree, uint64_t generation, std::span<const uint8_t> bytes) override
    {
        if (tree != 42 || generation != current_generation ||
            !std::equal(bytes.begin(), bytes.end(), committed_manifest.begin(), committed_manifest.end())) {
            return Status::unavailable();
        }
        const auto [found, inserted] = migrations.emplace(generation, std::vector<uint8_t>(bytes.begin(), bytes.end()));
        return inserted || found->second == std::vector<uint8_t>(bytes.begin(), bytes.end()) ? Status::Ok()
                                                                                             : Status::corruption();
    }

    Status load_legacy_migration_owned(uint64_t tree, uint64_t generation, detail::ChunkRootBytes *out) const override
    {
        const auto found = migrations.find(generation);
        if (tree != 42 || found == migrations.end()) {
            return Status::not_found();
        }
        out->assign(found->second.begin(), found->second.end());
        return Status::Ok();
    }

    Status load_legacy_reference_owned(uint64_t tree, uint64_t object, detail::ChunkRootBytes *out) const override
    {
        const auto found = segments.find({tree, object});
        if (found == segments.end()) {

            return Status::not_found();
        }
        out->assign(found->second.begin(), found->second.end());
        return Status::Ok();
    }

    Status pin_generation(uint64_t tree, uint64_t high, uint64_t low, uint64_t generation) override
    {
        if (tree != 42 || generation != current_generation) {

            return Status::unavailable();
        }
        const auto [position, inserted] = generation_pins.emplace(std::pair{high, low}, generation);
        return inserted || position->second == generation ? Status::Ok() : Status::unavailable();
    }

    Status unpin_generation(uint64_t tree, uint64_t high, uint64_t low) override
    {
        if (tree != 42) {

            return Status::invalid_argument();
        }
        generation_pins.erase({high, low});
        return Status::Ok();
    }
};

struct TestLegacyChunk
{
    test_read::LoadCredits             credits;
    std::shared_ptr<TestLegacyCatalog> catalog   = std::make_shared<TestLegacyCatalog>(credits.credits.bytes);
    std::shared_ptr<TestRootTransport> transport = std::make_shared<TestRootTransport>(catalog);
    std::vector<uint8_t>               fixture;
    std::map<std::array<uint64_t, 2>, std::vector<uint8_t>> physical;
    detail::LegacyChunkLayout                               layout;

    TestLegacyChunk()
    {
        const auto    path = std::filesystem::path(__FILE__).parent_path().parent_path() / "data/legacy-chunk-v7.bin";
        std::ifstream input(path, std::ios::binary);
        fixture.assign(std::istreambuf_iterator<char>(input), {});
        if (fixture.size() < 8 || std::string_view(reinterpret_cast<const char *>(fixture.data()), 7) != "CTLEGV7") {
            throw std::runtime_error("legacy Chunk fixture is missing");
        }
        size_t     offset = 8;
        const auto number = [&] {
            if (fixture.size() - offset < 8) {
                throw std::runtime_error("legacy fixture number truncated");
            }
            const auto value = frame_u64(fixture.data(), offset);
            offset += 8;
            return value;
        };
        const auto bytes = [&] {
            const auto size = number();
            if (size > fixture.size() - offset) {
                throw std::runtime_error("legacy fixture bytes truncated");
            }
            std::vector<uint8_t> value(fixture.data() + offset, fixture.data() + offset + size);
            offset += size;
            return value;
        };
        catalog->committed_manifest = bytes();
        EXPECT_TRUE(detail::LegacyChunkLayout::parse(catalog->committed_manifest, 42, 7, &layout).ok());
        catalog->current_generation = layout.generation();
        const auto segment_count    = number();
        for (uint64_t i = 0; i < segment_count; ++i) {
            const auto tree   = number();
            const auto object = number();
            catalog->segments.emplace(std::pair{tree, object}, bytes());
        }
        const auto pack_count = number();
        for (uint64_t i = 0; i < pack_count; ++i) {
            const auto high  = number();
            const auto low   = number();
            const auto begin = number();
            const auto value = bytes();
            auto      &chunk = physical[{high, low}];
            chunk.resize(std::max(chunk.size(), begin + value.size()));
            std::copy(value.begin(), value.end(), chunk.data() + begin);
        }
        EXPECT_EQ(offset, fixture.size());
        for (const auto &[id, chunk] : physical) {
            detail::ChunkId allocated;
            EXPECT_TRUE(transport->memory
                            .allocate_mirror_chunk(chunk.size(), 7, &allocated, detail::chunk_purpose({id[0], id[1]}))
                            .ok());
            transport->ids.emplace(id, allocated);
            for (uint32_t mirror = 0; mirror < 2; ++mirror) {
                EXPECT_TRUE(transport->memory.write_mirror(allocated, mirror, 0, chunk.data(), chunk.size()).ok());
            }
            EXPECT_TRUE(transport->memory.advance_write(allocated, 0, chunk.size()).ok());
            EXPECT_TRUE(transport->memory.seal_chunk(allocated, 7, chunk.size()).ok());
        }
    }

    [[nodiscard]] std::shared_ptr<detail::LegacyChunkReadStore> reader() const
    {
        return std::make_shared<detail::LegacyChunkReadStore>(catalog, transport, credits.credits.bytes, 42, 7, 1, 2);
    }
};
} // namespace crowdb::tree::test_chunk
