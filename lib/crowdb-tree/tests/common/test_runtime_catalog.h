// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "common/test_chunk_root_catalog.h"

#include <set>

namespace crowdb::tree::test_chunk
{
class TestRuntimeCatalog final : public TestRootCatalog
{
  public:
    explicit TestRuntimeCatalog(std::shared_ptr<AllocationBudget> budget)
        : TestRootCatalog(budget),
          budget(std::move(budget))
    {
        current_generation = 0;
    }

    std::shared_ptr<AllocationBudget>                 budget;
    std::map<std::pair<uint64_t, uint64_t>, uint64_t> pins;

    std::map<detail::ChunkStageId, std::set<detail::ChunkStageId>> stage_pins;
    bool (*before_root_load)(void *) = nullptr;
    std::shared_ptr<void> root_load_owner;

    Status load_root_owned(uint64_t tree, uint64_t generation, detail::ChunkRootBytes *out) const override
    {
        if (before_root_load != nullptr && !before_root_load(root_load_owner.get())) {
            return Status::internal_error("test root load callback failed");
        }
        return TestRootCatalog::load_root_owned(tree, generation, out);
    }

    [[nodiscard]] std::shared_ptr<TestRuntimeCatalog> next_writer() const
    {
        auto next                = std::make_shared<TestRuntimeCatalog>(budget);
        next->queue              = queue;
        next->records            = records;
        next->candidate          = candidate;
        next->manifest           = manifest;
        next->committed_manifest = committed_manifest;
        next->retained_roots     = retained_roots;
        next->current_generation = current_generation;
        next->checksum           = checksum;
        next->expected_checksum  = expected_checksum;
        next->pins               = pins;
        next->stage_pins         = stage_pins;
        next->before_root_load   = before_root_load;
        next->root_load_owner    = root_load_owner;
        return next;
    }

    Status change_pin(uint64_t /*tree*/, detail::ChunkStageId stage, detail::ChunkStageId pin, bool acquire) override
    {
        if (!acquire) {
            const auto found = stage_pins.find(stage);
            if (found != stage_pins.end()) {
                found->second.erase(pin);
                if (found->second.empty()) {
                    stage_pins.erase(found);
                }
            }
            return Status::Ok();
        }
        const auto record = records.find(stage);
        if (record == records.end() || record->second.state != detail::ChunkStageState::Reloadable) {
            return Status::unavailable();
        }
        stage_pins[stage].insert(pin);
        return Status::Ok();
    }

    Status pin_generation(uint64_t /*tree*/, uint64_t high, uint64_t low, uint64_t generation) override
    {
        if (generation == 0 || (generation != current_generation && !retained_roots.contains(generation))) {
            return Status::not_found();
        }
        const auto [found, inserted] = pins.emplace(std::pair{high, low}, generation);
        return inserted || found->second == generation ? Status::Ok() : Status::invalid_argument();
    }

    Status unpin_generation(uint64_t /*tree*/, uint64_t high, uint64_t low) override
    {
        pins.erase({high, low});
        return Status::Ok();
    }

    uint64_t reclaim_before(uint64_t /*tree*/, uint64_t generation) override
    {
        for (const auto &[id, pinned] : pins) {
            (void)id;
            generation = std::min(generation, pinned);
        }
        const auto before = retained_roots.size();
        std::erase_if(retained_roots,
                      [generation](const auto &entry) { return entry.first != 0 && entry.first < generation; });
        return before - retained_roots.size();
    }
};
} // namespace crowdb::tree::test_chunk
