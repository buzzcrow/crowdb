// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "btree/tree_options.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/backend/page_store.h"

namespace crowdb::tree
{
Status Crowdbtree::open(const Config &opt, std::unique_ptr<Crowdbtree> *out)
try {
    if (out == nullptr) {
        return Status::invalid_argument("open requires output");
    }
    Config                    options = opt;
    auto                      status  = configure_tree_options(&options);
    std::shared_ptr<TreeCore> core;
    if (status.ok()) {
        status = TreeCore::open(options, &core);
    }
    if (status.ok()) {
        auto tree = std::unique_ptr<Crowdbtree>(new Crowdbtree(options, std::move(core)));
        tree->init_metrics("s." + std::to_string(options.store_id) + ".g." + std::to_string(options.group_id) + ".tree",
                           options.backend_label);
        CRB_LOG_INFO("[{}] open: frame_bytes={}", tree->name_, options.frame_bytes);
        *out = std::move(tree);
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("Tree open allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status Crowdbtree::snapshot(uint64_t *out_last_applied, uint64_t *out_snapshot_seq)
{
    uint64_t applied  = 0;
    uint64_t sequence = 0;
    auto     status   = core_->checkpoint(&applied, &sequence);
    if (status.ok()) {
        if (out_last_applied != nullptr) {
            *out_last_applied = applied;
        }
        if (out_snapshot_seq != nullptr) {
            *out_snapshot_seq = sequence;
        }
        CRB_LOG_INFO("[{}] snapshot committed: slot={} sequence={}", name_, applied, sequence);
    }
    return status;
}

void Crowdbtree::snapshot_async(std::function<void(Status, uint64_t)> on_done)
{
    core_->checkpoint_async(std::move(on_done));
}

Status Crowdbtree::materialize_ownership(uint64_t *bytes_written, bool *complete)
{
    if (opt_.page_store == nullptr || bytes_written == nullptr || complete == nullptr) {
        return Status::invalid_argument("materialize_ownership: invalid argument");
    }
    *bytes_written = 0;
    *complete      = true;
    return Status::Ok();
}

Status Crowdbtree::compact_sparse_blocks(MergeGcStats *out)
{
    if (out == nullptr || opt_.page_store == nullptr) {
        return Status::invalid_argument("compaction requires backend and output");
    }
    *out = {};
    if (opt_.page_store->block_size() == 0 || opt_.merge_gc_max_relocation_bytes == 0) {
        return Status::Ok();
    }
    return core_->compact_sparse_blocks(out);
}
} // namespace crowdb::tree
