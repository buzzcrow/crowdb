// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "btree/tree_options.h"

namespace crowdb::tree
{
Crowdbtree::Crowdbtree(Config opt) : opt_(std::move(opt)), name_(opt_.name)
{
    auto status = configure_tree_options(&opt_);
    if (status.ok() && opt_.page_store != nullptr) {
        status = TreeCore::open(opt_, &core_);
    }
    else if (status.ok()) {
        std::shared_ptr<TreeRuntimeBinding> binding;
        status = TreeRuntimeBinding::create(opt_.runtime, opt_.runtime_binding, &binding);
        if (status.ok()) {
            status = TreeCore::create(opt_, binding, {}, &core_);
        }
    }
    if (!status.ok()) {
        throw std::runtime_error(status.to_string());
    }
    core_facade_charge_ =
        AllocationLease(core_->binding()->lifecycle_budget(), sizeof(Crowdbtree), AllocationKind::kMetadata);
}

Crowdbtree::Crowdbtree(Config opt, std::shared_ptr<TreeCore> core)
    : opt_(std::move(opt)),
      name_(opt_.name),
      core_(std::move(core)),
      core_facade_charge_(core_->binding()->lifecycle_budget(), sizeof(Crowdbtree), AllocationKind::kMetadata)
{
}

Crowdbtree::~Crowdbtree()
{
    core_->close_and_drain();
}

void Crowdbtree::set_gc_watermark(uint64_t snapshot_slot, uint64_t safe_slot)
{
    core_->request_gc_floor(std::min(snapshot_slot, safe_slot));
}

uint64_t Crowdbtree::gc_watermark() const
{
    return core_->requested_gc_floor();
}

Status Crowdbtree::put(Slice key, Slice value)
{
    Batch b;
    b.ops.push_back({
        .key   = std::string(key.data(), key.size()),
        .kind  = OpKind::kPut,
        .value = std::string(value.data(), value.size()),
    });
    return core_->apply_auto(b);
}

Status Crowdbtree::del(Slice key)
{
    Batch b;
    b.ops.push_back({.key = std::string(key.data(), key.size()), .kind = OpKind::kDelete, .value = std::string()});
    return core_->apply_auto(b);
}

Status Crowdbtree::batch_put(const Batch &batch)
{
    return core_->apply_auto(batch);
}

Status Crowdbtree::validate_key(Slice key) const
{
    if (!opt_.key_range.contains(key)) {
        return Status::invalid_argument("key is outside the tree range");
    }
    return Status::Ok();
}

size_t Crowdbtree::evict_clean_leaves(size_t max_resident_leaves)
{
    return core_->evict_clean_pages(PageCacheKind::Leaf, max_resident_leaves);
}

size_t Crowdbtree::evict_clean_inner(size_t max_resident_inner)
{
    return core_->evict_clean_pages(PageCacheKind::Inner, max_resident_inner);
}
} // namespace crowdb::tree
