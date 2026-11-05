// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/immutable_snapshot_c_api.h"
#include "snapshot/published_snapshot.h"

struct ct_immutable_snapshot
{
    explicit ct_immutable_snapshot(std::shared_ptr<crowdb::tree::PublishedSnapshotContext> value)
        : allocation(value->binding()->lifecycle_budget(), sizeof(ct_immutable_snapshot),
                     crowdb::tree::AllocationKind::kMetadata),
          context(std::move(value))
    {
    }

    crowdb::tree::AllocationLease                           allocation;
    std::shared_ptr<crowdb::tree::PublishedSnapshotContext> context;
};
