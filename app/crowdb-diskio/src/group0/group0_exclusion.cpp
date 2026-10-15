// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-kv-client/c_api.h"
#include "group0/ffi_result.h"
#include "group0/group0_sync.h"

#include <folly/json.h>

#include <cstdio>

namespace crowdb::diskio
{
void Group0Sync::confirm_exclusions()
{
    const auto batch = exclusions_.pending(*disk_set_, retired_disks_);
    if (batch.empty()) {
        return;
    }
    const auto json = folly::toJson(batch);
    auto       ctx  = std::make_shared<FfiResult>();
    crowdb_hw_ack_diskio_exclusions(hw_client_, cfg_.rack_id, cfg_.node_id, cfg_.instance_id, json.c_str(),
                                    complete_ffi_result, retain_ffi_result(ctx));
    if (wait_for_ctx(*ctx, 4000) && ctx->status.load() == 0) {
        exclusions_.confirm(batch);
    }
    else {
        std::fprintf(stderr, "warning: disk exclusion confirmation remains pending\n");
    }
}

} // namespace crowdb::diskio
