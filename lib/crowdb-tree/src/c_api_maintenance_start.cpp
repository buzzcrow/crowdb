// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_maintenance_start.h"

#include "btree/tree_core_checkpoint_internal.h"
using namespace crowdb::tree;
using crowdb::tree::c_api_detail::begin_report;

int32_t ct_flush_report_async(ct_tree *tree, ct_future **out)
{
    return begin_report<FlushOperation>(
        tree, out, ct_future_impl::Kind::kFlushReport,
        [](TreeCore &core, std::shared_ptr<FlushOperation> *operation) { return core.prepare_flush(operation); });
}

int32_t ct_writeback_async(ct_tree *tree, ct_future **out)
{
    return begin_report<CheckpointOperation>(tree, out, ct_future_impl::Kind::kCheckpointReport,
                                             [](TreeCore &core, std::shared_ptr<CheckpointOperation> *operation) {
                                                 return core.prepare_writeback(operation);
                                             });
}

int32_t ct_checkpoint_report_async(ct_tree *tree, ct_future **out)
{
    return begin_report<CheckpointOperation>(tree, out, ct_future_impl::Kind::kCheckpointReport,
                                             [](TreeCore &core, std::shared_ptr<CheckpointOperation> *operation) {
                                                 return core.prepare_checkpoint(checkpoint_detail::defaults(core),
                                                                                operation);
                                             });
}
