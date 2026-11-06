// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/snapshot/native_frame.h"
#include "snapshot/published_native.h"

namespace crowdb::tree
{
Status NativeFrameIterator::next_owned(std::optional<NativeFrameView> *out, bool *complete)
{
    if (!runtime_ || out == nullptr || complete == nullptr) {
        return Status::invalid_argument("native owned iteration requires output");
    }
    out->reset();
    return runtime_->next(out, complete);
}

NativeStreamInfo NativeFrameIterator::stream_info() const
{
    return runtime_ ? runtime_->info() : NativeStreamInfo{};
}
} // namespace crowdb::tree
