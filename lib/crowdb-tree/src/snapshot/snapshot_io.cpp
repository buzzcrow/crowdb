// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/snapshot/snapshot_io.h"

#include "btree/tree_core_access.h"
#include "snapshot/published_byte_export.h"
#include "snapshot/runtime_snapshot_import.h"

#include <array>
#include <fstream>

namespace crowdb::tree
{
Status snapshot_export_begin(Crowdbtree &tree, snapshot_format fmt, size_t chunk_bytes,
                             std::unique_ptr<SnapshotExport> *out)
try {
    if (out == nullptr) {
        return Status::invalid_argument("snapshot export needs output");
    }
    auto                                *core = TreeCoreAccess::get(tree);
    std::shared_ptr<PublishedByteExport> runtime;
    auto status = PublishedByteExport::create(core->shared_from_this(), fmt, chunk_bytes, &runtime);
    if (status.ok()) {
        *out = std::make_unique<SnapshotExport>(std::move(runtime));
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot export allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status SnapshotExport::next_chunk(std::string *out, bool *done)
{
    if (out == nullptr) {
        return Status::invalid_argument("snapshot chunk needs output");
    }
    if (runtime_) {
        SnapshotChunk chunk;
        bool          complete = false;
        auto          status   = next_owned(&chunk, &complete);
        out->clear();
        if (status.ok()) {
            if (!chunk.bytes.empty()) {
                out->assign(reinterpret_cast<const char *>(chunk.bytes.data()), chunk.bytes.size());
            }
            if (done != nullptr) {
                *done = complete;
            }
        }
        return status;
    }
    if (snapshot_ != nullptr) {
        return next_portable_chunk(out, done);
    }
    out->clear();
    size_t remaining = stream_.size() - pos_;
    size_t n         = remaining < chunk_bytes_ ? remaining : chunk_bytes_;
    out->assign(stream_, pos_, n);
    pos_ += n;
    if (done != nullptr) {
        *done = (pos_ >= stream_.size());
    }
    return Status::Ok();
}

Status snapshot_dump_to_file(Crowdbtree &tree, snapshot_format fmt, const std::string &path)
{
    std::unique_ptr<SnapshotExport> exp;
    Status                          s = snapshot_export_begin(tree, fmt, kSnapshotChunkBytes, &exp);
    if (!s.ok()) {
        return s;
    }
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) {
        return Status::io_error("snapshot dump: cannot open " + path);
    }
    bool done = false;
    while (!done) {
        std::string chunk;
        Status      cs = exp->next_chunk(&chunk, &done);
        if (!cs.ok()) {
            return cs;
        }
        if (!chunk.empty()) {
            f.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        }
        if (!f) {
            return Status::io_error("snapshot dump: write failed");
        }
    }
    f.flush();
    if (!f) {
        return Status::io_error("snapshot dump: flush failed");
    }
    return Status::Ok();
}

SnapshotImport::SnapshotImport(Crowdbtree &tree) : tree_(tree)
{
}

SnapshotImport::~SnapshotImport() = default;

Status SnapshotImport::feed(Slice chunk)
{
    if (!runtime_) {
        auto status = RuntimeSnapshotImport::create(*TreeCoreAccess::get(tree_), &runtime_);
        if (!status.ok()) {
            return status;
        }
    }
    return runtime_->feed(chunk);
}

Status SnapshotImport::finish(uint64_t *out_at_slot)
{
    if (!runtime_) {
        return Status::invalid_argument("snapshot: stream truncated");
    }
    return runtime_->finish(out_at_slot);
}

Status snapshot_load_from_file(Crowdbtree &tree, const std::string &path)
{
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        return Status::io_error("snapshot load: cannot open " + path);
    }
    SnapshotImport             imp(tree);
    std::array<char, 1U << 16> buf{};
    while (f) {
        f.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = f.gcount();
        if (got > 0) {
            Status s = imp.feed(Slice(buf.data(), static_cast<size_t>(got)));
            if (!s.ok()) {
                return s;
            }
        }
    }
    if (f.bad()) {
        return Status::io_error("snapshot load: read failed");
    }
    return imp.finish(nullptr);
}

} // namespace crowdb::tree
