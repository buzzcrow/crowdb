// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "runtime_tree_access.h"
#include "snapshot/operation_wait.h"
#ifdef CROWDB_TREE_TEST_UTIL
namespace crowdb::tree
{
struct MemTableAccess_for_tests
{
    struct Capture
    {
        std::shared_ptr<FlushOperation> operation;
        uint64_t                        frontier = 0;
        SourceCatalog::Tables           tables;
    };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        detail::SnapshotWaitCompletion *done;

        void set_value(FlushResult report) const noexcept
        {
            set_error(report.handoff_status);
        }

        void set_error(Status status) const noexcept
        {
            done->status = std::move(status);
            done->done.signal();
        }

        void set_stopped() const noexcept
        {
            set_error(Status::unavailable());
        }
    };

    static Capture capture(Crowdbtree &tree)
    {
        Capture result;
        auto    status = RuntimeTreeAccess_for_tests::core(tree).prepare_flush(&result.operation);
        if (!status.ok()) {
            throw std::runtime_error(status.to_string());
        }
        detail::SnapshotWaitCompletion done;
        auto operation = stdexec::connect(result.operation->capture_sender(), Receiver{&done});
        stdexec::start(operation);
        done.done.wait();
        if (!done.status.ok()) {
            throw std::runtime_error(done.status.to_string());
        }
        auto frozen = result.operation->frozen_sources();
        if (!frozen.catalog) {
            throw std::runtime_error("capture requested without a materializable contiguous frontier");
        }
        result.frontier = frozen.frontier;
        result.tables.assign(frozen.catalog->tables.begin(), frozen.catalog->tables.end());
        return result;
    }

    static auto active(Crowdbtree &tree)
    {
        return RuntimeTreeAccess_for_tests::capture(tree)->sources->tables.back();
    }

    static auto sources(Crowdbtree &tree)
    {
        return RuntimeTreeAccess_for_tests::capture(tree)->sources->tables;
    }

    static void publish([[maybe_unused]] Crowdbtree &tree, Capture &capture)
    {
        auto status = detail::wait_snapshot_operation(capture.operation);
        if (!status.ok()) {
            throw std::runtime_error(status.to_string());
        }
    }
};
} // namespace crowdb::tree
#endif
