// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
class TestUnavailableAnchorStore : public MemPageStore
{
  public:
    uint64_t unavailable_slots = 0;
    bool     io_error          = false;

    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override
    {
        if (off < 8192 && (unavailable_slots & (uint64_t{1} << (off / 4096))) != 0) {
            if (io_error) {
                return Status::io_error("anchor medium read failed");
            }
            return Status::unavailable("anchor mirror is temporarily unavailable");
        }
        return MemPageStore::read_at(off, buf, len);
    }
};

TEST(Persist, UnavailableAnchorCannotBecomeAnEmptyOrOlderTree)
{
    TestUnavailableAnchorStore store;
    Config                     opt;
    opt.page_store = &store;
    {
        Crowdbtree tree(opt);
        ASSERT_TRUE(tree.apply(1, Batch{{{.key = "retained", .kind = OpKind::kPut, .value = "first"}}}).ok());
        ASSERT_TRUE(tree.flush().ok());
        ASSERT_TRUE(tree.snapshot(nullptr).ok());
        ASSERT_TRUE(tree.apply(2, Batch{{{.key = "retained", .kind = OpKind::kPut, .value = "latest"}}}).ok());
        ASSERT_TRUE(tree.flush().ok());
        ASSERT_TRUE(tree.snapshot(nullptr).ok());
    }
    for (bool io_error : {false, true}) {
        for (uint64_t mask : {uint64_t{1}, uint64_t{2}, uint64_t{3}}) {
            SCOPED_TRACE(mask);
            store.unavailable_slots = mask;
            store.io_error          = io_error;
            std::unique_ptr<Crowdbtree> recovered;
            const auto                  status = Crowdbtree::open(opt, &recovered);
            EXPECT_EQ(status.code(), io_error ? Code::kIoError : Code::kUnavailable) << status.to_string();
            EXPECT_EQ(recovered, nullptr);
        }
    }
    store.unavailable_slots = 0;
    std::unique_ptr<Crowdbtree> recovered;
    ASSERT_TRUE(Crowdbtree::open(opt, &recovered).ok());
    std::string value;
    uint64_t    slot = 0;
    ASSERT_TRUE(recovered->get(Slice("retained"), &slot, &value));
    EXPECT_EQ(slot, 2U);
    EXPECT_EQ(value, "latest");
}
} // namespace
} // namespace crowdb::tree
