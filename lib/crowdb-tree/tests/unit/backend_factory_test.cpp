// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/backend/page_store.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
class FactoryPageStore final : public PageStore
{
  public:
    static Status open(const Config &config, std::shared_ptr<TreeCore> *out)
    {
        if (config.page_store == nullptr || out == nullptr || *out) {
            return Status::invalid_argument();
        }
        return Status::unavailable("injected backend factory");
    }

    [[nodiscard]] RuntimeFactory runtime_factory() const noexcept override
    {
        return &open;
    }

    Status write_at(uint64_t /*off*/, const uint8_t * /*buf*/, size_t /*len*/) override
    {
        return Status::internal_error();
    }

    Status read_at(uint64_t /*off*/, uint8_t * /*buf*/, size_t /*len*/) const override
    {
        return Status::internal_error();
    }

    Status sync() override
    {
        return Status::internal_error();
    }

    [[nodiscard]] uint64_t size() const override
    {
        return 0;
    }

    [[nodiscard]] uint32_t iu_size() const override
    {
        return 1;
    }
};
} // namespace

TEST(BackendFactory, InjectedFactoryReceivesRuntimeOpenWithoutByteRecovery)
{
    FactoryPageStore store;
    Config           config;
    config.page_store = &store;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    std::shared_ptr<TreeCore> core;
    const auto                status = TreeCore::open(config, &core);
    EXPECT_EQ(status.code(), Status::unavailable().code());
    EXPECT_EQ(status.message(), "injected backend factory");
    EXPECT_FALSE(core);
}

TEST(BackendFactory, LocalStoreDoesNotAdvertiseAnImmutableAuthority)
{
    MemPageStore store(1);
    EXPECT_EQ(store.runtime_factory(), nullptr);
}
} // namespace crowdb::tree
