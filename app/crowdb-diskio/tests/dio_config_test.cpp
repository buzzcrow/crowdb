// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "dio_config.h"

#include <gtest/gtest.h>

#include <string>

namespace crowdb::diskio
{

TEST(DioConfigTest, ParsesRpcWorkerCount)
{
    char        program[] = "crowdb-diskio";
    char        flag[]    = "--rpc-workers";
    char        value[]   = "4";
    char       *argv[]    = {program, flag, value};
    DioConfig   config;
    std::string error;

    ASSERT_TRUE(DioConfig::parse_args(3, argv, config, error)) << error;
    EXPECT_EQ(config.rpc_workers, 4U);
    EXPECT_TRUE(config.validate(error)) << error;
}

TEST(DioConfigTest, RejectsZeroRpcWorkers)
{
    char        program[] = "crowdb-diskio";
    char        flag[]    = "--rpc-workers";
    char        value[]   = "0";
    char       *argv[]    = {program, flag, value};
    DioConfig   config;
    std::string error;

    ASSERT_TRUE(DioConfig::parse_args(3, argv, config, error)) << error;
    EXPECT_FALSE(config.validate(error));
    EXPECT_EQ(error, "rpc_workers must be > 0");
}

} // namespace crowdb::diskio
