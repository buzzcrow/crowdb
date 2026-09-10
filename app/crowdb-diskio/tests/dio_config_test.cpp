// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "dio_config.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace crowdb::diskio
{

namespace
{
class TempConfig
{
  public:
    explicit TempConfig(const std::string &content)
        : path_(std::filesystem::temp_directory_path() /
                ("crowdb-diskio-config-" + std::to_string(::getpid()) + "-" + std::to_string(next_id_++) + ".toml"))
    {
        std::ofstream output(path_);
        output << content;
    }

    ~TempConfig()
    {
        std::error_code error;
        std::filesystem::remove(path_, error);
    }

    const std::filesystem::path &path() const
    {
        return path_;
    }

  private:
    inline static uint32_t next_id_ = 0;
    std::filesystem::path  path_;
};
} // namespace

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

TEST(DioConfigTest, LoadsCompleteToml)
{
    TempConfig  file(R"(
[server]
bind_address = "0.0.0.0"
listen_port = 13042
rpc_workers = 7
node_id = 9
dummy_disk_type = "mem"
o_direct = false
fault_latency_min_ms = 2
fault_latency_max_ms = 8
fault_error_rate = 0.25

[engine]
thread_pool_size = 6
sq_entries = 512

[group0]
kv_seeds = ["http://127.0.0.1:10000", "http://127.0.0.1:10001"]
instance_id = 10
rack_id = 11
disk_group_id = 12
sync_interval_ms = 900
auto_discover_disks = true

[metrics]
log_dir = "/var/log/crowdb"
interval_secs = 13

[[disk]]
id = "a:b"
path = "/dev/test"
zone_capacity = 4096
)");
    DioConfig   config;
    std::string error;

    ASSERT_TRUE(DioConfig::load_file(file.path().string(), config, error)) << error;
    EXPECT_EQ(config.bind_address, "0.0.0.0");
    EXPECT_EQ(config.listen_port, 13042);
    EXPECT_EQ(config.rpc_workers, 7U);
    EXPECT_EQ(config.node_id, 9U);
    EXPECT_EQ(config.dummy_disk_type, DummyDiskType::Mem);
    EXPECT_FALSE(config.o_direct);
    ASSERT_TRUE(config.dummy_props.has_value());
    EXPECT_EQ(config.dummy_props->latency_min_ms, 2U);
    EXPECT_EQ(config.dummy_props->latency_max_ms, 8U);
    EXPECT_DOUBLE_EQ(config.dummy_props->error_rate, 0.25);
    EXPECT_EQ(config.thread_pool_size, 6U);
    EXPECT_EQ(config.sq_entries, 512U);
    EXPECT_EQ(config.kv_seeds.size(), 2U);
    EXPECT_EQ(config.instance_id, 10U);
    EXPECT_EQ(config.rack_id, 11U);
    EXPECT_EQ(config.dg_id, 12U);
    EXPECT_EQ(config.sync_interval_ms, 900U);
    EXPECT_TRUE(config.auto_discover_disks);
    EXPECT_EQ(config.metrics_log_dir, "/var/log/crowdb");
    EXPECT_EQ(config.metrics_interval_secs, 13U);
    ASSERT_EQ(config.disks.size(), 1U);
    EXPECT_EQ(config.disks[0].id.high, 0xAU);
    EXPECT_EQ(config.disks[0].id.low, 0xBU);
    EXPECT_EQ(config.disks[0].path, "/dev/test");
    ASSERT_EQ(config.disks[0].zones.size(), 1U);
    EXPECT_EQ(config.disks[0].zones[0].capacity, 4096);
}

TEST(DioConfigTest, CliOverridesFileRegardlessOfArgumentOrder)
{
    TempConfig  file("[server]\nlisten_port = 13042\nrpc_workers = 7\n[engine]\nthread_pool_size = 6\n");
    std::string path           = file.path().string();
    char        program[]      = "crowdb-diskio";
    char        config_flag[]  = "--config";
    char        workers_flag[] = "--rpc-workers";
    char        workers[]      = "3";
    char        port_flag[]    = "--port";
    char        port[]         = "13099";
    char       *argv[]         = {program, workers_flag, workers, config_flag, path.data(), port_flag, port};
    DioConfig   config;
    std::string error;

    ASSERT_TRUE(DioConfig::parse_args(7, argv, config, error)) << error;
    EXPECT_EQ(config.rpc_workers, 3U);
    EXPECT_EQ(config.listen_port, 13099);
    EXPECT_EQ(config.thread_pool_size, 6U);
}

TEST(DioConfigTest, FailedLoadLeavesOutputUnchanged)
{
    TempConfig file("[server]\nrpc_workers = \"many\"\n");
    DioConfig  config;
    config.rpc_workers = 9;
    std::string error;

    EXPECT_FALSE(DioConfig::load_file(file.path().string(), config, error));
    EXPECT_EQ(config.rpc_workers, 9U);
    EXPECT_NE(error.find("rpc_workers"), std::string::npos);
}

TEST(DioConfigTest, RejectsReversedFaultLatencyRange)
{
    TempConfig  file("[server]\nfault_latency_min_ms = 8\nfault_latency_max_ms = 2\n");
    DioConfig   config;
    std::string error;

    EXPECT_FALSE(DioConfig::load_file(file.path().string(), config, error));
    EXPECT_NE(error.find("must be <="), std::string::npos);
}

TEST(DioConfigTest, RejectsDiskWithoutPath)
{
    TempConfig  file("[[disk]]\nid = \"1\"\n");
    DioConfig   config;
    std::string error;

    EXPECT_FALSE(DioConfig::load_file(file.path().string(), config, error));
    EXPECT_NE(error.find("disk.path"), std::string::npos);
}

TEST(DioConfigTest, LoadsTrackedTemplate)
{
    const auto path =
        std::filesystem::path(__FILE__).parent_path().parent_path() / "conf" / "crowdb_diskio_config.toml";
    DioConfig   config;
    std::string error;

    ASSERT_TRUE(DioConfig::load_file(path.string(), config, error)) << error;
    EXPECT_EQ(config.rpc_workers, 4U);
    EXPECT_TRUE(config.validate(error)) << error;
}

} // namespace crowdb::diskio
