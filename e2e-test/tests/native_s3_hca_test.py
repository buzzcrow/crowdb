# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host-network HCA fixture retains ownership without inventing Docker IPs."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from allocation import create_network, observed_address, start_node
from fixture import Cluster, LABEL


class NativeS3HcaTest(unittest.TestCase):
    def test_explicit_profile_binds_owned_loopback_and_passes_only_selected_devices(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            cluster.host_network = True
            cluster.sdk_environment = {"CROWDB_NATIVE_S3_RDMA": "1"}
            cluster.server_rdma_environment = {"CROWDB_RDMA_ENABLED": "1"}
            cluster.hca_devices = ("--device", "/dev/infiniband/rdma_cm")
            cluster.command = Mock()
            subnet = create_network(cluster)
            self.assertTrue(subnet.is_loopback)
            self.assertFalse(cluster.network_created)
            cluster.command.assert_not_called()
            address = str(subnet.network_address + 2)
            info = {"HostConfig": {"NetworkMode": "host"},
                    "Config": {"Labels": {LABEL: cluster.run_id}}}
            cluster.command.return_value = json.dumps([info])
            config = cluster.artifacts / "disk.toml"
            config.write_text('http_listen_addr = "0.0.0.0:7101"')
            self.assertEqual(start_node(cluster, 0, Path(directory), ("server", "diskio"), address,
                             mounts=(f"type=bind,source={config},target=/config,readonly",)), address)
            self.assertIn(address, config.read_text())
            self.assertIn("CROWDB_RDMA_ENABLED=1", cluster.command.call_args_list[0].args)
            self.assertIn("/dev/infiniband/rdma_cm", cluster.command.call_args_list[0].args)
            self.assertEqual(cluster.servers, cluster.containers)
            info["Config"]["Labels"][LABEL] = "foreign"
            with self.assertRaisesRegex(ValueError, "owned host network"):
                observed_address(cluster, info, address)

    def test_profile_rejects_foreign_config_before_starting_process(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            cluster.host_network = True
            cluster.command = Mock()
            foreign = Path(directory) / "foreign.toml"
            foreign.write_text('address = "0.0.0.0:80"')
            with self.assertRaisesRegex(ValueError, "own generated"):
                start_node(cluster, 0, Path(directory), ("server", "diskio"), "127.1.2.3",
                           mounts=(f"type=bind,source={foreign},target=/config,readonly",))
            cluster.command.assert_not_called()
            self.assertEqual(foreign.read_text(), 'address = "0.0.0.0:80"')
