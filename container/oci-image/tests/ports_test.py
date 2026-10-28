# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Port defaults and deployment arguments agree with the protocol source."""

from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "container/oci-image"))
import ports
from docker_local_launch import PUBLIC_PORTS, verify_container


class PortsTest(unittest.TestCase):
    def test_generated_defaults_are_current(self):
        subprocess.run([sys.executable, str(ROOT / "tools/ports/generate.py"), "--check"], check=True)

    def test_default_bridge_ports_use_node_offsets_zero_one_two(self):
        all_ports = set()
        for offset in range(3):
            for base, _ in PUBLIC_PORTS:
                self.assertNotIn(base + offset, all_ports)
                all_ports.add(base + offset)
        self.assertEqual([port for port, _ in PUBLIC_PORTS], [6135, 7135, 8135, 9135])

    def test_second_container_verification_uses_service_spacing_and_node_offset(self):
        record = dict(cluster="ports", owner="owner", nodes=3, image_id="sha256:fixture",
                      cpus=1, memory_mib=1536, port_base=ports.WEB_BASE + 100)
        env = ["CROWDB_CLUSTER=ports", "CROWDB_INITIAL_VOTERS=3"]
        for base, protocol in PUBLIC_PORTS:
            if protocol:
                env.append(f"CROWDB_{protocol}_PUBLIC_URI=http://127.0.0.1:{base + 101}")
        item = {
            "Name": "/crowdb-ports-2", "Image": record["image_id"],
            "Config": {"Labels": {"org.crowdb.local.cluster": "ports", "org.crowdb.local.owner": "owner"}, "Env": env},
            "HostConfig": {"NanoCpus": 1_000_000_000, "Memory": 1536 * 1024 * 1024,
                           "NetworkMode": "crowdb-ports", "PortBindings": {
                               f"{base}/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(base + 101)}]
                               for base, _ in PUBLIC_PORTS}},
            "Mounts": [{"Destination": "/opt/crowdb/data", "Type": "volume", "Name": "data", "RW": True}],
        }
        from docker_local_state import CLUSTER, OWNER
        item["Config"]["Labels"] = {CLUSTER: record["cluster"], OWNER: record["owner"]}
        verify_container(item, record, 2, "data", "crowdb-ports")
        item["HostConfig"]["PortBindings"][f"{ports.WEB_BASE}/tcp"][0]["HostPort"] = str(ports.WEB_BASE + 110)
        with self.assertRaises(ValueError):
            verify_container(item, record, 2, "data", "crowdb-ports")
