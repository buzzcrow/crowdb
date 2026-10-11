# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Normal Bare Metal Web registry starts a packaged KV child without topology."""

from allocation import create_network, node_data, start_node
from fixture import HTTP, wait
from console_standalone_fixture import StandaloneWebCluster


class RegistryWebCluster(StandaloneWebCluster):
    def start(self):
        subnet = create_network(self)
        data = node_data(self, 0)
        (data / "service.toml").write_text("")
        (data / "launches.toml").write_text('''version = 1
[[launch]]
node_id = 701
service_id = "kv"
host = "localhost"
ssh_port = 22
binary_path = "/opt/crowdb/bin/crowdb-kv-server"
service_config_path = "/opt/crowdb/data/service.toml"
workspace = "/opt/crowdb/data"
auto_start = true
args = ["--root", "/opt/crowdb/data/registry-kv", "--management-port", "7000", "--ports", "7001", "--log-dir", "/opt/crowdb/data/log/kv"]
readiness_url = "http://127.0.0.1:7000/topology"
''')
        (data / "web.toml").write_text('''version = 1
mode = "bare-metal"
bind = "0.0.0.0"
port = 9090
group0_management_seeds = ["http://127.0.0.1:9"]
ui_root = "/opt/crowdb/ui"
log_dir = "/opt/crowdb/data/log/web"
log_max_file_mb = 30
log_max_files = 5
request_timeout_ms = 200
''')
        self.addresses.append(start_node(self, 0, data,
            ("server", "web", "--config", "/opt/crowdb/data/web.toml",
             "--registry", "/opt/crowdb/data/launches.toml"),
            address=str(subnet.network_address + 2)))
        wait(lambda: HTTP.open(f"http://{self.addresses[0]}:9090/healthz", timeout=3).read(),
             lambda body: body == b"ok", "registry Web listener", seconds=5)
        return self
