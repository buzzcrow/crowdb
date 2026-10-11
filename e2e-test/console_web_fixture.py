# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Three release KV voters and two independent release Console Web servers."""

import json
from allocation import node_data, start_node
from fixture import Cluster, wait
import urllib.request


class ConsoleWebCluster(Cluster):
    def __init__(self, image, artifacts, **kwargs):
        super().__init__(image, artifacts, node_count=3, store_ids=(0, 0, 0),
                         groups=(0,), wire_replicas=True, advertise=True, **kwargs)

    def start(self):
        super().start()
        for index in (3, 4):
            data = node_data(self, index)
            profile = self.artifacts / f"web-{index}.toml"
            profile.write_text('version = 1\nmode = "bare-metal"\nbind = "0.0.0.0"\nport = 9090\n'
                               f'group0_management_seeds = {json.dumps(self.origins)}\n'
                               'ui_root = "/opt/crowdb/ui"\nlog_dir = "/opt/crowdb/data/log/web"\n'
                               'log_max_file_mb = 30\nlog_max_files = 5\nrequest_timeout_ms = 500\n')
            address = start_node(self, index, data,
                                 ("server", "web", "--config", "/opt/crowdb/etc/e2e-web.toml"),
                                 mounts=(f"type=bind,source={profile},target=/opt/crowdb/etc/e2e-web.toml,readonly",))
            self.addresses.append(address)
            origin = f"http://{address}:9090"
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            wait(lambda: opener.open(origin + "/healthz", timeout=5).read(),
                 lambda body: body == b"ok", "release Web listener")
        (self.artifacts / "web-endpoints.json").write_text(json.dumps({
            "kv": self.origins, "web": [f"http://{address}:9090" for address in self.addresses[3:]]}, indent=2))
        return self
