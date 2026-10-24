# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Accept the owned diskless policy before attaching unadmitted fixture media."""

from allocation import LABEL


def accept(cluster, index, data, physical_host):
    policy = cluster.artifacts / f"node-policy-{index}.toml"
    kinds = ("paxos-kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access-server")
    services = ", ".join(f'{{ kind = "{kind}", enabled = false }}' for kind in kinds)
    policy.write_text(f'services = [{services}]\n[storage]\nmode = "diskless"\n')
    environment = ("CROWDB_DEPLOYMENT_MODE=production", "CROWDB_INITIAL_VOTERS=3",
        f"CROWDB_CLUSTER={cluster.run_id}", "CROWDB_SSH_PASSWORD=crowdb",
        f"CROWDB_PHYSICAL_HOST_ID={physical_host}",
        "CROWDB_NODE_POLICY_FILE=/run/native-policy.toml")
    mount = f"type=bind,source={policy},target=/run/native-policy.toml,readonly"
    name = f"{cluster.run_id}-policy{index}"
    cluster.containers.append(name)
    output = cluster.command("run", "--rm", "--name", name,
        "--label", f"{LABEL}={cluster.run_id}", "--network", "none",
        "--cpu-shares", "1024", "--memory", "128m", "--pids-limit", "32",
        "--mount", f"type=bind,source={data},target=/opt/crowdb/data", "--mount", mount,
        *(part for value in environment for part in ("--env", value)),
        "--entrypoint", "/opt/crowdb/bin/crowdb-monitor", cluster.image, "accept-policy")
    assert output.strip() == "production", output
    return environment, (mount,)
