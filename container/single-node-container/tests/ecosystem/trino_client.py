# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Pinned direct Trino REST-catalog probe, with explicit first-divergence evidence."""
import json
import os
from pathlib import Path
import subprocess
import socket
import sys
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

IMAGE = "trinodb/trino:483@sha256:fca43d1fdfdcd45f36b791f7117a47f8ce69c58232e5398bfdb8539cd28e778b"
TRINO_URI = ""


def query(sql):
    request = Request(f"{TRINO_URI}/v1/statement", data=sql.encode(),
                      headers={"X-Trino-User": "ecosystem", "Content-Type": "text/plain"})
    rows = []
    deadline = time.monotonic() + 60
    while True:
        with urlopen(request, timeout=5) as response:
            body = json.load(response)
        if "error" in body:
            raise RuntimeError(json.dumps(body["error"]))
        rows.extend(body.get("data", []))
        if "nextUri" not in body:
            return rows
        if time.monotonic() >= deadline:
            raise TimeoutError("Trino query exceeded the functional bound")
        request = Request(body["nextUri"], headers={"X-Trino-User": "ecosystem"})


def main():
    global TRINO_URI
    name, private = sys.argv[1:]
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    TRINO_URI = f"http://127.0.0.1:{port}"
    config = Path(private).joinpath("trino-config")
    config.mkdir(exist_ok=True)
    config.joinpath("catalog").mkdir(exist_ok=True)
    config.joinpath("config.properties").write_text(
        f"coordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.port={port}\n"
        f"discovery.uri={TRINO_URI}\nquery.max-memory=1GB\nquery.max-memory-per-node=512MB\n")
    config.joinpath("node.properties").write_text("node.environment=ecosystem\nnode.id=ecosystem\nnode.data-dir=/tmp/trino-data\n")
    config.joinpath("catalog/crowdb.properties").write_text(
        "connector.name=iceberg\niceberg.catalog.type=rest\n"
        f"iceberg.rest-catalog.uri={os.environ['CROWDB_PREVIEW_ICEBERG_URI']}\n"
        "iceberg.rest-catalog.security=OAUTH2\n"
        f"iceberg.rest-catalog.oauth2.token={os.environ['ICEBERG_TOKEN']}\n"
        "iceberg.rest-catalog.oauth2.token-refresh-enabled=false\n"
        "iceberg.rest-catalog.vended-credentials-enabled=true\n"
        "iceberg.rest-catalog.view-endpoints-enabled=false\nfs.s3.enabled=true\ns3.region=us-east-1\n"
        f"s3.endpoint={os.environ['CROWDB_PREVIEW_ICEBERG_URI']}\ns3.path-style-access=true\n")
    command = ["docker", "run", "-d", "--name", name, "--network", "host", "--platform", "linux/amd64"]
    for entry in ["config.properties", "node.properties", "catalog"]:
        command.extend(["--mount", f"type=bind,source={config.absolute() / entry},target=/etc/trino/{entry},readonly"])
    subprocess.run([*command, IMAGE], check=True)
    report = {"client": "trino", "version": "483", "image": IMAGE, "status": "failed", "stage": "startup"}
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with urlopen(f"{TRINO_URI}/v1/info", timeout=1) as response:
                    info = json.load(response)
                if not info["starting"]:
                    assert info["nodeVersion"]["version"] == "483", info
                    break
            except URLError:
                pass
            state = subprocess.run(["docker", "inspect", "--format", "{{.State.Running}}", name],
                                   check=True, capture_output=True, text=True).stdout.strip()
            if state != "true" or time.monotonic() >= deadline:
                raise RuntimeError("Pinned Trino did not become ready")
            time.sleep(0.1)
        report["stage"] = "REST discovery"
        assert ["handoff"] in query("SHOW TABLES FROM crowdb.ecosystem")
        report["stage"] = "delegated FileIO SELECT"
        rows = query("SELECT id, amount FROM crowdb.ecosystem.handoff ORDER BY id")
        assert rows == [[1, 10], [3, 30], [4, 40], [5, 50], [6, 60]], rows
        assert query("SELECT count(*), sum(amount) FROM crowdb.ecosystem.handoff") == [[5, 190]]
        report.update(status="passed", rows=rows, operations=["REST discovery", "SELECT", "BI aggregate"])
    except Exception as error:
        report["diagnostic"] = str(error).replace(os.environ["ICEBERG_TOKEN"], "[REDACTED]")
        raise
    finally:
        Path(os.environ["CROWDB_ECOSYSTEM_RESULTS"]).joinpath("trino.json").write_text(json.dumps(report, indent=2))
        subprocess.run(["docker", "logs", name], check=False)
        subprocess.run(["docker", "rm", "-fv", name], check=True)
    print(json.dumps(report))


if __name__ == "__main__":
    main()
