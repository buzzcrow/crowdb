# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host components and native measurements; product lifecycle belongs to E2E."""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUST_GROUPS = {
    "core": ("common", "test-harness", "protocol", "kv", "kv-client"),
    "storage": ("chunkdb-client", "chunk-kv", "chunk-kv-client", "chunk-kv-server",
                "chunk-stream", "kv-server", "diskdb", "diskdb-client", "chunkdb",
                "chunk-client", "diskio-client"),
    "access": ("access-multipart", "access-s3", "sdk-native", "sdk-native-python",
               "access-iceberg", "access-server", "access-dataset", "monitor"),
    "console": ("console-shared", "cli", "web"),
    "cpp": ("tree-ffi", "rpc-ffi", "rpc-test"),
}
DOMAINS = ("kv", "diskdb", "diskio", "chunkdb", "chunk-stream", "chunk-client",
           "chunk-kv", "s3", "iceberg", "dataset", "console", "deployment")
PROFILES = ("bridge-admission", "host", "containerd-host", "containerd-cluster",
            "ecosystem", "ecosystem-python")


def package_domain(package):
    if package == "chunk-client":
        return "chunk-client"
    if package.startswith("access-"):
        return {"access-s3": "s3", "access-iceberg": "iceberg",
                "access-dataset": "dataset"}.get(package, "access")
    if package in ("cli", "web", "console-shared"):
        return "console"
    if package == "monitor":
        return "deployment"
    if package == "test-harness":
        return "test-infrastructure"
    return package.removesuffix("-client").removesuffix("-server").removesuffix("-ffi")


class Measurement:
    def __init__(self, group):
        self.directory = ROOT / ".crowdb-runtime/artifacts/unit" / f"{group}-{uuid.uuid4().hex}"
        self.directory.mkdir(parents=True)
        self.records = []

    def run(self, domain, command, cwd=ROOT, env=None):
        log = self.directory / f"{len(self.records):02d}-{domain}.log"
        started = time.monotonic()
        print("+ " + " ".join(map(str, command)), flush=True)
        with log.open("w") as stream:
            with subprocess.Popen(command, cwd=cwd, env=env, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
                for line in process.stdout:
                    stream.write(line)
                    print(line, end="", flush=True)
                code = process.wait()
        self.records.append(dict(domain=domain, command=command, exit_code=code,
                                 seconds=round(time.monotonic() - started, 3), log=str(log)))
        (self.directory / "results.json").write_text(json.dumps(self.records, indent=2) + "\n")
        print(f"Measurement: {self.directory / 'results.json'}", flush=True)
        if code:
            raise subprocess.CalledProcessError(code, command)


def ctest_command(directory):
    command = ["ctest", "--test-dir", directory, "--output-on-failure"]
    listing = json.loads(subprocess.check_output(
        ["ctest", "--test-dir", directory, "--show-only=json-v1"], cwd=ROOT, text=True))
    own, nested = set(), set()
    for test in listing["tests"]:
        arguments = test.get("command", [])
        if not arguments:
            raise ValueError(f"CTest has no executable: {test['name']}")
        executable = next((arg.removeprefix("TEST_EXECUTABLE=") for arg in arguments
                           if arg.startswith("TEST_EXECUTABLE=")), arguments[0])
        parent = Path(executable).resolve().parent
        owner = (ROOT / directory).resolve()
        if not parent.is_relative_to(owner):
            raise ValueError(f"CTest executable has unknown ownership: {test['name']}")
        target = own if parent == owner else nested
        target.add(test["name"])
    if not own or own & nested:
        raise ValueError(f"CTest ownership is empty or ambiguous in {directory}")
    if nested:
        command += ["--exclude-regex", "^(" + "|".join(re.escape(name) for name in sorted(nested)) + ")$"]
    return command


def unit(group):
    environment = {"sdk": "iceberg-e2e", "boto3": "s3-e2e"}.get(group)
    if environment and os.environ.get("PIXI_ENVIRONMENT_NAME") != environment:
        subprocess.run(["pixi", "run", "-e", environment, "--", "python",
                        "tools/pixi-tasks/test.py", "unit", group], cwd=ROOT, check=True)
        return
    measurement = Measurement(group)
    if group == "cpp":
        for domain, directory in (("tree", "lib/crowdb-tree/build"),
                                  ("rpc", "lib/crowdb-rpc/build"),
                                  ("diskio", "app/crowdb-diskio/build")):
            measurement.run(domain, ctest_command(directory))
        measurement.run("common", ["lib/crowdb-tree/build/crowdb-common-build/crowdbcommon_tests"])
    if group in RUST_GROUPS:
        for package in RUST_GROUPS[group]:
            measurement.run(package_domain(package), ["cargo", "test", "-p", f"crowdb-{package}", "--tests"])
    if group == "console":
        # Test-only native measurement ownership is separate from product OCI acceptance.
        measurement.run("console", ["cargo", "build", "-p", "crowdb-cli", "-p", "crowdb-kv-server"])
        measurement.run("console", ["cargo", "build", "-p", "crowdb-console-shared", "--features", "test-util",
                                    "--example", "regression-fixture"])
        measurement.run("console", ["python", "tools/benchmark/fixture-smoke.py", "--worker",
                                    "target/debug/examples/regression-fixture", "--client", "target/debug/crowdb-cli"])
    if group == "access":
        # Explicitly run the otherwise ignored SDK case with pinned dependencies.
        measurement.run("access", ["pixi", "run", "-e", "s3-e2e", "test-boto3-copy-error"])
    elif group == "boto3":
        env = dict(os.environ, CROWDB_S3_E2E_PYTHON=str(Path(os.environ["CONDA_PREFIX"]) / "bin/python"))
        measurement.run("access", ["pixi", "run", "-e", "default", "--", "cargo", "test",
                                  "-p", "crowdb-access-server", "--test", "s3_copy_body_test",
                                  "official_boto3_recognizes_a_copy_error_after_http_200_and_keepalives",
                                  "--", "--ignored", "--exact"], env=env)
    elif group == "sdk":
        prefix = Path(os.environ["CONDA_PREFIX"])
        env = dict(os.environ, CROWDB_ICEBERG_E2E_PYTHON=str(prefix / "bin/python"),
                   CROWDB_ICEBERG_E2E_MVN=str(prefix / "bin/mvn"))
        if not env.get("JAVA_HOME"):
            raise ValueError("run unit sdk in the iceberg-e2e environment")
        measurement.run("iceberg", ["mvn", "--batch-mode", "--no-transfer-progress", "-f",
                                   "e2e-test/clients/iceberg-java/pom.xml",
                                   "dependency:go-offline", "compile", "exec:help"], env=env)
        command = ["pixi", "run", "-e", "default", "--", "cargo", "test", "--release",
                   "-p", "crowdb-access-server", "--features", "iceberg-e2e"]
        for binary in ("iceberg_namespace_sdk_test", "iceberg_table_sdk_test",
                       "iceberg_commit_sdk_test", "iceberg_java_response_loss_test"):
            command += ["--test", binary]
        measurement.run("iceberg", command + ["--", "--ignored", "--test-threads=1"], env=env)
    elif group == "ui":
        for command in (["npm", "test"], ["npm", "run", "lint"]):
            measurement.run("console-ui", command, ROOT / "app/crowdb-web/ui")


def container_command(kind, selection, extra):
    memory = {"kv": 4096, "iceberg": 12288, "console": 16384, "all": 32768,
              "ecosystem": 10240, "ecosystem-python": 6144}.get(selection, 8192)
    command = ["pixi", "run", "-e", "default", "--", "python", "e2e-test/run.py",
               "--layer" if kind == "e2e" else "--profile", selection]
    for flag, variable, default in (
            ("--image", "CROWDB_E2E_IMAGE", "crowdb-node:dev"),
            ("--concurrency", "CROWDB_E2E_CONCURRENCY", "2" if kind == "e2e" else "1"),
            ("--node-cpus", "CROWDB_E2E_NODE_CPUS", "1"),
            ("--cpus", "CROWDB_E2E_CPUS", "4"),
            ("--memory-mib", "CROWDB_E2E_MEMORY_MIB", str(memory))):
        command += [flag, os.environ.get(variable, default)]
    if kind == "e2e" and selection in ("kv", "all"):
        command.append("--require-isolation")
    return command + extra


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    units = commands.add_parser("unit")
    units.add_argument("group", choices=(*RUST_GROUPS, "ui", "sdk", "boto3"))
    for kind, choices in (("e2e", (*DOMAINS, "all")), ("profile", PROFILES)):
        sub = commands.add_parser(kind)
        sub.add_argument("selection", choices=choices)
    commands.add_parser("all")
    args, extra = parser.parse_known_args()
    if args.command in ("unit", "all") and extra:
        parser.error(f"unexpected arguments: {' '.join(extra)}")
    if args.command == "unit":
        unit(args.group)
    elif args.command == "all":
        for task in ("test-cpp", "test-core", "test-storage", "test-access",
                     "test-access-sdk", "test-console", "test-console-ui"):
            subprocess.run(["pixi", "run", task], cwd=ROOT, check=True)
        subprocess.run(["pixi", "run", "clean-env"], cwd=ROOT, check=True)
        subprocess.run(container_command("e2e", "all", []), cwd=ROOT, check=True)
    else:
        subprocess.run(container_command(args.command, args.selection, extra), cwd=ROOT, check=True)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode)
