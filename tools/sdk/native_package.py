# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Build and install the optional main-workspace native Dataset wheel."""
import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def run(command, **kwargs):
    print("Native packaging:", " ".join(map(str, command)), flush=True)
    subprocess.run(command, check=True, timeout=900, **kwargs)


def package(destination):
    destination = Path(destination).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    project = ROOT / "lib/crowdb-sdk-native-python"
    run(["maturin", "build", "--release", "--locked", "--interpreter", sys.executable,
         "--out", str(destination)], cwd=project)
    wheel, = destination.glob("crowdb_dataset_native-*.whl")
    environment = destination / "consumer"
    run([sys.executable, "-m", "venv", "--without-pip", str(environment)])
    python = environment / "bin/python"
    env = dict(os.environ, PATH=str(environment / "bin"))
    run([sys.executable, "-m", "pip", "--python", str(python), "install", "--no-index",
         "--no-deps", "--force-reinstall", str(wheel)], env=env, cwd=destination)
    run([str(python), "-c", "import crowdb_dataset_native as n; print(n.NativeRuntime, n.Lease)"], env=env, cwd=destination)
    return python


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", required=True)
    package(parser.parse_args().destination)
