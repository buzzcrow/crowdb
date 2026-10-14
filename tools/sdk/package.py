# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Build the sdist outside the repo and install its wheel without a compiler."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def run(command, **kwargs):
    print("SDK packaging:", " ".join(map(str, command)), flush=True)
    subprocess.run(command, check=True, timeout=600, **kwargs)


def package(destination):
    destination = Path(destination).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="crowdb-sdk-source-") as directory:
        source = Path(directory)
        run(["maturin", "sdist", "--manifest-path", str(ROOT / "sdk/crates/crowdb-sdk-python/Cargo.toml"), "--out", str(source)])
        archive, = source.glob("*.tar.gz")
        shutil.copy2(archive, destination)
        with tarfile.open(archive) as stream:
            stream.extractall(source / "unpacked", filter="data")
        project, = (source / "unpacked").iterdir()
        run(["cargo", "metadata", "--manifest-path", str(project / "Cargo.toml"), "--locked", "--no-deps", "--format-version", "1"], stdout=subprocess.DEVNULL)
        run(["maturin", "build", "--locked", "--interpreter", sys.executable, "--out", str(destination)], cwd=project)
    wheel, = destination.glob("*.whl")
    environment = destination / "consumer"
    run([sys.executable, "-m", "venv", "--without-pip", str(environment)])
    python = environment / "bin/python"
    # No compiler in the consumer PATH and no editable/source-tree installation.
    env = dict(os.environ, PATH=str(environment / "bin"))
    run([sys.executable, "-m", "pip", "--python", str(python), "install", "--no-index", "--no-deps", "--force-reinstall", str(wheel)], env=env, cwd=destination)
    run([str(python), "-c", "from crowdb_dataset import DatasetClient; from crowdb_dataset._native import NativeClient; print('installed native Dataset SDK')"], env=env, cwd=destination)
    return python


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination")
    args = parser.parse_args()
    if args.destination:
        package(args.destination)
    else:
        with tempfile.TemporaryDirectory(prefix="crowdb-sdk-consumer-") as directory:
            python = package(directory)
            tests = Path(directory) / "tests"
            shutil.copytree(ROOT / "sdk/tests", tests)
            shutil.copy2(ROOT / "sdk/test_iterable.py", tests)
            run([str(python), "-m", "unittest", "discover", "-s", str(tests), "-v"], cwd=directory)


if __name__ == "__main__":
    main()
