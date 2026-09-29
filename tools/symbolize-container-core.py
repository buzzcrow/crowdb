#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Show source-line stacks from a private core and exact image symbol asset."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import tempfile


def output(*command: str) -> str:
    return subprocess.check_output(command, text=True).strip()


def extract_symbols(archive: Path, destination: Path) -> None:
    process = subprocess.Popen(["zstd", "-dc", str(archive)], stdout=subprocess.PIPE)
    try:
        assert process.stdout is not None
        with tarfile.open(fileobj=process.stdout, mode="r|") as bundle:
            for member in bundle:
                name = member.name.removeprefix("./")
                if name in {"", ".", "bin", "lib"} and member.isdir():
                    continue
                path = PurePosixPath(name)
                valid = (
                    name in {"VERSION", "SOURCE_REVISION", "RUNTIME_SHA256SUMS"}
                    or (len(path.parts) == 2 and path.parts[0] in {"bin", "lib"} and path.name.endswith(".debug"))
                )
                if not valid or not member.isfile() or path.is_absolute() or ".." in path.parts:
                    raise ValueError("unexpected symbol archive entry")
                target = destination.joinpath(*path.parts)
                target.parent.mkdir(exist_ok=True)
                source = bundle.extractfile(member)
                if source is None:
                    raise ValueError("missing symbol archive contents")
                with source, target.open("wb") as saved:
                    shutil.copyfileobj(source, saved)
        if process.wait() != 0:
            raise ValueError("symbol archive decompression failed")
    except BaseException:
        process.kill()
        process.wait()
        raise
    finally:
        if process.stdout is not None:
            process.stdout.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="Exact Docker image tag or digest")
    parser.add_argument("--symbols", type=Path, required=True, help="Matching release symbol archive")
    parser.add_argument("--binary", required=True, help="Crashed binary name, such as crowdb-monitor")
    parser.add_argument("--core", type=Path, required=True, help="Privately exported core file")
    args = parser.parse_args()
    if Path(args.binary).name != args.binary or not args.binary.startswith("crowdb-"):
        parser.error("--binary must be a CROWDB binary name")
    if not args.core.is_file() or not args.symbols.is_file():
        parser.error("core and symbol archive must be readable files")

    labels = json.loads(output("docker", "image", "inspect", args.image))[0]["Config"]["Labels"]
    revision = labels["org.opencontainers.image.revision"]
    version = labels["org.opencontainers.image.version"]
    with tempfile.TemporaryDirectory(prefix="crowdb-core-") as temporary:
        root = Path(temporary)
        root.chmod(0o700)
        runtime = root / "runtime"
        runtime.mkdir()
        container = output("docker", "create", "--entrypoint", "/bin/true", args.image)
        try:
            for directory in ("bin", "lib"):
                subprocess.run(
                    ["docker", "cp", f"{container}:/opt/crowdb/{directory}", str(runtime / directory)],
                    check=True,
                )
        finally:
            subprocess.run(["docker", "rm", container], check=True, capture_output=True)
        symbols = root / "symbols"
        symbols.mkdir()
        extract_symbols(args.symbols.resolve(), symbols)
        if (symbols / "SOURCE_REVISION").read_text().strip() != revision:
            raise ValueError("symbol source revision differs from image")
        if (symbols / "VERSION").read_text().strip() != version:
            raise ValueError("symbol version differs from image")
        for line in (symbols / "RUNTIME_SHA256SUMS").read_text().splitlines():
            expected, relative = line.split(maxsplit=1)
            relative = relative.lstrip("*")
            path = Path(relative)
            if path.is_absolute() or path.parts[0] not in {"bin", "lib"} or ".." in path.parts:
                raise ValueError("invalid runtime checksum path")
            actual = hashlib.sha256((runtime / path).read_bytes()).hexdigest()
            if actual != expected:
                raise ValueError(f"image binary differs from symbols: {relative}")
        binary = runtime / "bin" / args.binary
        if not binary.is_file():
            parser.error("crashed binary is absent from the image")
        if not (symbols / "bin" / f"{args.binary}.debug").is_file():
            raise ValueError("exact debug symbols for the crashed binary are absent")
        for directory in ("bin", "lib"):
            for debug in (symbols / directory).glob("*.debug"):
                if debug.is_symlink() or not debug.is_file():
                    raise ValueError("invalid symbol archive entry")
                shutil.copyfile(debug, runtime / directory / debug.name)
        print(f"Verified image version {version}, revision {revision}", flush=True)
        subprocess.run(
            [
                "gdb", "-q", "-batch", "-ex", "set pagination off",
                "-ex", "set print frame-arguments none",
                "-ex", f"set solib-search-path {runtime / 'lib'}",
                "-ex", "thread apply all bt", str(binary), str(args.core.resolve()),
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
