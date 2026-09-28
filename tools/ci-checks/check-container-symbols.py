#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Verify the symbol archive matches the staged release runtime exactly."""

import re
import struct
import subprocess
import tempfile
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "target/container-runtime"
SYMBOLS = ROOT / "target/container-symbols"


def sections(path: Path) -> str:
    return subprocess.run(
        ["readelf", "-W", "-S", str(path)], check=True, text=True,
        capture_output=True,
    ).stdout


def check_debuglink(binary: Path, symbol: Path) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        section = Path(temp_dir) / "debuglink"
        subprocess.run(
            ["objcopy", f"--dump-section=.gnu_debuglink={section}", str(binary)],
            check=True,
        )
        data = section.read_bytes()
    name, _, _ = data.partition(b"\0")
    offset = (len(name) + 4) & ~3
    if name.decode() != symbol.name or len(data) < offset + 4:
        raise ValueError(f"Wrong debuglink name or size: {binary}")
    expected_crc = struct.unpack_from("<I", data, offset)[0]
    actual_crc = zlib.crc32(symbol.read_bytes())
    if expected_crc != actual_crc:
        raise ValueError(f"Debuglink CRC mismatch: {binary} and {symbol}")


def main() -> None:
    for metadata in ("VERSION", "SOURCE_REVISION"):
        if (RUNTIME / metadata).read_bytes() != (SYMBOLS / metadata).read_bytes():
            raise ValueError(f"Runtime and symbol {metadata} differ")
    subprocess.run(
        ["sha256sum", "--check", str(SYMBOLS / "RUNTIME_SHA256SUMS")],
        cwd=RUNTIME, check=True,
    )
    binaries = sorted((RUNTIME / "bin").iterdir())
    libraries = sorted((RUNTIME / "lib").glob("libcrowdb*.so"))
    for binary in binaries + libraries:
        symbol = SYMBOLS / binary.parent.name / f"{binary.name}.debug"
        if not symbol.is_file():
            raise ValueError(f"Missing debug symbols: {symbol}")
        if not re.search(r"\s\.debug_line\s", sections(symbol)):
            raise ValueError(f"Missing source lines: {symbol}")
        if re.search(r"\s\.debug_line\s", sections(binary)):
            raise ValueError(f"Runtime still has source lines: {binary}")
        check_debuglink(binary, symbol)
        print(f"Verified symbols for {binary.relative_to(RUNTIME)}")


if __name__ == "__main__":
    main()
