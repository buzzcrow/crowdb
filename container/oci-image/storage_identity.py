# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Read-only host lineage for whole-device storage inputs."""

import fcntl
import os
import stat
import struct
from pathlib import Path


def resource_identity(source, metadata):
    """Collapse loop aliases to their backing inode; reject ambiguous devices."""
    if stat.S_ISREG(metadata.st_mode):
        return f"file:{metadata.st_dev}:{metadata.st_ino}"
    device = metadata.st_rdev
    sysfs = Path(f"/sys/dev/block/{os.major(device)}:{os.minor(device)}")
    if not sysfs.is_dir():
        raise ValueError(f"{source}: block-device lineage is unavailable")
    if (sysfs / "partition").exists() or any((sysfs / "slaves").iterdir()):
        raise ValueError(f"{source}: partition or stacked-device lineage is unsupported")
    if any((child / "partition").exists() for child in sysfs.iterdir() if child.is_dir()):
        raise ValueError(f"{source}: whole device contains partitions")
    descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISBLK(opened.st_mode) or opened.st_rdev != device:
            raise ValueError(f"{source}: device changed during lineage verification")
        if (sysfs / "loop").is_dir():
            return _loop_identity(descriptor)
        if not (sysfs / "device").exists():
            raise ValueError(f"{source}: virtual device has no verified backing lineage")
        return f"device:{device}"
    finally:
        os.close(descriptor)


def _loop_identity(descriptor):
    # Linux loop_info64: device/inode/rdevice/offset/sizelimit, then fixed fields.
    # https://github.com/torvalds/linux/blob/master/include/uapi/linux/loop.h
    status = fcntl.ioctl(descriptor, 0x4C05, bytes(232))  # LOOP_GET_STATUS64
    device, inode, raw_backing, offset, size_limit = struct.unpack_from("=5Q", status)
    if not inode or raw_backing or offset or size_limit:
        raise ValueError("loop device must map one complete backing file")
    return f"file:{device}:{inode}"
