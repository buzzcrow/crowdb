# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Delete recorded host node data without following links or supplied media."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import stat

from host_cleanup import selected, selected_records

MARKER = 'container-storage.json'


def mount_paths():
    # ismount does not detect bind mounts on the same filesystem.
    return {Path(re.sub(r'\\([0-7]{3})', lambda match: chr(int(match[1], 8)), line.split()[4]))
            for line in Path('/proc/self/mountinfo').read_text().splitlines()}


class HostDataCleanup:
    def __init__(self, record, runtime_root):
        self.record = record
        self.runtime_root = runtime_root
        self.path = Path(record['options']['data_root'])
        if (not self.path.is_absolute() or self.path.parent == self.path
                or '..' in self.path.parts
                or any(path.is_symlink() for path in (self.path, *self.path.parents))):
            raise ValueError('host data directory must be an absolute non-root path without symlinks')
        for protected in (runtime_root.resolve(), Path(__file__).resolve().parents[2], Path.home()):
            if protected == self.path or self.path in protected.parents:
                raise ValueError('host data directory overlaps runtime receipts, repository or home')
        self.protected = set()
        for other in selected_records(None, runtime_root):
            other_root = Path(other['options']['data_root'])
            if other['cluster'] != record['cluster'] and (self.path == other_root
                    or self.path in other_root.parents or other_root in self.path.parents):
                raise ValueError('host data directories overlap between recorded clusters')
            paths = [disk['source'] for disk in other['disks']]
            paths += [other['options'].get(key) for key in ('node_policy_file', 'password_file')]
            self.protected.update(Path(path) for path in paths if path)
        self.metadata = self.path.stat() if self.path.exists() else None
        if self.metadata is not None:
            with self.opened() as descriptor:
                self.validate(descriptor)

    @contextmanager
    def opened(self):
        if any(path.is_symlink() for path in (self.path, *self.path.parents)):
            raise ValueError('host data directory path changed to a symlink')
        descriptor = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            observed = os.fstat(descriptor)
            if (observed.st_dev, observed.st_ino) != (self.metadata.st_dev, self.metadata.st_ino):
                raise ValueError('host data directory identity changed before cleanup')
            yield descriptor
        finally:
            os.close(descriptor)

    def validate(self, descriptor):
        try:
            marker = os.open(MARKER, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                             dir_fd=descriptor)
        except FileNotFoundError:
            if self.walk(descriptor, self.path, mount_paths(), remove=False):
                raise ValueError('nonempty host data directory has no retained storage declaration')
            return
        try:
            metadata = os.fstat(marker)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 1024 * 1024:
                raise ValueError('host storage declaration must be a bounded regular file')
            value = json.loads(os.read(marker, 1024 * 1024 + 1))
            if value != dict(version=1, disks=self.record['disks']):
                raise ValueError('host storage declaration differs from accepted media')
        finally:
            os.close(marker)
        self.walk(descriptor, self.path, mount_paths(), remove=False)

    def walk(self, descriptor, path, mounts, remove):
        removable = False
        with os.scandir(descriptor) as entries:
            for entry in entries:
                current = path / entry.name
                if current in self.protected or (path == self.path and entry.name == MARKER):
                    continue
                metadata = entry.stat(follow_symlinks=False)
                if current in mounts or metadata.st_dev != self.metadata.st_dev:
                    raise ValueError(f'host data contains a nested mount: {current}')
                if stat.S_ISDIR(metadata.st_mode):
                    child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                    dir_fd=descriptor)
                    try:
                        removable |= self.walk(child, current, mounts, remove)
                    finally:
                        os.close(child)
                    if not any(current in protected.parents for protected in self.protected):
                        removable = True
                        if remove:
                            os.rmdir(entry.name, dir_fd=descriptor)
                else:
                    if stat.S_ISBLK(metadata.st_mode) or stat.S_ISCHR(metadata.st_mode):
                        raise ValueError(f'host data contains an unrecorded device: {current}')
                    removable = True
                    if remove:
                        os.unlink(entry.name, dir_fd=descriptor)
        return removable

    def remove(self):
        if selected(self.record['cluster'], self.runtime_root) != self.record:
            raise ValueError('host receipt changed before data cleanup')
        if self.metadata is not None:
            with self.opened() as descriptor:
                self.validate(descriptor)
                self.walk(descriptor, self.path, mount_paths(), remove=True)
                try:
                    os.unlink(MARKER, dir_fd=descriptor)
                except FileNotFoundError:
                    pass
            if (self.path not in mount_paths()
                    and not any(self.path in path.parents for path in self.protected)):
                self.path.rmdir()
        elif self.path.exists() or self.path.is_symlink():
            raise ValueError('host data directory appeared after cleanup selection')
        # Keep launch ownership retryable until the data and optional binding are gone.
        if selected(self.record['cluster'], self.runtime_root) != self.record:
            raise ValueError('host receipt changed before retiring ownership')
        binding = self.runtime_root / 'persistent/clusters' / self.record['cluster'] / 'target.json'
        binding.unlink(missing_ok=True)
        receipt = self.runtime_root / 'persistent/oci-hosts' / (self.record['cluster'] + '.json')
        receipt.unlink()
