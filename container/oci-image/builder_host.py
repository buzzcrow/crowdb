# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Targeted AppArmor setup for the pinned rootless builder executable."""

import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


def profile_name(binary):
    identity = hashlib.sha256(str(Path(binary).resolve()).encode()).hexdigest()[:16]
    return 'crowdb-oci-rootlesskit-' + identity


def apparmor_profile(binary):
    binary = str(Path(binary).resolve())
    if any(ord(character) < 32 for character in binary):
        raise ValueError('RootlessKit path cannot contain control characters')
    attachment = re.sub(r'([\\"*?\[\]{}])', r'\\\1', binary)
    return ('# Allow user namespaces only for this Pixi RootlessKit executable.\n'
            'abi <abi/4.0>,\ninclude <tunables/global>\n\n'
            f'profile {profile_name(binary)} "{attachment}" flags=(unconfined) {{\n'
            '  userns,\n}\n')


def apparmor_denied(diagnostics):
    restriction = Path('/proc/sys/kernel/apparmor_restrict_unprivileged_userns')
    return (restriction.is_file() and restriction.read_text().strip() == '1'
            and 'apparmor_restrict_unprivileged_userns' in diagnostics
            and 'fork/exec /proc/self/exe: permission denied' in diagnostics)


def setup_apparmor(binary):
    parser, install = shutil.which('apparmor_parser'), shutil.which('install')
    if not parser or not install:
        raise RuntimeError('AppArmor setup requires host apparmor_parser and install')
    privilege = []
    if os.geteuid() != 0:
        sudo = shutil.which('sudo')
        if not sudo:
            raise RuntimeError('AppArmor setup requires sudo or administrator provisioning')
        privilege = [sudo] + ([] if sys.stdin.isatty() else ['-n']) + ['--']
    target = Path('/etc/apparmor.d') / profile_name(binary)
    print(f'RootlessKit is blocked by AppArmor; installing {target}. '
          'sudo may request your password; setup will then continue.', flush=True)
    with tempfile.TemporaryDirectory(prefix='crowdb-apparmor-') as directory:
        source = Path(directory) / target.name
        source.write_text(apparmor_profile(binary))
        commands = [[parser, '--skip-kernel-load', '--skip-cache', str(source)],
                    [*privilege, install, '-m', '0644', str(source), str(target)],
                    [*privilege, parser, '-r', str(target)]]
        for command in commands:
            # Inherit the terminal so sudo handles authentication itself.
            result = subprocess.run(command, timeout=60, check=False)
            if result.returncode:
                raise RuntimeError('AppArmor profile setup failed; see the command error above. '
                                   'Run pixi run oci-build in an interactive terminal for sudo authentication, '
                                   'or have an administrator provision the profile for unattended builds.')
