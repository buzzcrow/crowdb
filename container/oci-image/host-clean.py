#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Remove only the recorded host container, preserving disks, data and receipts."""

import argparse
import os
from pathlib import Path

from host_cleanup import clean, selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster', required=True)
    parser.add_argument('--runtime-root', type=Path,
                        default=Path(os.environ.get('CROWDB_RUNTIME_ROOT',
                                     Path(__file__).resolve().parents[2] / '.crowdb-runtime')))
    args = parser.parse_args()
    try:
        clean(selected(args.cluster, args.runtime_root))
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(f'Removed owned host container for {args.cluster}; preserved data, media and receipts')


if __name__ == '__main__':
    main()
