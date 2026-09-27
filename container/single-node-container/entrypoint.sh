#!/bin/sh
set -eu

if ! grep -q ' /opt/crowdb/data ' /proc/self/mountinfo; then
    echo 'CROWDB preview requires one volume mounted at /opt/crowdb/data' >&2
    exit 1
fi

echo 'CROWDB preview data: Docker creates an anonymous volume when none is specified. For data you want to keep across container recreation, use --mount type=volume,source=crowdb-data,target=/opt/crowdb/data.'

exec /opt/crowdb/bin/crowdb-monitor run --profile /opt/crowdb/etc/profile.toml
