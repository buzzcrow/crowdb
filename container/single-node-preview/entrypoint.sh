#!/bin/sh
set -eu

if ! grep -q ' /opt/crowdb/data ' /proc/self/mountinfo; then
    echo 'CROWDB preview requires one volume mounted at /opt/crowdb/data' >&2
    exit 1
fi

exec /opt/crowdb/bin/crowdb-monitor run --profile /opt/crowdb/etc/profile.toml
