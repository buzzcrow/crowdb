#!/bin/sh
set -eu

if ! grep -q ' /opt/crowdb/data ' /proc/self/mountinfo; then
    echo 'CROWDB preview requires one volume mounted at /opt/crowdb/data' >&2
    exit 1
fi

echo 'CROWDB preview data: Docker creates an anonymous volume when none is specified. For data you want to keep across container recreation, use --mount type=volume,source=crowdb-data,target=/opt/crowdb/data.'

CROWDB_CORE_DIR=/opt/crowdb/data/crash
export CROWDB_CORE_DIR

case "$(cat /proc/sys/kernel/core_pattern)" in
    core|core.*)
        if [ "$(ulimit -c)" = 0 ]; then
            echo 'CROWDB core files require a nonzero Docker --ulimit core setting.' >&2
        fi
        ;;
    '|'*)
        echo 'CROWDB core dumps use the Docker host collector; inspect the host for dumps.' >&2
        ;;
    *)
        echo 'CROWDB core_pattern does not use a relative core filename; inspect the Docker host for dumps.' >&2
        ;;
esac

exec /opt/crowdb/bin/crowdb-monitor run --profile /opt/crowdb/etc/profile.toml
