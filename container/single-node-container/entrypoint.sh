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

mode=${CROWDB_STARTUP_MODE:-manual}
case "$mode" in single|manual) ;; *) echo 'CROWDB_STARTUP_MODE must be single or manual' >&2; exit 1 ;; esac
previous=
if test -f /opt/crowdb/data/accepted-node.json; then
    previous=manual
elif test -f /opt/crowdb/data/bootstrap/manifest.json; then
    previous=single
fi
if test -n "$previous" && test "$previous" != "$mode"; then
    echo 'Startup mode differs from persistent node policy; restore the original mode.' >&2
    exit 1
fi
if test "$mode" = manual && test -z "${CROWDB_PHYSICAL_HOST_ID:-}"; then
    echo 'Manual mode requires CROWDB_PHYSICAL_HOST_ID' >&2
    exit 1
fi
ssh_password_auth=yes
if test -n "${CROWDB_SSH_PASSWORD_FILE:-}"; then
    test -f "$CROWDB_SSH_PASSWORD_FILE" || { echo 'SSH password file missing' >&2; exit 1; }
    CROWDB_SSH_PASSWORD=$(cat "$CROWDB_SSH_PASSWORD_FILE")
fi
if test -n "${CROWDB_SSH_PASSWORD:-}"; then
    printf 'crowdb:%s\n' "$CROWDB_SSH_PASSWORD" | chpasswd
    unset CROWDB_SSH_PASSWORD
elif test "${CROWDB_DEPLOYMENT_MODE:-test}" = production; then
    if ! test -s /opt/crowdb/data/ssh/authorized_keys; then
        echo 'Production requires explicit SSH credentials or preinstalled authorized_keys' >&2
        exit 1
    fi
    passwd -d crowdb
    ssh_password_auth=no
fi
mkdir -p /opt/crowdb/data/ssh /opt/crowdb/run
chmod 700 /opt/crowdb/data/ssh
for name in id_ed25519 ssh_host_ed25519_key; do
    if ! test -f "/opt/crowdb/data/ssh/$name"; then
        ssh-keygen -q -t ed25519 -N '' -f "/opt/crowdb/data/ssh/$name"
    fi
done
if ! test -f /opt/crowdb/data/ssh/authorized_keys; then
    cp /opt/crowdb/data/ssh/id_ed25519.pub /opt/crowdb/data/ssh/authorized_keys
fi
chmod 600 /opt/crowdb/data/ssh/authorized_keys
chown -R crowdb:crowdb /opt/crowdb/data/ssh /opt/crowdb/run
if ! test -e /opt/crowdb/.ssh; then ln -s /opt/crowdb/data/ssh /opt/crowdb/.ssh; fi
cat > /opt/crowdb/run/sshd_config <<EOF
Port 2222
HostKey /opt/crowdb/data/ssh/ssh_host_ed25519_key
AuthorizedKeysFile /opt/crowdb/data/ssh/authorized_keys
PermitRootLogin no
AllowUsers crowdb
PasswordAuthentication $ssh_password_auth
KbdInteractiveAuthentication no
UsePAM no
PidFile /opt/crowdb/run/sshd.pid
Subsystem sftp internal-sftp
EOF
/usr/sbin/sshd -f /opt/crowdb/run/sshd_config
if test "$mode" = manual; then
    test -n "${CROWDB_PHYSICAL_HOST_ID:-}" || { echo 'Manual mode requires CROWDB_PHYSICAL_HOST_ID' >&2; exit 1; }
    exec setpriv --reuid=crowdb --regid=crowdb --init-groups /opt/crowdb/bin/crowdb-monitor run \
        --profile /opt/crowdb/etc/profile.toml --mode manual --interface "${CROWDB_MANAGEMENT_INTERFACE:-eth0}" --physical-host-id "$CROWDB_PHYSICAL_HOST_ID"
fi
exec setpriv --reuid=crowdb --regid=crowdb --init-groups /opt/crowdb/bin/crowdb-monitor run --profile /opt/crowdb/etc/profile.toml --mode single
