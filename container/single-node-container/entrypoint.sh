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

if test "$#" -gt 0; then
    test "$1" = server || { echo 'Expected server SERVICE [arguments]' >&2; exit 1; }
    shift
    service=${1:-}
    case "$service" in
        kv) program=crowdb-kv-server ;;
        diskdb) program=crowdb-diskdb ;;
        diskio) program=crowdb-diskio ;;
        chunkdb) program=crowdb-chunkdb ;;
        chunk-kv) program=crowdb-chunk-kv-server ;;
        access) program=crowdb-access-server ;;
        web) program=crowdb-web ;;
        *) echo 'Unknown server service' >&2; exit 1 ;;
    esac
    shift
    if test -e /opt/crowdb/data/accepted-node.json || test -e /opt/crowdb/data/bootstrap/manifest.json; then
        echo 'Standalone server mode requires a separate data root from a managed node' >&2
        exit 1
    fi
    mkdir -p "$CROWDB_CORE_DIR" "/opt/crowdb/data/log/$service"
    chown crowdb:crowdb /opt/crowdb/data /opt/crowdb/data/log "$CROWDB_CORE_DIR" "/opt/crowdb/data/log/$service"
    cd "$CROWDB_CORE_DIR"
    exec setpriv --reuid=crowdb --regid=crowdb --init-groups "/opt/crowdb/bin/$program" "$@"
fi

profile=${CROWDB_PROFILE_PATH:-/opt/crowdb/etc/profile.toml}
/opt/crowdb/bin/crowdb-monitor validate "$profile"

if test -n "${CROWDB_STARTUP_MODE:-}"; then
    echo 'CROWDB_STARTUP_MODE was removed; use CROWDB_DEPLOYMENT_MODE and CROWDB_INITIAL_VOTERS.' >&2
    exit 1
fi
accepted_mode=$(setpriv --reuid=crowdb --regid=crowdb --init-groups \
    /opt/crowdb/bin/crowdb-monitor accept-policy --profile "$profile")
CROWDB_DEPLOYMENT_MODE=$accepted_mode
export CROWDB_DEPLOYMENT_MODE
disks=/opt/crowdb/data/disks
if test -L "$disks" || { test -e "$disks" && ! test -d "$disks"; }; then
    echo 'Disk directory must be a real directory, not a symlink or file.' >&2
    exit 1
fi
mkdir -p "$disks"
chown -h crowdb:crowdb "$disks"
if test -L /opt/crowdb/data/ssh || { test -e /opt/crowdb/data/ssh && ! test -d /opt/crowdb/data/ssh; }; then
    echo 'SSH identity directory must be a real directory.' >&2
    exit 1
fi
mkdir -p /opt/crowdb/data/ssh
chmod 700 /opt/crowdb/data/ssh
ssh_password_auth=yes
if test -n "${CROWDB_SSH_PASSWORD_FILE:-}"; then
    test -f "$CROWDB_SSH_PASSWORD_FILE" || { echo 'SSH password file missing' >&2; exit 1; }
    CROWDB_SSH_PASSWORD=$(cat "$CROWDB_SSH_PASSWORD_FILE")
fi
if test "$accepted_mode" = local; then
    local_password=/opt/crowdb/data/ssh/local-admission-password
    if test -L "$local_password" || { test -e "$local_password" && ! test -f "$local_password"; }; then
        echo 'Local admission credential must be a regular private file.' >&2
        exit 1
    fi
    if test -f "$local_password"; then
        test -s "$local_password" && test "$(stat -c %s "$local_password")" -le 4096 &&
            test "$(stat -c %a "$local_password")" = 600 || {
                echo 'Retained local admission credential is empty, oversized or not private.' >&2
                exit 1
            }
    fi
    if test -z "${CROWDB_SSH_PASSWORD:-}" && test -f "$local_password"; then
        CROWDB_SSH_PASSWORD=$(cat "$local_password")
        test -n "$CROWDB_SSH_PASSWORD" || { echo 'Retained local admission credential is invalid.' >&2; exit 1; }
    fi
    CROWDB_SSH_PASSWORD=${CROWDB_SSH_PASSWORD:-crowdb}
    password_tmp=$(mktemp /opt/crowdb/data/ssh/.local-admission.XXXXXX)
    printf '%s' "$CROWDB_SSH_PASSWORD" > "$password_tmp"
    chmod 600 "$password_tmp"
    chown crowdb:crowdb "$password_tmp"
    mv "$password_tmp" "$local_password"
fi
if test -n "${CROWDB_SSH_PASSWORD:-}"; then
    printf 'crowdb:%s\n' "$CROWDB_SSH_PASSWORD" | chpasswd
    unset CROWDB_SSH_PASSWORD
elif test "$accepted_mode" = production; then
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
exec setpriv --reuid=crowdb --regid=crowdb --init-groups /opt/crowdb/bin/crowdb-monitor run \
    --profile "$profile" --interface "${CROWDB_MANAGEMENT_INTERFACE:-eth0}"
