#!/bin/sh
set -eu
package_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
task_pg_config=${PG_CONFIG:-pg_config}
task_destination_root=${DESTDIR:-}
task_recovery=no
case "${1:-}" in '') ;; --rollback) task_recovery=rollback;; --finish) task_recovery=finish;; --discard) task_recovery=discard;; *) echo 'Usage: install.sh [--rollback|--finish|--discard]' >&2;exit 1;; esac
test "$#" -le 1 || { echo 'Usage: install.sh [--rollback|--finish|--discard]' >&2;exit 1; }
command -v flock >/dev/null || { echo 'The optional installer requires flock' >&2;exit 1; }
(cd "$package_root" && sha256sum -c --quiet SHA256SUMS)
test "$($task_pg_config --version)" = "$(cat "$package_root/POSTGRES_VERSION")" || {
    echo 'The target PostgreSQL build differs from this qualified native prototype' >&2;exit 1;
}
case "$(cat "$package_root/TARGETARCH"):$(uname -m)" in arm64:aarch64|amd64:x86_64) ;; *) echo 'Bundle architecture mismatch' >&2;exit 1;; esac
task_shared=$($task_pg_config --sharedir)/extension
task_library=$($task_pg_config --pkglibdir)
task_component=$(cat "$package_root/EXTENSION")
case "$task_component" in lake_replication_capture|lake_replication_push) ;; *) echo 'Unknown optional component' >&2;exit 1;; esac
if [ -z "$task_destination_root" ]; then
    : "${PGDATA:?Set PGDATA to the stopped target server data directory}"
    test -f "$PGDATA/PG_VERSION" && test "$(cat "$PGDATA/PG_VERSION")" = 18 || {
        echo 'PGDATA must identify the PostgreSQL 18 target server' >&2;exit 1;
    }
    if [ -s "$PGDATA/postmaster.pid" ]; then
        task_postmaster_pid=$(head -n 1 "$PGDATA/postmaster.pid")
        case "$task_postmaster_pid" in ''|*[!0-9]*) echo 'Invalid postmaster PID file' >&2;exit 1;; esac
        if [ -d "/proc/$task_postmaster_pid" ] || kill -0 "$task_postmaster_pid" 2>/dev/null; then
            echo 'Stop the target server before replacing optional workers or libraries' >&2;exit 1
        fi
    fi
    cmp -s "$package_root/PG_LAKE_RUNTIME.json" "$($task_pg_config --sharedir)/pg_lake/lake-replication-patchset.json" || {
        echo 'The target pg_lake runtime marker differs from the qualified prerequisite' >&2;exit 1;
    }
    for task_dependency in pg_lake pg_extension_base; do
        test -f "$task_shared/$task_dependency.control" || { echo "Missing $task_dependency prerequisite" >&2;exit 1; }
    done
    if [ "$task_component" = lake_replication_push ]; then
        test -f "$task_shared/lake_replication.control" || { echo 'Install the matching default destination bundle first' >&2;exit 1; }
        sha256sum -c --quiet "$package_root/DEFAULT_DESTINATION_SHA256SUMS" || {
            echo 'The default destination workers or extension differ from the qualified prerequisite' >&2;exit 1;
        }
    fi
fi
install -d "$task_destination_root/usr/local/bin" "$task_destination_root$task_shared" "$task_destination_root$task_library"
task_lock="$task_destination_root$task_library/.lake-push-install.lock"
if [ "$task_recovery" = no ]; then
    mkdir -m 700 "$task_lock" || { echo 'Another optional installation or interrupted recovery holds the installation lock' >&2;exit 1; }
    exec 9>"$task_lock/actor"
else
    test -d "$task_lock" && test ! -L "$task_lock" && test -f "$task_lock/actor" && test ! -L "$task_lock/actor" || {
        echo 'No recoverable installation journal; inspect the retained installation lock' >&2;exit 1;
    }
    exec 9<>"$task_lock/actor"
fi
# Children inherit this descriptor, so a surviving copy/rename also prevents repair.
flock -n 9 || { echo 'An active installer or child still holds the installation lock' >&2;exit 1; }
task_count=0
task_success=no
task_rollback_failed=no
task_finish() {
    task_exit=$?
    trap - 0 HUP INT TERM
    task_index=$task_count
    while [ "$task_index" -gt 0 ]; do
        task_item="$task_lock/$task_index"
        task_target=$(cat "$task_item/target")
        task_stage=$(cat "$task_item/stage")
        if [ "$task_success" != yes ] && [ -f "$task_item/publishing" ]; then
            if [ -f "$task_stage/old" ]; then
                # Keep the backup until every restoration succeeds. A second
                # recovery attempt must not mistake an already restored file
                # for a target which did not previously exist.
                if task_restore=$(mktemp "$task_stage/restore.XXXXXX") &&
                    cp -p "$task_stage/old" "$task_restore" && mv -f "$task_restore" "$task_target"; then :;else
                    task_rollback_failed=yes
                fi
            else
                rm -f "$task_target" || task_rollback_failed=yes
            fi
        fi
        task_index=$((task_index-1))
    done
    if [ "$task_rollback_failed" = yes ]; then
        echo "Installation rollback needs repair; recovery files remain in $task_lock. Keep the server stopped." >&2
        exit 1
    fi
    task_index=$task_count
    while [ "$task_index" -gt 0 ]; do
        task_stage=$(cat "$task_lock/$task_index/stage")
        rm -rf "$task_stage"
        task_index=$((task_index-1))
    done
    rm -rf "$task_lock"
    if [ "$task_recovery" != no ]; then
        if [ "$task_recovery" = discard ]; then
            echo 'Discarded unpublished optional installation staging data.'
        elif [ "$task_recovery" = finish ]; then
            echo 'Verified the committed optional installation and finished staging cleanup.'
        else
            echo 'Restored the previous optional installation. Keep the server stopped until prerequisites and extension versions are verified.'
        fi
    fi
    exit "$task_exit"
}
task_package_hash=$(sha256sum "$package_root/SHA256SUMS" | cut -d ' ' -f 1)
if [ "$task_recovery" != no ]; then
    # Validate every entry before restoring files or removing staging data.
    test "$(cat "$task_lock/package")" = "$task_package_hash" &&
        test "$(cat "$task_lock/component")" = "$task_component" || {
            echo 'Installation journal is incomplete or belongs to another bundle; inspect it before repair' >&2;exit 1;
        }
    if [ "$task_recovery" = discard ]; then
        test "$(cat "$task_lock/plan_format")" = 2 &&
            test ! -e "$task_lock/prepared" && test ! -e "$task_lock/committed" || {
                echo 'Only an unpublished preparation journal can be discarded' >&2;exit 1;
            }
        if [ ! -e "$task_lock/planned" ]; then
            # Format 2 records the entire plan before any external stage exists.
            rm -rf "$task_lock"
            echo 'Discarded interrupted installation planning; no payload was staged.'
            exit 0
        fi
    elif [ "$task_recovery" = finish ]; then
        test -f "$task_lock/committed" || { echo 'Only a committed installation can finish cleanup' >&2;exit 1; }
    else
        test -f "$task_lock/prepared" || { echo 'Unpublished preparation requires --discard' >&2;exit 1; }
        test ! -e "$task_lock/committed" || { echo 'Committed installation requires --finish, not --rollback' >&2;exit 1; }
    fi
    task_count=$(cat "$task_lock/count")
    case "$task_count" in ''|*[!0-9]*) echo 'Invalid installation journal count' >&2;exit 1;; esac
    test "$task_count" -gt 0 && test "$task_count" -le 64 || { echo 'Invalid installation journal count' >&2;exit 1; }
    if [ "$task_recovery" != rollback ]; then
        task_expected_count=0
        for task_source in "$package_root"/payload/bin/* "$package_root"/payload/lib/* "$package_root"/payload/extension/*; do
            task_expected_count=$((task_expected_count+1))
        done
        test "$task_count" -eq "$task_expected_count" || { echo 'Installation journal has an incomplete payload inventory' >&2;exit 1; }
    fi
    task_index=1
    task_seen=''
    while [ "$task_index" -le "$task_count" ]; do
        task_item="$task_lock/$task_index"
        test -d "$task_item" && test ! -L "$task_item" &&
            test -f "$task_item/target" && test ! -L "$task_item/target" &&
            test -f "$task_item/stage" && test ! -L "$task_item/stage" || { echo 'Invalid installation journal entry' >&2;exit 1; }
        task_target=$(cat "$task_item/target")
        task_stage=$(cat "$task_item/stage")
        task_allowed=no
        task_expected=
        task_expected_mode=755
        for task_source in "$package_root"/payload/bin/*; do
            if [ "$task_target" = "$task_destination_root/usr/local/bin/$(basename "$task_source")" ]; then task_allowed=yes;task_expected=$task_source;fi
        done
        for task_source in "$package_root"/payload/extension/*; do
            if [ "$task_target" = "$task_destination_root$task_shared/$(basename "$task_source")" ]; then task_allowed=yes;task_expected=$task_source;task_expected_mode=644;fi
        done
        if [ "$task_target" = "$task_destination_root$task_library/$task_component.so" ]; then task_allowed=yes;task_expected="$package_root/payload/lib/$task_component.so";fi
        task_stage_suffix=${task_stage#"$(dirname "$task_target")/.lake-push-stage."}
        case "$task_stage_suffix" in ''|*[!a-zA-Z0-9]*) task_allowed=no;; esac
        test "$task_allowed" = yes &&
            test "$task_stage" = "$(dirname "$task_target")/.lake-push-stage.$task_stage_suffix" &&
            test ! -L "$task_stage" &&
            { test -d "$task_stage" || { [ "$task_recovery" != rollback ] && [ ! -e "$task_stage" ]; }; } &&
            test ! -L "$task_target" && { test ! -e "$task_target" || test -f "$task_target"; } || {
                echo 'Unsafe or missing installation recovery target/stage' >&2;exit 1;
            }
        if [ "$task_recovery" = discard ]; then
            test ! -e "$task_item/publishing" || { echo 'Published installation cannot discard preparation' >&2;exit 1; }
        elif [ "$task_recovery" = finish ]; then
            test -f "$task_target" && cmp -s "$task_expected" "$task_target" &&
                test "$(stat -c %a "$task_target")" = "$task_expected_mode" || {
                    echo 'Committed installation payload failed verification; retain the journal for inspection' >&2;exit 1;
                }
        elif [ -f "$task_stage/old-present" ]; then
            test ! -L "$task_stage/old-present" && test -f "$task_stage/old.sha256" &&
                test ! -L "$task_stage/old.sha256" && test -f "$task_stage/old" && test ! -L "$task_stage/old" &&
                test "$(cat "$task_stage/old.sha256")" = "$(cd "$task_stage" && sha256sum old)" || {
                    echo 'Installation recovery backup failed verification' >&2;exit 1;
                }
        elif [ -e "$task_stage/old" ] || [ -L "$task_stage/old" ] || [ -L "$task_stage/old-present" ]; then
            echo 'Installation recovery backup has no presence marker' >&2;exit 1
        fi
        task_seen="$task_seen
$task_target"
        task_index=$((task_index+1))
    done
    test -z "$(printf '%s\n' "$task_seen" | sort | uniq -d)" || { echo 'Duplicate installation recovery target' >&2;exit 1; }
    if [ "$task_recovery" != rollback ]; then task_success=yes;fi
    trap task_finish 0
    trap 'exit 1' HUP INT TERM
    exit 0
fi
printf '%s\n' "$task_package_hash" > "$task_lock/package"
printf '%s\n' "$task_component" > "$task_lock/component"
printf '%s\n' 2 > "$task_lock/plan_format"
trap task_finish 0
trap 'exit 1' HUP INT TERM
# Allocate a nonce inside the journal; all external stage paths are recorded
# before their directories can exist, including an interrupted mkdir/copy.
task_nonce=$(mktemp -d "$task_lock/id.XXXXXX")
task_nonce=${task_nonce##*/id.}
task_plan() {
    task_source=$1
    task_target=$2
    task_mode=$3
    if [ -L "$task_target" ] || { [ -e "$task_target" ] && [ ! -f "$task_target" ]; }; then
        echo "Refusing nonregular installation target: $task_target" >&2;exit 1
    fi
    task_item="$task_lock/$((task_count+1))"
    mkdir "$task_item"
    printf '%s\n' "$task_target" > "$task_item/target"
    task_stage="$(dirname "$task_target")/.lake-push-stage.$task_nonce$((task_count+1))"
    printf '%s\n' "$task_stage" > "$task_item/stage"
    printf '%s\n' "$task_source" > "$task_item/source"
    printf '%s\n' "$task_mode" > "$task_item/mode"
    task_count=$((task_count+1))
}
task_prepare() {
    task_target=$(cat "$task_item/target")
    task_stage=$(cat "$task_item/stage")
    task_source=$(cat "$task_item/source")
    task_mode=$(cat "$task_item/mode")
    mkdir -m 700 "$task_stage"
    if [ -f "$task_target" ]; then
        cp -p "$task_target" "$task_stage/old"
        (cd "$task_stage" && sha256sum old > old.sha256)
        touch "$task_stage/old-present"
    fi
    install -m "$task_mode" "$task_source" "$task_stage/new"
}
for task_binary in "$package_root"/payload/bin/*; do task_plan "$task_binary" "$task_destination_root/usr/local/bin/$(basename "$task_binary")" 0755;done
task_plan "$package_root/payload/lib/$task_component.so" "$task_destination_root$task_library/$task_component.so" 0755
for task_file in "$package_root"/payload/extension/*; do task_plan "$task_file" "$task_destination_root$task_shared/$(basename "$task_file")" 0644;done
printf '%s\n' "$task_count" > "$task_lock/count"
touch "$task_lock/planned"
task_index=1
while [ "$task_index" -le "$task_count" ]; do
    task_item="$task_lock/$task_index"
    task_prepare
    task_index=$((task_index+1))
done
# Every file is prepared before replacing anything. Same-filesystem renames
# preserve individual-file atomicity; ordinary errors/signals restore prior
# contents and modes. A completed preparation journal permits explicit rollback
# after SIGKILL. Committed payloads permit verified cleanup with --finish;
# --discard removes unpublished format-2 plans; power loss and interrupted
# journal deletion still need inspection.
printf '%s\n' "$task_count" > "$task_lock/count"
touch "$task_lock/prepared"
task_index=1
while [ "$task_index" -le "$task_count" ]; do
    task_item="$task_lock/$task_index"
    task_target=$(cat "$task_item/target")
    task_stage=$(cat "$task_item/stage")
    touch "$task_item/publishing"
    mv -f "$task_stage/new" "$task_target"
    task_index=$((task_index+1))
done
touch "$task_lock/committed"
task_success=yes
printf '%s\n' "Installed optional $task_component files. Configure the prerequisites and create the extension explicitly after server restart."
