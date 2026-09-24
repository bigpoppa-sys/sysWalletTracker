#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${SYS_TRACKER_APP_DIR:-$HOME/sysWalletTracker}"
ENV_FILE="$APP_DIR/.env"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/sn_comp_snapshot_cron.log"
LOCK_FILE="$APP_DIR/.sn-comp-snapshot.lock"
PUBLIC_DIR="${SYS_TRACKER_PUBLIC_DIR:-/var/www/html/syswallettracker}"

mkdir -p "$LOG_DIR"

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  flock -n 9 || exit 0
else
  LOCK_DIR="$APP_DIR/.sn-comp-snapshot.lockdir"
  mkdir "$LOCK_DIR" 2>/dev/null || exit 0
  trap 'rmdir "$LOCK_DIR"' EXIT
fi

cd "$APP_DIR"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

: "${SYS_BLOCKBOOK_URL:=https://explorer-blockbook.syscoin.org}"
: "${SYS_SYSNODE_MNLIST_URL:=https://sysnode.info/mnlist}"
: "${SYS_SYSNODE_TIME_LOOKUP_LIMIT:=900}"
: "${SYS_SN_COMP_REFRESH_SECONDS:=0}"

RPC_ARGS=()
if [ -n "${SYS_RPC_URL:-}" ] || [ -n "${SYS_RPC_HOST:-}" ] || { [ -n "${SYS_RPC_USER:-}" ] && [ -n "${SYS_RPC_PASSWORD:-}" ]; }; then
  : "${SYS_RPC_URL:=http://127.0.0.1:8370/}"
  : "${SYS_RPC_USER:?SYS_RPC_USER is required when SYS_RPC_URL/SYS_RPC_HOST is configured}"
  : "${SYS_RPC_PASSWORD:?SYS_RPC_PASSWORD is required when SYS_RPC_URL/SYS_RPC_HOST is configured}"
  RPC_ARGS=(--rpc-url "$SYS_RPC_URL" --rpc-user "$SYS_RPC_USER" --rpc-password "$SYS_RPC_PASSWORD")
fi

{
  printf '\n[%s] SN Comp snapshot sync\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  mkdir -p "$PUBLIC_DIR"
  python3 syscoin_tracker.py \
    "${RPC_ARGS[@]}" \
    --blockbook-url "$SYS_BLOCKBOOK_URL" \
    --sysnode-mnlist-url "$SYS_SYSNODE_MNLIST_URL" \
    --sysnode-time-lookup-limit "$SYS_SYSNODE_TIME_LOOKUP_LIMIT" \
    publish-sn-comp \
    --output-dir "$PUBLIC_DIR" \
    --refresh-seconds "$SYS_SN_COMP_REFRESH_SECONDS" \
    --csv network_masternodes.csv
} >>"$LOG_FILE" 2>&1
