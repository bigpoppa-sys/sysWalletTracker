#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${SYS_TRACKER_APP_DIR:-$HOME/sysWalletTracker}"
ENV_FILE="$APP_DIR/.env"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/masternode_cron.log"
LOCK_FILE="$APP_DIR/.masternode-cron.lock"

mkdir -p "$LOG_DIR"

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  flock -n 9 || exit 0
else
  LOCK_DIR="$APP_DIR/.masternode-cron.lockdir"
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

: "${SYS_RPC_URL:=http://127.0.0.1:8370/}"
: "${SYS_RPC_USER:?SYS_RPC_USER is required in $ENV_FILE}"
: "${SYS_RPC_PASSWORD:?SYS_RPC_PASSWORD is required in $ENV_FILE}"
: "${SYS_BLOCKBOOK_URL:=https://explorer-blockbook.syscoin.org}"

{
  printf '\n[%s] masternode cron sync\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  python3 syscoin_tracker.py \
    --rpc-url "$SYS_RPC_URL" \
    --rpc-user "$SYS_RPC_USER" \
    --rpc-password "$SYS_RPC_PASSWORD" \
    --blockbook-url "$SYS_BLOCKBOOK_URL" \
    sync-masternodes \
    --csv network_masternodes.csv
} >>"$LOG_FILE" 2>&1
