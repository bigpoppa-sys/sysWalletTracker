#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${SYS_TRACKER_APP_DIR:-$HOME/sysWalletTracker}"
ENV_FILE="$APP_DIR/.env"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/static_snapshot_cron.log"
LOCK_FILE="$APP_DIR/.static-snapshot.lock"
PUBLIC_DIR="${SYS_TRACKER_PUBLIC_DIR:-/var/www/html/syswallettracker}"

mkdir -p "$LOG_DIR"

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  flock -n 9 || exit 0
else
  LOCK_DIR="$APP_DIR/.static-snapshot.lockdir"
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
: "${SYS_TRACKER_SINCE_DATE:=2026-04-14 12:30}"
: "${SYS_TRACKER_FROM_HEIGHT:=2221358}"
: "${SYS_TOP_WALLET_MAX_BLOCKS:=0}"
: "${SYS_TOP_WALLET_TOP:=100}"
: "${SYS_TOP_WALLET_BATCH_SIZE:=50}"
: "${SYS_TOP_WALLET_CONFIRMATIONS:=6}"
: "${SYS_TOP_WALLET_CLUSTER_MAX_BLOCKS:=0}"
: "${SYS_TOP_WALLET_CLUSTER_BATCH_SIZE:=50}"
: "${SYS_TOP_WALLET_CLUSTER_CONFIRMATIONS:=$SYS_TOP_WALLET_CONFIRMATIONS}"
: "${SYS_EMISSIONS_MAX_BLOCKS:=0}"
: "${SYS_EMISSIONS_BATCH_SIZE:=50}"
: "${SYS_EMISSIONS_CONFIRMATIONS:=6}"
: "${SYS_NEVM_RPC_URL:=http://127.0.0.1:8545/}"
: "${SYS_NEVM_EMISSIONS_MAX_BLOCKS:=0}"
: "${SYS_NEVM_EMISSIONS_BATCH_SIZE:=50}"
: "${SYS_NEVM_EMISSIONS_CONFIRMATIONS:=6}"

RPC_ARGS=()
if [ -n "${SYS_RPC_URL:-}" ] || [ -n "${SYS_RPC_HOST:-}" ] || { [ -n "${SYS_RPC_USER:-}" ] && [ -n "${SYS_RPC_PASSWORD:-}" ]; }; then
  : "${SYS_RPC_URL:=http://127.0.0.1:8370/}"
  : "${SYS_RPC_USER:?SYS_RPC_USER is required when SYS_RPC_URL/SYS_RPC_HOST is configured}"
  : "${SYS_RPC_PASSWORD:?SYS_RPC_PASSWORD is required when SYS_RPC_URL/SYS_RPC_HOST is configured}"
  RPC_ARGS=(--rpc-url "$SYS_RPC_URL" --rpc-user "$SYS_RPC_USER" --rpc-password "$SYS_RPC_PASSWORD")
fi

require_rpc() {
  if [ "${#RPC_ARGS[@]}" -eq 0 ]; then
    printf 'Core RPC is required for %s. Configure SYS_RPC_URL, SYS_RPC_USER, and SYS_RPC_PASSWORD, or set its max-blocks to 0.\n' "$1" >&2
    exit 1
  fi
}

{
  printf '\n[%s] static snapshot sync\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  mkdir -p "$PUBLIC_DIR"
  if [ "$SYS_TOP_WALLET_MAX_BLOCKS" -gt 0 ]; then
    require_rpc "sync-top-wallets"
    python3 syscoin_tracker.py \
      "${RPC_ARGS[@]}" \
      sync-top-wallets \
      --max-blocks "$SYS_TOP_WALLET_MAX_BLOCKS" \
      --top "$SYS_TOP_WALLET_TOP" \
      --batch-size "$SYS_TOP_WALLET_BATCH_SIZE" \
      --confirmations "$SYS_TOP_WALLET_CONFIRMATIONS" \
      --json "$PUBLIC_DIR/top-wallets.json"
  fi
  if [ "$SYS_TOP_WALLET_CLUSTER_MAX_BLOCKS" -gt 0 ]; then
    require_rpc "sync-top-wallet-clusters"
    python3 syscoin_tracker.py \
      "${RPC_ARGS[@]}" \
      sync-top-wallet-clusters \
      --max-blocks "$SYS_TOP_WALLET_CLUSTER_MAX_BLOCKS" \
      --top "$SYS_TOP_WALLET_TOP" \
      --batch-size "$SYS_TOP_WALLET_CLUSTER_BATCH_SIZE" \
      --confirmations "$SYS_TOP_WALLET_CLUSTER_CONFIRMATIONS" \
      --json "$PUBLIC_DIR/top-wallet-clusters.json"
  fi
  if [ "$SYS_EMISSIONS_MAX_BLOCKS" -gt 0 ]; then
    require_rpc "sync-emissions"
    python3 syscoin_tracker.py \
      "${RPC_ARGS[@]}" \
      sync-emissions \
      --max-blocks "$SYS_EMISSIONS_MAX_BLOCKS" \
      --batch-size "$SYS_EMISSIONS_BATCH_SIZE" \
      --confirmations "$SYS_EMISSIONS_CONFIRMATIONS" \
      --json "$PUBLIC_DIR/emissions.json"
  fi
  if [ "$SYS_NEVM_EMISSIONS_MAX_BLOCKS" -gt 0 ]; then
    python3 syscoin_tracker.py \
      --nevm-rpc-url "$SYS_NEVM_RPC_URL" \
      sync-nevm-emissions \
      --max-blocks "$SYS_NEVM_EMISSIONS_MAX_BLOCKS" \
      --batch-size "$SYS_NEVM_EMISSIONS_BATCH_SIZE" \
      --confirmations "$SYS_NEVM_EMISSIONS_CONFIRMATIONS" \
      --json "$PUBLIC_DIR/emissions.json"
  fi
  python3 syscoin_tracker.py \
    "${RPC_ARGS[@]}" \
    --blockbook-url "$SYS_BLOCKBOOK_URL" \
    --sysnode-mnlist-url "$SYS_SYSNODE_MNLIST_URL" \
    --sysnode-time-lookup-limit "$SYS_SYSNODE_TIME_LOOKUP_LIMIT" \
    publish-static \
    --output-dir "$PUBLIC_DIR" \
    --since-date "$SYS_TRACKER_SINCE_DATE" \
    --from-height "$SYS_TRACKER_FROM_HEIGHT" \
    --csv network_masternodes.csv
} >>"$LOG_FILE" 2>&1
