#!/usr/bin/env bash
set -euo pipefail

VPS_HOST="${SYS_TRACKER_VPS_HOST:-root@142.93.241.64}"
SSH_KEY="${SYS_TRACKER_SSH_KEY:-$HOME/.ssh/codex_syswallettracker_ed25519}"
APP_DIR="${SYS_TRACKER_APP_DIR:-/root/sysWalletTracker}"
PUBLIC_DIR="${SYS_TRACKER_PUBLIC_DIR:-/var/www/html/syswallettracker}"
BUNDLE_URL="${SYS_TRACKER_BUNDLE_URL:-https://syswallettracker.vercel.app/sysWalletTracker-vps.tgz}"
LOCK_TIMEOUT_SECONDS="${SYS_TRACKER_LOCK_TIMEOUT_SECONDS:-1800}"

ssh_opts=(
  -i "$SSH_KEY"
  -o IdentitiesOnly=yes
  -o BatchMode=yes
)

ssh "${ssh_opts[@]}" "$VPS_HOST" \
  "APP_DIR='$APP_DIR' PUBLIC_DIR='$PUBLIC_DIR' BUNDLE_URL='$BUNDLE_URL' LOCK_TIMEOUT_SECONDS='$LOCK_TIMEOUT_SECONDS' bash -s" <<'REMOTE'
set -euo pipefail

LOCK_FILE="$APP_DIR/.static-snapshot.lock"

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  echo "Waiting for static snapshot lock..."
  flock -w "$LOCK_TIMEOUT_SECONDS" 9
fi

cd "$APP_DIR"

echo "Downloading fresh Vercel VPS bundle..."
curl -fsSL "$BUNDLE_URL" -o /tmp/sysWalletTracker-vps.tgz
tar -xzf /tmp/sysWalletTracker-vps.tgz -C "$APP_DIR"
chmod +x scripts/*.sh || true

python3 -m py_compile syscoin_tracker.py api/index.py

set -a
# shellcheck disable=SC1091
. "$APP_DIR/.env"
set +a

: "${SYS_RPC_URL:=http://127.0.0.1:8370/}"
: "${SYS_RPC_USER:?SYS_RPC_USER is required in $APP_DIR/.env}"
: "${SYS_RPC_PASSWORD:?SYS_RPC_PASSWORD is required in $APP_DIR/.env}"
: "${SYS_BLOCKBOOK_URL:=https://explorer-blockbook.syscoin.org}"
: "${SYS_TRACKER_SINCE_DATE:=2026-04-14 12:30}"
: "${SYS_TRACKER_FROM_HEIGHT:=2221358}"

echo "Publishing static pages with current database..."
python3 syscoin_tracker.py \
  --rpc-url "$SYS_RPC_URL" \
  --rpc-user "$SYS_RPC_USER" \
  --rpc-password "$SYS_RPC_PASSWORD" \
  --blockbook-url "$SYS_BLOCKBOOK_URL" \
  publish-static \
  --output-dir "$PUBLIC_DIR" \
  --since-date "$SYS_TRACKER_SINCE_DATE" \
  --from-height "$SYS_TRACKER_FROM_HEIGHT" \
  --csv network_masternodes.csv

echo "VPS updated and static pages published to $PUBLIC_DIR"
REMOTE
