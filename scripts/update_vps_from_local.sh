#!/usr/bin/env bash
set -euo pipefail

VPS_HOST="${SYS_TRACKER_VPS_HOST:-root@142.93.241.64}"
SSH_KEY="${SYS_TRACKER_SSH_KEY:-$HOME/.ssh/codex_syswallettracker_ed25519}"
APP_DIR="${SYS_TRACKER_APP_DIR:-/root/sysWalletTracker}"
PUBLIC_DIR="${SYS_TRACKER_PUBLIC_DIR:-/var/www/html/syswallettracker}"
LOCK_TIMEOUT_SECONDS="${SYS_TRACKER_LOCK_TIMEOUT_SECONDS:-1800}"

files=(
  "syscoin_tracker.py"
  "DEPLOYMENT.md"
  "README.md"
  "package.json"
  "package-lock.json"
  "vercel.json"
  ".gitignore"
  ".vercelignore"
  "destinations.csv"
  "destinations_since_monitoring.csv"
  "destinations_since_monitoring_all_ranked.csv"
  "exchange_hot_wallets.csv"
  "exchange_cold_wallets.csv"
  "exchange_routes.csv"
  "exchange_tags.csv"
  "wallet_labels.csv"
  "network_masternodes.csv"
  "node_outputs.csv"
  "verified_sentries.csv"
  "emissions.json"
  "api/index.py"
  "static/assets/chart.umd.js"
  "scripts/install_vps_cron.sh"
  "scripts/masternode_cron_sync.sh"
  "scripts/static_snapshot_cron.sh"
  "scripts/update_vps_from_vercel_bundle.sh"
  "scripts/update_vps_from_local.sh"
)

ssh_opts=(
  -i "$SSH_KEY"
  -o IdentitiesOnly=yes
  -o BatchMode=yes
)

existing_files=()
for file in "${files[@]}"; do
  if [[ -e "$file" ]]; then
    existing_files+=("$file")
  else
    echo "Skipping missing bundle file: $file"
  fi
done

ssh "${ssh_opts[@]}" "$VPS_HOST" "cat > /tmp/syswallettracker-local-deploy.sh" <<'REMOTE'
set -euo pipefail

mkdir -p "$APP_DIR"
LOCK_FILE="$APP_DIR/.static-snapshot.lock"

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  echo "Waiting for static snapshot lock..."
  flock -w "$LOCK_TIMEOUT_SECONDS" 9
fi

mkdir -p "$APP_DIR"
cd "$APP_DIR"

echo "Extracting local deploy bundle..."
tar -xzf -
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

echo "VPS updated from local files and static pages published to $PUBLIC_DIR"
REMOTE

tar -czf - "${existing_files[@]}" | ssh "${ssh_opts[@]}" "$VPS_HOST" \
  "APP_DIR='$APP_DIR' PUBLIC_DIR='$PUBLIC_DIR' LOCK_TIMEOUT_SECONDS='$LOCK_TIMEOUT_SECONDS' bash /tmp/syswallettracker-local-deploy.sh"

ssh "${ssh_opts[@]}" "$VPS_HOST" "rm -f /tmp/syswallettracker-local-deploy.sh" >/dev/null 2>&1 || true
