#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${SYS_TRACKER_REPO_URL:-https://github.com/bigpoppa-sys/sysWalletTracker.git}"
APP_DIR="${SYS_TRACKER_APP_DIR:-$HOME/sysWalletTracker}"
RPC_URL="${SYS_RPC_URL:-http://127.0.0.1:8370/}"
RPC_USER="${SYS_RPC_USER:-u}"
BLOCKBOOK_URL="${SYS_BLOCKBOOK_URL:-https://explorer-blockbook.syscoin.org}"

if [ -z "${SYS_RPC_PASSWORD:-}" ]; then
  echo "Set SYS_RPC_PASSWORD before running this installer." >&2
  exit 1
fi

for required in git python3 crontab; do
  if ! command -v "$required" >/dev/null 2>&1; then
    echo "Missing required command: $required" >&2
    exit 1
  fi
done

if [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$APP_DIR"
fi

mkdir -p "$APP_DIR/logs"

{
  printf 'SYS_RPC_URL=%q\n' "$RPC_URL"
  printf 'SYS_RPC_USER=%q\n' "$RPC_USER"
  printf 'SYS_RPC_PASSWORD=%q\n' "$SYS_RPC_PASSWORD"
  printf 'SYS_BLOCKBOOK_URL=%q\n' "$BLOCKBOOK_URL"
} >"$APP_DIR/.env"
chmod 600 "$APP_DIR/.env"

chmod +x "$APP_DIR/scripts/masternode_cron_sync.sh"

"$APP_DIR/scripts/masternode_cron_sync.sh"

tmp_cron="$(mktemp)"
crontab -l 2>/dev/null | sed '/# sysWalletTracker masternode watcher start/,/# sysWalletTracker masternode watcher end/d' >"$tmp_cron" || true
cat >>"$tmp_cron" <<CRON
# sysWalletTracker masternode watcher start
* * * * SYS_TRACKER_APP_DIR="$APP_DIR" "$APP_DIR/scripts/masternode_cron_sync.sh"
# sysWalletTracker masternode watcher end
CRON
crontab "$tmp_cron"
rm -f "$tmp_cron"

echo "Installed sysWalletTracker masternode cron."
echo "App dir: $APP_DIR"
echo "Log: $APP_DIR/logs/masternode_cron.log"
echo "Current cron entry:"
crontab -l | sed -n '/# sysWalletTracker masternode watcher start/,/# sysWalletTracker masternode watcher end/p'
